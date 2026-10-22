import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { NodeBlocked, GatewayHandoffNeeded, isBreakerTrippingFailure } from "./executor-errors.js";
import { detectBlockedSentinel, detectSkippedSentinel, defaultAgentSessionKey, assembleState } from "./executor-helpers.js";
import { makeNeedsCtx } from "./needs-resolver.js";
import { registerDispatch, clearDispatch } from "../state/tool-handoff.js";
import { registerSessionOwner, clearSessionOwner, runWithLockOwner } from "../locks/lock.js";

// The lock owner for work done on behalf of one node invocation: held locks
// stay held until the invocation resolves (see locks/invocation-owner.js), not
// for a fixed time. `node` supplies the browser lease cooldown it declares.
function invocationLockOwner(node, { invocationId, flowId, nodeId, pipelineId }) {
  if (!invocationId) return null;
  return {
    kind: "invocation",
    id: invocationId,
    flowId,
    nodeId,
    pipelineId: pipelineId ?? null,
    ...(node?.browserLeaseCooldownMs ? { cooldownMs: node.browserLeaseCooldownMs } : {}),
    ...(node?.browserLeaseCooldownJitterMs ? { cooldownJitterMs: node.browserLeaseCooldownJitterMs } : {}),
  };
}

/**
 * Factory for the node-execution dispatcher: code, agent, subflow, human-gate.
 * The subflow branch is delegated to `executeSubflowNode`, a thin wrapper around
 * `dispatchSubflowChildren` (engine/executor-subflow-children.js) constructed in
 * executor.js.
 *
 * `getNodeCtx` is a getter closure that returns the frozen nodeCtx object (defined
 * in executor.js after start/runFlow). It reads the binding at call time so it's
 * safe to construct this factory before nodeCtx is assigned.
 */
export function createNodeRunner({
  maybeBudgetGate,
  dispatchAgentWithRetry,
  executeSubflowNode,
  withCrossProcessLockMaybe,
  getNodeCtx,
  api,
  logger,
  runStore,
  nodeExecutionsStore,
  nodeHealthStore,
  pipelinesRef,
}) {
  // Shared by the plain "code" branch below and by cascade sub-nodes of
  // kind "code" (cascade sub-nodes dispatch through the ordinary "code"/
  // "agent" branches in executeNodeBody — this function has no fallback-
  // specific caller anymore). Returns the RAW result from `entry.run(...)`;
  // sentinel detection (_blocked/_skipped) and stateUpdate wrapping is the
  // caller's job.
  async function dispatchCodeEntry(entry, { state, flowId, nodeId }) {
    const flowId8 = flowId.slice(0, 8);
    const artifactDir = state.runId && state.pipelineId
      ? path.join(os.homedir(), ".openclaw", "workspace", ".pipelines", state.pipelineId, "runs", state.runId)
      : null;
    // Same pattern as executeSubflowNode's selector call in executor.js:
    // assembleState(baseState: state). Every kind:"code" node's run() goes
    // through this one call.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
    return entry.run(enrichedState, {
      ...getNodeCtx(),
      flowId,
      nodeId,
      pipelineId: state.pipelineId ?? null,
      log: {
        info:  (msg) => logger?.info?.(`[${flowId8}] ${nodeId}: ${msg}`),
        warn:  (msg) => logger?.warn?.(`[${flowId8}] ${nodeId}: ${msg}`),
        error: (msg) => logger?.error?.(`[${flowId8}] ${nodeId}: ${msg}`),
      },
      artifact: async (name, content) => {
        if (!artifactDir) throw new Error("ctx.artifact: state.runId or state.pipelineId not set");
        await mkdir(artifactDir, { recursive: true });
        return writeFile(path.join(artifactDir, name), content, "utf8");
      },
      readArtifact: async (name) => {
        if (!artifactDir) throw new Error("ctx.readArtifact: state.runId or state.pipelineId not set");
        return readFile(path.join(artifactDir, name), "utf8");
      },
      blocked: (reason, hint) => { throw new NodeBlocked({ reason, hint }); },
      // "needs:" resolution is lazy, only resolved if this node's own code
      // actually awaits ctx.needs(varName); a NodeBlocked thrown here propagates
      // straight up (no try/catch around entry.run() in this "code" branch
      // — see executeNodeBody below) to executor.js's own catch, same path
      // every other NodeBlocked already uses.
      needs: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }),
    });
  }

  // Shared by the plain "agent" branch below and by cascade sub-nodes of
  // kind "agent". `nodeId` is the id used for session-key derivation,
  // lineage, and budget-gate scoping.
  //
  // No admission here: the admission pool governs flow runs, and the run
  // executing this node already holds its own slot (flow-start/child-
  // dispatch) for as long as it's running. A run executes one node at a
  // time, so the run cap already bounds concurrent agent dispatches — a
  // second slot taken mid-node from the same pool is what made hold-and-
  // wait deadlock (keeping the run slot) or churn (giving it back) possible.
  //
  // Circuit breaker: an agent node opts in via `entry.circuitBreaker: true`,
  // checked here regardless of which kind of flow it's running inside —
  // generalist, not flow-kind-aware. Fails fast before doing any work (no
  // prompt build, no session spawn) when already blocked, naming the reset
  // command because a block has no expiry.
  //
  // Which failures are allowed to trip it is decided by
  // isBreakerTrippingFailure (engine/executor-errors.js) rather than
  // inlined here, so the rule sits beside the error classes it inspects.
  async function dispatchAgentEntry(entry, nodeId, { state, flowId, reviveMessage, invocationId }) {
    const pipelineId = state?.pipelineId ?? null;
    if (entry.circuitBreaker && await nodeHealthStore?.isBlocked(pipelineId, nodeId)) {
      throw new Error(
        `unhealthy agent dispatch: "${nodeId}" is blocked after a prior failure — reset via "openclaw manifold node-health reset ${pipelineId} ${nodeId}" once the underlying issue is confirmed fixed`
      );
    }
    const gate = await maybeBudgetGate({
      flowId, nodeId, node: entry,
      pipelineId, pipelineName: pipelineId,
      itemCount: 1,
    });
    if (gate.skip) return {};
    if (gate.pause) {
      // NodeBlocked's constructor only keeps {reason, needed, hint} — budget
      // gate pauses carry a _kind ("budget-gate-approval") that downstream
      // wait_json routing depends on, so preserve the full object on top.
      const err = new NodeBlocked(gate.blockedInfo ?? {});
      err._blocked = { ...(gate.blockedInfo ?? {}) };
      throw err;
    }
    // promptBuilder reads via assembleState(baseState: state) rather than
    // raw state — that's what makes _subflowGroups visible to an agent
    // node's promptBuilder. Same lazy ctx.needs(varName) accessor code
    // nodes get, passed as promptBuilder's 2nd arg (a single-arg
    // promptBuilder ignores it). A thrown NodeBlocked here propagates out
    // of dispatchAgentEntry uncaught (this call is deliberately outside the
    // try/catch below, same as the gate.pause throw above) to
    // executeNodeBody's own "agent" branch catch, which converts
    // NodeBlocked into blockedInfo.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
    const message = reviveMessage ?? await Promise.resolve(entry.promptBuilder(enrichedState, { needs: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }) }));
    const sessionKey = entry.sessionKey
      ?? entry.sessionKeyBuilder?.(state, flowId)
      ?? defaultAgentSessionKey(entry.agent, flowId, nodeId);
    api.runtime?.lineage?.recordSpawn?.({
      childSessionKey: sessionKey, flowId, nodeId,
      pipelineId, agentId: entry.agent ?? null,
    });
    // tool-handoff.js: lets a tool this session calls mid-turn (e.g.
    // submit_score) resolve its own invocationId from ctx.sessionKey and
    // hand structured data back to this node's apply() — see that module's
    // header comment. Cleared in the finally below once the dispatch
    // settles, so the sessionKey -> invocationId map doesn't grow
    // unbounded over gateway uptime.
    registerDispatch(sessionKey, invocationId);
    // Browser tool calls this session makes lock as this invocation
    // (extensions/browser-lease), the same owner the node-level
    // crossProcessLock uses — so the node's own lock and its agent's tool
    // leases re-enter instead of blocking each other.
    const lockOwner = invocationLockOwner(entry, { invocationId, flowId, nodeId, pipelineId });
    if (lockOwner) registerSessionOwner(sessionKey, lockOwner);
    try {
      return await dispatchAgentWithRetry({
        node: entry, nodeId, sessionKey, initialMessage: message,
        applyCtx: state, applyExtraArg: undefined, flowId,
        applyMeta: { flowId, nodeId, invocationId },
        // apply() gets the same lazy ctx.needs() accessor every other
        // call site gets.
        needsCtx: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }),
      });
    } catch (err) {
      // Which failures may park a node, and why the excluded ones must not,
      // is documented once on isBreakerTrippingFailure — not restated here.
      if (entry.circuitBreaker && isBreakerTrippingFailure(err)) {
        await nodeHealthStore?.markUnhealthy(pipelineId, nodeId, { reason: err?.message ?? String(err), flowId });
      }
      throw err;
    } finally {
      clearDispatch(sessionKey);
      clearSessionOwner(sessionKey);
    }
  }

  /**
   * Acquire the node's declared crossProcessLock (if any) before running the
   * node body, release after it returns or throws. Runs under the
   * invocation's lock owner, so that lock and any withLock the node's own
   * code takes belong to the invocation rather than to no one.
   */
  async function executeNode({ node, nodeId, state, flowId, reviveMessage, invocationId }) {
    const body = () => executeNodeBody({ node, nodeId, state, flowId, reviveMessage, invocationId });
    const owner = invocationLockOwner(node, { invocationId, flowId, nodeId, pipelineId: state?.pipelineId });
    return runWithLockOwner(owner, () => withCrossProcessLockMaybe(node.crossProcessLock, body, {
      flowId, nodeId,
      cooldownMs: node.crossProcessLockCooldownMs,
      cooldownJitterMs: node.crossProcessLockCooldownJitterMs,
    }));
  }

  // `reviveMessage`, when set, is used in place of node.promptBuilder(state)
  // for an "agent" node — boot revival's continuation prompt to an existing
  // session (see engine/revival-messages.js), never a fresh re-dispatch of
  // the original task.
  async function executeNodeBody({ node, nodeId, state, flowId, reviveMessage, invocationId }) {
    if (node.kind === "code") {
      const update = await dispatchCodeEntry(node, { state, flowId, nodeId });
      const blocked = detectBlockedSentinel(update);
      if (blocked) return { blockedInfo: blocked };
      const skipped = detectSkippedSentinel(update);
      if (skipped) return { skippedInfo: skipped };
      return { stateUpdate: update ?? {} };
    }

    if (node.kind === "agent") {
      let update;
      try {
        update = await dispatchAgentEntry(node, nodeId, { state, flowId, reviveMessage, invocationId });
      } catch (err) {
        if (err instanceof NodeBlocked) return { blockedInfo: err._blocked };
        throw err;
      }
      const blocked = detectBlockedSentinel(update);
      if (blocked) return { blockedInfo: blocked };
      const skipped = detectSkippedSentinel(update);
      if (skipped) return { skippedInfo: skipped };
      return { stateUpdate: update ?? {} };
    }

    if (node.kind === "subflow") {
      return executeSubflowNode({ node, nodeId, flowId, state });
    }

    if (node.kind === "human-gate") {
      // Both pendingSelector and approvalRequestBuilder read via
      // assembleState(baseState: state) rather than raw state.
      //
      // The two INTERNAL checks below (existingGate, openedAt
      // preservation) deliberately do NOT read gates off enrichedState —
      // state.gates is written by every gate node via a full-snapshot
      // embed of the WHOLE shared key (required by state_json's own
      // shallow-merge accumulation), so an earlier gate's resultJson row
      // can go stale relative to a later gate and silently erase it on
      // replay. gate_records (state/node-executions.js) sidesteps this
      // entirely — one row per (flowId, nodeId), no shared key, no
      // replay, so a direct point lookup is always correct. This is
      // engine-internal bookkeeping, not a pipeline-authored-code read.
      const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
      const pending = await Promise.resolve(node.pendingSelector(enrichedState));
      if (!Array.isArray(pending) || pending.length === 0) {
        // Gate satisfied — stamp consumedAt so consumption is observable
        // (diagnostics, revival) even after wait_json is cleared.
        const existingGate = await nodeExecutionsStore.getGateRecord(flowId, nodeId);
        if (existingGate && !existingGate.consumedAt) {
          const updated = { ...existingGate, consumedAt: Date.now() };
          await nodeExecutionsStore.writeGateRecord(flowId, nodeId, updated);
          return { stateUpdate: {} };
        }
        return { stateUpdate: {} };
      }
      const items = await Promise.all(pending.map(async (itemId) => {
        const requestPayload = await Promise.resolve(node.approvalRequestBuilder(enrichedState, itemId));
        return { itemId, requestPayload };
      }));
      const now = Date.now();
      // Write the full gate payload into gate_records only, never to
      // stateJson.gates[nodeId]: routes/runs.js sources .gates exclusively
      // from gate_records, and a full-extension grep confirmed nothing else
      // reads state.gates off state_json. openedAt is preserved across
      // re-entries (partial decisions) so the record always reflects when
      // the gate batch was first created.
      const existingBeforeOpen = await nodeExecutionsStore.getGateRecord(flowId, nodeId);
      const gateRecord = {
        nodeId,
        kind: "human-gate",
        pending: items.map((it) => it.itemId),
        items,
        openedAt: existingBeforeOpen?.openedAt ?? now,
        lastOpenedAt: now,
      };
      await nodeExecutionsStore.writeGateRecord(flowId, nodeId, gateRecord);
      const waitJson = {
        kind: "human-gate",
        nodeId,
        pending: gateRecord.pending,
        items,
        renderedAt: now,
      };
      return {
        stateUpdate: {},
        waitJson,
        // Without this, executor.js's waitJson-persist branch stamps
        // lastStep to THIS node's own id (nextNodeId) instead of
        // preserving the predecessor's — so a resume after a PARTIAL
        // decision (pendingSelector still non-empty) would read lastStep
        // as "already past this gate" and evaluate its outgoing edge
        // instead of re-invoking pendingSelector, silently advancing
        // past any still-undecided items. The children-pending path in
        // executor-subflow-children.js sets the same flag for the same
        // reason: neither has actually advanced past its own node.
        reentrant: true,
      };
    }

    throw new Error(`unknown node kind: ${node.kind}`);
  }

  return { executeNode, executeNodeBody };
}
