import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { NodeBlocked } from "./executor-errors.js";
import { detectBlockedSentinel, detectSkippedSentinel, assembleState, usesNamedParams, mintAgentSessionKey } from "./executor-helpers.js";
import { makeNeedsCtx } from "./needs-resolver.js";
import { registerTicket } from "../state/tool-handoff.js";
import { runWithLockOwner } from "../locks/lock.js";

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
  executeSubflowNode,
  withCrossProcessLockMaybe,
  getNodeCtx,
  api,
  config,
  lineage,
  pipelinesDir,
  notifyPush,
  logger,
  runStore,
  nodeExecutionsStore,
  pipelinesRef,
  emit,
}) {
  // Explicit params win; api is the legacy fallback the adapter still
  // supplies during cutover (tests construct with api only).
  const _lineage = lineage ?? api?.runtime?.lineage ?? null;
  const _pipelinesDir = pipelinesDir ?? config?.pipelinesDir ?? null;
  const _notifyPush = typeof notifyPush === "function" ? notifyPush : null;
  // Canonical event sink for agent-claim park events (events/emit.js's
  // emitCanonical in production, a capturing stub in tests). Optional —
  // the pull branch skips emission without one, never fails for it.
  const _emit = typeof emit === "function" ? emit : null;
  // Shared by the plain "code" branch below and by cascade sub-nodes of
  // kind "code" (cascade sub-nodes dispatch through the ordinary "code"/
  // "agent" branches in executeNodeBody — this function has no fallback-
  // specific caller anymore). Returns the RAW result from `entry.run(...)`;
  // sentinel detection (_blocked/_skipped) and stateUpdate wrapping is the
  // caller's job.
  async function dispatchCodeEntry(entry, { state, flowId, nodeId }) {
    const flowId8 = flowId.slice(0, 8);
    const artifactDir = state.runId && state.pipelineId && _pipelinesDir
      ? path.join(_pipelinesDir, state.pipelineId, "runs", state.runId)
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

  async function executeNode({ node, nodeId, state, flowId, invocationId }) {
    const body = () => executeNodeBody({ node, nodeId, state, flowId, invocationId });
    const owner = invocationLockOwner(node, { invocationId, flowId, nodeId, pipelineId: state?.pipelineId });
    return runWithLockOwner(owner, () => withCrossProcessLockMaybe(node.crossProcessLock, body, {
      flowId, nodeId,
      cooldownMs: node.crossProcessLockCooldownMs,
      cooldownJitterMs: node.crossProcessLockCooldownJitterMs,
    }));
  }

  // Every agent node parks: core builds the prompt at park (stored with
  // the ticket, handed to the claimant on claim), the claimant executes
  // externally, and core runs node.apply on resume. A resumed dispatch
  // whose ticket is already reported consumes the stored result instead
  // of parking again.
  //
  // Prompt and apply mirror the deleted push-world dispatchAgentEntry:
  // promptBuilder reads assembleState(baseState: state) with the lazy
  // ctx.needs accessor, and apply runs with the same ctx shape the push
  // dispatch used. A throwing apply propagates uncaught, the same failure
  // path a throwing code node takes.
  async function executePullNode({ node, nodeId, state, flowId, invocationId }) {
    // Anonymous dispatch is refused: every agent turn is keyed to its
    // agent for session attribution and push wake. No default, ever.
    if (!node?.agent) {
      throw new Error(`manifold: refusing anonymous dispatch of node "${nodeId}" (no entry.agent)`);
    }
    const claim = invocationId
      ? await nodeExecutionsStore.getClaim?.(invocationId).catch(() => null)
      : null;
    // Reported tickets carry the claimant's output in resultJson (written
    // by the report before resume) — run it through node.apply here so
    // pipeline-author logic shapes the state update, never raw JSON trust.
    // assembleState() replays the same row for edge functions; this is the
    // dispatch-side read.
    if (claim && (claim.claimState === "reported" || claim.claimState === "applied")) {
      const recorded = await nodeExecutionsStore.getResultForNode?.(flowId, nodeId).catch(() => null) ?? null;
      if (recorded?.resultWrittenAt == null || recorded.resultJson == null) {
        throw new Error(`pull-claim: ticket ${invocationId} is ${claim.claimState} but has no stored result (flow=${flowId} node=${nodeId})`);
      }
      let reportedOutput;
      try {
        reportedOutput = JSON.parse(recorded.resultJson);
      } catch {
        throw new Error(`pull-claim: ticket ${invocationId} has a corrupt stored result (flow=${flowId} node=${nodeId})`);
      }
      const applyMeta = { flowId, nodeId, invocationId };
      const stateUpdate = typeof node.apply === "function"
        ? (usesNamedParams(node.apply)
          ? await node.apply({ state, output: reportedOutput, item: undefined, meta: applyMeta, needs: makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef }) })
          : await node.apply(state, reportedOutput, undefined, applyMeta, makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef })))
        : (reportedOutput ?? {});
      await nodeExecutionsStore.markApplied?.(invocationId).catch(() => null);
      const blocked = detectBlockedSentinel(stateUpdate);
      if (blocked) return { blockedInfo: blocked };
      const skipped = detectSkippedSentinel(stateUpdate);
      if (skipped) return { skippedInfo: skipped };
      return { stateUpdate: stateUpdate ?? {} };
    }
    // Park: build the prompt over the assembled state (what makes
    // _subflowGroups visible to promptBuilder, same as the push dispatch),
    // persist it with the ticket, then return waitJson so runFlow persists
    // it through the human-gate waitJson path (reentrant — lastStep stays
    // put so resume re-enters this same node). Register the ticket so
    // tools the claiming agent calls (e.g. submit_score) resolve this
    // invocation from the ticket the claim instructions carry.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
    const needsCtx = makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef });
    const prompt = typeof node.promptBuilder === "function"
      ? await node.promptBuilder(enrichedState, { needs: needsCtx })
      : null;
    // Core-namespace handle only (`mf-<flow8>-<node>`); explicit
    // entry.sessionKey / sessionKeyBuilder overrides pass through verbatim.
    const sessionKey = node.sessionKey
      ?? node.sessionKeyBuilder?.(state, flowId)
      ?? mintAgentSessionKey(flowId, nodeId);
    const parked = await nodeExecutionsStore.markPullReady?.(invocationId, { prompt }) ?? { ok: true };
    registerTicket(invocationId, invocationId);
    if (!parked?.ok) {
      throw new Error(`pull-claim: could not park ticket ${invocationId} (${parked?.reason ?? "store without claim support"})`);
    }
    if (_emit) {
      await _emit("agent.claim.parked", { flowId, ticket: invocationId, nodeId });
    }
    return {
      stateUpdate: {},
      waitJson: { kind: "agent-claim", ticket: invocationId, nodeId, sessionKey, renderedAt: Date.now() },
      reentrant: true,
    };
  }

  async function executeNodeBody({ node, nodeId, state, flowId, invocationId }) {
    if (node.kind === "code") {
      const update = await dispatchCodeEntry(node, { state, flowId, nodeId });
      const blocked = detectBlockedSentinel(update);
      if (blocked) return { blockedInfo: blocked };
      const skipped = detectSkippedSentinel(update);
      if (skipped) return { skippedInfo: skipped };
      return { stateUpdate: update ?? {} };
    }

    if (node.kind === "agent") {
      return executePullNode({ node, nodeId, state, flowId, invocationId });
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
      if (_notifyPush) {
        try {
          await _notifyPush({ event: "gate.opened", flowId, nodeId, pipelineId: state?.pipelineId ?? null, sessionKey: null, agent: null });
        } catch {}
      }
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
