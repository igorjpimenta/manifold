import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { NodeBlocked, GatewayHandoffNeeded, isBreakerTrippingFailure } from "./executor-errors.js";
import { detectBlockedSentinel, detectSkippedSentinel, assembleState } from "./executor-helpers.js";
import { makeNeedsCtx } from "./needs-resolver.js";

export function createNodeRunner({
  maybeBudgetGate,
  dispatchAgentWithRetry,
  executeSubflowNode,
  getNodeCtx,
  api,
  config,
  lineage,
  pipelinesDir,
  logger,
  runStore,
  nodeExecutionsStore,
  nodeHealthStore,
  pipelinesRef,
  sessions,
}) {
  const _lineage = lineage ?? api?.runtime?.lineage ?? null;
  const _pipelinesDir = pipelinesDir ?? config?.pipelinesDir ?? null;
  // Shared by the plain "code" branch and cascade sub-nodes of kind "code".
  // Returns the RAW run() result — sentinel detection and stateUpdate
  // wrapping are the caller's job.
  async function dispatchCodeEntry(entry, { state, flowId, nodeId }) {
    const flowId8 = flowId.slice(0, 8);
    const artifactDir = state.runId && state.pipelineId && _pipelinesDir
      ? path.join(_pipelinesDir, state.pipelineId, "runs", state.runId)
      : null;
    // run() reads via assembleState, not raw state — that's what makes
    // _subflowGroups and resultJson replay visible to it.
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
      needs: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }),
    });
  }

  // Shared by the plain "agent" branch and cascade sub-nodes of kind
  // "agent". `nodeId` scopes session-key derivation, lineage and budget.
  //
  // No admission here: the pool governs flow RUNS, and this run already
  // holds its own slot for as long as it runs — one node at a time, so the
  // run cap already bounds concurrent agent dispatches. A second slot taken
  // mid-node made hold-and-wait deadlock (keeping the run slot) or churn
  // (giving it back) possible.
  //
  // Circuit breaker: `entry.circuitBreaker: true` opts in; checked here
  // regardless of flow kind. Fails fast before any prompt build or session
  // spawn when already blocked; a block has no expiry, so the error names
  // the reset command.
  async function dispatchAgentEntry(entry, nodeId, { state, flowId, reviveMessage, invocationId }) {
    const pipelineId = state?.pipelineId ?? null;
    // Refused loudly: every agent turn is keyed to its agent for session
    // attribution and push wake — no default, ever.
    if (!entry.agent) {
      throw new Error(`manifold: refusing anonymous dispatch of node "${nodeId}" (no entry.agent)`);
    }
    if (entry.circuitBreaker && await nodeHealthStore?.isBlocked(pipelineId, nodeId)) {
      throw new Error(
        `unhealthy agent dispatch: "${nodeId}" is blocked after a prior failure — clear its node-health block once the underlying issue is confirmed fixed`
      );
    }
    const gate = await maybeBudgetGate({
      flowId, nodeId, node: entry,
      pipelineId, pipelineName: pipelineId,
      itemCount: 1,
    });
    if (gate.skip) return {};
    if (gate.pause) {
      // NodeBlocked's constructor only keeps {reason, needed, hint} — the
      // budget-gate pause's _kind drives downstream wait_json routing, so
      // preserve the full object on top.
      const err = new NodeBlocked(gate.blockedInfo ?? {});
      err._blocked = { ...(gate.blockedInfo ?? {}) };
      throw err;
    }
    // promptBuilder reads via assembleState (not raw state) so
    // _subflowGroups is visible; needs ctx rides as its 2nd arg (a
    // single-arg promptBuilder ignores it). A thrown NodeBlocked here is
    // deliberately outside the try/catch below — it propagates to
    // executeNodeBody's agent-branch catch, which converts it to
    // blockedInfo.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
    const message = reviveMessage ?? await Promise.resolve(entry.promptBuilder(enrichedState, { needs: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }) }));
    // Session ids belong to the harness, not manifold: entry.session /
    // sessionBuilder pass through verbatim; otherwise the harness adapter
    // mints one so pipelines carry no harness key syntax. A refused
    // dispatch means no mint function was injected — tool-handoff, the
    // session index and the lineage ledger all key off this value, so
    // neither core nor a silent default may invent one.
    let session = entry.session ?? entry.sessionBuilder?.(state, flowId) ?? null;
    // The mint may be a bridge call to the harness process, so it's
    // awaited and type-checked before anything keys off it.
    if (!session && typeof sessions?.newNodeSessionId === "function") {
      session = await sessions.newNodeSessionId({ agentId: entry.agent, flowId, nodeId });
    }
    if (typeof session !== "string" || !session) {
      throw new Error(`manifold: refusing dispatch of node "${nodeId}" with no session (declare entry.session / entry.sessionBuilder, or pass a harness sessions.newNodeSessionId)`);
    }
    _lineage?.recordSpawn?.({
      childSession: session, flowId, nodeId,
      pipelineId, agentId: entry.agent ?? null,
    });

    try {
      return await dispatchAgentWithRetry({
        node: entry, nodeId, session, initialMessage: message,
        applyCtx: state, applyExtraArg: undefined, flowId,
        applyMeta: { flowId, nodeId, invocationId },
        needsCtx: makeNeedsCtx({ node: entry, flowId, runStore, nodeExecutionsStore, pipelinesRef }),
      });
    } catch (err) {
      if (entry.circuitBreaker && isBreakerTrippingFailure(err)) {
        await nodeHealthStore?.markUnhealthy(pipelineId, nodeId, { reason: err?.message ?? String(err), flowId });
      }
      throw err;
    }
  }

  async function executeNode({ node, nodeId, state, flowId, reviveMessage, invocationId }) {
    if (typeof node.crossProcessLock === "string" && node.crossProcessLock) {
      throw new Error(`manifold: node "${nodeId}" declares crossProcessLock "${node.crossProcessLock}" — the lock mechanism was removed; drop the option from the pipeline`);
    }
    return executeNodeBody({ node, nodeId, state, flowId, reviveMessage, invocationId });
  }

  // reviveMessage, when set, replaces promptBuilder(state) for an "agent"
  // node — boot revival's continuation prompt to an existing session, never
  // a fresh re-dispatch of the original task.
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
      // pendingSelector / approvalRequestBuilder read enrichedState; the two
      // internal checks below deliberately do NOT read gates off it —
      // state.gates is a full-snapshot embed of the whole shared key (its
      // shallow-merge accumulation), so an earlier gate's resultJson can go
      // stale relative to a later gate and silently erase it on replay.
      // gate_records is one row per (flowId, nodeId), no shared key, no
      // replay — a direct point lookup is always correct. This is
      // engine-internal bookkeeping, not a pipeline-authored read.
      const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore, baseState: state });
      const pending = await Promise.resolve(node.pendingSelector(enrichedState));
      if (!Array.isArray(pending) || pending.length === 0) {
        // Stamp consumedAt so consumption stays observable (diagnostics,
        // revival) even after wait_json is cleared.
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
      // Gate payload lives in gate_records only, never stateJson.gates —
      // routes/runs sources .gates exclusively from gate_records. openedAt
      // is preserved across re-entries (partial decisions) so the record
      // still shows when the batch was first created.
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
        // Without reentrant, executor's waitJson-persist stamps lastStep
        // to THIS node instead of preserving the predecessor — a resume
        // after a PARTIAL decision would then evaluate the outgoing edge
        // instead of re-invoking pendingSelector, silently advancing past
        // still-undecided items. Same flag, same reason, on the
        // children-pending path.
        reentrant: true,
      };
    }

    throw new Error(`unknown node kind: ${node.kind}`);
  }

  return { executeNode, executeNodeBody };
}
