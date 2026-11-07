// executor.js — runs a manifold pipeline against a managed flow_run.
//
// Lifecycle of one flow:
//   1. Caller invokes start(pipelineId)
//   2. We create_flow_runs row at status='running', lastStep='__start__'.
//   3. We enter the run loop:
//        a. Read flow row.
//        b. If terminal — return.
//        c. If blocked — return (resume() wakes us back up later).
//        d. Resolve edge from lastStep → nextNodeId.
//        e. If nextNodeId === '__end__': persist finish() with current state; return.
//        f. Execute the node; collect stateUpdate (and waitJson if gate).
//        g. Apply update onto state.
//        h. Persist: setRunning() to advance lastStep, or setWaiting() to block.
//        i. Loop.

import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { nodeEmitter } from "../events.js";
import { resolveStorePaths } from "../state/paths.js";

import {
  isTerminal, isBlocked, isQueued, shortSummary, deepClone, resolveEdge,
  validateGateLayout, isFreshNodeEntry, describeWaitFallback, assembleState,
} from "./executor-helpers.js";
import { NodeBlocked, GatewayHandoffNeeded } from "./executor-errors.js";
import { REVIVAL_MSG } from "./revival-messages.js";
import { createBudgetGate } from "./executor-budget.js";
import { createPersist } from "./executor-persist.js";
import { createAgentDispatch } from "./agent-dispatch.js";
import { dispatchSubflowChildren } from "./executor-subflow-children.js";
import { createNodeRunner } from "./executor-nodes.js";
import { completeCancellation } from "./executor-cancel.js";

export function createExecutor({ api, config, approvalQueue, pipelinesDir, store, pipelinesRef, ensurePipelinesLoaded, logger, runTurn, locks, failureStore, nodeHealthStore, notifyFlowEnteredWaiting, sessionIndex, nodeExecutionsStore, runStore, admissionQueueStore, emit }) {
  // Explicit params win; api is the legacy fallback the adapter still
  // supplies during cutover (tests construct with api only).
  const _config = config ?? api?.config ?? null;
  const _lineage = api?.runtime?.lineage ?? null;
  const _pipelinesDir = pipelinesDir ?? _config?.pipelinesDir ?? resolveStorePaths().pipelinesDir;
  // Injectable for tests (defaults to the real singleton in production).
  // A test constructing a real createExecutor() without this override would
  // write its fixture rows into the real store's runtime.sqlite; store/
  // branchService/failureStore etc. above already follow this DI pattern.
  const nodeExecutions = nodeExecutionsStore ?? getNodeExecutionsStore();

  // Wraps nodeExecutions.write() for the write-then-fail call sites below —
  // write() can throw (its own self-validating kind-invariant check), and
  // unlike its normal position (after persistFailRetrying, so the fail
  // status is already durably persisted before any crash), these sites
  // call it FIRST so the failure can be linked to the invocation/execution
  // it just created. A local catch here preserves "the fail status always
  // gets persisted" even if write() throws — persistFailRetrying still
  // runs, just without a linked id, instead of the throw propagating out
  // of runFlow() before the fail status is ever written.
  async function safeWriteExecution(args) {
    try {
      return (await nodeExecutions.write(args)) ?? {};
    } catch (err) {
      logger.warn(`manifold [${args.flowId.slice(0, 8)}] nodeExecutions.write threw: ${err?.message ?? err}`);
      return {};
    }
  }

  const { maybeBudgetGate } = createBudgetGate({
    getApprovalQueue: () => approvalQueue ?? api?.runtime?.approvalQueue,
    logger,
  });

  const { persistRunningRetrying, persistWaitingRetrying, persistSkipRetrying, persistFinishRetrying, persistFailRetrying } = createPersist({
    store, failureStore, notifyFlowEnteredWaiting, logger,
  });

  // The harness injects runTurn — core never speaks the runtime's call
  // shape, so a node dispatch is one function call from the engine's point
  // of view (see engine/agent-dispatch.js and the harness's run-turn.js).
  const { dispatchAgentWithRetry } = createAgentDispatch({
    runTurn,
    sessionIndex,
    logger,
    emit,
  });

  // Direct store.setQueued/setRunning, not the CAS-retry-wrapped
  // persistQueuedRetrying/persistRunningRetrying — best-effort
  // observability only: a lost/raced status write never affects
  // correctness, the lock file itself is what enforces mutual exclusion.
  //
  // withLock() hides the acquire->fn() handoff internally (unlike the
  // semaphore's tryAcquire()/acquire(), called inline in the sibling), so
  // there's no seam to inject an "acquired after having blocked" step —
  // instead wrap fn so the flip back to running happens the moment fn is
  // actually invoked, which only happens post-acquire.
  async function withCrossProcessLockMaybe(lockName, fn, { flowId, nodeId, cooldownMs, cooldownJitterMs } = {}) {
    if (!lockName || typeof lockName !== "string") return fn();
    // The lock service is injected by the harness (createExecutor's `locks`
    // param): a declared lock with no service refuses loudly at dispatch
    // time rather than running unlocked.
    const withLock = locks?.withLock;
    if (typeof withLock !== "function") {
      throw new Error(`manifold: refusing dispatch of node "${nodeId}" — cross-process lock "${lockName}" needs a harness lock service (pass locks to createExecutor)`);
    }
    let wasQueued = false;
    const wrappedFn = async () => {
      if (wasQueued) {
        const after = await store.get(flowId);
        if (after) {
          await store.setRunning({
            flowId, expectedRevision: after.revision, lastStep: after.lastStep, stateJson: after.stateJson,
          });
        }
      }
      return fn();
    };
    // The browser tab-lifecycle guarantee (open-before/close-after for any
    // "browser:<profile>" lock name) lives inside the injected lock
    // service's withLock, not here — it needs to cover every acquirer of
    // that lock, including application code that calls withLock directly
    // outside the engine's node dispatch.
    // `logger` is threaded through opts so the service's internal wrapping
    // can log a warning if the browser plugin is unreachable.
    return withLock(lockName, wrappedFn, {
      cooldownMs,
      cooldownJitterMs,
      logger,
      onWouldBlock: async () => {
        wasQueued = true;
        const before = await store.get(flowId);
        if (!before) return;
        await store.setQueued({
          flowId, expectedRevision: before.revision, lastStep: before.lastStep,
          stateJson: before.stateJson,
          waitJson: { kind: "resource-lock", resource: lockName, nodeId },
          statusReason: `queued: waiting for cross-process lock ${lockName} (node ${nodeId})`,
        });
      },
    });
  }

  // nodeCtx is assigned after start/runFlow are defined (see below).
  // Subflow/node runners read it via getter at call time, not at factory-construction
  // time, so the undefined-at-construction gap is safe.
  let nodeCtx;

  // Every pipeline's subflow node now runs under childFlows:true — this is a
  // direct call to the only dispatch path that remains. `runFlow` is a
  // hoisted function declaration defined below — safe to reference here.
  // `kind:"subflow"` covers both the many-children-via-selector shape and
  // single-child use.
  async function executeSubflowNode({ node, nodeId, flowId, state }) {
    // A subflow node's own selector(state) can legitimately need a PRIOR
    // subflow node's aggregated per-subflow results (e.g. PROPOSE's selector
    // reading state._subflowGroups.SCORE to resolve each shortlisted
    // listing's score data) — same reason dispatchCodeEntry
    // (executor-nodes.js) enriches state before calling a "code" node's
    // run(). Subflow dispatch never does this enrichment, so any such
    // selector silently sees undefined subflow groups and resolves to zero
    // items regardless of how many upstream branches actually succeeded.
    //
    // assembleState(baseState: state) replaces enrichWithSubflows(state, ...)
    // here: it subsumes enrichWithSubflows's own _subflowGroups computation
    // (same getAllSubflowGroups call, folded in) and additionally overlays
    // each node's resultJson on top of the real stateJson passed as
    // baseState. The overlay is a no-op against real flows (resultJson-derived
    // values are byte-identical to state_json's), so this is the same
    // "additive, verified before cutover" pattern as every prior phase,
    // scoped to the single narrowest read site (node.selector) rather than
    // all 5 root call sites at once.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
    return dispatchSubflowChildren({
      node, nodeId, flowId, state: enrichedState, store, runStore, nodeExecutionsStore: nodeExecutions,
      maybeBudgetGate, runFlow, logger, lineage: _lineage, pipelinesRef, admissionQueueStore,
    });
  }

  const { executeNode } = createNodeRunner({
    maybeBudgetGate, dispatchAgentWithRetry, executeSubflowNode,
    withCrossProcessLockMaybe, locks, api, config: _config, lineage: _lineage, pipelinesDir: _pipelinesDir, logger,
    getNodeCtx: () => nodeCtx,
    runStore, nodeExecutionsStore: nodeExecutions,
    nodeHealthStore, pipelinesRef,
  });

  /**
   * Create the flow_run row for a new pipeline run and return the record.
   * Does NOT start the execution loop — call `runFlow(record.flowId)` to drive it.
   */
  async function start(pipelineId, args = {}, triggeredBy = null) {
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline) throw new Error(`unknown pipeline: ${pipelineId}`);

    const runId = randomUUID();
    const initialState = {
      runId,
      startedAt: Date.now(),
      args,
      triggeredBy: triggeredBy ?? null,
      ...deepClone(pipeline.state),
    };

    const record = await store.create({ pipelineId, initialState });
    logger.info(`manifold [${record.flowId.slice(0, 8)}] start: pipeline=${pipelineId} runId=${runId.slice(0, 8)}`);

    // Per-pipeline singleton gate. Default (unset) is unlimited — the
    // shared admission pool below is the only gate almost every pipeline
    // needs. `allowConcurrent: false` opts a pipeline out of overlap
    // entirely (e.g. a browser-automation poller that would race itself
    // over shared session/gate state): reject this run before it ever
    // competes for an admission slot if another non-terminal run of the
    // same pipelineId already exists.
    if (pipeline.allowConcurrent === false) {
      const others = await runStore.listRunsPage({ pipelineId });
      const active = others.find((r) => r.flowId !== record.flowId && !isTerminal(r.status));
      if (active) {
        logger.info(`manifold [${record.flowId.slice(0, 8)}] start: skipped — ${pipelineId} already has an active run (flowId=${active.flowId.slice(0, 8)}, status=${active.status})`);
        await store.skip({
          flowId: record.flowId,
          expectedRevision: record.revision,
          stateJson: record.stateJson,
          reason: `another run of ${pipelineId} is already active (flowId=${active.flowId}, status=${active.status})`,
        });
        return await store.get(record.flowId);
      }
    }

    // Whole-flow-run admission — the shared pool (state/admission-queue-
    // store.js), same one subflow child-dispatch draws from. Every new flow
    // competes for a real slot the instant it's created, and queues (rather
    // than being rejected) if the pool is full — the caller gets back a
    // record with status:"queued", exactly like an unadmitted subflow
    // child. state-store.js's
    // release/promotion wiring wakes it the moment a slot frees, no restart
    // or caller retry needed.
    const admission = await admissionQueueStore.admit({ kind: "flow-start", flowId: record.flowId });
    if (admission.status === "queued") {
      logger.info(`manifold [${record.flowId.slice(0, 8)}] start: queued — no admission slot available yet`);
      await store.setQueued({
        flowId: record.flowId,
        expectedRevision: record.revision,
        lastStep: record.lastStep,
        stateJson: record.stateJson,
        waitJson: { kind: "flow-start" },
        statusReason: "queued: waiting for a shared admission slot",
      });
      return await store.get(record.flowId);
    }
    return record;
  }

  /**
   * Drive the run loop until the flow reaches a terminal status or blocks.
   * Safe to call multiple times — re-enters the loop wherever the flow is.
   *
   * `kind` describes why THIS runFlow() call was made — "fresh" | "retry" |
   * "revival" | "recheck" — and is only actually consulted for the specific
   * node the flow was already sitting at (a non-fresh re-entry); every
   * subsequent genuinely-new node this loop advances to resolves to "fresh"
   * regardless of what was passed here (see the execKind computation below).
   * Required, not defaulted — see state/node-executions.js's write() for why
   * a caller must always be explicit about this.
   */
  async function runFlow(flowId, kind) {
    while (true) {
      const flow = await store.get(flowId);
      if (!flow) {
        logger.warn(`manifold [${flowId.slice(0, 8)}] vanished mid-execution`);
        return;
      }
      if (isTerminal(flow.status)) {
        logger.info(`manifold [${flowId.slice(0, 8)}] terminal (${flow.status})`);
        return;
      }
      if (isBlocked(flow.status)) {
        logger.info(`manifold [${flowId.slice(0, 8)}] ${flow.status} at ${flow.lastStep} on ${flow.waitJson?.kind ?? "?"}`);
        return;
      }
      // A flow queued for its OWN flow-start or child-dispatch admission
      // (kind:"flow-start" / "child-dispatch") is not yet cleared to run at
      // all — its admission check lives OUTSIDE this loop (in start()'s
      // caller for flow-start, in the parent's dispatch loop for
      // child-dispatch), so a fresh runFlow() call here has no code path
      // that would naturally re-request admission; it would just dispatch
      // lastStep's outbound edge unconditionally, bypassing the pool
      // entirely. Return immediately — admission-queue-store's own
      // release()/promotion wakes it later. Deliberately NOT the same for
      // resource-lock (or no waitJson at all): that wait lives INSIDE this
      // same node's own re-entered dispatch path (withCrossProcessLockMaybe),
      // so falling through and re-resolving the edge is exactly how a
      // boot-revived queued orphan on a lock is meant to retry.
      if (isQueued(flow.status) && (flow.waitJson?.kind === "flow-start" || flow.waitJson?.kind === "child-dispatch")) {
        logger.info(`manifold [${flowId.slice(0, 8)}] queued at ${flow.lastStep} on ${flow.waitJson?.kind}`);
        return;
      }

      const state = flow.stateJson ?? {};
      if (state._paused) {
        logger.info(`manifold [${flowId.slice(0, 8)}] paused at ${flow.lastStep}`);
        return;
      }
      const pipelineId = state.pipelineId;
      if (pipelineId && !pipelinesRef.value.get(pipelineId) && typeof ensurePipelinesLoaded === "function") {
        try { await ensurePipelinesLoaded(); } catch (err) {
          logger.warn(`manifold [${flowId.slice(0, 8)}] ensurePipelinesLoaded failed: ${err?.message ?? err}`);
        }
      }
      const pipeline = pipelineId ? pipelinesRef.value.get(pipelineId) : null;
      if (!pipeline) {
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `pipeline not loaded: ${pipelineId}`,
          failureNode: "<engine:pipeline-load>",
        });
        return;
      }

      const lastStep = flow.lastStep ?? "__start__";
      // A cascade leg's own edge function (2-arg mode, see resolveEdge) can
      // read the error the leg just threw — carried here via a transient
      // `_cascadeError` state key set right before the retry-continue below,
      // guarded on nodeId===lastStep so a stale marker from an earlier
      // loop-back is never read once lastStep has moved past it. Survives a
      // gateway restart for free — it's just persisted state_json, same as
      // every other field the executor writes.
      const cascadeError = state?._cascadeError?.nodeId === lastStep ? state._cascadeError : null;
      // Fourth root dispatch-loop read site cut over: edge functions now
      // read via assembleState(baseState: state) instead of raw state — the
      // single call site every edge decision in the engine goes through
      // (root and child flows alike, since they share this one runFlow
      // loop). A separate local var, not a reassignment of `state` itself —
      // `state` is still what gets persisted below; only the value handed
      // to the edge function changes.
      const edgeState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
      const nextNodeId = resolveEdge(pipeline.edges, lastStep, edgeState, {
        logger, flowId,
        error: cascadeError ? { message: cascadeError.message, errorType: cascadeError.errorType } : null,
      });
      if (!nextNodeId) {
        // If this exhaustion traces back to a throw already recorded
        // immediately at throw-time (the cascade catch branch below),
        // thread that same invocationId/executionId through so
        // upsertFailureRow's execution_id-based upsert collapses this into
        // the SAME failure_events row instead of creating a second,
        // less-detailed one for the identical logical moment. A genuine
        // non-throw dead-end (unknown node, business-logic exhaustion with
        // no antecedent throw) has no cascadeError and gets the plain
        // generic message, same as always.
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: cascadeError?.message ?? `edge ${lastStep} returned no next node`,
          failureNode: lastStep,
          invocationId: cascadeError?.invocationId ?? null,
          executionId: cascadeError?.executionId ?? null,
          errorType: cascadeError?.errorType ?? null,
        });
        return;
      }
      if (nextNodeId === "__end__") {
        // This root read site carries the same resultJson-replay need as
        // every other: pipeline.display() and successCriteria() both read
        // node output through here. assembleState() replaces the old
        // enrichWithSubflows, which only ever added _subflowGroups onto raw
        // `state` and never replayed resultJson at all. Harmless while
        // state_json still carried the full accumulated blob; broken once
        // the state-write filter made it entry-only.
        const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
        await runAuditIfConfigured({ flowId, pipeline, state: enrichedState, logger });
        // failureStore.recordFromTerminalState's soft-marker scan was
        // deleted — zero soft-error producers remain anywhere in
        // `.pipelines`/`.shared`; every failure now persists directly from
        // this file's own throw-catch paths via recordEngineFailure.
        // finalState is enrichedState-based, not state-based — a node's own
        // output (e.g. a runAudit-only sentinel field like
        // `_successCriteriaReason`) only exists via resultJson replay now,
        // so finalState._successCriteriaReason below would silently read
        // undefined off raw `state` otherwise. Safe to carry _subflowGroups
        // through here too: the persist-layer filter
        // (executor-persist.js's toPersistableState) already narrows
        // whatever's passed to persistFailRetrying/persistSkipRetrying/
        // persistFinishRetrying down to entry-only fields regardless of
        // what finalState itself contains.
        let finalState = enrichedState;
        if (typeof pipeline.display === "function") {
          try {
            const out = await Promise.resolve(pipeline.display(enrichedState));
            if (out && typeof out === "object") {
              finalState = {
                ...enrichedState,
                _display: {
                  title: typeof out.title === "string" ? out.title : null,
                  summary: typeof out.summary === "string" ? out.summary : null,
                  funnels: Array.isArray(out.funnels) ? out.funnels : null,
                  metrics: Array.isArray(out.metrics) ? out.metrics : null,
                },
              };
            }
          } catch (err) {
            logger?.warn?.(`manifold [${flowId.slice(0, 8)}] pipeline.display() threw: ${err?.message ?? err}`);
          }
        }
        const successCriteria = pipeline.runAudit?.successCriteria;
        if (typeof successCriteria === "function") {
          let verdict;
          try {
            verdict = await Promise.resolve(successCriteria({ ...enrichedState, ...finalState }));
          } catch (err) {
            logger?.warn?.(`manifold [${flowId.slice(0, 8)}] successCriteria threw: ${err?.message ?? err}`);
          }
          if (verdict === "failed") {
            await persistFailRetrying({
              flowId, revision: flow.revision,
              state: finalState,
              message: finalState._successCriteriaReason ?? "successCriteria: failed",
              failureNode: "__end__",
            });
            logger.info(`manifold [${flowId.slice(0, 8)}] __end__ → failed (successCriteria)`);
            return;
          }
          if (verdict === "skipped") {
            await persistSkipRetrying({
              flowId, revision: flow.revision,
              state: finalState,
              reason: finalState._successCriteriaReason ?? "successCriteria: skipped",
            });
            logger.info(`manifold [${flowId.slice(0, 8)}] __end__ → skipped (successCriteria)`);
            return;
          }
        }
        await persistFinishRetrying({ flowId, revision: flow.revision, state: finalState });
        logger.info(`manifold [${flowId.slice(0, 8)}] __end__`);
        return;
      }

      const node = pipeline.nodes[nextNodeId];
      if (!node) {
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `unknown node: ${nextNodeId}`,
          failureNode: nextNodeId,
        });
        return;
      }

      logger.info(`manifold [${flowId.slice(0, 8)}] → ${nextNodeId} (${node.kind})`);

      const enteredAt = Date.now();
      // Re-pointed off flow_node_invocations by a later cleanup pass (was
      // state._stepHistory): computed once per dispatch, from this flow's
      // own last-dispatched node — see executor-helpers.js's isFreshNodeEntry
      // for the full reasoning. execKind is the actual value threaded to every write()
      // call below in this iteration (only one ever fires per iteration) —
      // a genuinely fresh node entry is always "fresh" regardless of why
      // this runFlow() call happened; only a non-fresh re-entry (the node
      // the flow was already at) "spends" the caller-supplied kind.
      const lastNodeId = await nodeExecutions.findCurrentStep(flowId);
      const forceNew = isFreshNodeEntry(lastNodeId, nextNodeId);
      const execKind = forceNew ? "fresh" : kind;
      // Create the invocation row pre-execution: "last invocation for this
      // flow" now correctly tracks the in-flight node even during
      // execution, not just after completion. Replaces markExecuting() as the
      // mechanism for identifying the currently-running node.
      const enterResult = await nodeExecutions.enter({
        flowId, pipelineId: state.pipelineId ?? null,
        nodeId: nextNodeId, subflowKey: null, kind: execKind,
      });
      const invocationId = enterResult?.invocationId ?? null;
      const isNew       = enterResult?.isNew          ?? (execKind === "fresh");
      process.nextTick(() => nodeEmitter.emit("node.executing", {
        flowId, nodeId: nextNodeId,
        pipelineId: state.pipelineId ?? null,
        parentInvocationId: flow.parentInvocationId ?? null,
      }));
      let stateUpdate = {};
      let waitJson = null;
      let blockedInfo = null;
      let skippedInfo = null;
      let reentrant = false;
      let exitedAt = null;
      try {
        // "needs:" resolution is lazy, via ctx.needs(varName) called
        // from inside the node's own run()/promptBuilder()/selector(), NOT
        // pre-checked here. executor-nodes.js/executor-subflow-children.js
        // build that accessor (needs-resolver.js's makeNeedsCtx) and pass
        // it through as part of ctx — nothing to do at this call site
        // beyond letting a NodeBlocked(_kind:"needs-not-ready") thrown from
        // inside executeNode() propagate to the catch block below exactly
        // like any other NodeBlocked (the budget-gate pause already relies
        // on this same path). Known gap, deliberately deferred (needs: is
        // dormant, zero pipelines declare it yet): reviveAgentKindOrphan's
        // own dispatch path does not pass a needs ctx at all, so an agent-
        // kind node that crashes mid-flight can't call ctx.needs() on
        // revival — worth fixing before any pipeline adopts needs: on an
        // agent-kind node, not before.
        const result = await executeNode({ node, nodeId: nextNodeId, state, flowId, invocationId });
        exitedAt = Date.now();
        stateUpdate = result.stateUpdate ?? {};
        waitJson = result.waitJson ?? null;
        blockedInfo = result.blockedInfo ?? null;
        skippedInfo = result.skippedInfo ?? null;
        reentrant = result.reentrant === true;
      } catch (err) {
        exitedAt ??= Date.now();
        if (err instanceof NodeBlocked) {
          blockedInfo = err._blocked;
        } else if (err instanceof GatewayHandoffNeeded) {
          blockedInfo = {
            reason: `node ${nextNodeId} needs gateway context: ${err.message}`,
            needed: [],
            hint: "No automatic resume — requires a scope-gap fix at the call site or manual task-revival.",
            _kind: "needs-gateway-handoff",
          };
          logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} → needs-gateway-handoff: ${err.message}`);
        } else {
          const msg = err?.message ?? String(err);
          // Duck-typed, not `instanceof NodeFailure` — pipeline/shared code
          // and the engine itself can load executor-errors.js through
          // different module registries (the gateway daemon's data:-URL
          // pipeline loader vs. jiti-transpiled CJS for CLI tooling), which
          // gives structurally-identical but reference-distinct NodeFailure
          // classes; instanceof silently returns false across that
          // boundary even for a genuine NodeFailure. A plain Error never
          // has .errorType set, so this is equivalent and robust either way.
          const errorType = err?.errorType ?? null;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          // No error/errorType here — flow_node_executions is purely an
          // execution-window/identity ledger; every throw's actual content
          // now goes to failure_events instead, via the recordEngineFailure
          // call(s) below.
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          if (pipeline.kind === "cascade") {
            // A cascade-kind flow treats ANY throw like a completed dispatch,
            // not a failure
            // — lastStep advances to the node that just threw (same persist
            // shape as the ordinary success path further below), and it's
            // THAT node's own edge function that decides what happens next,
            // the same way it would after a success. A cascade leg throwing
            // NodeFailure (see executor-errors.js) carries WHY it failed via
            // `errorType` — threaded here into `_cascadeError`, a transient
            // state key the next loop iteration reads (guarded on
            // nodeId===lastStep, see the resolveEdge call above) and passes
            // to a 2-arg edge function as its error argument. This is what
            // lets a cascade's own edge decide retry-vs-fallback-vs-give-up
            // from the REAL thrown error, not a soft-returned field.
            //
            // Every throw persists a permanent failure_events row
            // IMMEDIATELY, right here, regardless of what the edge
            // decides next — not just on eventual give-up. This is the one
            // gap the pre-existing persistFailRetrying->recordEngineFailure
            // path never covered (a cascade-continuing throw never reaches
            // persistFailRetrying at all). If this attempt turns out to be
            // the cascade's final one, the `!nextNodeId` branch at the top
            // of this loop reuses this SAME invocationId/executionId when
            // it fails the flow, so upsertFailureRow's execution_id upsert
            // collapses onto this row instead of creating a second one.
            try {
              await failureStore?.recordEngineFailure?.({
                flowId, pipelineId: state.pipelineId ?? null, failureNode: nextNodeId,
                message: msg, state, invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
              });
            } catch (recErr) {
              logger?.warn?.(`manifold [${flowId.slice(0, 8)}] recordEngineFailure (cascade) threw: ${recErr?.message ?? recErr}`);
            }
            logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} → cascading (its own edge decides what happens next)`);
            await persistRunningRetrying({
              flowId, revision: flow.revision, lastStep: nextNodeId,
              state: {
                ...state,
                _cascadeError: {
                  nodeId: nextNodeId, message: msg, errorType,
                  invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
                },
              },
            });
            continue;
          }
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
          });
          return;
        }
      }

      if (blockedInfo) {
        if (!blockedInfo._kind) {
          const msg = blockedInfo.reason ?? `${nextNodeId} blocked without declaring a wait kind`;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        const waitJsonForBlock = {
          kind: blockedInfo._kind,
          nodeId: nextNodeId,
          reason: blockedInfo.reason ?? "node blocked",
          needed: blockedInfo.needed ?? [],
          hint: blockedInfo.hint ?? "",
          ...(blockedInfo.responder ? { responder: blockedInfo.responder } : {}),
          ...(Array.isArray(blockedInfo.choices) && blockedInfo.choices.length > 0
                ? { choices: blockedInfo.choices.map((c) => String(c)) }
                : {}),
          ...(Array.isArray(blockedInfo.items) ? { items: blockedInfo.items } : {}),
          renderedAt: Date.now(),
          retry: true,
        };
        try {
          validateGateLayout(waitJsonForBlock.items);
        } catch (err) {
          const msg = err?.message ?? String(err);
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        const summary = `${nextNodeId}: ${waitJsonForBlock.reason}`;
        logger.info(`manifold [${flowId.slice(0, 8)}] ${waitJsonForBlock.kind} → waiting (will re-run ${nextNodeId} on resume)\n  reason: ${waitJsonForBlock.reason}\n  hint: ${waitJsonForBlock.hint}`);
        await persistWaitingRetrying({
          flowId, revision: flow.revision, lastStep,
          blockingNodeId: nextNodeId,
          state,
          waitJson: waitJsonForBlock,
          statusReason: shortSummary(summary),
        });
        nodeExecutions.write({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      if (skippedInfo) {
        logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId}: _skipped — ${skippedInfo.reason}`);
        await persistSkipRetrying({
          flowId, revision: flow.revision,
          state,
          reason: skippedInfo.reason ?? "skipped",
        });
        nodeExecutions.write({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      const freshAfterNode = (await store.get(flowId)) ?? { stateJson: state, revision: flow.revision };
      // entryState is taken directly from freshAfterNode.stateJson, not
      // recomputed as a merge of this node's own output. Every persist call
      // below filters its `state:` argument down to entry/bookkeeping fields
      // only (executor-persist.js's toPersistableState), so a wide merge of
      // the node's output here would never reach the database. A node's own
      // output reaches result_json separately, via writeResult(invocationId,
      // stateUpdate) further down — entirely independent of this variable.
      // freshAfterNode.stateJson is already entry-only (it was itself
      // persisted through the same filter), so it's used directly.
      let entryState = freshAfterNode.stateJson ?? state;
      // This node just completed successfully (no throw, no blockedInfo, no
      // skippedInfo — we're past all of those branches above) — any
      // _cascadeError left over from an earlier failed attempt AT THIS SAME
      // node is now stale. Only ever lives on this entry-only state (a
      // success's own stateUpdate never mentions _cascadeError), so a node
      // retried after a cascade throw and then succeeding would otherwise
      // carry a stale error into its own next edge-function call if that
      // edge checks `error` before `state`. Clear it explicitly.
      if (entryState._cascadeError?.nodeId === nextNodeId) {
        const { _cascadeError, ...rest } = entryState;
        entryState = rest;
      }
      const persistRevision = freshAfterNode.revision ?? flow.revision;

      if (waitJson) {
        try {
          validateGateLayout(waitJson.items);
        } catch (err) {
          const msg = err?.message ?? String(err);
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: persistRevision, state: entryState,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        // node.renderForOperator was an optional hook nothing in this
        // codebase ever implemented (see describeWaitFallback's own header
        // comment) — removed rather than keep computing a wide state merge
        // just to feed an argument no pipeline reads.
        const summary = describeWaitFallback(waitJson, nextNodeId);
        logger.info(`manifold [${flowId.slice(0, 8)}] ${reentrant ? "reentrant gate" : "gate"} → blocked\n${summary}`);
        await persistWaitingRetrying({
          flowId, revision: persistRevision,
          lastStep: reentrant ? lastStep : nextNodeId,
          blockingNodeId: nextNodeId,
          state: entryState,
          waitJson,
          statusReason: shortSummary(summary),
        });
        nodeExecutions.write({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      // Write this node's own result FIRST and durably, before advancing
      // flow_runs.last_step to nextNodeId via persistRunningRetrying below.
      // last_step is what boot-time orphan revival and the pipeline-
      // concurrency dequeue path both trust as "this node completed" — they
      // jump straight to resolving its outbound edge without re-running
      // apply(). Under the old order (state persist first, result write
      // second) a restart landing between the two left last_step durably
      // pointing at a node whose result_json was never committed, so the
      // edge function read an empty state.<nodeId> and returned null — the
      // engine surfaced this as "edge <nodeId> returned no next node"
      // (SCORE_LLM, live in production). Writing the result first makes that
      // invariant crash-safe: if a restart now lands in the new gap
      // instead, last_step simply never advances and revival re-runs this
      // node's apply() again — a duplicate call, not a silent failure.
      //
      // Also still AWAITED, not fire-and-forget: once the state-write
      // filter cutover stopped persisting the full state_json blob, the
      // very next loop iteration's assembleState() call (line ~288, feeding
      // the edge function) became the ONLY way this node's own output
      // becomes visible again — if
      // that read raced ahead of this write, the edge function would see
      // this node's output as still-missing, some edges then re-route back
      // to the same node (or worse), producing a tight busy-loop. Found
      // live: 4 real E2E suites spun at ~45% CPU indefinitely once this
      // filter shipped. Awaiting costs one DB round-trip per dispatch,
      // already priced into every other persist call this loop makes.
      const resultWrite = await nodeExecutions.writeResult(invocationId, stateUpdate).catch(() => null);
      if (!resultWrite || !resultWrite.ok) {
        const reason = resultWrite?.reason ?? "threw";
        // "race" means a CONCURRENT writer already committed real content
        // for this exact invocation — advancing is correct, the data
        // genuinely is there, just not from this call. Every other reason
        // ("no-invocation-id"/"no-such-invocation"/"unserializable"/
        // "db-error"/a genuine throw, normalized via the catch above) means
        // NO ONE wrote anything. Advancing last_step in that case would
        // move the flow past this node with its output never durably
        // written: the next runFlow() iteration's assembleState() reads
        // nothing for this node, and resolveEdge() dead-ends with "no next
        // node" — the SCORE_LLM "edge returned no next node" shape, and the
        // boot-revival gap it left uncovered (see reviveAgentKindOrphan
        // below). Fail the flow outright for any non-race reason instead of
        // silently continuing, so the operator sees the true cause.
        if (reason !== "race") {
          const msg = `resultJson write failed (${reason})`;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: persistRevision, state: entryState,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? invocationId, executionId: written.executionId ?? null,
          });
          return;
        }
        logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} resultJson write failed (race) — another writer already completed this invocation`);
      }

      const runResult = await persistRunningRetrying({
        flowId, revision: persistRevision, lastStep: nextNodeId,
        state: entryState,
      });
      if (!runResult.applied) {
        const msg = `state persist failed: ${runResult.code}`;
        logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
        const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
        await persistFailRetrying({
          flowId, revision: persistRevision, state: entryState,
          message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
          invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
        });
        return;
      }
      nodeExecutions.write({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
    }
  }

  // Object passed to `code`-kind node bodies as ctx. `invokePipeline` fires a
  // background flow start and returns immediately. Assigned after start/runFlow
  // are defined; subflow/node runners read it via getter so the gap is safe.
  nodeCtx = Object.freeze({
    async invokePipeline(targetPipelineId, args = {}) {
      const record = await start(targetPipelineId, args);
      // start() no longer throws when the shared admission pool is full —
      // it queues instead (see start()'s own comment). Preserves the
      // "singleton-busy" contract at this call site specifically: a caller
      // handling invoked:false is expected to retry invokePipeline itself
      // later, not rely on this record being silently promoted and run out
      // from under it with nobody tracking the outcome — so the just-created
      // queued record is cancelled rather than left to run unobserved.
      if (record.status === "queued") {
        await cancel(record.flowId, "invokePipeline: shared admission pool was full").catch(() => {});
        return {
          invoked: false, reason: "singleton-busy", target: targetPipelineId,
          message: `pipeline "${targetPipelineId}" is at its concurrency limit — queued instead of started`,
        };
      }
      setImmediate(() => {
        runFlow(record.flowId, "fresh").catch((err) => {
          logger?.warn?.(`manifold invokePipeline(${targetPipelineId}) [${record.flowId.slice(0, 8)}]: runFlow failed: ${err?.message ?? err}`);
        });
      });
      return { invoked: true, target: targetPipelineId, flowId: record.flowId };
    },
  });

  // resume() is the generic "continue a waiting flow" entry point, called
  // both after a gate decision resolves (gate-resolver.js, routes/
  // operator.js) and by boot-time revival's children-pending check
  // (boot/flow-revival.js) — both are the same underlying mechanism (a
  // node the flow was already sitting at gets re-evaluated), just
  // triggered by different external events, so both resolve to "recheck".
  async function resume(flowId) {
    return runFlow(flowId, "recheck");
  }

  async function rerun(sourceFlowId, argsOverride) {
    const source = await store.get(sourceFlowId);
    if (!source) throw new Error(`source flow not found: ${sourceFlowId}`);
    return rerunFromSource(source, argsOverride);
  }

  async function rerunFromSource(sourceFlow, argsOverride) {
    if (!sourceFlow) throw new Error(`source flow not found`);
    const sourceFlowId = sourceFlow.flowId;
    const pipelineId = sourceFlow.stateJson?.pipelineId;
    if (!pipelineId) throw new Error(`source flow has no pipelineId in state; cannot determine which pipeline to rerun`);
    const args = argsOverride !== undefined ? argsOverride : (sourceFlow.stateJson?.args ?? {});
    const triggeredBy = {
      kind: "rerun",
      label: `Rerun of ${sourceFlowId.slice(0, 8)}`,
      parentFlowId: sourceFlowId,
      ts: Date.now(),
    };
    const record = await start(pipelineId, args, triggeredBy);
    try {
      const fresh = await store.get(record.flowId);
      if (fresh) {
        await store.setRunning({
          flowId: record.flowId,
          expectedRevision: fresh.revision,
          lastStep: fresh.lastStep,
          stateJson: { ...fresh.stateJson, rerunOf: sourceFlowId },
        });
      }
    } catch (err) {
      logger?.warn?.(`manifold rerun: failed to stamp rerunOf on [${record.flowId.slice(0, 8)}]: ${err?.message ?? err}`);
    }
    return record;
  }

  async function cancel(flowId, reason = null) {
    const flow = await store.get(flowId);
    if (!flow) return { cancelled: false, reason: "Flow not found." };
    // completeCancellation stamps in-flight branches with cancelledAt, kills any
    // active LLM sessions, then writes status='cancelled' + stamped stateJson to DB.
    return completeCancellation({
      store,
      flowId,
      stateJson: flow.stateJson ?? {},
      lastStep: flow.lastStep,
      revision: flow.revision,
      cfg: _config,
      logger,
      runStore,
      nodeExecutionsStore: nodeExecutions,
      reason,
    });
  }

  /**
   * Revive an "agent"-kind orphan left mid-flight by a crashed process.
   *
   * Unlike a code-kind orphan (safe to just re-enter via runFlow — the
   * node hadn't produced any side effect yet), an agent-kind node may have
   * already dispatched an agent turn that completed (or partially acted)
   * before the crash. Re-running node.promptBuilder(state) fresh would give
   * the agent no way to know it already tried this once, risking a
   * duplicate side effect. Instead this sends REVIVAL_MSG to the SAME
   * deterministic session key the original dispatch used, so the agent can
   * check its own history and decide whether to finish or resume.
   */
  // nodeId is the crashed/in-flight node — the caller (boot/flow-revival.js)
  // resolves it from f.currentStep/waitJson.nodeId, since flow.lastStep is
  // the last COMPLETED node, not this one. Falls back to flow.lastStep only
  // for callers that don't pass it explicitly (defensive, not the expected path).
  async function reviveAgentKindOrphan(flow, nodeId = flow.lastStep) {
    const flowId = flow.flowId;
    const flowId8 = flowId.slice(0, 8);
    const state = flow.stateJson ?? {};
    const pipeline = pipelinesRef.value.get(state.pipelineId);
    const node = pipeline?.nodes?.[nodeId];
    if (!node || node.kind !== "agent") {
      logger.warn(`manifold revival [${flowId8}]: reviveAgentKindOrphan called for non-agent node "${nodeId}"`);
      return;
    }

    logger.info(`manifold revival [${flowId8}]: reviving interrupted agent-node "${nodeId}"`);
    const enteredAt = Date.now();
    // Resolved up front (not after executeNode returns) so a tool call made
    // during this revived dispatch can register itself under the SAME
    // invocationId the original, interrupted attempt already created — see
    // tool-handoff.js. No fresh nodeExecutions.enter() call here; the
    // invocation row already exists from the original attempt.
    const invocation = await nodeExecutions.getLatestInvocationForNode(flowId, nodeId, null);
    const invocationId = invocation?.id ?? null;
    let result;
    try {
      result = await executeNode({ node, nodeId, state, flowId, reviveMessage: REVIVAL_MSG, invocationId });
    } catch (err) {
      if (err instanceof GatewayHandoffNeeded) {
        // A request-scope gap in the revived dispatch: block rather than
        // failing the flow outright. No automatic resume exists for this
        // wait kind (see executor-errors.js's GatewayHandoffNeeded
        // docstring) — recovery
        // is either a scope-gap fix at the actual call site or manual
        // task-revival.
        await persistWaitingRetrying({
          flowId, revision: flow.revision, lastStep: nodeId, blockingNodeId: nodeId,
          state,
          waitJson: {
            kind: "needs-gateway-handoff", nodeId, reason: err.message, needed: [],
            hint: "No automatic resume — requires a scope-gap fix at the call site or manual task-revival.",
            renderedAt: Date.now(), retry: true,
          },
          statusReason: shortSummary(`${nodeId}: needs gateway context: ${err.message}`),
        });
        return;
      }
      const msg = err?.message ?? String(err);
      // Duck-typed — see the main loop's identical check above for why
      // `instanceof NodeFailure` isn't reliable across this codebase's
      // module-loading boundaries.
      const errorType = err?.errorType ?? null;
      logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" revival failed: ${msg}`);
      // Revival always continues the exact node that crashed — there's no
      // fresh dispatch decision here (unlike runFlow's loop,
      // which resolves an edge to decide nextNodeId), so kind is hardcoded
      // "revival" rather than derived from isFreshNodeEntry.
      const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
      await persistFailRetrying({
        flowId, revision: flow.revision, state,
        message: `${nodeId}: revival failed: ${msg}`, failureNode: nodeId,
        invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
      });
      return;
    }

    const { stateUpdate = {}, blockedInfo, skippedInfo } = result;

    if (blockedInfo) {
      if (!blockedInfo._kind) {
        const msg = blockedInfo.reason ?? `${nodeId} blocked without declaring a wait kind`;
        logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" ${msg}`);
        // Same reasoning as above — revival is always a continuation.
        const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `${nodeId}: ${msg}`, failureNode: nodeId,
          invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
        });
        return;
      }
      const waitJsonForBlock = {
        kind: blockedInfo._kind, nodeId,
        reason: blockedInfo.reason ?? "node blocked", needed: blockedInfo.needed ?? [],
        hint: blockedInfo.hint ?? "", renderedAt: Date.now(), retry: true,
      };
      await persistWaitingRetrying({
        flowId, revision: flow.revision, lastStep: nodeId, blockingNodeId: nodeId,
        state,
        waitJson: waitJsonForBlock,
        statusReason: shortSummary(`${nodeId}: ${waitJsonForBlock.reason}`),
      });
      return;
    }

    if (skippedInfo) {
      await persistSkipRetrying({
        flowId, revision: flow.revision,
        state,
        reason: skippedInfo.reason ?? "skipped",
      });
      return;
    }

    // No wide stateUpdate merge before persisting — `state` here is
    // already entry-only (freshly read via store.get() upstream), and
    // stateUpdate's own node-output content reaches
    // result_json separately via writeResult(), not through this. See the
    // main dispatch loop's identical cleanup above for the full reasoning.
    let entryState = state;
    // Same staleness clear as the main loop's success path above — an
    // agent-kind cascade leg (e.g. SUBMIT_AGENT) revived and now succeeding
    // shouldn't carry a leftover _cascadeError from its earlier crashed
    // attempt into its own next edge-function call.
    if (entryState._cascadeError?.nodeId === nodeId) {
      const { _cascadeError, ...rest } = entryState;
      entryState = rest;
    }
    // A gap surfaced live (SCORE_LLM "edge returned no next node" during a
    // gateway restart storm): the main loop's fix writes a
    // node's result BEFORE advancing last_step so a crash-then-revival never
    // reads a stale/empty result for the node it just resumed. This revival
    // path resumes the SAME crashed dispatch (no fresh nodeExecutions.enter()
    // call here — the invocation row already exists from the original,
    // interrupted attempt) but was never given the matching writeResult()
    // call, so last_step advanced to `nodeId` while its result_json stayed
    // empty from the pre-crash attempt. The very next runFlow() iteration's
    // assembleState() then read nothing for this node, and resolveEdge()
    // dead-ended with "no next node" — deterministic on every revived
    // agent-node whose outbound edge depends on its own output, not a race.
    const resultWrite = await nodeExecutions.writeResult(invocationId, stateUpdate).catch(() => null);
    if (!resultWrite || !resultWrite.ok) {
      const reason = resultWrite?.reason ?? "threw";
      // "race" means a CONCURRENT writer already committed real content for
      // this exact invocation — advancing is correct, the data genuinely is
      // there, just not from this call. Every other reason (db-error,
      // no-such-invocation, no-invocation-id, unserializable, threw) means
      // NO ONE wrote anything: advancing here would move last_step past
      // this node with its output never durably written. The next runFlow()
      // iteration's assembleState() then reads nothing for this node, and
      // resolveEdge() dead-ends with "no next node" — deterministic given
      // the failure, not a race, and completely swallowing the REAL reason
      // behind a generic edge-resolution message. Fail the flow outright
      // instead — same shape as persistRunningRetrying's own CAS-failure
      // branch just below — so the operator sees the true cause and can
      // retry this one node, rather than a downstream "no next node" that
      // hides it.
      if (reason !== "race") {
        const msg = `resultJson write failed (${reason})`;
        logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" ${msg}`);
        const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `${nodeId}: ${msg}`, failureNode: nodeId,
          invocationId: written.invocationId ?? invocationId, executionId: written.executionId ?? null,
        });
        return;
      }
      logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" resultJson write failed (race) — another writer already completed this invocation`);
    }
    await persistRunningRetrying({ flowId, revision: flow.revision, lastStep: nodeId, state: entryState });
    logger.info(`manifold revival [${flowId8}]: agent-node "${nodeId}" revived — continuing`);
    return runFlow(flowId, "revival");
  }

  return { start, runFlow, resume, cancel, rerun, rerunFromSource, reviveAgentKindOrphan };
}

// ── audit ─────────────────────────────────────────────────────────────────────

async function runAuditIfConfigured({ flowId, pipeline, state, logger }) {
  const audit = pipeline?.runAudit;
  if (!audit) return;
  if (typeof audit.runsDir !== "string" || typeof audit.summarize !== "function") {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit malformed (need runsDir:string + summarize:function); skipping`);
    return;
  }
  const runId = state?.runId;
  if (!runId) {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit: state.runId missing; skipping`);
    return;
  }
  try {
    const runDir = path.join(audit.runsDir, runId);
    await mkdir(runDir, { recursive: true });
    await writeFile(path.join(runDir, "final-state.json"), JSON.stringify(state, null, 2), "utf-8");
    let summary;
    try { summary = await Promise.resolve(audit.summarize(state)); }
    catch (err) {
      logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit.summarize threw: ${err?.message ?? err}`);
      return;
    }
    if (summary && typeof summary === "object") {
      await writeFile(
        path.join(audit.runsDir, "log.jsonl"),
        JSON.stringify(summary) + "\n",
        { flag: "a", encoding: "utf-8" },
      );
    }
  } catch (err) {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit failed: ${err?.message ?? err}`);
  }
}
