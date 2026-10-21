import { getNodeExecutionsStore } from "../state/node-executions.js";
import { admitAndSetRunning } from "../engine/child-resume-trigger.js";

// The four statuses casSetRunningStmt's CAS excludes (run-store.js) — a flow
// sitting in one of these needs store.forceBlock before setRunning will
// accept it. "blocked" and "running" are already fine as-is.
const CAS_EXCLUDED_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

// Force a terminal flow (succeeded/failed/cancelled/skipped) into `blocked`
// so setRunning's CAS will accept it (its WHERE clause excludes those four
// statuses — see run-store.js's casSetRunningStmt). Goes through the
// injected `store` (state-store.js → run-store.js's forceBlock CAS) rather
// than a raw DatabaseSync write — a prior version of this function opened
// its own `new DatabaseSync(runtime.sqlite path)` here, bypassing `store`/
// `readStore` entirely, so tests injecting in-memory store doubles could
// neither observe nor control it (the write landed against a real DB file
// the doubles knew nothing about, always returning "revision changed
// during force-block" in tests).
// Exported so other retry paths can reuse this instead of hitting the same
// `setRunning failed: terminal` result and having no recovery — a subflow
// item can finish failed on a flow that has since gone fully terminal (the
// OTHER items in the same subflow resolved and closed the flow out from under
// it).
// Returns { applied: true, nextRevision } or { applied: false, reason }.
export async function forceBlockOutOfMapFlow({ store, flowId, revision, stateJson, ownerSessionKey }) {
  const result = await store.forceBlock({
    flowId,
    expectedRevision: revision,
    stateJson,
    statusReason: "retry-pending",
    ownerKey: ownerSessionKey,
  });
  if (!result.applied) return { applied: false, reason: `force-block failed: ${result.code ?? "unknown"}` };
  return { applied: true, nextRevision: revision + 1 };
}

// Walks down through a subflow node's spawned children to find the real,
// non-propagated failure(s) beneath a "subflow-mode-not-satisfied" row
// (recursing through any further nested propagations), rather than
// retrying the propagation's own flow — which already dispatched cleanly
// and would just immediately re-fail with the identical reason. Collects
// EVERY real failure found, not just the first: under mode:"all" with more
// than one failed child, fixing only one still leaves the fan-out
// unsatisfied. `visited` guards a pathological cycle; real topologies are
// trees, this is defense only.
async function findRealFailuresBeneath({ flowId, nodeId, runStore, nodeExecutionsStore, failureStore, visited }) {
  const key = `${flowId}:${nodeId}`;
  if (visited.has(key)) return [];
  visited.add(key);
  const itemInvocationIds = await nodeExecutionsStore.listInvocationIdsForNode(flowId, nodeId);
  if (!itemInvocationIds.length) return [];
  const children = await runStore.listForParentInvocations(itemInvocationIds);
  const found = [];
  for (const child of children) {
    if (child.status === "succeeded") continue;
    // Legitimately still pending (e.g. blocked on its own human-gate) has
    // no failure_events row at all — correctly nothing to retry here yet.
    const [childFailure] = await failureStore.list({ flowId: child.flowId, limit: 1 });
    if (!childFailure) continue;
    if (childFailure.error_type === "subflow-mode-not-satisfied") {
      found.push(...await findRealFailuresBeneath({
        flowId: child.flowId, nodeId: childFailure.failure_node, runStore, nodeExecutionsStore, failureStore, visited,
      }));
    } else {
      found.push(childFailure);
    }
  }
  return found;
}

export function createRetryFailure({
  store,
  readStore,
  failureStore,
  pipelinesRef,
  executor,
  runStore,
  ownerSessionKey,
  admissionQueueStore,
  logger,
  api,
}) {
  async function retryFailureBranch({ id, via, awaitCompletion = false }) {
    const failure = await failureStore.get(id);
    if (!failure) return { code: "not_found", reason: "failure event not found" };
    const flowId = failure.flow_id;
    const failureNode = failure.failure_node;

    // Redirect before falling into the normal same-flow retry below. This
    // flow's own dispatch (SUBMIT/PROPOSE/etc.) already succeeded; retrying
    // IT re-runs a clean dispatch, finds the same still-failed/still-blocked
    // children, and (for the still-failed ones) immediately re-fails with
    // the identical reason within the same tick.
    if (failure.error_type === "subflow-mode-not-satisfied") {
      const realFailures = await findRealFailuresBeneath({
        flowId, nodeId: failureNode, runStore, nodeExecutionsStore: getNodeExecutionsStore(), failureStore, visited: new Set(),
      });
      if (realFailures.length === 0) {
        return { code: "conflict", reason: `no real failure found beneath "${failureNode}" — may already be resolving` };
      }
      const results = [];
      for (const real of realFailures) {
        results.push(await retryFailureBranch({ id: real.id, via, awaitCompletion }));
      }
      const anyOk = results.some((r) => r.code === "ok");
      return {
        code: anyOk ? "ok" : "conflict",
        reason: anyOk ? undefined : "none of the underlying failures could be retried",
        mode: "cascade-redirect",
        redirectedTo: realFailures.map((f) => f.id),
        results,
      };
    }

    // Every failure — whether at flow's own top level or inside a
    // childFlows:true subflow item — retries the same way: failure.flow_id
    // already names the exact flow to walk and re-run. A subflow item runs
    // as a real child flow with its own flow_id (executor-subflow-children.js),
    // so branch_item_id (sourced from flow_node_invocations.subflow_key, the
    // per-item dispatch key) is purely descriptive here — it doesn't
    // change which flow gets retried.
    {
      // ── Flow-level retry ─────────────────────────────────────────────────────
      const inMapFlow = await store.get(flowId);
      const flow = inMapFlow ?? await readStore.get(flowId);
      if (!flow) return { code: "not_found", reason: "flow gone from registry" };
      const state = flow.stateJson ?? {};
      const pipeline = pipelinesRef.value.get(state.pipelineId);
      if (!pipeline) return { code: "not_found", reason: `pipeline not loaded: ${state.pipelineId}` };
      const FLOW_TERMINAL = new Set(["succeeded", "failed", "cancelled", "blocked", "skipped"]);
      if (!FLOW_TERMINAL.has(flow.status)) {
        return { code: "conflict", reason: `flow is ${flow.status}; refusing to race executor` };
      }

      // A failure with no failure_node happened before any node on this flow
      // ever dispatched (failure_node is derived from invocation_id, which is
      // only ever set once a flow_node_invocations row exists — e.g. the
      // flow's own pipeline failed to resolve). There's nothing to partially
      // clear in that case, so the only sane retry target is the whole flow
      // from __start__ — same lastStep a brand-new flow gets from
      // state-store.js's create().
      if (!failureNode) {
        const nextState = JSON.parse(JSON.stringify(state));
        nextState._attemptNumber = (state._attemptNumber ?? 0) + 1;
        let effectiveRevision = flow.revision;
        if (CAS_EXCLUDED_STATUSES.has(flow.status)) {
          const forced = await forceBlockOutOfMapFlow({
            store, flowId, revision: flow.revision, stateJson: nextState, ownerSessionKey,
          });
          if (!forced.applied) return { code: "conflict", reason: forced.reason };
          effectiveRevision = forced.nextRevision;
        }
        const result = await admitAndSetRunning({
          flowId, expectedRevision: effectiveRevision, lastStep: "__start__", stateJson: nextState,
          parentInvocationId: flow.parentInvocationId, store, admissionQueueStore,
          runStore, resume: executor.resume, logger, api,
        });
        if (!result.applied) {
          return { code: "conflict", reason: `setRunning failed: ${result.reason ?? result.code ?? "unknown"}` };
        }
        if (result.queued) {
          logger.info(
            `manifold retry-flow: flowId=${flowId.slice(0, 8)} failureNode=<none> rootNode=__start__ via=${via} queued for admission`
          );
          return { code: "queued", flowId, failureNode: null, rootNode: "__start__", priorStep: null, cleared: [], mode: "flow-restart", via };
        }
        const runRetryFlow = () => executor.runFlow(flowId, "retry").catch((err) => {
          logger.warn(`manifold retry-flow [${flowId.slice(0, 8)}/__start__] runFlow err: ${err?.message ?? err}`);
        });
        if (awaitCompletion) await runRetryFlow();
        else setImmediate(runRetryFlow);
        logger.info(
          `manifold retry-flow: flowId=${flowId.slice(0, 8)} failureNode=<none> rootNode=__start__ via=${via}`
        );
        return { code: "ok", flowId, failureNode: null, rootNode: "__start__", priorStep: null, cleared: [], mode: "flow-restart", via };
      }

      // Walk topology to resolve rootNode
      const topo = [];
      const visited = new Set();
      let cur = "__start__";
      let safety = 256;
      while (cur && cur !== "__end__" && !visited.has(cur) && safety-- > 0) {
        visited.add(cur);
        topo.push(cur);
        const e = pipeline.edges?.[cur];
        try { cur = typeof e === "function" ? e(state) : (typeof e === "string" ? e : null); }
        catch { cur = null; }
      }
      // subflow_key IS NULL is the structural filter for top-level errors —
      // a real column, not an inferred string shape.
      //
      // queryErrorsByFlow returns EVERY historical errored execution, not
      // just the latest attempt per node — a node that failed once, was
      // retried, and now succeeds still has its old failed row in this
      // list. A raw nodeId Set can't tell "still failing" apart from
      // "failed once, historically." Narrow to nodes whose CURRENT (most
      // recent) execution is the one that errored — getLatestExecutionForNode
      // (added for failure_events invocation-linking) makes this exact
      // instead of inferred from string/nodeId equality alone.
      //
      // queryErrorsByFlow moved from nodeExecutionsStore to failureStore
      // ("Eradicate soft errors") — flow_node_executions no longer stores
      // error content at all, failure_events is the only place "which
      // nodes errored" can be answered from now. getLatestExecutionForNode
      // stays on nodeExecutionsStore below — it's identity/timing only,
      // unaffected.
      const nodeExecutionsStore = getNodeExecutionsStore();
      const allRootErrors = (await failureStore.queryErrorsByFlow(flowId)).filter((e) => e.subflowKey == null);
      const errorNodes = new Set();
      for (const e of allRootErrors) {
        const latest = await nodeExecutionsStore.getLatestExecutionForNode({ flowId, nodeId: e.nodeId, subflowKey: null });
        if (latest?.executionId === e.executionId) errorNodes.add(e.nodeId);
      }
      // failure_node is always derived from a live flow_node_executions
      // entry at the moment the failure was first recorded
      // (state/failure-store.js's flow-level sweep). If it's gone from the
      // CURRENT set of root-level errors, this specific failure has already
      // been resolved — most commonly by an earlier retry of the same
      // failure event — so there is nothing left to retry. Fail clean
      // instead of silently re-running the node.
      //
      // Prefer the exact execution_id anchor when the failure row has one
      // (every failure recorded since this session's failure-linking work
      // does) — ties eligibility to the SPECIFIC execution that produced
      // this failure, not just "some node named failureNode currently has
      // an error" (which errorNodes alone can't distinguish across
      // visit_seq-repeated loop-back re-entries of the same node). Legacy
      // rows recorded before invocation_id/execution_id existed fall back
      // to the errorNodes Set membership check.
      const stillLive = failure.execution_id
        ? (await nodeExecutionsStore.getLatestExecutionForNode({ flowId, nodeId: failureNode, subflowKey: null }))?.executionId === failure.execution_id
        : errorNodes.has(failureNode);
      if (!stillLive) {
        return {
          code: "conflict",
          reason: `failure_node "${failureNode}" has no matching error in the flow's current state — this failure was likely already resolved by an earlier retry`,
        };
      }
      let rootNode = failureNode;
      for (const node of topo) { if (errorNodes.has(node)) { rootNode = node; break; } }
      let priorStep = null;
      for (const [step, edge] of Object.entries(pipeline.edges ?? {})) {
        try {
          const next = typeof edge === "function" ? edge(state) : edge;
          if (next === rootNode) { priorStep = step; break; }
        } catch { /* skip */ }
      }
      if (!priorStep) {
        return {
          code: "conflict",
          reason: `no edge in pipeline ${state.pipelineId} resolves to "${rootNode}" — graph may have changed since the flow ran`,
        };
      }
      const nextState = JSON.parse(JSON.stringify(state));
      const rootIdx = topo.indexOf(rootNode);
      const nodesToClear = rootIdx >= 0 ? topo.slice(rootIdx) : [rootNode];
      for (const node of nodesToClear) { delete nextState[node]; }
      nextState._attemptNumber = (state._attemptNumber ?? 0) + 1;
      let effectiveRevision = flow.revision;
      if (CAS_EXCLUDED_STATUSES.has(flow.status)) {
        const forced = await forceBlockOutOfMapFlow({
          store, flowId, revision: flow.revision, stateJson: nextState, ownerSessionKey,
        });
        if (!forced.applied) return { code: "conflict", reason: forced.reason };
        effectiveRevision = forced.nextRevision;
      }
      const result = await admitAndSetRunning({
        flowId, expectedRevision: effectiveRevision, lastStep: priorStep, stateJson: nextState,
        parentInvocationId: flow.parentInvocationId, store, admissionQueueStore,
        runStore, resume: executor.resume, logger, api,
      });
      if (!result.applied) {
        return { code: "conflict", reason: `setRunning failed: ${result.reason ?? result.code ?? "unknown"}` };
      }
      if (result.queued) {
        logger.info(
          `manifold retry-flow: flowId=${flowId.slice(0, 8)} failureNode=${failureNode} rootNode=${rootNode} priorStep=${priorStep} cleared=[${nodesToClear.join(",")}] via=${via} queued for admission`
        );
        return { code: "queued", flowId, failureNode, rootNode, priorStep, cleared: nodesToClear, mode: "flow-step", via };
      }
      const runRetryFlow = () => executor.runFlow(flowId, "retry").catch((err) => {
        logger.warn(`manifold retry-flow [${flowId.slice(0, 8)}/${rootNode}] runFlow err: ${err?.message ?? err}`);
      });
      // HTTP/Telegram callers pass neither, and need this call to return
      // fast — the flow keeps running in the background via setImmediate.
      // The CLI passes awaitCompletion:true instead: its one-shot process
      // exits right after this function returns, so a merely-SCHEDULED
      // setImmediate callback would just get dropped mid-flight — it needs
      // the retry to have genuinely finished before returning.
      if (awaitCompletion) await runRetryFlow();
      else setImmediate(runRetryFlow);
      logger.info(
        `manifold retry-flow: flowId=${flowId.slice(0, 8)} failureNode=${failureNode} rootNode=${rootNode} priorStep=${priorStep} cleared=[${nodesToClear.join(",")}] via=${via}`
      );
      return { code: "ok", flowId, failureNode, rootNode, priorStep, cleared: nodesToClear, mode: "flow-step", via };
    }
  }

  return { retryFailureBranch };
}
