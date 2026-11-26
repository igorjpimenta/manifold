import { getNodeExecutionsStore } from "../state/node-executions.js";
import { admitAndSetRunning } from "../engine/child-resume-trigger.js";

// The four statuses casSetRunningStmt's CAS excludes (run-store.js) — a flow
// in one of these needs store.forceBlock before setRunning will accept it.
const CAS_EXCLUDED_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

// Force a terminal flow into `blocked` so setRunning's CAS accepts it (its
// WHERE clause excludes those four statuses). Goes through the injected
// `store`, not a raw SQL write: a DatabaseSync write would bypass
// store/readStore entirely and land against a real DB file that tests'
// in-memory store doubles cannot observe. Returns { applied, nextRevision? }
// or { applied: false, reason }.
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

// Walks beneath a "subflow-mode-not-satisfied" row to the real failure(s)
// (recursing through nested propagations) — retrying the propagation's own
// flow would just re-fail with the identical reason. Collects EVERY real
// failure, not just the first: under mode:"all" with multiple failed
// children, fixing one still leaves the fan-out unsatisfied. `visited`
// guards a pathological cycle; real topologies are trees.
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
    // Still-pending children (e.g. blocked on their own gate) have no
    // failure_events row — nothing to retry yet.
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

    // Redirect before the same-flow retry below: this flow's own dispatch
    // already succeeded, so retrying IT finds the same still-failed
    // children and re-fails with the identical reason in the same tick.
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

    // failure.flow_id already names the exact flow to walk and re-run — for
    // a subflow item too (it runs as a real child flow with its own
    // flow_id). branch_item_id is purely descriptive here and does not
    // change which flow gets retried.
    {
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

      // failure_node derives from invocation_id, so null means the failure
      // predates any node dispatch (e.g. the pipeline failed to resolve).
      // Nothing to partially clear — retry the whole flow from __start__,
      // the same lastStep a brand-new flow gets.
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
      // subflow_key IS NULL filters to top-level errors (a real column).
      // queryErrorsByFlow returns EVERY historical errored execution, not
      // just the latest attempt per node — narrow to nodes whose CURRENT
      // execution is the one that errored, or a node that failed once and
      // was retried into success would look "still failing".
      const nodeExecutionsStore = getNodeExecutionsStore();
      const allRootErrors = (await failureStore.queryErrorsByFlow(flowId)).filter((e) => e.subflowKey == null);
      const errorNodes = new Set();
      for (const e of allRootErrors) {
        const latest = await nodeExecutionsStore.getLatestExecutionForNode({ flowId, nodeId: e.nodeId, subflowKey: null });
        if (latest?.executionId === e.executionId) errorNodes.add(e.nodeId);
      }
      // If the failure's execution isn't the node's latest, it was already
      // resolved (usually by an earlier retry) — fail clean rather than
      // silently re-run. Anchored to the failure's exact execution_id
      // because errorNodes can't distinguish visit_seq-repeated loop-back
      // re-entries of the same node; a row with no execution_id never
      // matches and takes the conflict path.
      const stillLive =
        (await nodeExecutionsStore.getLatestExecutionForNode({ flowId, nodeId: failureNode, subflowKey: null }))?.executionId === failure.execution_id;
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
        } catch {}
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
      // setImmediate returns before the retry runs — awaitCompletion:true
      // callers must observe it genuinely finish (a scheduled callback is
      // dropped if their process exits right after).
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
