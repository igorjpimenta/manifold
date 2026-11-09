import { mergeForConflict, shortSummary } from "./executor-helpers.js";
import { REVISION_RETRY_LIMIT } from "./executor-errors.js";
import { ENTRY_FIELDS } from "./needs-resolver.js";

export { REVISION_RETRY_LIMIT };

// state_json carries no per-dispatched-node key — a node's own output lives
// exclusively in flow_node_invocations.result_json and is reconstructed on
// read via assembleState() (already wired into every dispatch-loop read
// site). What's persisted here narrows to entry/bookkeeping data only:
// pipelineId, everything in needs-resolver.js's ENTRY_FIELDS (the same
// allowlist ctx.needs({entry}) already validates against — this is its exact
// intended meaning), and the 3 fields buildChildInitialState
// (executor-subflow-children.js) sets on a freshly spawned child. `gates` is
// deliberately NOT in this list — it is sourced exclusively from
// gate_records (executor-nodes.js's human-gate branch does not write
// state.gates) and reading it back through state_json would reopen the
// staleness bug gate_records exists to prevent.
//
// Applied centrally, inside these 6 functions — every persist path funnels
// through here, so the filter is a single choke point to audit instead of
// scattered per-call-site changes.
//
// A global, unconditional filter, not a per-flow gate: the alternative
// ("check whether THIS flow's current state_json has 100% resultJson
// coverage before dropping its blob") was considered and rejected — the
// runtime check would test for a coverage gap the write path does not
// produce, and it would run on every persist.
const PERSISTED_FIELDS = new Set([...ENTRY_FIELDS, "pipelineId", "item", "itemIndex", "parentItem"]);

function toPersistableState(state) {
  if (!state || typeof state !== "object") return state;
  const out = {};
  for (const key of PERSISTED_FIELDS) {
    if (key in state) out[key] = state[key];
  }
  return out;
}

export function createPersist({ store, failureStore, notifyFlowEnteredWaiting, logger }) {
  async function persistRunningRetrying({ flowId, revision, lastStep, state }) {
    let rev = revision;
    let curState = state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.setRunning({ flowId, expectedRevision: rev, lastStep, stateJson: toPersistableState(curState) });
      if (result.applied) return result;
      if (result.code !== "revision_conflict") {
        logger.warn(`manifold [${flowId.slice(0, 8)}] setRunning failed: ${result.code}`);
        return result;
      }
      const fresh = await store.get(flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  async function persistWaitingRetrying(args) {
    let rev = args.revision;
    let curState = args.state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.setWaiting({
        flowId: args.flowId,
        expectedRevision: rev,
        lastStep: args.lastStep,
        blockingNodeId: args.blockingNodeId ?? null,
        stateJson: toPersistableState(curState),
        waitJson: args.waitJson,
        statusReason: args.statusReason,
      });
      if (result.applied) {
        // Push-from-node: tell whoever's listening (typically the
        // telegram-block-notifier) that this flow just transitioned to waiting.
        if (typeof notifyFlowEnteredWaiting === "function") {
          try {
            const fresh = await store.get(args.flowId);
            if (fresh) {
              const ret = notifyFlowEnteredWaiting(fresh);
              if (ret && typeof ret.then === "function") {
                ret.catch((err) => logger?.warn?.(`manifold [${args.flowId.slice(0, 8)}] notifyFlowEnteredWaiting rejected: ${err?.message ?? err}`));
              }
            }
          } catch (err) {
            logger?.warn?.(`manifold [${args.flowId.slice(0, 8)}] notifyFlowEnteredWaiting threw: ${err?.message ?? err}`);
          }
        }
        return result;
      }
      if (result.code !== "revision_conflict") return result;
      const fresh = await store.get(args.flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, args.state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  // Same shape as persistWaitingRetrying, but for status='queued' (waiting
  // on an internal resource — a shared admission-pool slot, cross-process
  // lock — rather than an operator decision). No notifyFlowEnteredWaiting push:
  // that hook exists specifically for external-channel delivery (e.g. the
  // telegram-block-notifier) on human-gate waits, which queued transitions
  // are not.
  async function persistQueuedRetrying(args) {
    let rev = args.revision;
    let curState = args.state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.setQueued({
        flowId: args.flowId,
        expectedRevision: rev,
        lastStep: args.lastStep,
        blockingNodeId: args.blockingNodeId ?? null,
        stateJson: toPersistableState(curState),
        waitJson: args.waitJson,
        statusReason: args.statusReason,
      });
      if (result.applied) return result;
      if (result.code !== "revision_conflict") return result;
      const fresh = await store.get(args.flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, args.state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  async function persistSkipRetrying({ flowId, revision, state, reason }) {
    let rev = revision;
    let curState = state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.skip({ flowId, expectedRevision: rev, stateJson: toPersistableState(curState), reason });
      if (result.applied) return result;
      if (result.code === "terminal") return result;
      if (result.code !== "revision_conflict") return result;
      const fresh = await store.get(flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  async function persistFinishRetrying({ flowId, revision, state }) {
    let rev = revision;
    let curState = state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.finish({ flowId, expectedRevision: rev, stateJson: toPersistableState(curState) });
      if (result.applied) return result;
      if (result.code !== "revision_conflict") return result;
      const fresh = await store.get(flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  async function persistFailRetrying({ flowId, revision, state, message, failureNode, invocationId = null, executionId = null, errorType = null }) {
    let rev = revision;
    let curState = state;
    for (let i = 0; i < REVISION_RETRY_LIMIT; i++) {
      const result = await store.fail({
        flowId,
        expectedRevision: rev,
        stateJson: toPersistableState(curState),
        statusReason: shortSummary(message),
      });
      if (result.applied) {
        try {
          await failureStore?.recordEngineFailure?.({
            flowId,
            pipelineId: curState?.pipelineId,
            failureNode,
            message,
            // Deliberately unfiltered — recordEngineFailure's own `state`
            // param is currently unused in its body (see state/failure-store.js),
            // separate from flow_runs.state_json entirely, so there's nothing
            // here that needs entry-only narrowing.
            state: curState,
            invocationId,
            executionId,
            errorType,
          });
        } catch (err) {
          logger?.warn?.(`manifold [${flowId.slice(0, 8)}] recordEngineFailure threw: ${err?.message ?? err}`);
        }
        return result;
      }
      if (result.code !== "revision_conflict") return result;
      const fresh = await store.get(flowId);
      if (!fresh) return result;
      rev = fresh.revision;
      curState = mergeForConflict(fresh.stateJson, state);
    }
    return { applied: false, code: "retry_exhausted" };
  }

  return {
    persistRunningRetrying,
    persistWaitingRetrying,
    persistQueuedRetrying,
    persistSkipRetrying,
    persistFinishRetrying,
    persistFailRetrying,
  };
}
