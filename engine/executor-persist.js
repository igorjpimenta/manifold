import { mergeForConflict, shortSummary } from "./executor-helpers.js";
import { REVISION_RETRY_LIMIT } from "./executor-errors.js";
import { ENTRY_FIELDS } from "./needs-resolver.js";

export { REVISION_RETRY_LIMIT };

// state_json is entry-only: a node's own output lives exclusively in
// flow_node_invocations.result_json, reconstructed on read via
// assembleState(). PERSISTED_FIELDS narrows every persist path to
// entry/bookkeeping data (needs-resolver's ENTRY_FIELDS plus pipelineId
// and the 3 fields buildChildInitialState sets on a spawned child).
// Applied centrally inside these 6 functions rather than at executor.js's
// ~20 call sites — one choke point to audit. Unconditional, not a per-flow
// coverage check: a runtime guard would protect against a scenario the
// backfill plus the live write path already rule out structurally.
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

  // Same shape as persistWaitingRetrying, but for status='queued'. No
  // notifyFlowEnteredWaiting push — that hook is for external-channel
  // delivery of human-gate waits, which queued transitions are not.
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
            // Deliberately unfiltered — this `state` param is separate from
            // flow_runs.state_json, so entry-only narrowing doesn't apply.
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
