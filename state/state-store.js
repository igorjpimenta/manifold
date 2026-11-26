// state-store.js — thin wrapper over run-store for manifold flow state
// (runtime.sqlite only; registry.sqlite is never touched). Every
// successful write fires flowStateEmitter so trigger-based consumers wake
// instead of polling.

import { randomUUID } from "node:crypto";
import { flowStateEmitter, gateEmitter, flowEmitter } from "../events.js";
import { maybeResumeParent } from "../engine/child-resume-trigger.js";
import { wakePromotedRelease } from "../engine/admission-promotion-resume.js";

function notifyChange(flowId) {
  if (!flowId) return;
  process.nextTick(() => flowStateEmitter.emit("change", flowId));
}

// Fire-and-forget after a terminal CAS write: if every sibling of a
// subflow-spawned child is now terminal, wake (or mode-fail) the parent.
// `failFlow` is passed in because this is a free function outside the
// store object's closure. `admissionQueueStore` is threaded through
// because the parent's own admission was released when it entered its
// children-pending wait — resuming it must re-request one, not assume
// it's still held.
function triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow, failureStore, admissionQueueStore, api, logger }) {
  if (!nodeExecutionsStore || !resumeParentRef?.resume) return;
  maybeResumeParent({ flowId, runStore, nodeExecutionsStore, pipelinesRef, resume: resumeParentRef.resume, failFlow, failureStore, admissionQueueStore, api, logger })
    .catch((err) => logger?.warn?.(`state-store: triggerParentResume(${flowId}) failed: ${err?.message ?? err}`));
}

// Releases whatever live admission this flow holds — kind "flow-start"
// for a root flow, "child-dispatch" for a subflow child; a flow holds at
// most one of the two, so trying both is a cheap no-op for the other.
// Awaiting (not fire-and-forget) matters: a detached release lets a
// short-lived caller (CLI command, gateway dying in that window) race
// ahead of it, leaving the promoted flow permanently 'admitted' with
// nothing to drive it to 'running'. The promoted flow's own run still
// starts on a later tick.
async function triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger }) {
  if (!admissionQueueStore || !runStore) return;
  try {
    const results = await Promise.all([
      admissionQueueStore.releaseByFlow({ flowId, kind: "flow-start" }),
      admissionQueueStore.releaseByFlow({ flowId, kind: "child-dispatch" }),
    ]);
    if (!resumeParentRef?.resume) return;
    await wakePromotedRelease(results, {
      runStore, resume: resumeParentRef.resume, logger, api,
      context: `state-store: triggerAdmissionRelease(${flowId})`,
    });
  } catch (err) {
    logger?.warn?.(`state-store: triggerAdmissionRelease(${flowId}) failed: ${err?.message ?? err}`);
  }
}

// resumeParentRef is a mutable box because createStateStore runs before
// createExecutor exists (composition order in index.js), so `resume` is
// set onto the box after the fact.
export function createStateStore(api, session, { runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger } = {}) {
  const store = {
    async create({ pipelineId, initialState, parentInvocationId }) {
      const flowId = randomUUID();
      const now = Date.now();
      // pipelineId spread LAST — initialState is caller-supplied and must
      // never be able to override which pipeline this flow runs as.
      const stateJson = { ...initialState, pipelineId };
      await runStore.insert({
        flowId,
        ownerKey:     session ?? "",
        pipelineId,
        revision:     0,
        status:       "running",
        currentStep:  null,
        lastStep:     "__start__",
        stateJson,
        createdAt:    now,
        updatedAt:    now,
        parentInvocationId: parentInvocationId ?? null,
      });
      notifyChange(flowId);
      return {
        flowId,
        pipelineId,
        status:       "running",
        revision:     0,
        lastStep:     "__start__",
        currentStep:  null,
        stateJson,
        createdAt:    now,
        updatedAt:    now,
        parentInvocationId: parentInvocationId ?? null,
      };
    },

    async get(flowId) {
      return runStore.get(flowId);
    },

    async list() {
      return runStore.list();
    },

    async listFor(pipelineId) {
      return runStore.listFor(pipelineId);
    },

    async setRunning({ flowId, expectedRevision, lastStep, stateJson }) {
      const result = await runStore.casSetRunning(flowId, { expectedRevision, lastStep, stateJson });
      if (result.applied) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.step.changed", { flowId, lastStep, stateJson, status: "running" }));
      }
      return result;
    },

    async setWaiting({ flowId, expectedRevision, lastStep, blockingNodeId, stateJson, waitJson, statusReason }) {
      const updatedAt = Date.now();
      const result = await runStore.casSetWaiting(flowId, {
        expectedRevision,
        lastStep,
        blockingNodeId: blockingNodeId ?? null,
        stateJson,
        waitJson,
        statusReason,
      });
      if (result.applied) {
        notifyChange(flowId);
        if (waitJson?.kind === "human-gate") {
          process.nextTick(() => gateEmitter.emit("gate.opened", {
            flowId, nodeId: blockingNodeId,
            items: waitJson.items ?? [], waitJson, stateJson, lastStep,
            updatedAt,
          }));
        }
        await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
      }
      return result;
    },

    async setQueued({ flowId, expectedRevision, lastStep, blockingNodeId, stateJson, waitJson, statusReason }) {
      const result = await runStore.casSetQueued(flowId, {
        expectedRevision,
        lastStep,
        blockingNodeId: blockingNodeId ?? null,
        stateJson,
        waitJson,
        statusReason,
      });
      if (result.applied) {
        notifyChange(flowId);
        // Release the flow's own admission — but ONLY when this queuing is
        // for a DIFFERENT resource (e.g. a cross-process lock) than the
        // pool slot itself. When waitJson.kind is "flow-start" or
        // "child-dispatch" the flow holds nothing yet: releaseByFlow would
        // delete the very row admit() just created (releaseRowById deletes
        // on any live status, not just 'admitted'), permanently orphaning
        // it.
        if (waitJson?.kind !== "flow-start" && waitJson?.kind !== "child-dispatch") {
          await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
        }
      }
      return result;
    },

    async finish({ flowId, expectedRevision, stateJson }) {
      const result = await runStore.casFinish(flowId, { expectedRevision, stateJson });
      if (result.applied) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.terminal", { flowId, status: "succeeded", stateJson }));
        triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow: store.fail, failureStore, admissionQueueStore, api, logger });
        await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
      }
      return result;
    },

    async skip({ flowId, expectedRevision, stateJson, reason }) {
      const result = await runStore.casSkip(flowId, { expectedRevision, stateJson, reason });
      if (result.applied) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.terminal", { flowId, status: "skipped", stateJson }));
        triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow: store.fail, failureStore, admissionQueueStore, api, logger });
        await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
      }
      return result;
    },

    async fail({ flowId, expectedRevision, stateJson, statusReason }) {
      const result = await runStore.casFail(flowId, { expectedRevision, stateJson, statusReason });
      if (result.applied) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.terminal", { flowId, status: "failed", stateJson }));
        triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow: store.fail, failureStore, admissionQueueStore, api, logger });
        await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
      }
      return result;
    },

    // Forces a terminal flow back to 'blocked' so a subsequent setRunning
    // (whose CAS excludes terminal statuses) will accept it. Goes through
    // this DI seam rather than a raw write so tests observe it via the
    // same store double as every other transition.
    async forceBlock({ flowId, expectedRevision, stateJson, statusReason, ownerKey }) {
      const result = await runStore.forceBlock(flowId, { expectedRevision, stateJson, statusReason, ownerKey });
      if (result.applied) notifyChange(flowId);
      return result;
    },

    async cancel({ flowId, stateJson, reason = null }) {
      const result = await runStore.cancelWithStateJson(flowId, stateJson, reason);
      if (result.cancelled) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.terminal", { flowId, status: "cancelled", stateJson }));
        triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow: store.fail, failureStore, admissionQueueStore, api, logger });
        await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
      }
      return result;
    },

    async requestCancel({ flowId }) {
      const current = await runStore.get(flowId);
      if (!current) return { applied: false, code: "not_found" };
      await runStore.stampCancelRequested(flowId);
      return { applied: true };
    },
  };

  return store;
}
