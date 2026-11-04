// state-store.js — thin wrapper over run-store for manifold flow state.
//
// All reads and CAS writes go directly through runStore (runtime.sqlite).
// No dual-write; registry.sqlite is never touched.
//
// After each successful write, flowStateEmitter fires so trigger-based
// consumers (event-log, live views) wake immediately instead of polling.

import { randomUUID } from "node:crypto";
import { flowStateEmitter, gateEmitter, flowEmitter } from "../events.js";
import { maybeResumeParent } from "../engine/child-resume-trigger.js";
import { wakePromotedRelease } from "../engine/admission-promotion-resume.js";

function notifyChange(flowId) {
  if (!flowId) return;
  process.nextTick(() => flowStateEmitter.emit("change", flowId));
}

// After a terminal CAS write, check whether this flow was a subflow-spawned
// child and, if every sibling is now also terminal, wake the parent (or
// fail it, if its subflow node's `mode` isn't satisfied — see child-resume-
// trigger.js's maybeResumeParent). Fire-and-forget — never blocks the
// caller's terminal-transition return. No-ops cleanly until
// resumeParentRef.resume is set (see index.js wiring) — currently dormant,
// since no real flow sets parentInvocationId yet. `failFlow` is `store.fail`
// itself — passed
// in by the caller (this module's own store methods, below) rather than
// referenced directly, since `triggerParentResume` is a free function
// outside the `store` object literal's closure. `admissionQueueStore` is
// threaded through to maybeResumeParent's own flipToRunningAndResume call —
// the parent's own admission was released when it entered its
// children-pending wait (this file's own setWaiting hook, below), so
// resuming it must re-request one, not assume it's still held.
function triggerParentResume(flowId, { nodeExecutionsStore, runStore, resumeParentRef, pipelinesRef, failFlow, failureStore, admissionQueueStore, api, logger }) {
  if (!nodeExecutionsStore || !resumeParentRef?.resume) return;
  maybeResumeParent({ flowId, runStore, nodeExecutionsStore, pipelinesRef, resume: resumeParentRef.resume, failFlow, failureStore, admissionQueueStore, api, logger })
    .catch((err) => logger?.warn?.(`state-store: triggerParentResume(${flowId}) failed: ${err?.message ?? err}`));
}

// The OTHER thing that needs to run on every setWaiting (blocked) and every
// terminal transition below: releasing whatever live admission this flow
// currently holds in the shared combined pool (state/admission-queue-
// store.js) — kind:"flow-start" for a root flow, kind:"child-dispatch" for
// a subflow child (a flow can only ever hold one of the two, permanently,
// so trying both kinds is cheap and safe: releaseByFlow is a one-SELECT
// no-op for whichever kind doesn't apply). Per the pool's own design (see
// admission-queue-store.js's header), a flow only occupies a slot while
// genuinely "running"; leaving running for ANY other reason (blocked or
// terminal) must give it back — and if that release promotes a queued
// flow/child, wake it via wakePromotedRelease (engine/admission-promotion-
// resume.js).
//
// Awaited by every call site below (async, not fire-and-forget): firing it
// detached lets a caller that returns/exits right after
// setWaiting/finish/skip/fail/cancel (a short-lived CLI command, or
// the gateway process dying in that exact window) race ahead of the
// release and the promoted flow's flip to 'running', leaving it permanently
// 'admitted' with nothing having ever driven it to 'running'. Only that
// durable part is awaited — the promoted flow's run itself starts on its
// own tick (see engine/admission-promotion-resume.js's header for why).
// Awaiting here doesn't make the DB promotion and the
// resume literally one transaction (still two separate writes; a genuine
// process crash between them is what boot/flow-revival.js's reconciliation
// sweep exists to catch), but it does close the far more common
// short-lived-caller race at its source, for every one of the 5 terminal/
// blocked transitions below.
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

// _session kept in signature for call-site compatibility, used as
// ownerKey on new flows.
//
// nodeExecutionsStore + resumeParentRef support subflow-parent resumption
// (see child-resume-trigger.js) — resumeParentRef is a mutable box because
// createStateStore runs before createExecutor exists (index.js's
// composition order), so `resume` is set onto the box after the fact.
// pipelinesRef is read-only here, used only to look up a subflow node's
// `mode` when deciding whether to resume or fail a parent (see child-
// resume-trigger.js's maybeResumeParent). failureStore is optional,
// threaded through to maybeResumeParent so a mode-not-satisfied failure
// gets a real failure_events row instead of a bare status write.
export function createStateStore(api, _session, { runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger } = {}) {
  const store = {
    /**
     * Create a managed flow_run for a fresh pipeline execution.
     * Returns the new flow record (status='running', lastStep='__start__').
     *
     * parentInvocationId links a subflow-spawned child to the
     * flow_node_invocations row that spawned it (branch/flow
     * unification) — undefined/null for ordinary top-level flows.
     */
    async create({ pipelineId, initialState, parentInvocationId }) {
      const flowId = randomUUID();
      const now = Date.now();
      // pipelineId spread LAST — deliberately: initialState is caller-
      // supplied and must never be able to override which pipeline this
      // flow runs as.
      const stateJson = { ...initialState, pipelineId };
      await runStore.insert({
        flowId,
        ownerKey:     _session ?? "",
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

    /**
     * Persist a "still running, advance to next step" transition.
     */
    async setRunning({ flowId, expectedRevision, lastStep, stateJson }) {
      const result = await runStore.casSetRunning(flowId, { expectedRevision, lastStep, stateJson });
      if (result.applied) {
        notifyChange(flowId);
        process.nextTick(() => flowEmitter.emit("flow.step.changed", { flowId, lastStep, stateJson, status: "running" }));
      }
      return result;
    },

    /**
     * Persist a "block on external signal" transition (human-gate).
     */
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

    /**
     * Persist a "block on an internal resource" transition (an admission
     * slot, a cross-process lock) — same shape as setWaiting.
     */
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
        // Release this flow's own admission (flow-start/child-dispatch),
        // same as setWaiting above — but ONLY when this queuing is for a
        // DIFFERENT internal resource (a cross-process lock) than the pool
        // slot itself. setQueued is also called when a
        // flow's OWN flow-start/child-dispatch admission request is what's
        // queuing (executor.js's start(), executor-subflow-children.js's
        // per-child dispatch) — at that point the flow holds nothing yet,
        // so releaseByFlow would delete the very row admit() just created
        // moments ago (releaseRowById deletes unconditionally on status,
        // not just 'admitted' ones), permanently orphaning it. waitJson.kind
        // === "flow-start" or "child-dispatch" is exactly that
        // self-referential case; anything else (e.g. "resource-lock")
        // means the flow is already running and giving back a slot it
        // separately holds.
        if (waitJson?.kind !== "flow-start" && waitJson?.kind !== "child-dispatch") {
          await triggerAdmissionRelease(flowId, { admissionQueueStore, runStore, resumeParentRef, api, logger });
        }
      }
      return result;
    },

    /**
     * Terminal success.
     */
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

    /**
     * Terminal skip — pipeline ran but produced no output (nothing to do).
     * runtime.sqlite gets status='skipped'. `reason` is the node's own
     * _skipped.reason (see engine/executor-helpers.js's detectSkippedSentinel).
     */
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

    /**
     * Terminal failure.
     */
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

    /**
     * Force a terminal flow back to 'blocked' so a subsequent setRunning
     * (whose CAS excludes terminal statuses) will accept it. Used by
     * operator-retry.js's flow-level retry path — goes through this DI
     * seam (not a raw DatabaseSync write) so tests can observe/control it
     * via the same store double every other transition uses.
     */
    async forceBlock({ flowId, expectedRevision, stateJson, statusReason, ownerKey }) {
      const result = await runStore.forceBlock(flowId, { expectedRevision, stateJson, statusReason, ownerKey });
      if (result.applied) notifyChange(flowId);
      return result;
    },

    /**
     * Terminal cancellation. Stamps stateJson and flips status atomically
     * (run-store.js's cancelWithStateJson — a single db.batch(), not two
     * sequential writes, so a crash between them can't leave state_json
     * updated with status still not 'cancelled'). Returns { cancelled: boolean }.
     */
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
