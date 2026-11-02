// events/ws-run-events.js — gateway WS projection of flow writes.
//
// Subscribes to flowStateEmitter ("change", bare flowId) and emits the
// canonical per-revision run patch through the shared emitter. Revision is
// the per-write granularity (status alone would drop every same-status
// node transition).
import { buildRunPatch as _buildRunPatch } from "../utils/run-patch.js";

export function createWsRunEventsListener({ store, nodeExecutionsStore, pipelineLookup, getBroadcast, emit }) {
  const knownRevisions = new Map();

  // Builds a RunSummary-shaped patch from the in-memory flow object.
  // The wrapper binds the pipeline lookup; the pure builder lives in
  // utils/run-patch.js.
  const buildRunPatch = (flow, now) =>
    _buildRunPatch(flow, now, { pipelineLookup });

  return async function wsRunEventsListener(flowId) {
    try {
      const flow = await store.get(flowId);
      if (!flow) return;
      // Primary gate: has this write already been processed. Keyed on
      // revision, not status — a flow's status stays "running" across
      // many internal node transitions (the common case: current_step
      // advances every node, status only changes at
      // start/gate/terminal), so a status-keyed gate would drop every
      // transition after the first and never reach the "rich patch"
      // broadcast below. revision (state/run-store.js,
      // CAS-incremented on every write) is the per-write granularity.
      const revision = flow.revision;
      const priorRevision = knownRevisions.get(flowId);
      if (priorRevision !== undefined && revision <= priorRevision) return;
      const status = flow.status;
      knownRevisions.set(flowId, revision);
      const rawBroadcast = getBroadcast();
      if (!rawBroadcast) return;
      const now = Date.now();
      // Rich patch — truly per-revision: subscribed clients upsert without
      // a re-fetch on every transition.
      const ownPatch = buildRunPatch(flow, now);
      emit("run.updated", { ...ownPatch }).catch(() => {});
      // If this is a child flow (spawned by a subflow node), also push a
      // subflow-aggregate patch to the parent so RunDetailView refreshes
      // the Branches section on every subflow state change, not just when
      // all siblings go terminal and the parent resumes. Per-revision,
      // like the rich patch above.
      if (flow.parentInvocationId) {
        try {
          const inv = await nodeExecutionsStore.getInvocation(flow.parentInvocationId);
          if (inv?.flowId) {
            const parentFlow = await store.get(inv.flowId);
            if (parentFlow) {
              const ps = parentFlow.status;
              const parentIsTerminal = ps === "succeeded" || ps === "failed"
                || ps === "cancelled" || ps === "skipped";
              if (!parentIsTerminal) {
                emit("run.updated", { ...buildRunPatch(parentFlow, now) }).catch(() => {});
              }
            }
          }
        } catch {}
      }
      // Terminal flows get no further revisions — drop the entry so the
      // map doesn't grow unbounded across flow_ids.
      if (status === "succeeded" || status === "failed" || status === "cancelled" || status === "skipped") {
        knownRevisions.delete(flowId);
      }
    } catch {}
  };
}
