// events/ws-run-events.js — gateway WS projection of flow writes.
//
// Subscribes to flowStateEmitter ("change", bare flowId) and emits the
// canonical run/gate/stats kinds through the shared emitter. Revision is
// the per-write granularity (status alone would drop every same-status
// node transition); the narrow compat events additionally gate on real
// status change. Settle order matters: the terminal scan waits past the
// terminal write; the start scan waits longer, past the new run row.
import { buildRunPatch as _buildRunPatch } from "../utils/run-patch.js";

export function createWsRunEventsListener({ store, readStore, nodeExecutionsStore, pipelineLookup, getBroadcast, emit }) {
  const knownRevisions = new Map();
  const knownStatuses = new Map();

  // Builds a RunSummary-shaped patch from the in-memory flow object.
  // The wrapper binds the pipeline lookup; the pure builder lives in
  // utils/run-patch.js.
  const buildRunPatch = (flow, now) =>
    _buildRunPatch(flow, now, { pipelineLookup });

  // Scans readStore for a single pipeline's aggregate stats. Called in a
  // setImmediate after a terminal transition so the executor has finished
  // writing the run row to the DB before the scan starts. Mirrors the
  // bucketing logic in routes/pipelines.js (createdAt window, local TZ).
  const computePipelineStats = async (pipelineId) => {
    const now = Date.now();
    const d = new Date(now);
    const todayStartMs = new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
    const sevenDaysMs = now - 7 * 86_400_000;
    const s = await readStore.statsFor(pipelineId, { todayStartMs, sevenDaysMs });
    return { id: pipelineId, ...s };
  };

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
      // Secondary check, for the narrow backward-compat events only —
      // those are meant to fire once per real status change (an
      // operator doesn't want a fresh "run started" push for every
      // node), not once per revision.
      const statusChanged = status !== knownStatuses.get(flowId);
      knownRevisions.set(flowId, revision);
      knownStatuses.set(flowId, status);
      const rawBroadcast = getBroadcast();
      if (!rawBroadcast) return;
      const now = Date.now();
      // Gate pauses are always persisted as "blocked" — run-store.js's
      // setWaiting always writes status='blocked' despite the name.
      // "waiting" itself is never actually written anywhere (confirmed
      // repo-wide), so it's not checked here at all.
      const isWaiting = status === "blocked";
      // Terminal set is exactly what state/run-store.js persists —
      // succeeded, failed, cancelled, skipped. "finished" is written
      // nowhere, so it is not part of it.
      const isTerminal = status === "succeeded" || status === "failed"
        || status === "cancelled" || status === "skipped";
      // Backward-compat narrow events — kept so older iOS builds still
      // work. Gated on statusChanged, not on revision.
      if (statusChanged) {
        if (status === "running") {
          // Full buildRunPatch payload so a run's start carries the
          // same display metadata (pipelineDisplayName/pipelineEmoji/
          // currentNodeId) as every other patch; this event also
          // carries what live_activity.start/PipelineProgress carries.
          emit("run.started", { ...buildRunPatch(flow, now) }).catch(() => {});
          const pipelineId = flow.stateJson?.pipelineId;
          if (pipelineId) {
            // setTimeout instead of setImmediate — the executor writes the
            // new run row asynchronously; the delay lets it land before the
            // stats read, so computePipelineStats returns fresh todayActive.
            setTimeout(async () => {
              try { await emit("pipeline.stats.updated", { ...(await computePipelineStats(pipelineId)) }); } catch {}
            }, 100);
          }
        } else if (isWaiting) {
          if (flow.waitJson?.kind === "human-gate") {
            // Canonical gate.opened: identical wire string
            // (`plugin.gate.opened`); envelope adds kind/emittedAtMs.
            // Fire-and-forget — a dead broadcast must not fail the listener.
            emit("gate.opened", { flowId, updatedAt: now }).catch(() => {});
          }
        } else if (isTerminal) {
          // Full buildRunPatch payload so a just-finished run carries
          // real display metadata (and the real terminal status) instead
          // of nothing.
          emit("run.ended", { ...buildRunPatch(flow, now) }).catch(() => {});
          knownStatuses.delete(flowId);
          // A terminal flow won't get further revisions — drop it too
          // so these maps don't grow unbounded across flow_ids.
          knownRevisions.delete(flowId);
        }
      }
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
      // Pipeline aggregate on terminal. Stays tied to the status
      // transition (not every revision) — a terminal flow's stats
      // should recompute once. setImmediate lets the executor finish
      // writing the terminal row to the DB before the readStore scan
      // starts.
      if (statusChanged && isTerminal) {
        const pipelineId = flow.stateJson?.pipelineId;
        if (pipelineId) {
          setImmediate(async () => {
            try { await emit("pipeline.stats.updated", { ...(await computePipelineStats(pipelineId)) }); } catch {}
          });
        }
      }
    } catch {}
  };
}
