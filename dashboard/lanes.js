// lanes.js — the admission pool as the dashboard shows it.
//
// Every running flow holds exactly one lane; blocked/terminal flows release
// theirs. So a lane held by a non-running flow is a leak, and a lane held by
// a running flow whose row hasn't moved is a dead dispatch (only its own
// timeout eventually frees it). Both are surfaced here rather than inferred
// from a shrinking free count.

/** How long a running flow's row may sit un-updated before its lane is called
 *  suspect — far above any real node duration (dispatches legitimately run
 *  minutes); the point is to catch a lane only a timeout will free, not to
 *  report slow work. */
export const SUSPECT_STALE_MS = 15 * 60_000;

export function buildLanesSnapshot({ limit, queued, admitted, runsByFlow, now, staleMs = SUSPECT_STALE_MS }) {
  const held = admitted.length;
  const lanes = admitted.map((a) => {
    const run = runsByFlow.get(a.flowId) ?? null;
    const ageMs = a.admittedAt != null ? Math.max(0, now - a.admittedAt) : null;
    const idleMs = run?.updatedAt != null ? Math.max(0, now - run.updatedAt) : null;
    let holder = "ok";
    if (!run) holder = "orphan";                                   // row gone: nothing will release it
    else if (run.status !== "running") holder = "orphan";           // terminal/blocked should have released
    else if (idleMs != null && idleMs > staleMs) holder = "suspect"; // dead dispatch holding the lane
    return {
      flowId: a.flowId,
      nodeId: a.nodeId ?? null,
      kind: a.kind,
      pipelineId: run?.pipelineId ?? null,
      lastStep: run?.lastStep ?? null,
      flowStatus: run?.status ?? null,
      admittedAt: a.admittedAt,
      ageMs,
      idleMs,
      holder,
    };
  });
  const count = (name) => lanes.filter((l) => l.holder === name).length;
  return {
    limit,
    held,
    queued,
    free: Math.max(0, limit - held),
    ok: count("ok"),
    suspect: count("suspect"),
    orphan: count("orphan"),
    staleMs,
    lanes,
  };
}
