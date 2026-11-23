// lanes.js — the admission pool as the dashboard shows it.
//
// Every running flow holds exactly one lane (state/admission-queue-store.js);
// a flow that goes blocked or terminal releases it, so a lane still held by a
// flow that is not running is a leak, and a lane held by a running flow whose
// row has not moved for a long time is the signature of a dead agent dispatch
// (the dispatch's own timeout is the only thing that eventually frees it).
// Both are visible here rather than inferred from a shrinking free count.
//
// Pure: callers pass the admitted rows, the queued count and a flow lookup, so
// the attribution rules are testable without a database.

/**
 * How long a running flow's row may sit un-updated before its lane is called
 * suspect. Agent dispatches legitimately run for minutes (a draft turn), so
 * this is deliberately far above any normal node duration; the point is to
 * catch a lane that will only be freed by a timeout, not to report slow work.
 */
export const SUSPECT_STALE_MS = 15 * 60_000;

/**
 * @param {object} args
 * @param {number} args.limit          pool size (global concurrency limit)
 * @param {number} args.queued         entries waiting for a lane
 * @param {Array<{flowId: string, nodeId?: string|null, kind: string, admittedAt: number|null}>} args.admitted
 * @param {Map<string, object>} args.runsByFlow  flowId → { status, lastStep, pipelineId, updatedAt }
 * @param {number} args.now            current epoch ms
 * @param {number} [args.staleMs]
 */
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
