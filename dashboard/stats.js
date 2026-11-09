// stats.js — aggregations over flow_runs.
//
// All computations are pure functions taking the array of rows returned by
// store.list(). At ~60 rows this runs in well under a millisecond, so the
// endpoint recomputes on every request — no caching needed.

import { getSubflowGroup } from "../state/subflow-state.js";
//
// Funnel logic (computePipelineStats):
//   1. If the pipeline declares `funnel: [...]` in its index.js, use it.
//      Stage shape:
//        { id, label, key, size: "array" | "objectKeys" | "objectKeysNoError" }
//        { id, label, size: (state) => number }   // subflow-derived
//      The function form lets in-flight progress show up in the funnel
//      before a terminal step rolls per-subflow state into top-level slots.
//   2. Otherwise, auto-derive a node-visit funnel from pipeline.nodes
//      in declaration order — each stage counts how many runs reached
//      that node, per a batched visitedByFlow map
//      (flow_node_invocations). Generic, agnostic
//      to pipeline content; the cost breakdown carries the same per-node
//      calls with cost/latency, so the auto funnel adds the per-stage
//      attrition story for pipelines without a custom declaration.

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
// "waiting" is never actually written anywhere (confirmed repo-wide) — gate
// pauses are always persisted as "blocked".
const ACTIVE = new Set(["running", "blocked"]);

/**
 * Resolve the "display step" for a run — the node the run was actually
 * at when it terminated / is currently at.
 *
 * The registry's `last_step` field (exposed here as `run.lastStep`) is
 * the last node that successfully completed (executor.js's
 * persistRunningRetrying writes it AFTER node.run() resolves). So for
 * any non-succeeded run it points to the PRIOR node, not the
 * throwing/running/blocking one — the executor relies on this
 * invariant for resume (it walks `edges[lastStep]` to find the next
 * node on unblock). This function only reads `lastStep`; the node currently
 * in flight is not a flow_runs column — it lives in flow_node_invocations
 * (findCurrentStep) and waitJson.nodeId — and isn't consulted here.
 *
 * For failed runs, use the LAST entry in `runErrors` (sourced from
 * flow_node_executions) — the executor records the throw site there.
 * Handles cascade sub-nodes and other
 * subflow-internal failures the registry's lastStep can't see (e.g.
 * ENSURE_LOGIN_CODE fails, the cascade routes to ENSURE_LOGIN_AGENT,
 * that times out — the error entry has `nodeId: "ENSURE_LOGIN_AGENT"`
 * even though `lastStep` registry value is still `ENSURE_LOGIN_CODE`).
 *
 * Falls back to the registry value for every other case (succeeded /
 * cancelled / blocked / running, or a failed run with no runErrors
 * entry) — same as any client's own RunsAPI, which reads currentStep/lastStep
 * directly off the run row rather than walking a pipeline's edge
 * function against stateJson (dashboard/server.js has no direct DB
 * access to state that would make that walk correct here either —
 * state._subflowGroups is dispatch-scoped, never persisted).
 *
 * Presentation-only — the executor still reads the raw registry value.
 */
export function resolveDisplayStep(run, runErrors = []) {
  if (!run) return null;
  if (run.status === "succeeded") return run.lastStep ?? null;

  if (run.status === "failed" && runErrors.length > 0) {
    const last = runErrors[runErrors.length - 1];
    if (last?.nodeId) return last.nodeId;
  }

  return run.lastStep ?? null;
}

// Shallow-copy runs with `lastStep` replaced by the resolved value
// so downstream aggregations see the corrected semantics without each
// site having to call the resolver itself. errorsByFlow is the
// Map<flowId, ErrorEntry[]> callers fetch once via the gateway's
// GET /manifold/errors (failureStore.queryErrorsByFlowIds) —
// this function stays DB-free, just threading the lookup through.
function withResolvedSteps(runs, errorsByFlow = null) {
  return runs.map((r) => {
    const resolved = resolveDisplayStep(r, errorsByFlow?.get(r.flowId) ?? []);
    if (resolved === r.lastStep) return r;
    return { ...r, lastStep: resolved };
  });
}

export function computeGlobalStats(runs, errorsByFlow = null) {
  runs = withResolvedSteps(runs, errorsByFlow);
  const counts = countByStatus(runs);
  const total = runs.length;
  const succeeded = counts.succeeded ?? 0;
  const failed = counts.failed ?? 0;
  const cancelled = counts.cancelled ?? 0;
  const active = (counts.running ?? 0) + (counts.waiting ?? 0) + (counts.blocked ?? 0);
  const terminated = succeeded + failed + cancelled;
  const successRate = terminated > 0 ? succeeded / terminated : null;

  const succeededRuns = runs.filter((r) => r.status === "succeeded");
  const durations = succeededRuns
    .map((r) => durationMs(r))
    .filter((d) => d != null && d >= 0);

  // Per-day count over the last 30 days, only days that actually had
  // activity. Matches the cost section's window (30d) and the cost
  // chart's behaviour (gaps not rendered as empty bars — the chart
  // expands to fill available width across the active days only).
  const now = Date.now();
  const dayMs = 86_400_000;
  const buckets = [];
  for (let i = 29; i >= 0; i--) {
    const start = startOfDay(now - i * dayMs);
    const end = start + dayMs;
    const day = runs.filter((r) => (r.createdAt ?? 0) >= start && (r.createdAt ?? 0) < end);
    if (day.length === 0) continue;
    buckets.push({
      day: new Date(start).toISOString().slice(0, 10),
      total: day.length,
      succeeded: day.filter((r) => r.status === "succeeded").length,
      failed:    day.filter((r) => r.status === "failed").length,
      cancelled: day.filter((r) => r.status === "cancelled").length,
    });
  }

  // Failure-step histogram (where do failed/cancelled runs end?). Each
  // entry tracks p50 block-time alongside the count so the operator can
  // sort by total impact, not just by frequency.
  const failureStops = stepImpactHistogram(
    runs.filter((r) => r.status === "failed" || r.status === "cancelled"), {}, errorsByFlow,
  );

  return {
    total,
    statusCounts: counts,
    succeeded, failed, cancelled, active, terminated,
    successRate,
    duration: {
      n: durations.length,
      p50Ms: percentile(durations, 0.5),
      p95Ms: percentile(durations, 0.95),
      meanMs: mean(durations),
    },
    perDay: buckets,
    failureStops,
    pipelineBreakdown: pipelineBreakdown(runs),
  };
}

export async function computePipelineStats(runs, pipelineId, pipeline = null, errorsByFlow = null, runStore = null, nodeExecutionsStore = null, visitedByFlow = null, lineageByNodeId = null) {
  runs = withResolvedSteps(runs, errorsByFlow);
  const mine = runs.filter((r) => (r.stateJson?.pipelineId ?? "") === pipelineId);
  const counts = countByStatus(mine);
  const total = mine.length;
  const succeeded = counts.succeeded ?? 0;
  const failed = counts.failed ?? 0;
  const cancelled = counts.cancelled ?? 0;
  const terminated = succeeded + failed + cancelled;
  const successRate = terminated > 0 ? succeeded / terminated : null;

  const durations = mine.filter((r) => r.status === "succeeded")
    .map(durationMs).filter((d) => d != null && d >= 0);

  const failureStops = stepImpactHistogram(
    mine.filter((r) => r.status === "failed" || r.status === "cancelled"), {}, errorsByFlow,
  );

  const revStats = mine.filter((r) => r.status === "succeeded").map((r) => r.revision ?? 0);

  // Funnel samples succeeded + active runs (running/waiting/blocked). Active
  // runs' branches contribute their in-progress counts; failed and cancelled
  // are excluded since their attrition is captured by the failure histogram.
  const funnelDef = Array.isArray(pipeline?.funnel) && pipeline.funnel.length > 0
    ? pipeline.funnel
    : autoFunnelFromNodes(pipeline, visitedByFlow, lineageByNodeId);
  const funnel = funnelDef && funnelDef.length > 0
    ? await computeFunnel(mine.filter((r) => r.status === "succeeded" || ACTIVE.has(r.status)), funnelDef, runStore, nodeExecutionsStore)
    : null;

  // Recent runs ordered by createdAt desc (for sparkline / table on detail page).
  const recent = [...mine].sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0)).slice(0, 30);

  // Last-24h rollup for the per-pipeline TodayStrip — same window /
  // status breakdown as the home overview, just scoped to this
  // pipeline. updatedAt is the canonical "last touched" timestamp
  // (createdAt would miss a long-running flow that started before
  // the window but is still active).
  const since = Date.now() - 86_400_000;
  const last24h = mine.filter((r) => (r.updatedAt ?? 0) >= since);
  const recent24h = {
    total: last24h.length,
    ok: last24h.filter((r) => r.status === "succeeded").length,
    failed: last24h.filter((r) => r.status === "failed").length,
    active: last24h.filter((r) => r.status === "running" || r.status === "blocked").length,
  };

  return {
    pipelineId,
    recent24h,
    total,
    statusCounts: counts,
    succeeded, failed, cancelled, terminated,
    successRate,
    duration: {
      n: durations.length,
      p50Ms: percentile(durations, 0.5),
      p95Ms: percentile(durations, 0.95),
      meanMs: mean(durations),
    },
    revision: {
      n: revStats.length,
      p50: percentile(revStats, 0.5),
      p95: percentile(revStats, 0.95),
    },
    failureStops,
    funnel,
    recentRuns: recent.map(summariseForChart),
  };
}

export function compareRunToMedian(run, allRunsOfSamePipeline) {
  const peers = allRunsOfSamePipeline.filter((r) => r.flowId !== run.flowId && r.status === "succeeded");
  if (peers.length === 0) return null;
  const peerDurations = peers.map(durationMs).filter((d) => d != null && d >= 0);
  const medianMs = percentile(peerDurations, 0.5);
  const peerRevs = peers.map((r) => r.revision ?? 0);
  const medianRev = percentile(peerRevs, 0.5);
  return {
    peerCount: peers.length,
    durationMs: durationMs(run),
    durationMedianMs: medianMs,
    durationRatio: durationMs(run) != null && medianMs ? durationMs(run) / medianMs : null,
    revision: run.revision ?? 0,
    revisionMedian: medianRev,
  };
}

// ─── helpers ─────────────────────────────────────────────────────────────────

function countByStatus(runs) {
  const out = {};
  for (const r of runs) out[r.status] = (out[r.status] ?? 0) + 1;
  return out;
}

function pipelineBreakdown(runs) {
  const map = new Map();
  for (const r of runs) {
    const id = r.stateJson?.pipelineId ?? "?";
    if (!map.has(id)) map.set(id, { pipelineId: id, total: 0, succeeded: 0, failed: 0, cancelled: 0, active: 0 });
    const entry = map.get(id);
    entry.total++;
    if (r.status === "succeeded") entry.succeeded++;
    else if (r.status === "failed") entry.failed++;
    else if (r.status === "cancelled") entry.cancelled++;
    else if (ACTIVE.has(r.status)) entry.active++;
  }
  return [...map.values()].sort((a, b) => b.total - a.total);
}

function durationMs(r) {
  if (!r) return null;
  const end = r.endedAt ?? r.updatedAt ?? null;
  const start = r.createdAt ?? null;
  if (end == null || start == null) return null;
  return end - start;
}

/**
 * Failure histogram clustered by (step, normalized error signature).
 * Each row is one bug or one failure mode — two failures at SCAN_CODE
 * for "login broken" cluster together even when the underlying messages
 * differ in identifier noise; two SCAN_CODE failures for "DOM regression"
 * are a separate row. This converts the chart from a "which step" report
 * into a "which bug" triage queue.
 *
 * Each entry carries:
 *   - p50BlockMs: median wall time from run creation to terminal stop
 *   - impactMs: count × p50BlockMs (operator-hours-lost ranking)
 *   - lastFailedAt: timestamp of most recent failure in the cluster
 *   - perDay: failure count per day across a fixed N-day window (for
 *     the sparkline / recency read)
 *   - signature: the normalized error message excerpt
 *
 * Falls back to plain count-sort when no block-time signal exists yet.
 */
function stepImpactHistogram(failedRuns, { windowDays = 14 } = {}, errorsByFlow = null) {
  /** clusterKey ("STEP::SIG") → cluster acc */
  const acc = new Map();
  const now = Date.now();
  const dayMs = 86_400_000;
  const windowStart = startOfDay(now - (windowDays - 1) * dayMs);

  for (const r of failedRuns) {
    const step = r.lastStep ?? "?";
    const pipelineId = r.stateJson?.pipelineId ?? "?";
    const { signature, raw } = extractError(r, step, errorsByFlow?.get(r.flowId) ?? []);
    const sig = signature ?? "(no error message)";
    // Top-level cluster = (pipeline, step). Different errors at the
    // same step are tracked as SUB-clusters inside `errors` rather
    // than separate rows — keeps the overview compact and surfaces
    // the breakdown only on tap-to-expand.
    const clusterKey = `${pipelineId}::${step}`;
    if (!acc.has(clusterKey)) {
      acc.set(clusterKey, {
        pipelineId, step,
        count: 0, blockMsSamples: [], lastFailedAt: 0,
        perDay: new Map(),
        errors: new Map(), // signature → sub-cluster
      });
    }
    const e = acc.get(clusterKey);
    e.count++;
    const wallMs = durationMs(r);
    if (wallMs != null && wallMs >= 0) e.blockMsSamples.push(wallMs);
    const at = r.endedAt ?? r.updatedAt ?? 0;
    if (at > e.lastFailedAt) e.lastFailedAt = at;
    if (at >= windowStart) {
      const day = new Date(startOfDay(at)).toISOString().slice(0, 10);
      e.perDay.set(day, (e.perDay.get(day) ?? 0) + 1);
    }
    // Sub-cluster: tracks each distinct error signature within the
    // (pipeline, step) cluster. Exemplar message updates to the most
    // recent occurrence; recentRuns is sorted at rollup.
    if (!e.errors.has(sig)) {
      e.errors.set(sig, {
        signature: sig, count: 0, lastFailedAt: 0,
        exemplarMessage: null, recentRuns: [],
      });
    }
    const sub = e.errors.get(sig);
    sub.count++;
    if (at >= sub.lastFailedAt && raw) sub.exemplarMessage = raw;
    if (at > sub.lastFailedAt) sub.lastFailedAt = at;
    sub.recentRuns.push({ flowId: r.flowId, status: r.status, at });
  }

  // Build the fixed-width day axis up front so every cluster's
  // sparkline shares the same N-day canvas (gaps render as zeroes).
  const dayAxis = [];
  for (let i = windowDays - 1; i >= 0; i--) {
    dayAxis.push(new Date(startOfDay(now - i * dayMs)).toISOString().slice(0, 10));
  }

  const out = [...acc.values()].map((e) => {
    const p50BlockMs = percentile(e.blockMsSamples, 0.5);
    // Sort sub-clusters by recency desc — the freshest error first
    // when the operator expands. Each carries its own top-5 most
    // recent flow ids for one-tap deep-link.
    const errors = [...e.errors.values()]
      .sort((a, b) => (b.lastFailedAt ?? 0) - (a.lastFailedAt ?? 0))
      .map((sub) => ({
        signature: sub.signature,
        count: sub.count,
        lastFailedAt: sub.lastFailedAt || null,
        exemplarMessage: sub.exemplarMessage,
        recentRuns: sub.recentRuns
          .sort((a, b) => (b.at ?? 0) - (a.at ?? 0))
          .slice(0, 5),
      }));
    return {
      key: e.step,
      pipelineId: e.pipelineId,
      count: e.count,
      p50BlockMs,
      impactMs: e.count * (p50BlockMs ?? 0),
      lastFailedAt: e.lastFailedAt || null,
      perDay: dayAxis.map((day) => ({ day, count: e.perDay.get(day) ?? 0 })),
      errors,
    };
  });
  // Primary sort: impact desc. Secondary: count desc.
  out.sort((a, b) => (b.impactMs - a.impactMs) || (b.count - a.count));
  return out;
}

/**
 * Extract two views of the failure's error message: a normalized
 * clustering-friendly signature (UUIDs / numeric runs replaced), and
 * the raw untruncated text for the expanded detail block. Prefers
 * the error entry matching the failing step; falls back to the last
 * error in the array. runErrors is the run's ErrorEntry[] from
 * flow_node_executions (sourced by the caller via queryErrorsByFlow/
 * queryErrorsByFlowIds).
 */
function extractError(run, step, runErrors = []) {
  const stepErr = runErrors.find((e) => e?.nodeId === step);
  const lastErr = runErrors[runErrors.length - 1];
  const raw = (stepErr ?? lastErr)?.message;
  if (!raw || typeof raw !== "string") return { signature: null, raw: null };
  const signature = raw
    .replace(/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/gi, "<uuid>")
    .replace(/\b\d{2,}\b/g, "<n>")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
  return { signature, raw };
}

function percentile(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  // Linear interpolation between the two nearest ranks.
  const idx = (s.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return s[lo];
  return s[lo] + (s[hi] - s[lo]) * (idx - lo);
}

function mean(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function startOfDay(ms) {
  // UTC-anchored midnight — matches cost.js's dayKey convention so
  // every per-day bucket on the dashboard (cost, runs/day, failure
  // sparkline) shares the same time boundaries. Local-midnight would
  // misalign with the UTC-formatted day keys used downstream (e.g.
  // .toISOString().slice(0, 10) reports the UTC date).
  const d = new Date(ms);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

/**
 * Default node-visit funnel for pipelines without a custom `funnel: [...]`
 * declaration. One stage per top-level node in pipeline declaration order
 * (start/end skipped); per-run "size" is 1 when the node was ever visited,
 * per `visitedByFlow` — a Map<flowId, Set<nodeId>> the caller fetches in
 * one batched round-trip (GET /manifold/tree-summary, backed by
 * flow_node_invocations) rather than this function making one DB/HTTP
 * call per (run, stage) itself.
 * `visitedByFlow` may be null/undefined (caller couldn't fetch it, e.g. no
 * nodeExecutionsStore reachable) — every stage then reports 0, same as an
 * unvisited node, rather than throwing. Counts how many runs reached each
 * node — the attrition story without the pipeline having to declare
 * anything.
 *
 * A cascade sub-node (a subflow's own flow.nodes entry) lives inside its
 * parent subflow node's own child flow, not as a separate top-level node,
 * so it needs no special collapsing here — `visitedByFlow` records the
 * outer node id regardless of which cascade sub-node resolved it, same as
 * any other node.
 */
function autoFunnelFromNodes(pipeline, visitedByFlow = null, lineageByNodeId = null) {
  if (!pipeline) return null;
  // Node-side pipelinesRef stores `nodes` as an object keyed by id;
  // the dashboard's serialized pipeline ships it as an array. Handle
  // both shapes so this helper works in either context.
  const nodes = Array.isArray(pipeline.nodes)
    ? pipeline.nodes
    : (pipeline.nodes && typeof pipeline.nodes === "object")
      ? Object.entries(pipeline.nodes).map(([id, n]) => ({ ...n, id }))
      : null;
  if (!nodes) return null;
  const out = [];
  for (const n of nodes) {
    if (!n?.id || n.id === "__start__" || n.id === "__end__") continue;
    const nodeId = n.id;
    // lineageByNodeId lets an older run's invocation, recorded under
    // whatever node id was live when the node was later renamed, still
    // count toward THIS (current) stage — see pipeline-registry.js's
    // getNodeLineageBatch. Falls back to exact-match-only when the caller
    // doesn't supply it (e.g. tests, or a pipeline never touched by a
    // declared rename).
    const stage = {
      id: nodeId,
      label: nodeId,
      size: (state, flowId) => {
        const ids = lineageByNodeId?.get(nodeId) ?? new Set([nodeId]);
        const visited = visitedByFlow?.get(flowId);
        if (!visited) return 0;
        for (const id of ids) if (visited.has(id)) return 1;
        return 0;
      },
    };
    // subflow stages get a sub-funnel definition derived from
    // the subflow's nodes, in declaration order. Per-run sub-stage
    // size = number of branches that visited this sub-step (read off
    // each subflow child's own data/step via getSubflowGroup below).
    // This is the per-subflow attrition story — "of 7610 branches
    // probed, only 6 had updates worth persisting" — that the top-level
    // "POLL: visited" can't show.
    if (n.kind === "subflow" && n.flow) {
      const groupId = n.id;
      const subNodes = Array.isArray(n.flow?.nodes)
        ? n.flow.nodes
        : (n.flow?.nodes && typeof n.flow.nodes === "object")
          ? Object.entries(n.flow.nodes).map(([id, sn]) => ({ ...sn, id }))
          : [];
      const subStageDefs = subNodes
        .filter((sn) => sn?.id && sn.id !== "__start__" && sn.id !== "__end__")
        .map((sn) => ({
          id: `${groupId}.${sn.id}`,
          label: sn.id,
          size: async (state, flowId, runStore, nodeExecutionsStore) => {
            // A sub-step counts as "visited" when its key exists in branch.data
            // (executor writes return value there on successful completion) OR
            // when the branch's current `step` pointer is parked at it.
            const branches = await getSubflowGroup({ flowId, groupId, runStore, nodeExecutionsStore });
            if (!branches || typeof branches !== "object") return 0;
            let count = 0;
            for (const branch of Object.values(branches)) {
              if (!branch || typeof branch !== "object") continue;
              const data = branch.data;
              if (data && typeof data === "object" && sn.id in data) { count++; continue; }
              if (branch.step === sn.id) count++;
            }
            return count;
          },
        }));
      if (subStageDefs.length > 0) stage.subStages = subStageDefs;
    }
    out.push(stage);
  }
  return out;
}

async function computeFunnel(succeededRuns, def, runStore = null, nodeExecutionsStore = null) {
  // Per stage: list of measured sizes (one per run), then derive p50, sum, conversion.
  const stages = def.map((stage) => ({ ...stage, samples: [] }));
  for (const run of succeededRuns) {
    const state = run.stateJson ?? {};
    for (const stage of stages) {
      let size = 0;
      if (typeof stage.size === "function") {
        // Subflow-derived (or any other custom computation). Function returns
        // the per-run count for this stage. flowId/runStore/
        // nodeExecutionsStore are extra args auto-funnel's subflow sub-stages
        // use to read subflow-group data (see state/subflow-state.js);
        // pipeline-declared custom size(state) functions just ignore them.
        try { size = Number(await stage.size(state, run.flowId, runStore, nodeExecutionsStore)) || 0; } catch { size = 0; }
      } else {
        const v = state[stage.key];
        if (stage.size === "array") size = Array.isArray(v) ? v.length : 0;
        else if (stage.size === "objectKeys") size = (v && typeof v === "object" && !Array.isArray(v)) ? Object.keys(v).length : 0;
        else if (stage.size === "objectKeysNoError") {
          size = (v && typeof v === "object" && !Array.isArray(v))
            ? Object.values(v).filter((d) => d && !d._error).length
            : 0;
        }
      }
      stage.samples.push(size);
    }
  }
  // Backward pass: a run/subflow that reached stage N necessarily passed
  // every earlier stage, even when it never dispatched that node's own
  // invocation (e.g. ENSURE_LOGIN_* runs that shortcut when the session
  // is already valid — the "ensure login" stage PASSED, just with no
  // work to do). For visit-only samples (0/1) the bump is 0→1; for
  // subflow-count samples the prior stage takes the max of its own
  // count vs. the next stage's count, since each downstream subflow
  // implies an upstream visit.
  for (let i = stages.length - 2; i >= 0; i--) {
    for (let j = 0; j < stages[i].samples.length; j++) {
      const next = stages[i + 1].samples[j];
      if (next > stages[i].samples[j]) stages[i].samples[j] = next;
    }
  }
  const out = await Promise.all(stages.map(async (s) => ({
    id: s.id,
    label: s.label,
    key: s.key,
    n: s.samples.length,
    total: s.samples.reduce((a, b) => a + b, 0),
    p50: percentile(s.samples, 0.5),
    // Sub-funnel for subflow parents: recurse with the same
    // run set so each sub-stage gets its own per-subflow attrition
    // figures. The same backward-propagation kicks in inside the
    // recursive call.
    subStages: Array.isArray(s.subStages) && s.subStages.length > 0
      ? await computeFunnel(succeededRuns, s.subStages)
      : undefined,
    // p95: a stage's worst-realistic per-run yield. Pairs with min below
    // to catch "stage was healthy on average but bottomed out one day."
    p95: percentile(s.samples, 0.95),
    // min: surfaces the ran-dry days — a shortlist whose median was 5
    // but had two days at 0 has actually broken; the p50 line won't show it.
    min: s.samples.length > 0 ? Math.min(...s.samples) : null,
    mean: mean(s.samples),
  })));
  // Stage-to-stage conversion: total at stage i / total at stage i-1.
  for (let i = 1; i < out.length; i++) {
    const prev = out[i - 1].total;
    out[i].conversionFromPrev = prev > 0 ? out[i].total / prev : null;
  }
  // Overall: last / first.
  if (out.length > 1 && out[0].total > 0) {
    out[out.length - 1].conversionFromStart = out[out.length - 1].total / out[0].total;
  }
  return out;
}

function summariseForChart(r) {
  return {
    flowId: r.flowId,
    status: r.status,
    lastStep: r.lastStep,
    createdAt: r.createdAt,
    endedAt: r.endedAt,
    durationMs: durationMs(r),
    revision: r.revision,
  };
}

/**
 * Per-node aggregation across a pipeline's runs. Used by the graph-view
 * click panel to show:
 *   - visits: how many runs visited this node at any point, per
 *     `visitedByFlow` — a Map<flowId, Set<nodeId>> the caller fetches in
 *     one batched round-trip (GET /manifold/tree-summary), same as
 *     computePipelineStats' auto-funnel. May be null/undefined (caller
 *     couldn't fetch it) — visits then falls back to lastStep === nodeId
 *     only, same degrade-safe shape as everywhere else this map is
 *     threaded through.
 *   - failedHere / blockedHere: how many runs were terminal-failed /
 *     currently-blocked AT this node (lastStep === nodeId at terminal /
 *     non-terminal state respectively)
 *   - terminatedHere: failed + cancelled with lastStep === nodeId
 *   - recentRuns: the most-recent 10 runs that touched this node, sorted
 *     by updatedAt desc, with their final status + step
 *
 * A node id may be either top-level ("SCORE") or qualified
 * ("PROPOSE.DRAFT") for subflow children. Subflow-internal step
 * histories live in the branch store (not top-level state), and we don't
 * currently aggregate those — top-level visits is the source of truth for
 * v1.
 */
export function computeNodeStats(runs, pipelineId, nodeId, errorsByFlow = null, visitedByFlow = null, lineageByNodeId = null) {
  runs = withResolvedSteps(runs, errorsByFlow);
  const mine = runs.filter((r) => (r.stateJson?.pipelineId ?? "") === pipelineId);
  // Same lineage fallback as autoFunnelFromNodes — an older run recorded
  // under a since-renamed node id still counts as having visited nodeId.
  const lineageIds = lineageByNodeId?.get(nodeId) ?? new Set([nodeId]);
  let visits = 0;
  let failedHere = 0;
  let blockedHere = 0;
  let terminatedHere = 0;
  const touched = [];
  for (const r of mine) {
    const flowVisited = visitedByFlow?.get(r.flowId);
    const visited = (flowVisited && [...lineageIds].some((id) => flowVisited.has(id))) || r.lastStep === nodeId;
    if (!visited) continue;
    visits++;
    if (r.lastStep === nodeId) {
      if (r.status === "failed") { failedHere++; terminatedHere++; }
      else if (r.status === "cancelled") terminatedHere++;
      else if (r.status === "blocked") blockedHere++;
    }
    touched.push({
      flowId: r.flowId,
      status: r.status,
      lastStep: r.lastStep,
      updatedAt: r.updatedAt,
      revision: r.revision,
    });
  }
  touched.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
  return {
    pipelineId,
    nodeId,
    visits,
    failedHere,
    blockedHere,
    terminatedHere,
    recentRuns: touched.slice(0, 10),
    totalRuns: mine.length,
  };
}
