// stats.js — aggregations over flow_runs.

import { getSubflowGroup } from "../state/subflow-state.js";

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
// "waiting" is never written anywhere: gate pauses persist as "blocked".
const ACTIVE = new Set(["running", "blocked"]);

/**
 * `lastStep` is the last node that COMPLETED (written after node.run()
 * resolves), so on a non-succeeded run it points at the prior node, not the
 * one that threw or is running — resume depends on that. Failed runs prefer
 * the last `runErrors` entry instead, because subflow-internal failures
 * (cascade targets, timeouts) never update `lastStep`. Presentation-only:
 * the executor reads the raw registry value.
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

  // Failed and cancelled runs are excluded: their attrition lives in the
  // failure histogram.
  const funnelDef = Array.isArray(pipeline?.funnel) && pipeline.funnel.length > 0
    ? pipeline.funnel
    : autoFunnelFromNodes(pipeline, visitedByFlow, lineageByNodeId);
  const funnel = funnelDef && funnelDef.length > 0
    ? await computeFunnel(mine.filter((r) => r.status === "succeeded" || ACTIVE.has(r.status)), funnelDef, runStore, nodeExecutionsStore)
    : null;

  const recent = [...mine].sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0)).slice(0, 30);

  // updatedAt, not createdAt: a flow started before the window but still
  // active must appear.
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

function stepImpactHistogram(failedRuns, { windowDays = 14 } = {}, errorsByFlow = null) {
  const acc = new Map();
  const now = Date.now();
  const dayMs = 86_400_000;
  const windowStart = startOfDay(now - (windowDays - 1) * dayMs);

  for (const r of failedRuns) {
    const step = r.lastStep ?? "?";
    const pipelineId = r.stateJson?.pipelineId ?? "?";
    const { signature, raw } = extractError(r, step, errorsByFlow?.get(r.flowId) ?? []);
    const sig = signature ?? "(no error message)";
    const clusterKey = `${pipelineId}::${step}`;
    if (!acc.has(clusterKey)) {
      acc.set(clusterKey, {
        pipelineId, step,
        count: 0, blockMsSamples: [], lastFailedAt: 0,
        perDay: new Map(),
        errors: new Map(),
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

  const dayAxis = [];
  for (let i = windowDays - 1; i >= 0; i--) {
    dayAxis.push(new Date(startOfDay(now - i * dayMs)).toISOString().slice(0, 10));
  }

  const out = [...acc.values()].map((e) => {
    const p50BlockMs = percentile(e.blockMsSamples, 0.5);
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
  out.sort((a, b) => (b.impactMs - a.impactMs) || (b.count - a.count));
  return out;
}

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
  // UTC midnight, not local: cost.js's dayKey and every downstream
  // .toISOString().slice(0, 10) day key are UTC, so local buckets would
  // misalign with them.
  const d = new Date(ms);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

// `visitedByFlow` (Map<flowId, Set<nodeId>>) may be absent — every stage then
// reports 0 rather than throwing. A cascade sub-node needs no collapsing: it
// lives inside its parent subflow's child flow, and `visitedByFlow` records
// the outer node id regardless of which sub-node resolved it.
function autoFunnelFromNodes(pipeline, visitedByFlow = null, lineageByNodeId = null) {
  if (!pipeline) return null;
  // `nodes` is an object keyed by id on the node side, an array once the
  // pipeline is serialized for the dashboard.
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
    // Rename lineage: a run's old invocation id still counts toward this
    // stage (pipeline-registry.js getNodeLineageBatch). Exact match only when
    // the caller passes no lineage map.
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
            // Executor writes a branch's return value into data on completion,
            // so a key's presence there means the sub-step ran.
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
  const stages = def.map((stage) => ({ ...stage, samples: [] }));
  for (const run of succeededRuns) {
    const state = run.stateJson ?? {};
    for (const stage of stages) {
      let size = 0;
      if (typeof stage.size === "function") {
        // The extra args serve auto-funnel's subflow sub-stages; a
        // pipeline-declared size(state) ignores them.
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
  // Backward pass: reaching stage N implies passing every earlier stage even
  // when no invocation was recorded (a shortcut node that did no work still
  // "visited"). Visit samples bump 0→1; count samples take the max of the
  // pair, since each downstream subflow implies an upstream visit.
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
    subStages: Array.isArray(s.subStages) && s.subStages.length > 0
      ? await computeFunnel(succeededRuns, s.subStages)
      : undefined,
    p95: percentile(s.samples, 0.95),
    min: s.samples.length > 0 ? Math.min(...s.samples) : null,
    mean: mean(s.samples),
  })));
  for (let i = 1; i < out.length; i++) {
    const prev = out[i - 1].total;
    out[i].conversionFromPrev = prev > 0 ? out[i].total / prev : null;
  }
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

export function computeNodeStats(runs, pipelineId, nodeId, errorsByFlow = null, visitedByFlow = null, lineageByNodeId = null) {
  runs = withResolvedSteps(runs, errorsByFlow);
  const mine = runs.filter((r) => (r.stateJson?.pipelineId ?? "") === pipelineId);
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
