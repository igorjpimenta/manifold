// Per-run / per-pipeline cost aggregation backed by ops.sqlite::cost_by_call.
// The hook writer inserts one row per LLM call with full metadata (flow_id,
// node_id, item_key, model, tokens, cost_total) so all aggregations are
// single SQL queries rather than JSONL walks.
//
// Read-only, backed by state/db.js's shared getSharedDb() rather than a
// bespoke @libsql/client call — connections are cached per (dbPath, readonly)
// process-wide (see db.js) and reused across calls instead of opened+closed
// on every invocation, and shared with any other reader of the same file
// (e.g. state/node-cost.js's ops.sqlite reader) instead of each keeping its
// own redundant connection. The path mirrors telemetry/store.js's
// DEFAULT_DB_PATH so cost.js stays in sync with where telemetry writes — if
// telemetry ever moves its file, update both.

import { existsSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { getSharedDb } from "../state/db.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";

const OPS_DB_PATH = path.join(os.homedir(), ".openclaw", "ops.sqlite");

function getClient(dbPath) {
  return getSharedDb(dbPath, { readonly: true });
}

// costForRun — sum all cost_by_call rows for a given flow_id.
// opts: { sinceMs?, untilMs?, _dbPath? }
// _sessionIndex: legacy param, accepted and ignored.
//
// Returns:
//   { flowId, total, tokens, byModel, byNode, bySession }
//
// byModel: { [model]: { total, calls, inputTokens, outputTokens } }
// byNode:  { [nodeId]: { total, calls, items: { [itemKey]: { total, calls } } } }
// bySession: { [session]: { total, calls, inputTokens, outputTokens, model, agentId } }
export async function costForRun(flowId, { sinceMs = null, untilMs = null, _dbPath = null } = {}, _sessionIndex = null) {
  const result = {
    flowId,
    total: 0,
    tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    byModel: {},
    byNode: {},
    bySession: {},
  };

  const dbPath = _dbPath ?? OPS_DB_PATH;
  if (!existsSync(dbPath)) return result;

  const { client } = await getClient(dbPath);
  const conditions = ["flow_id = ?"];
  const args = [flowId];
  if (sinceMs != null) { conditions.push("ts >= ?"); args.push(sinceMs); }
  const effectiveUntil = untilMs ?? (sinceMs ? Date.now() : null);
  if (effectiveUntil != null) { conditions.push("ts <= ?"); args.push(effectiveUntil); }

  const where = conditions.join(" AND ");
  const { rows } = await client.execute({
    sql: `
      SELECT session_id, model, agent_id, node_id, item_key,
             input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
             cost_total
      FROM cost_by_call
      WHERE ${where}
    `,
    args,
  });

  for (const row of rows) {
    const cost = row.cost_total ?? 0;
    result.total += cost;
    result.tokens.input      += row.input_tokens       ?? 0;
    result.tokens.output     += row.output_tokens      ?? 0;
    result.tokens.cacheRead  += row.cache_read_tokens  ?? 0;
    result.tokens.cacheWrite += row.cache_write_tokens ?? 0;

    // byModel
    const modelKey = row.model ?? "unknown";
    const mAcc = result.byModel[modelKey] ?? { total: 0, calls: 0, inputTokens: 0, outputTokens: 0 };
    mAcc.total       += cost;
    mAcc.calls       += 1;
    mAcc.inputTokens  += row.input_tokens  ?? 0;
    mAcc.outputTokens += row.output_tokens ?? 0;
    result.byModel[modelKey] = mAcc;

    // byNode — lowercased: node_id casing has drifted across pipeline/executor
    // versions ("SCORE" vs "score"), and the dashboard's per-node lookups
    // (nodeKindMap, nodeGroupDefs, stepOrder) are all keyed lowercase.
    // Without normalizing here, case variants of the same node split into
    // separate rows instead of merging.
    const nodeId = (row.node_id ?? "unknown").toLowerCase();
    const nAcc = result.byNode[nodeId] ?? { total: 0, calls: 0, items: {} };
    nAcc.total += cost;
    nAcc.calls += 1;
    if (row.item_key) {
      const iAcc = nAcc.items[row.item_key] ?? { total: 0, calls: 0 };
      iAcc.total += cost;
      iAcc.calls += 1;
      nAcc.items[row.item_key] = iAcc;
    }
    result.byNode[nodeId] = nAcc;

    // bySession — keyed by the harness-written session_id column.
    const sid = row.session_id ?? "unknown";
    const sAcc = result.bySession[sid] ?? {
      total: 0, calls: 0, inputTokens: 0, outputTokens: 0,
      model: modelKey, agentId: row.agent_id ?? null,
    };
    sAcc.total        += cost;
    sAcc.calls        += 1;
    sAcc.inputTokens  += row.input_tokens  ?? 0;
    sAcc.outputTokens += row.output_tokens ?? 0;
    result.bySession[sid] = sAcc;
  }

  return result;
}

// costForPipeline — aggregate cost for one pipeline across all its runs.
// runs: full flow_run list from the state store (filtered to this pipeline here or by caller).
// opts: { sinceMs?, untilMs?, sinceDays?, priorWindowMs?, _dbPath? }
//
// Returns a PipelineCost:
//   { pipelineId, runs, succeeded, total, mean, p50, p95, costPerSuccess,
//     costPerSuccessPrior, byModel, byNode, byDay, since, until, byRun }
//
// byRun: [{ flowId, status, createdAt, total, byNode }] sorted by createdAt desc
// byDay: [{ day, total, runs }] sorted by day
// byNode: pipeline-wide per-node { cost, calls, p50Ms, p95Ms, meanMs, p50Cost, p95Cost, perDay }
//
// *Ms fields (byNode[x].p50Ms/p95Ms/meanMs, perDay[].p50Ms) come from
// runtime.sqlite::flow_step_timings (entered_at/exited_at), NOT cost_by_call
// — that table carries no duration column. Timings there only cover
// top-level pipeline steps (plus a fallback wrapper's own direct children);
// nodes running inside a subflow's per-item child iterations aren't timed at
// that granularity, so those legitimately stay null — the UI treats null
// as "—".
//
// `runs`/`succeeded` use the same active-in-window gate as
// costForAllPipelines: createdAt in window OR cost > 0 in window. A plain
// updatedAt filter inflates the count after any mass-touch event (see
// project memory reference_cost_window_gate).
export async function costForPipeline(runs, pipelineId, { sinceMs = null, untilMs = null, sinceDays = null, priorWindowMs = null, _dbPath = null } = {}) {
  // Resolve window bounds.
  let resolvedSince = sinceMs;
  if (resolvedSince == null && sinceDays != null && Number.isFinite(sinceDays) && sinceDays > 0) {
    resolvedSince = Date.now() - sinceDays * 86_400_000;
  }
  const resolvedUntil = untilMs ?? (resolvedSince ? Date.now() : null);

  // Filter runs to this pipeline.
  const mine = runs.filter((r) => {
    const pid = r.pipelineId ?? r.stateJson?.pipelineId ?? "";
    return pid === pipelineId;
  });

  const empty = {
    pipelineId, runs: 0, succeeded: 0, total: 0, mean: 0, p50: null, p95: null,
    costPerSuccess: null, costPerSuccessPrior: null,
    byModel: {}, byNode: {}, byDay: [],
    since: resolvedSince, until: resolvedUntil, byRun: [],
  };
  if (mine.length === 0) return empty;

  const flowIds = mine.map((r) => r.flowId).filter(Boolean);
  if (flowIds.length === 0) return empty;

  const dbPath = _dbPath ?? OPS_DB_PATH;
  if (!existsSync(dbPath)) return empty;

  const { client } = await getClient(dbPath);
  const result = await aggregatePipelineCost(client, pipelineId, flowIds, mine, resolvedSince, resolvedUntil);

  // Prior-window costPerSuccess for the trend delta — same-length window
  // immediately before `resolvedSince`. Only meaningful with a bounded
  // window (sinceMs set); an all-time query has no "prior" to compare.
  if (priorWindowMs != null && resolvedSince != null) {
    const prior = await aggregatePipelineCost(client, pipelineId, flowIds, mine, resolvedSince - priorWindowMs, resolvedSince);
    result.costPerSuccessPrior = prior.succeeded > 0 ? prior.total / prior.succeeded : null;
  }

  return result;
}

function dayString(ts) {
  return new Date(ts).toISOString().slice(0, 10);
}

// One windowed pass over cost_by_call for a pipeline's flows, producing a
// full PipelineCost (minus costPerSuccessPrior, filled in by the caller).
async function aggregatePipelineCost(client, pipelineId, flowIds, mine, sinceMs, untilMs) {
  const placeholders = flowIds.map(() => "?").join(",");
  const conditions = [`flow_id IN (${placeholders})`];
  const args = [...flowIds];
  if (sinceMs != null) { conditions.push("ts >= ?"); args.push(sinceMs); }
  if (untilMs != null) { conditions.push("ts <= ?"); args.push(untilMs); }
  const where = conditions.join(" AND ");

  const { rows } = await client.execute({
    sql: `
      SELECT flow_id, node_id, model, cost_total, ts
      FROM cost_by_call
      WHERE ${where}
    `,
    args,
  });

  const flowCost = new Map();      // flowId → total cost
  const modelCost = new Map();     // model → total cost
  const nodeAcc = new Map();       // nodeId → { calls: number[], dayMap: Map<day, {cost, calls: number[]}> }
  const dayAcc = new Map();        // day → { cost, calls: number[], runSet: Set<flowId> }
  // Per-run per-node cost accumulator
  const runNodeAcc = new Map();    // flowId → Map<nodeId, { total, calls }>

  for (const row of rows) {
    const cost = row.cost_total ?? 0;
    const fid = row.flow_id;
    flowCost.set(fid, (flowCost.get(fid) ?? 0) + cost);

    const modelKey = row.model ?? "unknown";
    modelCost.set(modelKey, (modelCost.get(modelKey) ?? 0) + cost);

    const day = dayString(row.ts);

    // Lowercased: node_id casing has drifted across pipeline/executor versions
    // ("SCORE" vs "score"), and the dashboard's per-node lookups (nodeKindMap,
    // nodeGroupDefs, stepOrder) are all keyed lowercase — without this,
    // case variants of the same node split into separate byNode rows instead
    // of merging, and node-group coalescing never matches.
    const nodeId = (row.node_id ?? "unknown").toLowerCase();
    const nAcc = nodeAcc.get(nodeId) ?? { calls: [], dayMap: new Map() };
    nAcc.calls.push(cost);
    const nDay = nAcc.dayMap.get(day) ?? { cost: 0, calls: [] };
    nDay.cost += cost;
    nDay.calls.push(cost);
    nAcc.dayMap.set(day, nDay);
    nodeAcc.set(nodeId, nAcc);

    const dAcc = dayAcc.get(day) ?? { cost: 0, calls: [], runSet: new Set() };
    dAcc.cost += cost;
    dAcc.calls.push(cost);
    dAcc.runSet.add(fid);
    dayAcc.set(day, dAcc);

    // Per-run per-node
    if (!runNodeAcc.has(fid)) runNodeAcc.set(fid, new Map());
    const rnMap = runNodeAcc.get(fid);
    const rnAcc = rnMap.get(nodeId) ?? { total: 0, calls: 0 };
    rnAcc.total += cost;
    rnAcc.calls += 1;
    rnMap.set(nodeId, rnAcc);
  }

  // Wall-time: separate pass over flow_node_executions, since it's a
  // different table at a different granularity (one row per node EXECUTION,
  // vs one cost_by_call row per LLM call) — joined only at the (nodeId, day)
  // key, same case-normalization as the cost pass above.
  const nodeExecutionsStore = await getNodeExecutionsStore();
  const timingRows = await nodeExecutionsStore.queryByPipeline({ pipelineId, sinceMs, untilMs });
  const nodeTimingAcc = new Map(); // nodeId → { durations: number[], dayMap: Map<day, number[]> }
  const dayTimingAcc = new Map();  // day → number[] (pipeline-wide durations)
  for (const row of timingRows) {
    const ms = row.exited_at - row.entered_at;
    if (!Number.isFinite(ms) || ms < 0) continue;
    const nodeId = (row.node_id ?? "unknown").toLowerCase();
    const day = dayString(row.exited_at);

    const tAcc = nodeTimingAcc.get(nodeId) ?? { durations: [], dayMap: new Map() };
    tAcc.durations.push(ms);
    const tDay = tAcc.dayMap.get(day) ?? [];
    tDay.push(ms);
    tAcc.dayMap.set(day, tDay);
    nodeTimingAcc.set(nodeId, tAcc);

    const dArr = dayTimingAcc.get(day) ?? [];
    dArr.push(ms);
    dayTimingAcc.set(day, dArr);
  }

  // Active-in-window gate: a run counts if it started in-window, OR it has
  // no createdAt-in-window overlap but still logged cost in-window (e.g. a
  // long-running flow that started earlier). Unbounded window (sinceMs
  // null) counts everything.
  const gatedRuns = mine.filter((r) => {
    if (sinceMs == null) return true;
    const startedInWindow = (r.createdAt ?? 0) >= sinceMs && (untilMs == null || (r.createdAt ?? 0) <= untilMs);
    const workedInWindow = (flowCost.get(r.flowId) ?? 0) > 0;
    return startedInWindow || workedInWindow;
  });

  const succeededCount = gatedRuns.filter((r) => r.status === "succeeded").length;
  const runTotals = gatedRuns.map((r) => flowCost.get(r.flowId) ?? 0);
  const grandTotal = runTotals.reduce((s, c) => s + c, 0);
  const mean = gatedRuns.length > 0 ? grandTotal / gatedRuns.length : 0;

  // byRun: per-run cost with per-run node breakdown, sorted by createdAt desc
  const byRun = gatedRuns
    .map((r) => {
      const rnMap = runNodeAcc.get(r.flowId);
      const byNode = rnMap
        ? Object.fromEntries([...rnMap.entries()].map(([nid, acc]) => [nid, { total: acc.total, calls: acc.calls }]))
        : {};
      return {
        flowId: r.flowId,
        status: r.status ?? null,
        createdAt: r.createdAt ?? null,
        total: flowCost.get(r.flowId) ?? 0,
        byNode,
      };
    })
    .sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0));

  const byModel = Object.fromEntries(modelCost);

  // Union of nodes with cost data and nodes with timing data — a code node
  // that never calls an LLM (no cost_by_call rows) can still have real
  // wall-time from flow_step_timings, and the UI's row filter already
  // expects that (`v.cost > 0 || (v.p50Ms ?? 0) > 0`).
  const byNode = {};
  const allNodeIds = new Set([...nodeAcc.keys(), ...nodeTimingAcc.keys()]);
  for (const nodeId of allNodeIds) {
    const cAcc = nodeAcc.get(nodeId);
    const tAcc = nodeTimingAcc.get(nodeId);
    const nodeCost = cAcc ? cAcc.calls.reduce((s, c) => s + c, 0) : 0;
    const days = new Set([...(cAcc?.dayMap.keys() ?? []), ...(tAcc?.dayMap.keys() ?? [])]);
    const perDayEntries = [...days]
      .map((day) => {
        const cDay = cAcc?.dayMap.get(day);
        const tDay = tAcc?.dayMap.get(day);
        return {
          day,
          cost: cDay?.cost ?? 0,
          calls: cDay?.calls.length ?? 0,
          ms: tDay ? tDay.reduce((s, m) => s + m, 0) : 0,
          p50Cost: cDay ? percentile(cDay.calls, 50) : null,
          p50Ms: tDay ? percentile(tDay, 50) : null,
        };
      })
      .sort((a, b) => a.day.localeCompare(b.day));
    byNode[nodeId] = {
      cost: nodeCost,
      calls: cAcc?.calls.length ?? 0,
      p50Ms: tAcc ? percentile(tAcc.durations, 50) : null,
      p95Ms: tAcc ? percentile(tAcc.durations, 95) : null,
      meanMs: tAcc && tAcc.durations.length > 0 ? tAcc.durations.reduce((s, m) => s + m, 0) / tAcc.durations.length : null,
      p50Cost: cAcc ? percentile(cAcc.calls, 50) : null,
      p95Cost: cAcc ? percentile(cAcc.calls, 95) : null,
      perDay: perDayEntries,
    };
  }

  const perDayDays = new Set([...dayAcc.keys(), ...dayTimingAcc.keys()]);
  const byDay = [...perDayDays]
    .map((day) => {
      const d = dayAcc.get(day);
      const t = dayTimingAcc.get(day);
      return {
        day,
        total: d?.cost ?? 0,
        runs: d?.runSet.size ?? 0,
        p50Cost: d ? percentile(d.calls, 50) : null,
        p50Ms: t ? percentile(t, 50) : null,
      };
    })
    .sort((a, b) => a.day.localeCompare(b.day));

  return {
    pipelineId,
    runs: gatedRuns.length,
    succeeded: succeededCount,
    total: grandTotal,
    mean,
    p50: percentile(runTotals, 50),
    p95: percentile(runTotals, 95),
    costPerSuccess: succeededCount > 0 ? grandTotal / succeededCount : null,
    costPerSuccessPrior: null,
    byModel,
    byNode,
    byDay,
    since: sinceMs,
    until: untilMs,
    byRun,
  };
}

// costForAllPipelines — home-page overview across every pipeline.
// runs: full flow_run list from the state store.
// opts: { sinceMs?, _dbPath? }
//
// Returns:
//   { sinceMs, untilMs, total, pipelines }
//
// pipeline entry: { pipelineId, total, runCount, succeededToday, failedToday, costToday }
// total: cross-pipeline aggregate { spend, runs, succeeded, costPerSuccess }
export async function costForAllPipelines(runs, { sinceMs = null, _dbPath = null } = {}) {
  const untilMs = sinceMs ? Date.now() : null;

  const empty = { sinceMs, untilMs, pipelines: [], total: { spend: 0, runs: 0, succeeded: 0, costPerSuccess: null } };
  if (!runs || runs.length === 0) return empty;

  // Build a flowId → pipeline mapping from the runs list.
  const flowToPipeline = new Map();
  for (const r of runs) {
    if (!r.flowId) continue;
    const pid = r.pipelineId ?? r.stateJson?.pipelineId ?? "";
    if (!pid) continue;
    flowToPipeline.set(r.flowId, pid);
  }

  const flowIds = [...flowToPipeline.keys()];
  if (flowIds.length === 0) return empty;

  const dbPath = _dbPath ?? OPS_DB_PATH;
  if (!existsSync(dbPath)) {
    // DB absent: still return pipeline entries derived purely from the runs list,
    // all with zero cost (active-in-window gate still applies for run counts).
    return _aggregateFromRunsOnly(runs, flowToPipeline, sinceMs, untilMs);
  }

  const { client } = await getClient(dbPath);
  const placeholders = flowIds.map(() => "?").join(",");
  const conditions = [`flow_id IN (${placeholders})`];
  const args = [...flowIds];
  if (sinceMs != null) { conditions.push("ts >= ?"); args.push(sinceMs); }
  if (untilMs != null) { conditions.push("ts <= ?"); args.push(untilMs); }
  const where = conditions.join(" AND ");

  // Per-flow cost total in one query — enough to build per-pipeline rollup.
  const { rows } = await client.execute({
    sql: `
      SELECT flow_id, SUM(cost_total) AS flow_total
      FROM cost_by_call
      WHERE ${where}
      GROUP BY flow_id
    `,
    args,
  });

  // flowId → total cost from the DB
  const dbFlowCost = new Map();
  for (const row of rows) {
    dbFlowCost.set(row.flow_id, row.flow_total ?? 0);
  }

  return _buildPipelineResult(runs, flowToPipeline, dbFlowCost, sinceMs, untilMs);
}

// Build pipeline result when DB has no rows (absent or empty cost).
// All pipeline entries still appear, but with zero cost.
function _aggregateFromRunsOnly(runs, flowToPipeline, sinceMs, untilMs) {
  const dbFlowCost = new Map(); // empty — no DB rows
  return _buildPipelineResult(runs, flowToPipeline, dbFlowCost, sinceMs, untilMs);
}

function _buildPipelineResult(runs, flowToPipeline, dbFlowCost, sinceMs, untilMs) {
  // Today boundary for succeededToday / failedToday / costToday.
  const todayStart = (() => {
    const d = new Date();
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  })();

  // Accumulate per-pipeline metrics.
  const byPipeline = new Map();

  for (const r of runs) {
    if (!r.flowId) continue;
    const pid = flowToPipeline.get(r.flowId);
    if (!pid) continue;

    // Active-in-window gate: must have been created in-window OR had DB cost in-window.
    const startedInWindow = sinceMs == null || (r.createdAt ?? 0) >= sinceMs;
    const workedInWindow = (dbFlowCost.get(r.flowId) ?? 0) > 0;
    if (sinceMs != null && !startedInWindow && !workedInWindow) continue;

    const runCost = dbFlowCost.get(r.flowId) ?? 0;
    const isToday = (r.updatedAt ?? 0) >= todayStart;

    const acc = byPipeline.get(pid) ?? {
      pipelineId: pid, total: 0, runCount: 0,
      succeeded: 0, succeededToday: 0, failedToday: 0, costToday: 0,
    };
    acc.total    += runCost;
    acc.runCount += 1;
    if (r.status === "succeeded") {
      acc.succeeded++;
      if (isToday) acc.succeededToday++;
    }
    if (r.status === "failed" && isToday) acc.failedToday++;
    if (isToday) acc.costToday += runCost;
    byPipeline.set(pid, acc);
  }

  const pipelines = Array.from(byPipeline.values()).map((acc) => ({
    pipelineId: acc.pipelineId,
    total: acc.total,
    runCount: acc.runCount,
    succeededToday: acc.succeededToday,
    failedToday: acc.failedToday,
    costToday: acc.costToday,
  }));
  pipelines.sort((a, b) => b.total - a.total);

  const totalSpend = pipelines.reduce((s, p) => s + p.total, 0);
  const totalRuns = pipelines.reduce((s, p) => s + p.runCount, 0);
  let allSucceeded = 0;
  for (const acc of byPipeline.values()) allSucceeded += acc.succeeded;

  return {
    sinceMs,
    untilMs,
    pipelines,
    total: {
      spend: totalSpend,
      runs: totalRuns,
      succeeded: allSucceeded,
      costPerSuccess: allSucceeded > 0 ? totalSpend / allSucceeded : null,
    },
  };
}

function percentile(arr, p) {
  if (!arr || arr.length === 0) return null;
  const sorted = arr.slice().sort((a, b) => a - b);
  const idx = Math.max(0, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}
