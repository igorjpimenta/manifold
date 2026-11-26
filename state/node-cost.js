// node-cost.js — lazy-singleton reader over cost_by_call
// (runtime.sqlite): a percentile query so estimateNodeCost can return
// historically-grounded low/high bounds instead of a static range.

import path from "node:path";
import { getSharedDb, resolveStorePaths } from "./db.js";
import os from "node:os";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

const COST_SQL = `
  SELECT cost_total FROM cost_by_call
  WHERE pipeline_id = ? AND node_id = ? AND ts >= ?
  ORDER BY cost_total ASC
`;

function percentile(sorted, p) {
  const idx = p * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

export function createNodeCostReader({ dbPath = defaultDbPath() } = {}) {
  let _client = null;

  async function getClient() {
    if (_client) return _client;
    try {
      ({ client: _client } = await getSharedDb(dbPath, { readonly: true }));
    } catch {
      _client = null;
    }
    return _client;
  }

  async function nodeCallStats(pipelineId, nodeId, { sinceDays = 30, minSamples = 3 } = {}) {
    if (!pipelineId || !nodeId) return null;
    const client = await getClient();
    if (!client) return null;
    const sinceTs = Date.now() - sinceDays * 86_400_000;
    let rows;
    try {
      const result = await client.execute({ sql: COST_SQL, args: [pipelineId, nodeId, sinceTs] });
      rows = result.rows;
    } catch {
      return null;
    }
    if (rows.length < minSamples) return null;
    const costs = rows.map((r) => Number(r.cost_total)).sort((a, b) => a - b);
    return {
      p25: percentile(costs, 0.25),
      p50: percentile(costs, 0.50),
      p75: percentile(costs, 0.75),
      sampleSize: costs.length,
    };
  }

  return { nodeCallStats };
}

// Default singleton backed by the store home's runtime.sqlite
const _default = createNodeCostReader();
export const { nodeCallStats } = _default;
