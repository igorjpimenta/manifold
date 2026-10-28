// Unit tests for dashboard/cost.js — costForRun, costForPipeline, costForAllPipelines:
// token aggregation, byModel/byNode/bySession fan-out, time-window filtering,
// pipeline identification, byDay grouping, p50/p95 percentiles, active-in-window
// gate, succeededToday/failedToday/costToday, and null/empty-input edge cases.

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createClient } from "@libsql/client";

// ─── isolated ops DB ──────────────────────────────────────────────────────────
// cost.js takes its DB path as a required injected dbPath (no default), so
// every call below passes one explicitly — nothing here can touch a real DB.

const TEST_HOME = path.join(os.tmpdir(), `gf-cost-${randomUUID()}`);
const TEST_OPS_DIR = path.join(TEST_HOME, ".openclaw");
const LINEAGE_DB_PATH = path.join(TEST_OPS_DIR, "ops.sqlite");

// Create the directory but NOT the .sqlite file yet — the first describe block
// verifies that the functions degrade gracefully when the file is absent.
mkdirSync(TEST_OPS_DIR, { recursive: true });

const { costForRun, costForPipeline, costForAllPipelines } =
  await import("../../../dashboard/cost.js");

// ─── DB absent — must run before before() creates the file ───────────────────

describe("DB absent — graceful empty returns", () => {
  test("costForRun returns zero-total struct when DB file is missing", async () => {
    const r = await costForRun(randomUUID(), { dbPath: "/nonexistent/path/ops.sqlite" });
    assert.equal(r.total, 0);
    assert.deepEqual(r.tokens, { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 });
    assert.deepEqual(r.byModel, {});
    assert.deepEqual(r.byNode, {});
    assert.deepEqual(r.bySession, {});
  });

  test("costForRun preserves flowId even when DB absent", async () => {
    const id = randomUUID();
    assert.equal((await costForRun(id, { dbPath: "/nonexistent/path/ops.sqlite" })).flowId, id);
  });

  test("costForPipeline returns empty when DB file is missing", async () => {
    const flowId = randomUUID();
    const runs = [makeRunRaw({ flowId, pipelineId: "x" })];
    // Point to a nonexistent path to exercise the absent-DB branch.
    const r = await costForPipeline(runs, "x", { dbPath: "/nonexistent/path/ops.sqlite" });
    assert.equal(r.total, 0);
    assert.deepEqual(r.byRun, []);
    assert.deepEqual(r.byDay, []);
    assert.equal(r.p50, null);
  });

  test("costForAllPipelines returns zero-spend pipeline entry when DB file is missing", async () => {
    const flowId = randomUUID();
    const runs = [makeRunRaw({ flowId, pipelineId: "all_absent" })];
    const r = await costForAllPipelines(runs, { dbPath: "/nonexistent/path/ops.sqlite" });
    // sinceMs=null → all runs included regardless of DB; pipeline entry exists but total=0
    const p = r.pipelines.find((x) => x.pipelineId === "all_absent");
    assert.ok(p, "pipeline entry should exist even when DB is absent");
    assert.equal(p.total, 0);
    assert.equal(r.total.spend, 0);
  });
});

// ─── schema ──────────────────────────────────────────────────────────────────

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS cost_by_call (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id         TEXT,
    model              TEXT,
    agent_id           TEXT,
    node_id            TEXT,
    item_key           TEXT,
    input_tokens       INTEGER DEFAULT 0,
    output_tokens      INTEGER DEFAULT 0,
    cache_read_tokens  INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    cost_total         REAL    DEFAULT 0,
    ts                 INTEGER NOT NULL,
    flow_id            TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_cbc_flow ON cost_by_call (flow_id);
  CREATE INDEX IF NOT EXISTS idx_cbc_ts   ON cost_by_call (ts);
`;

let _client = null;
function getClient() {
  if (!_client) {
    _client = createClient({ url: `file:${LINEAGE_DB_PATH}` });
  }
  return _client;
}

// Bootstrap DB (schema only) for all subsequent tests.
before(async () => {
  const c = getClient();
  await c.execute(SCHEMA);
});

after(() => {
  _client?.close();
  _client = null;
});

// ─── helpers ─────────────────────────────────────────────────────────────────

async function insertRow({
  flowId = randomUUID(),
  sessionId = randomUUID(),
  model = "claude-haiku-4-5",
  agentId = "agent-1",
  nodeId = "node-1",
  itemKey = null,
  inputTokens = 100,
  outputTokens = 50,
  cacheReadTokens = 0,
  cacheWriteTokens = 0,
  costTotal = 0.001,
  tsOffset = 0,
} = {}) {
  await getClient().execute({
    sql: `
      INSERT INTO cost_by_call
        (session_id, model, agent_id, node_id, item_key,
         input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
         cost_total, ts, flow_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    args: [
      sessionId, model, agentId, nodeId, itemKey,
      inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens,
      costTotal, Date.now() + tsOffset, flowId,
    ],
  });
  return flowId;
}

function makeRunRaw({
  flowId = randomUUID(),
  pipelineId = "test_pipeline",
  status = "succeeded",
  createdAt = Date.now() - 3_600_000,
  updatedAt = Date.now() - 1_800_000,
} = {}) {
  return {
    flowId,
    controllerId: `manifold:${pipelineId}`,
    status,
    createdAt,
    updatedAt,
    stateJson: { pipelineId },
  };
}

// Helper: pass dbPath so tests use the test DB, not ~/.openclaw/ops.sqlite
const DB = { dbPath: LINEAGE_DB_PATH };

// ═══════════════════════════════════════════════════════════════════════════════
// costForRun
// ═══════════════════════════════════════════════════════════════════════════════

describe("costForRun — basic aggregation", () => {
  test("sums cost_total across multiple rows for a flowId", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 0.01 });
    await insertRow({ flowId, costTotal: 0.02 });
    await insertRow({ flowId, costTotal: 0.03 });
    const r = await costForRun(flowId, DB);
    assert.ok(Math.abs(r.total - 0.06) < 1e-9, `expected 0.06 got ${r.total}`);
  });

  test("does not include rows from other flowIds", async () => {
    const flowA = randomUUID();
    const flowB = randomUUID();
    await insertRow({ flowId: flowA, costTotal: 1.0 });
    await insertRow({ flowId: flowB, costTotal: 99.0 });
    const r = await costForRun(flowA, DB);
    assert.ok(r.total < 2, "flowB cost must not bleed into flowA");
  });

  test("returns zero total when flowId has no rows", async () => {
    assert.equal((await costForRun(randomUUID(), DB)).total, 0);
  });

  test("flowId is preserved in the result", async () => {
    const id = randomUUID();
    await insertRow({ flowId: id, costTotal: 0.1 });
    assert.equal((await costForRun(id, DB)).flowId, id);
  });
});

describe("costForRun — token aggregation", () => {
  test("sums input, output, cacheRead, cacheWrite tokens across rows", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, inputTokens: 100, outputTokens: 50, cacheReadTokens: 20, cacheWriteTokens: 10 });
    await insertRow({ flowId, inputTokens: 200, outputTokens: 75, cacheReadTokens:  5, cacheWriteTokens:  0 });
    const r = await costForRun(flowId, DB);
    assert.equal(r.tokens.input,      300);
    assert.equal(r.tokens.output,     125);
    assert.equal(r.tokens.cacheRead,   25);
    assert.equal(r.tokens.cacheWrite,  10);
  });

  test("tokens default to zero for a flowId with no rows", async () => {
    const r = await costForRun(randomUUID(), DB);
    assert.deepEqual(r.tokens, { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 });
  });
});

describe("costForRun — byModel fan-out", () => {
  test("groups calls by model accumulating total, calls, inputTokens, outputTokens", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, model: "haiku",  costTotal: 0.001, inputTokens: 100, outputTokens: 50 });
    await insertRow({ flowId, model: "haiku",  costTotal: 0.002, inputTokens: 200, outputTokens: 60 });
    await insertRow({ flowId, model: "sonnet", costTotal: 0.010, inputTokens: 400, outputTokens: 80 });
    const r = await costForRun(flowId, DB);
    assert.ok(r.byModel.haiku,  "haiku key missing");
    assert.ok(r.byModel.sonnet, "sonnet key missing");
    assert.equal(r.byModel.haiku.calls,        2);
    assert.equal(r.byModel.haiku.inputTokens,  300);
    assert.equal(r.byModel.haiku.outputTokens, 110);
    assert.ok(Math.abs(r.byModel.haiku.total  - 0.003) < 1e-9);
    assert.ok(Math.abs(r.byModel.sonnet.total - 0.010) < 1e-9);
    assert.equal(r.byModel.sonnet.calls, 1);
  });

  test("null model falls into 'unknown' key", async () => {
    const flowId = randomUUID();
    await getClient().execute({
      sql: `INSERT INTO cost_by_call
        (session_id, model, agent_id, node_id, input_tokens, output_tokens,
         cache_read_tokens, cache_write_tokens, cost_total, ts, flow_id)
       VALUES (?, NULL, ?, ?, 0, 0, 0, 0, 0.005, ?, ?)`,
      args: [randomUUID(), "agent-1", "node-1", Date.now(), flowId],
    });
    const r = await costForRun(flowId, DB);
    assert.ok(r.byModel.unknown, "'unknown' key missing for null model");
  });

  test("byModel is empty for a flowId with no rows", async () => {
    assert.deepEqual((await costForRun(randomUUID(), DB)).byModel, {});
  });
});

describe("costForRun — byNode fan-out", () => {
  test("groups calls by node accumulating total and calls", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, nodeId: "draft", costTotal: 0.01 });
    await insertRow({ flowId, nodeId: "draft", costTotal: 0.02 });
    await insertRow({ flowId, nodeId: "gate",  costTotal: 0.05 });
    const r = await costForRun(flowId, DB);
    assert.equal(r.byNode.draft.calls, 2);
    assert.ok(Math.abs(r.byNode.draft.total - 0.03) < 1e-9);
    assert.equal(r.byNode.gate.calls,  1);
  });

  test("groups item_key under the node's items map", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, nodeId: "process", itemKey: "item-1", costTotal: 0.01 });
    await insertRow({ flowId, nodeId: "process", itemKey: "item-1", costTotal: 0.02 });
    await insertRow({ flowId, nodeId: "process", itemKey: "item-2", costTotal: 0.05 });
    const items = (await costForRun(flowId, DB)).byNode.process.items;
    assert.equal(items["item-1"].calls, 2);
    assert.ok(Math.abs(items["item-1"].total - 0.03) < 1e-9);
    assert.equal(items["item-2"].calls, 1);
  });

  test("null node_id falls into 'unknown' key", async () => {
    const flowId = randomUUID();
    await getClient().execute({
      sql: `INSERT INTO cost_by_call
        (session_id, model, agent_id, node_id, input_tokens, output_tokens,
         cache_read_tokens, cache_write_tokens, cost_total, ts, flow_id)
       VALUES (?, 'm', 'a', NULL, 0, 0, 0, 0, 0.001, ?, ?)`,
      args: [randomUUID(), Date.now(), flowId],
    });
    assert.ok((await costForRun(flowId, DB)).byNode.unknown, "'unknown' key missing for null node_id");
  });
});

describe("costForRun — bySession fan-out", () => {
  test("groups calls by session_id accumulating total, calls, inputTokens, outputTokens", async () => {
    const flowId = randomUUID();
    const sid1 = randomUUID();
    const sid2 = randomUUID();
    await insertRow({ flowId, sessionId: sid1, costTotal: 0.01, inputTokens: 100, outputTokens: 20 });
    await insertRow({ flowId, sessionId: sid1, costTotal: 0.02, inputTokens: 150, outputTokens: 30 });
    await insertRow({ flowId, sessionId: sid2, costTotal: 0.05, inputTokens: 400, outputTokens: 80 });
    const bs = (await costForRun(flowId, DB)).bySession;
    assert.ok(bs[sid1], "session 1 missing");
    assert.equal(bs[sid1].calls,        2);
    assert.equal(bs[sid1].inputTokens,  250);
    assert.equal(bs[sid1].outputTokens, 50);
    assert.ok(bs[sid2], "session 2 missing");
    assert.equal(bs[sid2].calls, 1);
  });

  test("bySession entry carries the model and agentId from the first row for that session", async () => {
    const flowId = randomUUID();
    const sid = randomUUID();
    await insertRow({ flowId, sessionId: sid, model: "opus", agentId: "agent-x" });
    const entry = (await costForRun(flowId, DB)).bySession[sid];
    assert.equal(entry.model,   "opus");
    assert.equal(entry.agentId, "agent-x");
  });
});

describe("costForRun — time-window filtering", () => {
  test("sinceMs excludes rows before the cutoff", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 1.0, tsOffset:      -500 }); // in window
    await insertRow({ flowId, costTotal: 9.0, tsOffset: -200_000 }); // before cutoff
    const sinceMs = Date.now() - 10_000;
    const r = await costForRun(flowId, { sinceMs, dbPath: LINEAGE_DB_PATH });
    assert.ok(r.total < 2, `expected ~1.0, got ${r.total}`);
  });

  test("untilMs excludes rows after the cutoff", async () => {
    const flowId = randomUUID();
    const now = Date.now();
    await insertRow({ flowId, costTotal: 1.0, tsOffset:  -2_000 }); // after cutoff → excluded
    await insertRow({ flowId, costTotal: 2.0, tsOffset: -20_000 }); // before cutoff → included
    const sinceMs  = now - 30_000;
    const untilMs  = now - 10_000;
    const r = await costForRun(flowId, { sinceMs, untilMs, dbPath: LINEAGE_DB_PATH });
    assert.ok(Math.abs(r.total - 2.0) < 1e-9, `expected 2.0, got ${r.total}`);
  });

  test("sinceMs with no untilMs defaults effectiveUntil to now", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 3.0, tsOffset: -1_000 }); // in window
    const r = await costForRun(flowId, { sinceMs: Date.now() - 10_000, dbPath: LINEAGE_DB_PATH });
    assert.ok(Math.abs(r.total - 3.0) < 1e-9);
  });

  test("_sessionIndex param is accepted and ignored", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 0.5 });
    const r = await costForRun(flowId, DB, { some: "legacy", data: true });
    assert.ok(r.total > 0);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// costForPipeline
// ═══════════════════════════════════════════════════════════════════════════════

describe("costForPipeline — empty / no match", () => {
  test("returns empty struct when runs is []", async () => {
    const r = await costForPipeline([], "test_pipeline", DB);
    assert.equal(r.total, 0);
    assert.deepEqual(r.byRun, []);
    assert.deepEqual(r.byDay, []);
    assert.equal(r.p50, null);
    assert.equal(r.p95, null);
    assert.equal(r.pipelineId, "test_pipeline");
  });

  test("returns empty when no runs match the pipelineId", async () => {
    const runs = [makeRunRaw({ pipelineId: "other_pipeline" })];
    const r = await costForPipeline(runs, "my_pipeline", DB);
    assert.equal(r.total, 0);
    assert.deepEqual(r.byRun, []);
  });
});

describe("costForPipeline — pipeline filtering", () => {
  test("matches runs via controllerId 'manifold:<pipelineId>'", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 0.5 });
    const r = await costForPipeline([makeRunRaw({ flowId, pipelineId: "pipe_ctrl" })], "pipe_ctrl", DB);
    assert.ok(Math.abs(r.total - 0.5) < 1e-9);
  });

  test("matches runs via stateJson.pipelineId when controllerId is null", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 0.7 });
    const run = {
      flowId,
      controllerId: null,
      status: "succeeded",
      createdAt: Date.now() - 3_600_000,
      updatedAt: Date.now() - 1_800_000,
      stateJson: { pipelineId: "pipe_json" },
    };
    const r = await costForPipeline([run], "pipe_json", DB);
    assert.ok(Math.abs(r.total - 0.7) < 1e-9);
  });

  test("does not include cost from a different pipeline's runs", async () => {
    const [flowA, flowB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: flowA, costTotal: 1.0 });
    await insertRow({ flowId: flowB, costTotal: 99.0 });
    const r = await costForPipeline([makeRunRaw({ flowId: flowA, pipelineId: "pipe_iso" })], "pipe_iso", DB);
    assert.ok(r.total < 2, "other pipeline's cost bled in");
  });
});

describe("costForPipeline — byRun shape", () => {
  test("byRun entries contain flowId, total, status, createdAt", async () => {
    const flowId = randomUUID();
    const now = Date.now();
    await insertRow({ flowId, costTotal: 0.3 });
    const run = makeRunRaw({ flowId, pipelineId: "pipe_shape", status: "failed", createdAt: now - 3_600_000 });
    const r = await costForPipeline([run], "pipe_shape", DB);
    assert.equal(r.byRun.length, 1);
    const entry = r.byRun[0];
    assert.equal(entry.flowId,    flowId);
    assert.equal(entry.status,    "failed");
    assert.equal(entry.createdAt, run.createdAt);
    assert.ok(Math.abs(entry.total - 0.3) < 1e-9);
  });

  test("byRun entry contains byNode breakdown", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, nodeId: "draft",  costTotal: 0.1 });
    await insertRow({ flowId, nodeId: "submit", costTotal: 0.2 });
    const r = await costForPipeline([makeRunRaw({ flowId, pipelineId: "pipe_byn" })], "pipe_byn", DB);
    const { byNode } = r.byRun[0];
    assert.ok(byNode.draft,  "draft missing");
    assert.ok(byNode.submit, "submit missing");
    assert.ok(Math.abs(byNode.draft.total  - 0.1) < 1e-9);
    assert.ok(Math.abs(byNode.submit.total - 0.2) < 1e-9);
  });

  test("byRun is sorted by createdAt descending", async () => {
    const now = Date.now();
    const [fA, fB, fC] = [randomUUID(), randomUUID(), randomUUID()];
    for (const fid of [fA, fB, fC]) await insertRow({ flowId: fid, costTotal: 0.01 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "pipe_sort", createdAt: now - 1_000 }),
      makeRunRaw({ flowId: fB, pipelineId: "pipe_sort", createdAt: now - 3_000 }),
      makeRunRaw({ flowId: fC, pipelineId: "pipe_sort", createdAt: now - 2_000 }),
    ];
    const r = await costForPipeline(runs, "pipe_sort", DB);
    assert.equal(r.byRun[0].flowId, fA); // newest first
    assert.equal(r.byRun[1].flowId, fC);
    assert.equal(r.byRun[2].flowId, fB);
  });
});

describe("costForPipeline — byDay grouping", () => {
  test("groups costs by UTC date and sums to one bucket for today's rows", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 0.1, tsOffset:  -1_000 });
    await insertRow({ flowId, costTotal: 0.2, tsOffset:  -2_000 });
    const r = await costForPipeline([makeRunRaw({ flowId, pipelineId: "pipe_bday" })], "pipe_bday", DB);
    assert.equal(r.byDay.length, 1);
    assert.ok(Math.abs(r.byDay[0].total - 0.3) < 1e-9);
    assert.equal(r.byDay[0].runs, 1);
    assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(r.byDay[0].day), "day should be YYYY-MM-DD");
  });
});

describe("costForPipeline — percentiles (p50 / p95)", () => {
  test("p50 is the median of per-run totals", async () => {
    const pid = "pipe_p50";
    const runs = [];
    for (const c of [1, 2, 3, 4, 5]) {
      const flowId = randomUUID();
      await insertRow({ flowId, costTotal: c });
      runs.push(makeRunRaw({ flowId, pipelineId: pid }));
    }
    const r = await costForPipeline(runs, pid, DB);
    // sorted [1,2,3,4,5]: idx = ceil(0.5*5)-1 = 2 → 3
    assert.equal(r.p50, 3);
  });

  test("p95 is the 95th-pct of per-run totals", async () => {
    const pid = "pipe_p95";
    const runs = [];
    for (const c of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) {
      const flowId = randomUUID();
      await insertRow({ flowId, costTotal: c });
      runs.push(makeRunRaw({ flowId, pipelineId: pid }));
    }
    const r = await costForPipeline(runs, pid, DB);
    // sorted [1..10]: idx = ceil(0.95*10)-1 = 9 → 10
    assert.equal(r.p95, 10);
  });

  test("p50 and p95 are null when byRun is empty", async () => {
    const r = await costForPipeline([], "pipe_empty_pct", DB);
    assert.equal(r.p50, null);
    assert.equal(r.p95, null);
  });
});

describe("costForPipeline — time-window filtering", () => {
  test("sinceMs excludes rows timestamped before the cutoff", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 1.0, tsOffset:       -500 }); // in window
    await insertRow({ flowId, costTotal: 9.0, tsOffset: -1_000_000 }); // before cutoff
    const sinceMs = Date.now() - 10_000;
    const r = await costForPipeline([makeRunRaw({ flowId, pipelineId: "pipe_win" })], "pipe_win", { sinceMs, dbPath: LINEAGE_DB_PATH });
    assert.ok(r.total < 2, `expected ~1.0, got ${r.total}`);
  });

  test("sinceDays converts to sinceMs and filters correctly", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 3.0, tsOffset: -2 * 86_400_000 }); // 2 days ago → in window
    await insertRow({ flowId, costTotal: 9.0, tsOffset: -8 * 86_400_000 }); // 8 days ago → excluded
    const r = await costForPipeline(
      [makeRunRaw({ flowId, pipelineId: "pipe_sincedays" })],
      "pipe_sincedays",
      { sinceDays: 7, dbPath: LINEAGE_DB_PATH }
    );
    assert.ok(Math.abs(r.total - 3.0) < 1e-9, `expected 3.0, got ${r.total}`);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// costForAllPipelines
// ═══════════════════════════════════════════════════════════════════════════════

describe("costForAllPipelines — empty / null input", () => {
  test("returns empty struct when runs is null", async () => {
    const r = await costForAllPipelines(null);
    assert.deepEqual(r.pipelines, []);
    assert.equal(r.total.spend,    0);
    assert.equal(r.total.runs,     0);
    assert.equal(r.total.succeeded, 0);
    assert.equal(r.total.costPerSuccess, null);
  });

  test("returns empty struct when runs is []", async () => {
    const r = await costForAllPipelines([]);
    assert.deepEqual(r.pipelines, []);
  });

  test("returns empty when no runs have a resolvable pipelineId", async () => {
    const runs = [{ flowId: randomUUID(), controllerId: null, stateJson: {} }];
    const r = await costForAllPipelines(runs, DB);
    assert.deepEqual(r.pipelines, []);
  });

  test("skips runs with no flowId", async () => {
    const runs = [{ controllerId: "manifold:x", stateJson: { pipelineId: "x" } }];
    const r = await costForAllPipelines(runs, DB);
    assert.deepEqual(r.pipelines, []);
  });
});

describe("costForAllPipelines — aggregation", () => {
  test("groups runs by pipeline and sums their cost", async () => {
    const [fA, fB, fC] = [randomUUID(), randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 1.0 });
    await insertRow({ flowId: fB, costTotal: 2.0 });
    await insertRow({ flowId: fC, costTotal: 3.0 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_grp_x" }),
      makeRunRaw({ flowId: fB, pipelineId: "all_grp_x" }),
      makeRunRaw({ flowId: fC, pipelineId: "all_grp_y" }),
    ];
    const r = await costForAllPipelines(runs, DB);
    const px = r.pipelines.find((p) => p.pipelineId === "all_grp_x");
    const py = r.pipelines.find((p) => p.pipelineId === "all_grp_y");
    assert.ok(px, "all_grp_x missing");
    assert.ok(py, "all_grp_y missing");
    assert.ok(Math.abs(px.total - 3.0) < 1e-9);
    assert.ok(Math.abs(py.total - 3.0) < 1e-9);
    assert.equal(px.runCount, 2);
    assert.equal(py.runCount, 1);
  });

  test("pipelines are sorted by total descending", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 1.0 });
    await insertRow({ flowId: fB, costTotal: 5.0 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_sort_low" }),
      makeRunRaw({ flowId: fB, pipelineId: "all_sort_high" }),
    ];
    const r = await costForAllPipelines(runs, DB);
    const idxHigh = r.pipelines.findIndex((p) => p.pipelineId === "all_sort_high");
    const idxLow  = r.pipelines.findIndex((p) => p.pipelineId === "all_sort_low");
    assert.ok(idxHigh < idxLow, "high-cost pipeline should come before low-cost");
  });
});

describe("costForAllPipelines — total aggregate fields", () => {
  test("total.succeeded counts only succeeded runs", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 0.1 });
    await insertRow({ flowId: fB, costTotal: 0.1 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_succ", status: "succeeded" }),
      makeRunRaw({ flowId: fB, pipelineId: "all_succ", status: "failed" }),
    ];
    const r = await costForAllPipelines(runs, DB);
    assert.equal(r.total.succeeded, 1);
  });

  test("total.costPerSuccess = spend / succeeded", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 4.0 });
    await insertRow({ flowId: fB, costTotal: 6.0 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_cps", status: "succeeded" }),
      makeRunRaw({ flowId: fB, pipelineId: "all_cps", status: "succeeded" }),
    ];
    const r = await costForAllPipelines(runs, DB);
    // spend from these two = 10, succeeded = 2 → 5
    assert.ok(Math.abs(r.total.costPerSuccess - 5.0) < 1e-9);
  });

  test("total.costPerSuccess is null when succeeded = 0", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 1.0 });
    const r = await costForAllPipelines([makeRunRaw({ flowId, pipelineId: "all_cps_null", status: "failed" })], DB);
    assert.equal(r.total.costPerSuccess, null);
  });
});

describe("costForAllPipelines — succeededToday / failedToday / costToday", () => {
  const todayStart = (() => {
    const d = new Date();
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  })();

  test("succeededToday counts succeeded runs with updatedAt >= today UTC midnight", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 0.1 });
    await insertRow({ flowId: fB, costTotal: 0.1 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_succ_today", status: "succeeded", updatedAt: todayStart + 1_000 }),
      makeRunRaw({ flowId: fB, pipelineId: "all_succ_today", status: "succeeded", updatedAt: todayStart - 1_000 }),
    ];
    const p = (await costForAllPipelines(runs, DB)).pipelines.find((x) => x.pipelineId === "all_succ_today");
    assert.equal(p.succeededToday, 1);
  });

  test("failedToday counts failed runs with updatedAt >= today UTC midnight", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 0.1 });
    await insertRow({ flowId: fB, costTotal: 0.1 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_fail_today", status: "failed", updatedAt: todayStart + 1_000 }),
      makeRunRaw({ flowId: fB, pipelineId: "all_fail_today", status: "failed", updatedAt: todayStart - 1_000 }),
    ];
    const p = (await costForAllPipelines(runs, DB)).pipelines.find((x) => x.pipelineId === "all_fail_today");
    assert.equal(p.failedToday, 1);
  });

  test("costToday sums cost for runs with updatedAt >= today UTC midnight", async () => {
    const [fA, fB] = [randomUUID(), randomUUID()];
    await insertRow({ flowId: fA, costTotal: 3.0 });
    await insertRow({ flowId: fB, costTotal: 5.0 });
    const runs = [
      makeRunRaw({ flowId: fA, pipelineId: "all_cost_today", updatedAt: todayStart + 1_000 }),
      makeRunRaw({ flowId: fB, pipelineId: "all_cost_today", updatedAt: todayStart - 1_000 }),
    ];
    const p = (await costForAllPipelines(runs, DB)).pipelines.find((x) => x.pipelineId === "all_cost_today");
    assert.ok(Math.abs(p.costToday - 3.0) < 1e-9, `expected 3.0, got ${p.costToday}`);
  });
});

describe("costForAllPipelines — active-in-window gate (sinceMs)", () => {
  test("without sinceMs all runs are included regardless of createdAt", async () => {
    const flowId = randomUUID();
    // No DB rows — included because sinceMs is null
    const runs = [makeRunRaw({ flowId, pipelineId: "all_gate_none" })];
    const p = (await costForAllPipelines(runs, DB)).pipelines.find((x) => x.pipelineId === "all_gate_none");
    assert.ok(p, "run should be included when sinceMs is null");
  });

  test("with sinceMs: excludes runs created before window with no DB cost", async () => {
    const flowId = randomUUID();
    // No rows in DB → workedInWindow=false; createdAt before sinceMs → excluded
    const sinceMs = Date.now() - 5_000;
    const runs = [makeRunRaw({ flowId, pipelineId: "all_gate_out", createdAt: sinceMs - 60_000 })];
    const p = (await costForAllPipelines(runs, { sinceMs, dbPath: LINEAGE_DB_PATH })).pipelines.find((x) => x.pipelineId === "all_gate_out");
    assert.equal(p, undefined, "run outside window with no DB cost should be excluded");
  });

  test("with sinceMs: includes a run created before window if it has DB cost in window", async () => {
    const flowId = randomUUID();
    await insertRow({ flowId, costTotal: 2.0, tsOffset: -1_000 }); // ts is in window
    const sinceMs = Date.now() - 10_000;
    const runs = [makeRunRaw({ flowId, pipelineId: "all_gate_in", createdAt: sinceMs - 60_000 })];
    const p = (await costForAllPipelines(runs, { sinceMs, dbPath: LINEAGE_DB_PATH })).pipelines.find((x) => x.pipelineId === "all_gate_in");
    assert.ok(p, "run with DB cost in window should be included even if createdAt < sinceMs");
    assert.ok(Math.abs(p.total - 2.0) < 1e-9);
  });

  test("with sinceMs: includes a run created inside window even with no DB cost", async () => {
    const flowId = randomUUID();
    const sinceMs = Date.now() - 60_000;
    // createdAt is in-window but no DB rows
    const runs = [makeRunRaw({ flowId, pipelineId: "all_gate_created", createdAt: sinceMs + 1_000 })];
    const p = (await costForAllPipelines(runs, { sinceMs, dbPath: LINEAGE_DB_PATH })).pipelines.find((x) => x.pipelineId === "all_gate_created");
    assert.ok(p, "run created in window should be included even with zero DB cost");
  });
});
