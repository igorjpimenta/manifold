/**
 * Unit tests for state/pipeline-registry.js
 *
 * Covers: schema creation; reconcilePipelineVersions' SCD2 diffing (new
 * pipeline registers, unchanged pipeline no-ops, changed node
 * retires-and-inserts, removed node/pipeline retires without inserting);
 * getNodeVersionAt/getNodeVersionsBatch/getPipelineVersionAt's
 * point-in-time correctness (returns the version active at a given
 * timestamp, not always the latest).
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createPipelineRegistryStore } from "../../../state/pipeline-registry.js";
import { openDb } from "../../../state/db.js";

function tmpDb() {
  return path.join(os.tmpdir(), `pipeline-registry-test-${randomUUID()}.sqlite`);
}

function makeStore() {
  return createPipelineRegistryStore(tmpDb());
}

async function ensurePipeline(store, id) {
  await store._db.execute({
    sql: `INSERT INTO pipelines (id, name, created_at, updated_at)
          VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
    args: [id, id, Date.now(), Date.now()],
  });
}

function pipelinesMap(entries) {
  return new Map(entries.map((p) => [p.id, p]));
}

// ─── schema ─────────────────────────────────────────────────────────────────

describe("createPipelineRegistryStore — schema", () => {
  test("creates both tables without throwing", async () => {
    const store = makeStore();
    const { rows } = await store._db.execute(
      "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('pipeline_versions', 'pipeline_node_versions')"
    );
    assert.equal(rows.length, 2);
    store.close();
  });

  test("only one active (retired_at IS NULL) row per pipeline_id is allowed", async () => {
    const store = makeStore();
    await ensurePipeline(store, "p1");
    await store._db.execute({
      sql: `INSERT INTO pipeline_versions (id, pipeline_id, is_synthetic, content_hash, registered_at, retired_at)
            VALUES (?, 'p1', 0, 'h1', 1000, NULL)`,
      args: [randomUUID()],
    });
    await assert.rejects(() => store._db.execute({
      sql: `INSERT INTO pipeline_versions (id, pipeline_id, is_synthetic, content_hash, registered_at, retired_at)
            VALUES (?, 'p1', 0, 'h2', 2000, NULL)`,
      args: [randomUUID()],
    }));
    store.close();
  });

  test("only one active row per (pipeline_id, node_name) is allowed", async () => {
    const store = makeStore();
    const pvId = randomUUID();
    await ensurePipeline(store, "p1");
    await store._db.execute({
      sql: `INSERT INTO pipeline_versions (id, pipeline_id, is_synthetic, content_hash, registered_at, retired_at)
            VALUES (?, 'p1', 0, 'h1', 1000, NULL)`,
      args: [pvId],
    });
    await store._db.execute({
      sql: `INSERT INTO pipeline_node_versions (id, pipeline_version_id, pipeline_id, node_name, kind, content_hash, registered_at, retired_at)
            VALUES (?, ?, 'p1', 'N1', 'code', 'nh1', 1000, NULL)`,
      args: [randomUUID(), pvId],
    });
    await assert.rejects(() => store._db.execute({
      sql: `INSERT INTO pipeline_node_versions (id, pipeline_version_id, pipeline_id, node_name, kind, content_hash, registered_at, retired_at)
            VALUES (?, ?, 'p1', 'N1', 'code', 'nh2', 2000, NULL)`,
      args: [randomUUID(), pvId],
    }));
    store.close();
  });
});

// ─── reconcilePipelineVersions ────────────────────────────────────────────────

describe("reconcilePipelineVersions", () => {
  test("a new pipeline registers a pipeline_versions row and one pipeline_node_versions row per node", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code" }, B: { kind: "agent", agent: "scorer" } } },
    ]));
    const { rows: pv } = await store._db.execute("SELECT * FROM pipeline_versions WHERE pipeline_id = 'p1'");
    assert.equal(pv.length, 1);
    assert.equal(pv[0].retired_at, null);
    const { rows: nv } = await store._db.execute("SELECT * FROM pipeline_node_versions WHERE pipeline_id = 'p1' ORDER BY node_name");
    assert.equal(nv.length, 2);
    assert.equal(nv[0].node_name, "A");
    assert.equal(nv[0].kind, "code");
    assert.equal(nv[1].node_name, "B");
    assert.equal(nv[1].kind, "agent");
    assert.equal(nv[1].agent, "scorer");
    store.close();
  });

  test("reconciling the identical pipeline twice is a true no-op — no new rows, same ids", async () => {
    const store = makeStore();
    const map = pipelinesMap([{ id: "p1", nodes: { A: { kind: "code" } } }]);
    await store.reconcilePipelineVersions(map);
    const { rows: before } = await store._db.execute("SELECT id FROM pipeline_node_versions WHERE pipeline_id = 'p1'");
    await store.reconcilePipelineVersions(map);
    const { rows: after } = await store._db.execute("SELECT id FROM pipeline_node_versions WHERE pipeline_id = 'p1'");
    assert.equal(after.length, 1);
    assert.equal(before[0].id, after[0].id, "no-op must not touch the existing row's identity");
    store.close();
  });

  test("a changed node retires the old active row and inserts a new one", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "agent", agent: "scorer-v1" } } },
    ]));
    const { rows: before } = await store._db.execute("SELECT id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "agent", agent: "scorer-v2" } } },
    ]));
    const { rows: all } = await store._db.execute("SELECT id, agent, retired_at FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    assert.equal(all.length, 2, "old row stays, new row added — never deleted");
    const oldRow = all.find((r) => r.id === before[0].id);
    const newRow = all.find((r) => r.id !== before[0].id);
    assert.notEqual(oldRow.retired_at, null, "old row must be retired");
    assert.equal(newRow.retired_at, null, "new row must be active");
    assert.equal(newRow.agent, "scorer-v2");
    store.close();
  });

  test("a node removed from the Map retires its active row without inserting a replacement", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code" }, B: { kind: "code" } } },
    ]));
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code" } } }, // B removed
    ]));
    const { rows } = await store._db.execute("SELECT retired_at FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'B'");
    assert.equal(rows.length, 1, "B's last version stays queryable, just retired — never deleted");
    assert.notEqual(rows[0].retired_at, null);
    store.close();
  });

  test("a whole pipeline removed from the Map retires both its pipeline_versions row and every active node row", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code" } } },
      { id: "p2", nodes: { X: { kind: "code" } } },
    ]));
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p2", nodes: { X: { kind: "code" } } }, // p1 gone entirely
    ]));
    const { rows: pv } = await store._db.execute("SELECT retired_at FROM pipeline_versions WHERE pipeline_id = 'p1'");
    assert.notEqual(pv[0].retired_at, null);
    const { rows: nv } = await store._db.execute("SELECT retired_at FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    assert.notEqual(nv[0].retired_at, null);
    store.close();
  });

  test("synthetic subflow pipelines set is_synthetic and parent_pipeline_id from the loader's own flags", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1::SCORE", nodes: { A: { kind: "code" } }, _synthetic: true, _parentPipelineId: "p1", _parentNodeId: "SCORE" },
    ]));
    const { rows } = await store._db.execute("SELECT is_synthetic, parent_pipeline_id FROM pipeline_versions WHERE pipeline_id = 'p1::SCORE'");
    assert.equal(rows[0].is_synthetic, 1);
    assert.equal(rows[0].parent_pipeline_id, "p1");
    store.close();
  });

  test("a mode change alone does not retire a node — mode isn't part of the content hash for live-behavior reasons, but IS still stored", async () => {
    // Scope note (see pipeline-registry.js's own explicit scope-boundary
    // comment): mode is captured for completeness but the live mode
    // check (child-resume-trigger.js) never reads this table — this test
    // only confirms the column is populated, not that changing mode alone
    // triggers a new version (it does, since mode IS part of the node
    // content hash — see nodeContentHash).
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "subflow", mode: "any" } } },
    ]));
    const { rows } = await store._db.execute("SELECT mode FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    assert.equal(rows[0].mode, "any");
    store.close();
  });

  test("empty or missing Map is a safe no-op", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(new Map());
    await store.reconcilePipelineVersions(null);
    const { rows } = await store._db.execute("SELECT COUNT(*) as c FROM pipeline_versions");
    assert.equal(Number(rows[0].c), 0);
    store.close();
  });
});

// ─── point-in-time lookups ─────────────────────────────────────────────────

describe("getNodeVersionAt / getNodeVersionsBatch — point-in-time correctness", () => {
  test("returns the version active AT the given timestamp, not the latest", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "agent", agent: "v1" } } },
    ]));
    const t1 = Date.now();
    await new Promise((r) => setTimeout(r, 2));
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "agent", agent: "v2" } } },
    ]));
    const t2 = Date.now();

    const atT1 = await store.getNodeVersionAt({ pipelineId: "p1", nodeName: "A", atMs: t1 });
    const atT2 = await store.getNodeVersionAt({ pipelineId: "p1", nodeName: "A", atMs: t2 });
    assert.equal(atT1.agent, "v1", "a timestamp from before the change must resolve to the OLD version");
    assert.equal(atT2.agent, "v2");
    store.close();
  });

  // A timestamp older than the earliest recorded version is expected, not
  // an edge case — every flow that dispatched before this feature first
  // started tracking pipelines has real invocations older than anything
  // reconcile ever saw. Falls back to the earliest known version rather
  // than null: returning null would lose correct rendering for every
  // invocation older than tracking began.
  test("a timestamp before any version was ever registered falls back to the EARLIEST known version, not null", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code" } } }]));
    const result = await store.getNodeVersionAt({ pipelineId: "p1", nodeName: "A", atMs: 1 });
    assert.equal(result.kind, "code", "falls back to the earliest known version instead of losing the row entirely");
    store.close();
  });

  test("an unknown (pipelineId, nodeName) pair resolves to null, not a throw", async () => {
    const store = makeStore();
    const result = await store.getNodeVersionAt({ pipelineId: "never_existed", nodeName: "X", atMs: Date.now() });
    assert.equal(result, null);
    store.close();
  });

  test("getNodeVersionsBatch returns results in the same order as requests, one query per distinct pair not per request", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code" }, B: { kind: "agent", agent: "x" } } },
    ]));
    const now = Date.now();
    const results = await store.getNodeVersionsBatch([
      { pipelineId: "p1", nodeName: "B", atMs: now },
      { pipelineId: "p1", nodeName: "A", atMs: now },
      { pipelineId: "p1", nodeName: "A", atMs: now }, // duplicate pair, same request list
      { pipelineId: "unknown", nodeName: "Z", atMs: now },
    ]);
    assert.equal(results.length, 4);
    assert.equal(results[0].kind, "agent");
    assert.equal(results[1].kind, "code");
    assert.equal(results[2].kind, "code");
    assert.equal(results[3], null);
    store.close();
  });

  test("empty requests array returns empty array", async () => {
    const store = makeStore();
    assert.deepEqual(await store.getNodeVersionsBatch([]), []);
    store.close();
  });
});

// ─── node identity (node_id) across a rename ───────────────────────────────

describe("node_id — persistent identity, author-declared or auto-minted", () => {
  test("a node with no declared id gets one auto-minted", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code" } } }]));
    const { rows } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    assert.ok(rows[0].node_id, "must have a non-null minted id");
    store.close();
  });

  test("a node WITH a declared id uses that exact value verbatim, not a minted one", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { A: { kind: "code", id: "author-chosen-slug" } } },
    ]));
    const { rows } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A'");
    assert.equal(rows[0].node_id, "author-chosen-slug");
    store.close();
  });

  test("a content-only change (same node_name, no declared id) keeps the same auto-minted identity across retire/reinsert", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "agent", agent: "v1" } } }]));
    const { rows: before } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A' AND retired_at IS NULL");
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "agent", agent: "v2" } } }]));
    const { rows: after } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A' AND retired_at IS NULL");
    assert.equal(after[0].node_id, before[0].node_id, "same node_name must keep its identity even as content drifts");
    store.close();
  });

  test("renaming a node's object key while keeping the SAME declared id preserves identity — no separate rename step", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { VERIFY_SUBMIT: { kind: "code", id: "submit-step" } } },
    ]));
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { SUBMIT_SUMMARY: { kind: "code", id: "submit-step" } } }, // renamed key, same declared id
    ]));
    const { rows } = await store._db.execute("SELECT node_name, node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_id = 'submit-step'");
    assert.deepEqual(rows.map((r) => r.node_name).sort(), ["SUBMIT_SUMMARY", "VERIFY_SUBMIT"], "both names share the one declared identity");
    store.close();
  });

  test("a rename chain (renamed twice, id kept constant throughout) shares one identity across all names", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code", id: "fixed-id" } } }]));
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { B: { kind: "code", id: "fixed-id" } } }]));
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { C: { kind: "code", id: "fixed-id" } } }]));
    const { rows } = await store._db.execute("SELECT DISTINCT node_name FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_id = 'fixed-id'");
    assert.deepEqual(rows.map((r) => r.node_name).sort(), ["A", "B", "C"]);
    store.close();
  });

  test("two node_names sharing the same declared id at the SAME time is a supported N:1 merge, no special-casing needed", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { OLD_A: { kind: "code", id: "merged-id" }, OLD_B: { kind: "code", id: "merged-id" } } },
    ]));
    const { rows } = await store._db.execute("SELECT node_name FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_id = 'merged-id' AND retired_at IS NULL ORDER BY node_name");
    assert.deepEqual(rows.map((r) => r.node_name), ["OLD_A", "OLD_B"]);
    store.close();
  });

  test("an undeclared node renamed WITHOUT keeping a declared id gets a fresh identity — expected, not a bug (opt-in trade-off)", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { VERIFY_SUBMIT: { kind: "code" } } }]));
    const { rows: oldRow } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'VERIFY_SUBMIT'");
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { SUBMIT_SUMMARY: { kind: "code" } } }]));
    const { rows: newRow } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'SUBMIT_SUMMARY'");
    assert.notEqual(newRow[0].node_id, oldRow[0].node_id, "no declared id means no continuity — this is the documented limitation, not a defect");
    store.close();
  });

  // Regression: nodeContentHash originally covered only {kind, agent, mode}
  // — _reconcileOneNode's hash-match short-circuit meant retroactively
  // adding `id` to an already-existing, otherwise-unchanged node was a
  // silent no-op (the declared value was never even read). This is
  // exactly the retroactive-backfill use case, so it must actually apply.
  test("retroactively declaring id on an already-existing, otherwise-unchanged node DOES take effect (not swallowed by the hash-match short-circuit)", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code" } } }]));
    const { rows: before } = await store._db.execute("SELECT node_id FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A' AND retired_at IS NULL");
    const mintedId = before[0].node_id;
    // Same kind, same node_name — only difference is the new declaration.
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code", id: "hand-declared" } } }]));
    const { rows: after } = await store._db.execute("SELECT node_id, content_hash FROM pipeline_node_versions WHERE pipeline_id = 'p1' AND node_name = 'A' AND retired_at IS NULL");
    assert.equal(after[0].node_id, "hand-declared", "the declared id must actually be applied, not silently ignored");
    assert.notEqual(after[0].node_id, mintedId);
    store.close();
  });
});

describe("getNodeLineageBatch", () => {
  test("returns the full historical node_name set for a node whose identity was carried across a rename", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { VERIFY_SUBMIT: { kind: "code", id: "submit-step" } } },
    ]));
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", nodes: { SUBMIT_SUMMARY: { kind: "code", id: "submit-step" } } },
    ]));
    const result = await store.getNodeLineageBatch("p1", ["SUBMIT_SUMMARY"]);
    assert.deepEqual([...result.get("SUBMIT_SUMMARY")].sort(), ["SUBMIT_SUMMARY", "VERIFY_SUBMIT"]);
    store.close();
  });

  test("a never-renamed node resolves to a singleton set containing just itself", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: { A: { kind: "code" } } }]));
    const result = await store.getNodeLineageBatch("p1", ["A"]);
    assert.deepEqual([...result.get("A")], ["A"]);
    store.close();
  });

  test("a node with no active row at all still resolves to a singleton set, not undefined", async () => {
    const store = makeStore();
    const result = await store.getNodeLineageBatch("never_existed", ["Z"]);
    assert.deepEqual([...result.get("Z")], ["Z"]);
    store.close();
  });

  test("empty nodeIds array returns an empty Map", async () => {
    const store = makeStore();
    assert.deepEqual(await store.getNodeLineageBatch("p1", []), new Map());
    store.close();
  });
});

describe("getPipelineVersionAt — pipeline-level point-in-time", () => {
  test("returns kind/allowConcurrent active at the given timestamp", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([
      { id: "p1", kind: "cascade", allowConcurrent: 3, nodes: {} },
    ]));
    const result = await store.getPipelineVersionAt({ pipelineId: "p1", atMs: Date.now() });
    assert.equal(result.kind, "cascade");
    assert.equal(result.allowConcurrent, 3);
    store.close();
  });

  test("a non-cascade pipeline has kind: null, not the string 'undefined'", async () => {
    const store = makeStore();
    await store.reconcilePipelineVersions(pipelinesMap([{ id: "p1", nodes: {} }]));
    const result = await store.getPipelineVersionAt({ pipelineId: "p1", atMs: Date.now() });
    assert.equal(result.kind, null);
    store.close();
  });

  test("unknown pipelineId resolves to null", async () => {
    const store = makeStore();
    const result = await store.getPipelineVersionAt({ pipelineId: "never_existed", atMs: Date.now() });
    assert.equal(result, null);
    store.close();
  });
});
