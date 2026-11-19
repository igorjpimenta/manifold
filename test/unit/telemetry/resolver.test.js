// Unit tests for telemetry/resolver.js — session → parent/root/breadcrumbs.
// Three layers: lineage cache, session grammar, external joins (flow_runs
// sqlite, cron jobs.json). The store is a fake with only the two members the
// resolver touches; the external fixtures are real temp files.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createClient } from "@libsql/client";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { makeResolver, rootLabelOf } from "../../../telemetry/resolver.js";

const CRON_KEY = "agent:scout:cron-nightly";
const CRON_ID = "11111111-2222-3333-4444-555555555555";
const TICK_KEY = `agent:scout:cron:${CRON_ID}:run:abc123`;
const BARE_SUB = "agent:scout:subagent:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

function fakeStore({ rows = {}, client = null } = {}) {
  return {
    getLineage: async (key) => rows[key] ?? null,
    _client: client ?? { execute: async () => ({ rows: [], columns: [] }) },
  };
}

function tempHome() {
  return mkdtempSync(path.join(os.tmpdir(), "resolver-test-"));
}

async function writeFlowsDb(dbPath, flows) {
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute(`CREATE TABLE flow_runs (flow_id text, owner_key text, pipeline_id text, status text, created_at integer)`);
  for (const f of flows) {
    await client.execute({
      sql: "INSERT INTO flow_runs VALUES (?, ?, ?, ?, ?)",
      args: [f.flow_id, f.owner_key ?? "graph-flow-owner", f.pipeline_id ?? null, f.status ?? "succeeded", f.created_at ?? 1],
    });
  }
  client.close();
}

describe("walk — lineage cache", () => {
  test("a cached leaf resolves through its cached parent and takes the cached root", async () => {
    const store = fakeStore({
      rows: {
        [BARE_SUB]: {
          kind: "bare-subagent", agent_id: "scout",
          parent_session_key: CRON_KEY, root_session_key: CRON_KEY,
          root_kind: "cron-owner", root_label: "Nightly",
          pipeline_id: null, flow_id: null, node_id: null, item_key: null,
        },
        [CRON_KEY]: {
          kind: "cron-owner", agent_id: "scout", cron_name: "nightly",
          parent_session_key: null, root_session_key: CRON_KEY,
          root_kind: "cron-owner", root_label: "Nightly",
        },
      },
    });
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const chain = await resolver.walk(BARE_SUB);
    assert.equal(chain.length, 2);
    assert.equal(chain[0].key, BARE_SUB);
    assert.equal(chain[1].key, CRON_KEY);
    for (const frame of chain) {
      assert.equal(frame.rootKey, CRON_KEY);
      assert.equal(frame.rootKind, "cron-owner");
      assert.equal(frame.rootLabel, "Nightly");
    }
  });

  test("a cycle stops with a synthetic cycle frame instead of looping", async () => {
    const store = fakeStore({
      rows: {
        A: { kind: "explicit", parent_session_key: "B", root_session_key: "A", root_kind: "explicit" },
        B: { kind: "explicit", parent_session_key: "A", root_session_key: "A", root_kind: "explicit" },
      },
    });
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const chain = await resolver.walk("A");
    assert.ok(chain.some((f) => f.kind === "cycle"), "cycle frame marks where the walk stopped");
    assert.ok(chain.length <= 3);
  });

  test("useCache:false skips the lineage table and classifies live", async () => {
    let reads = 0;
    const store = {
      getLineage: async () => { reads += 1; return { kind: "cron-owner", parent_session_key: null }; },
      _client: { execute: async () => ({ rows: [], columns: [] }) },
    };
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const chain = await resolver.walk(CRON_KEY, { useCache: false });
    assert.equal(reads, 0);
    assert.equal(chain[0].kind, "cron-owner");
  });

  test("a stale cached root on a synthetic flow key is re-derived, not propagated", async () => {
    const store = fakeStore({
      rows: {
        [BARE_SUB]: {
          kind: "bare-subagent", parent_session_key: CRON_KEY,
          root_session_key: "flow:stale", root_kind: "unknown", root_label: null,
        },
        [CRON_KEY]: { kind: "cron-owner", parent_session_key: null, root_session_key: CRON_KEY, root_kind: "cron-owner", root_label: "Nightly" },
      },
    });
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const chain = await resolver.walk(BARE_SUB);
    assert.notEqual(chain[0].rootKey, "flow:stale");
    assert.equal(chain[0].rootKey, CRON_KEY);
  });
});

describe("classifyWithExternal — synthetic flow keys", () => {
  test("flow:<id> with a trigger edge returns the edge's root and stops walking", async () => {
    const flowId = "1234abcd-0000-0000-0000-000000000000";
    const store = fakeStore({
      rows: {
        [`flow:${flowId}`]: {
          kind: "flow-triggered", pipeline_id: "p_review",
          parent_session_key: "agent:operator:cli:x",
          root_session_key: CRON_KEY, root_kind: "cron-owner", root_label: "Nightly",
        },
      },
    });
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const out = await resolver.classifyWithExternal(`flow:${flowId}`);
    assert.equal(out.kind, "flow-triggered");
    assert.equal(out.parentKey, null);
    assert.equal(out.pipelineId, "p_review");
    assert.equal(out.rootKey, CRON_KEY);
  });

  test("flow:<id> before the edge lands: pipelineId comes from flow_runs, walk still terminates", async () => {
    const home = tempHome();
    try {
      const flowsDb = path.join(home, "runtime.sqlite");
      await writeFlowsDb(flowsDb, [{ flow_id: "9999aaaa-0000-0000-0000-000000000000", pipeline_id: "p_discuss" }]);
      const resolver = makeResolver({
        store: fakeStore(), flowsDbPath: flowsDb, cronJsonPath: path.join(home, "none.json"),
      });
      const chain = await resolver.walk("flow:9999aaaa-0000-0000-0000-000000000000");
      assert.equal(chain.length, 1, "no parent → the synthetic key is its own root");
      assert.equal(chain[0].kind, "flow-triggered");
      assert.equal(chain[0].pipelineId, "p_discuss");
      assert.equal(chain[0].rootLabel, "p_discuss");
    } finally {
      rmSync(home, { recursive: true, force: true });
    }
  });

  test("flow:<id> absent from both lineage and flow_runs degrades to nulls, never a throw", async () => {
    const resolver = makeResolver({
      store: fakeStore(),
      flowsDbPath: "/nonexistent.sqlite",
      cronJsonPath: "/nonexistent.json",
    });
    const out = await resolver.classifyWithExternal("flow:7777abcd");
    assert.equal(out.kind, "flow-triggered");
    assert.equal(out.parentKey, null);
    assert.equal(out.pipelineId, null);
    assert.equal(out.rootKey, null);
    assert.equal(out.rootLabel, null);
  });
});

describe("external joins", () => {
  test("a cron tick joins through jobs.json to the cron owner session", async () => {
    const home = tempHome();
    try {
      const jobs = path.join(home, "jobs.json");
      writeFileSync(jobs, JSON.stringify({ jobs: [{ id: CRON_ID, name: "nightly", sessionKey: CRON_KEY }] }), "utf8");
      const resolver = makeResolver({
        store: fakeStore(), flowsDbPath: path.join(home, "none.sqlite"), cronJsonPath: jobs,
      });
      const chain = await resolver.walk(TICK_KEY);
      assert.equal(chain.length, 2);
      assert.equal(chain[0].kind, "cron-tick");
      assert.equal(chain[1].key, CRON_KEY);
      assert.equal(chain[1].kind, "cron-owner");
      assert.equal(chain[0].rootLabel, "nightly");
    } finally {
      rmSync(home, { recursive: true, force: true });
    }
  });

  test("a missing or corrupt jobs.json degrades to no parent join", async () => {
    const home = tempHome();
    try {
      const jobs = path.join(home, "jobs.json");
      writeFileSync(jobs, "{not json", "utf8");
      const resolver = makeResolver({
        store: fakeStore(), flowsDbPath: path.join(home, "none.sqlite"), cronJsonPath: jobs,
      });
      const out = await resolver.classifyWithExternal(TICK_KEY);
      assert.equal(out.parentKey, null);
      assert.equal(out.kind, "cron-tick");
    } finally {
      rmSync(home, { recursive: true, force: true });
    }
  });

  test("flow_runs lookup prefers the most recent of colliding prefixes", async () => {
    const home = tempHome();
    try {
      const flowsDb = path.join(home, "runtime.sqlite");
      await writeFlowsDb(flowsDb, [
        { flow_id: "aaaa1111-0000-0000-0000-000000000000", pipeline_id: "old", created_at: 10 },
        { flow_id: "aaaa1111-0000-0000-0000-000000000001", pipeline_id: "new", created_at: 20 },
      ]);
      const resolver = makeResolver({ store: fakeStore(), flowsDbPath: flowsDb, cronJsonPath: path.join(home, "n.json") });
      const hit = await resolver.flowFor("aaaa1111");
      assert.equal(hit.pipeline_id, "new");
    } finally {
      rmSync(home, { recursive: true, force: true });
    }
  });
});

describe("resolve — storeable row", () => {
  test("inherits pipeline/flow/node/item from the nearest ancestor that carries them", async () => {
    const parent = CRON_KEY;
    const store = fakeStore({
      rows: {
        [BARE_SUB]: {
          kind: "bare-subagent", agent_id: "scout", parent_session_key: parent,
          root_session_key: parent, root_kind: "cron-owner", root_label: "Nightly",
          pipeline_id: null, flow_id: null, node_id: null, item_key: null,
        },
        [parent]: {
          kind: "cron-owner", agent_id: "scout", parent_session_key: null,
          root_session_key: parent, root_kind: "cron-owner", root_label: "Nightly",
          pipeline_id: "p_review", flow_id: "f-1", node_id: "STEP", item_key: "item-1",
        },
      },
    });
    const resolver = makeResolver({ store, flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json" });
    const row = await resolver.resolve(BARE_SUB, { source: "hook", firstSeenAt: 42 });
    assert.equal(row.child_session_key, BARE_SUB);
    assert.equal(row.parent_session_key, parent);
    assert.equal(row.root_session_key, parent);
    assert.equal(row.kind, "bare-subagent");
    assert.equal(row.root_kind, "cron-owner");
    assert.equal(row.root_label, "Nightly");
    assert.equal(row.pipeline_id, "p_review");
    assert.equal(row.flow_id, "f-1");
    assert.equal(row.node_id, "STEP");
    assert.equal(row.item_key, "item-1");
    assert.equal(row.first_seen_at, 42);
    assert.equal(row.source, "hook");
  });

  test("defaults: lazy_resolve source, current time, null parent at the root", async () => {
    const resolver = makeResolver({
      store: fakeStore(), flowsDbPath: "/nonexistent.sqlite", cronJsonPath: "/nonexistent.json",
    });
    const before = Date.now();
    const row = await resolver.resolve(CRON_KEY);
    assert.equal(row.parent_session_key, null);
    assert.equal(row.source, "lazy_resolve");
    assert.ok(row.first_seen_at >= before);
  });
});

describe("rootLabelOf", () => {
  test("labels each root kind from its own field", () => {
    assert.equal(rootLabelOf({ kind: "cron-owner", cronName: "nightly", key: "k" }), "nightly");
    assert.equal(rootLabelOf({ kind: "telegram-dm", telegramChatId: "42", key: "k" }), "42");
    assert.equal(rootLabelOf({ kind: "explicit", explicitPurpose: "triage", key: "k" }), "triage");
    assert.equal(rootLabelOf({ kind: "agent-default", agentId: "scout", key: "k" }), "agent:scout");
    assert.equal(rootLabelOf({ kind: "memory-synth-chunk", memorySynthRun: "r1", key: "k" }), "memory-synth:r1");
    assert.equal(rootLabelOf({ kind: "flow-prime", primeRef: "abc", key: "k" }), "flow-prime:abc");
    assert.equal(rootLabelOf({ kind: "bare-subagent", bareUuid: "abcdef123", key: "k" }), "bare-subagent:abcdef12");
    assert.equal(rootLabelOf({ kind: "unknown", key: "agent:x:y" }), "agent:x:y");
    assert.equal(rootLabelOf(null), null);
  });

  test("operator-trigger distinguishes per flow, falling back to the group label", () => {
    assert.equal(
      rootLabelOf({ kind: "operator-trigger", triggerFlowId: "abcdef12-3456", key: "k" }),
      "Operator run abcdef12",
    );
    assert.equal(rootLabelOf({ kind: "operator-trigger", key: "k" }), "Operator runs");
  });
});
