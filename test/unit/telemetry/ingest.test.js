// The ingest contract: harnesses post semantic events, core derives the
// lineage/cost rows. No harness field names below.

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { applyTelemetryEvent, ingestKinds } from "../../../telemetry/ingest.js";

const logger = { info: () => {}, warn: () => {} };

function fakeStore() {
  const upserts = [];
  return {
    upserts,
    async getLineage() { return null; },
    async upsertLineage(row) { upserts.push(row); },
    async insertCost(row) { upserts.push({ cost: row }); },
  };
}

function fakeResolver({ row = null, chain = [] } = {}) {
  return {
    async resolve(session, opts = {}) { return row ? { ...row } : { key: session, kind: "session", source: opts.source }; },
    async walk() { return chain; },
  };
}

describe("ingestKinds", () => {
  it("is the semantic vocabulary, not harness hook names", () => {
    assert.deepEqual(ingestKinds().sort(), ["cost.usage", "lineage.session", "lineage.spawn"]);
  });
});

describe("applyTelemetryEvent", () => {
  it("rejects an unknown kind", async () => {
    const result = await applyTelemetryEvent({ kind: "nope" }, { getStore: () => fakeStore(), getResolver: () => fakeResolver(), logger });
    assert.equal(result.ok, false);
    assert.equal(result.code, "unknown_kind");
  });

  it("reports a store that is not ready instead of throwing", async () => {
    const result = await applyTelemetryEvent({ kind: "lineage.session", session: "s" }, { getStore: () => null, getResolver: () => null, logger });
    assert.equal(result.ok, false);
    assert.equal(result.code, "store_not_ready");
  });

  it("derives a lineage row for lineage.session", async () => {
    const store = fakeStore();
    const result = await applyTelemetryEvent(
      { kind: "lineage.session", source: "start", session: "s1", agentId: "a1", parentSession: "p1" },
      { getStore: () => store, getResolver: () => fakeResolver(), logger },
    );
    assert.equal(result.ok, true);
    assert.equal(store.upserts.length, 1);
    assert.equal(store.upserts[0].key, "s1");
    assert.equal(store.upserts[0].agent_id, "a1");
    assert.equal(store.upserts[0].parent_session_key, "p1");
    assert.equal(store.upserts[0].source, "start_hook");
  });

  it("does not re-derive a session already owned by spawn_hook", async () => {
    const store = fakeStore();
    store.getLineage = async () => ({ source: "spawn_hook" });
    await applyTelemetryEvent(
      { kind: "lineage.session", source: "start", session: "s1" },
      { getStore: () => store, getResolver: () => fakeResolver(), logger },
    );
    assert.equal(store.upserts.length, 0);
  });

  it("walks the parent chain for lineage.spawn and anchors the root", async () => {
    const store = fakeStore();
    const resolver = fakeResolver({
      row: { key: "child", kind: "unknown" },
      chain: [{ key: "mid", kind: "unknown" }, { key: "root", kind: "agent", rootLabel: "main" }],
    });
    const result = await applyTelemetryEvent(
      { kind: "lineage.spawn", session: "child", parentSession: "mid", agentId: "a1" },
      { getStore: () => store, getResolver: () => resolver, logger },
    );
    assert.equal(result.ok, true);
    const row = store.upserts[0];
    assert.equal(row.child_session_key, "child");
    assert.equal(row.parent_session_key, "mid");
    assert.equal(row.root_session_key, "root");
    assert.equal(row.root_kind, "agent");
    assert.equal(row.root_label, "main");
    assert.equal(row.source, "spawn_hook");
  });

  it("drops a cost event with no usage without throwing", async () => {
    const store = fakeStore();
    const result = await applyTelemetryEvent(
      { kind: "cost.usage", session: "s1" },
      { getStore: () => store, getResolver: () => fakeResolver(), logger },
    );
    assert.equal(result.ok, true);
    assert.equal(store.upserts.length, 0);
  });

  it("logs and reports a failing apply instead of rejecting", async () => {
    const store = fakeStore();
    store.upsertLineage = async () => { throw new Error("db down"); };
    const result = await applyTelemetryEvent(
      { kind: "lineage.session", session: "s1" },
      { getStore: () => store, getResolver: () => fakeResolver(), logger },
    );
    assert.equal(result.ok, false);
    assert.equal(result.code, "apply_failed");
    assert.match(result.error, /db down/);
  });
});
