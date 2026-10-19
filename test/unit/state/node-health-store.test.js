/**
 * Unit tests for state/node-health-store.js
 *
 * Covers: schema creation, markUnhealthy/isBlocked/reset round-trip,
 * markUnhealthy idempotency while already blocked, and list scoping.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createNodeHealthStore } from "../../../state/node-health-store.js";

function tmpDb() {
  return path.join(os.tmpdir(), `node-health-store-test-${randomUUID()}.sqlite`);
}

async function makeStore(overrides = {}) {
  return createNodeHealthStore({ dbPath: tmpDb(), ...overrides });
}

describe("createNodeHealthStore — schema", () => {
  test("opens DB and creates node_health table without throwing", async () => {
    const store = await makeStore();
    assert.equal(await store.isBlocked("pipe_a", "SCAN_AGENT"), false);
    store.close();
  });
});

describe("createNodeHealthStore — markUnhealthy / isBlocked / reset", () => {
  test("unblocked node is not blocked", async () => {
    const store = await makeStore();
    assert.equal(await store.isBlocked("pipe_a", "SCAN_AGENT"), false);
    store.close();
  });

  test("markUnhealthy blocks the node", async () => {
    const store = await makeStore();
    // markUnhealthy() defers through batch-writer and no longer returns a
    // value — its only production caller never inspected one. Assert the
    // effect after an explicit flush instead.
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "boom", flowId: "flow-1" });
    await store.flush();
    assert.equal(await store.isBlocked("pipe_a", "SCAN_AGENT"), true);
    store.close();
  });

  test("markUnhealthy while already blocked is a no-op (idempotent)", async () => {
    const store = await makeStore();
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "first", flowId: "flow-1" });
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "second", flowId: "flow-2" });
    await store.flush();
    const rows = await store.list("pipe_a", { status: "blocked" });
    assert.equal(rows.length, 1, "only one active blocked row should exist — the second mark no-op'd via the partial unique index");
    assert.equal(rows[0].reason, "first", "original block reason should be preserved");
    store.close();
  });

  test("reset flags the block resolved rather than deleting it", async () => {
    const store = await makeStore();
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "boom", flowId: "flow-1" });
    await store.flush(); // reset()'s WHERE status='blocked' guard needs the row committed first
    const r = await store.reset("pipe_a", "SCAN_AGENT", { via: "operator" });
    assert.equal(r.reset, 1);
    assert.equal(await store.isBlocked("pipe_a", "SCAN_AGENT"), false);
    const history = await store.list("pipe_a");
    assert.equal(history.length, 1, "row should survive reset, not be deleted");
    assert.equal(history[0].status, "resolved");
    assert.equal(history[0].resolved_via, "operator");
    assert.ok(history[0].ended_at);
    store.close();
  });

  test("reset on a node that was never blocked is a no-op", async () => {
    const store = await makeStore();
    const r = await store.reset("pipe_a", "NEVER_BLOCKED");
    assert.equal(r.reset, 0);
    store.close();
  });

  test("a node can be re-blocked after a reset (new episode)", async () => {
    const store = await makeStore();
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "first", flowId: "flow-1" });
    await store.flush();
    await store.reset("pipe_a", "SCAN_AGENT", { via: "operator" });
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "second episode", flowId: "flow-2" });
    await store.flush();
    assert.equal(await store.isBlocked("pipe_a", "SCAN_AGENT"), true);
    const history = await store.list("pipe_a");
    assert.equal(history.length, 2, "both episodes should be present in history");
    store.close();
  });
});

describe("createNodeHealthStore — list scoping", () => {
  test("list scopes by pipelineId and status independently", async () => {
    const store = await makeStore();
    await store.markUnhealthy("pipe_a", "SCAN_AGENT", { reason: "a" });
    await store.markUnhealthy("pipe_a", "ENSURE_LOGIN_AGENT", { reason: "b" });
    await store.markUnhealthy("pipe_b", "SCAN_AGENT", { reason: "c" });
    await store.flush(); // reset() below needs pipe_a/SCAN_AGENT's row committed first
    await store.reset("pipe_a", "SCAN_AGENT");

    assert.equal((await store.list("pipe_a")).length, 2);
    assert.equal((await store.list("pipe_a", { status: "blocked" })).length, 1);
    assert.equal((await store.list("pipe_a", { status: "resolved" })).length, 1);
    assert.equal((await store.list("pipe_b")).length, 1);
    assert.equal((await store.list()).length, 3, "no pipelineId filter returns everything");
    store.close();
  });
});
