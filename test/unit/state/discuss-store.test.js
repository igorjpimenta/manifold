// Unit tests for state/discuss-store.js — covers createDiscussStore schema setup,
// getState, appendMessage, stampMessageError, setTurnStatus, setPending,
// mergePendingFields, clearPending, setComponent, getComponents,
// getBulkSummaries, migration idempotency, ensureRow safety, and tryParse edge cases.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createDiscussStore } from "../../../state/discuss-store.js";

// ─── helpers ────────────────────────────────────────────────────────────────

function tmpDb() {
  return path.join(os.tmpdir(), `gf-discuss-test-${randomUUID()}.sqlite`);
}

async function makeStore(overrides = {}) {
  return createDiscussStore({ dbPath: tmpDb(), ...overrides });
}

// ─── createDiscussStore — schema + migration ──────────────────────────────────

describe("createDiscussStore — DB setup", () => {
  test("creates DB and table without throwing", async () => {
    const store = await makeStore();
    // Verify table exists by querying it
    const rows = await store.getState(randomUUID(), randomUUID());
    assert.equal(rows, null); // no row yet — just confirms table exists
  });

  test("schema is idempotent — opening twice on the same file does not throw", async () => {
    const dbPath = tmpDb();
    const s1 = createDiscussStore({ dbPath });
    await s1.close();
    const s2 = createDiscussStore({ dbPath });
    await s2.close();
  });

  test("migration column components_json exists after open", async () => {
    // We verify the column exists by calling a method that reads it
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    // setComponent requires components_json; if column were missing it would throw
    await store.setComponent(flowId, itemId, { kind: "shortlist", data: [1] });
    const comps = await store.getComponents(flowId, itemId);
    assert.equal(comps.length, 1);
  });

  test("runMigrations silently skips duplicate column errors", async () => {
    // Opening the same DB twice triggers MIGRATIONS again; duplicate-column
    // errors must be swallowed, not thrown.
    const dbPath = tmpDb();
    const s1 = await createDiscussStore({ dbPath });
    await s1.setComponent(randomUUID(), randomUUID(), { kind: "x" });
    // Re-open: migration re-runs ALTER TABLE → duplicate column → must not throw
    const s2 = await createDiscussStore({ dbPath });
    assert.ok(s2); // still alive
  });
});

// ─── getState ────────────────────────────────────────────────────────────────

describe("getState", () => {
  test("returns null for unknown (flowId, itemId)", async () => {
    const store = await makeStore();
    const result = await store.getState(randomUUID(), randomUUID());
    assert.equal(result, null);
  });

  test("returns correct defaults after row creation via appendMessage", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "hello" });
    const state = await store.getState(flowId, itemId);
    assert.ok(state !== null);
    assert.equal(state.turnStatus, "idle");
    assert.equal(state.activeTurnId, null);
    assert.equal(state.pending, null);
    assert.deepEqual(state.components, []);
    assert.ok(Array.isArray(state.messages));
    assert.equal(state.messages.length, 1);
    assert.ok(typeof state.updatedAt === "number");
  });

  test("returns deserialized messages array from messages_json", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "msg1" });
    await store.appendMessage(flowId, itemId, { role: "agent", text: "msg2" });
    const state = await store.getState(flowId, itemId);
    assert.equal(state.messages.length, 2);
    assert.equal(state.messages[0].role, "operator");
    assert.equal(state.messages[1].role, "agent");
  });
});

// ─── appendMessage ────────────────────────────────────────────────────────────

describe("appendMessage", () => {
  test("auto-generates id when omitted", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "hi" });
    const { messages } = await store.getState(flowId, itemId);
    assert.ok(typeof messages[0].id === "string" && messages[0].id.length > 0);
  });

  test("auto-generates ts when omitted", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const before = Date.now();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "hi" });
    const after = Date.now();
    const { messages } = await store.getState(flowId, itemId);
    assert.ok(messages[0].ts >= before && messages[0].ts <= after);
  });

  test("preserves caller-supplied id and ts", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const id = randomUUID();
    const ts = 1234567890000;
    await store.appendMessage(flowId, itemId, { id, ts, role: "operator", text: "pinned" });
    const { messages } = await store.getState(flowId, itemId);
    assert.equal(messages[0].id, id);
    assert.equal(messages[0].ts, ts);
  });

  test("accumulates multiple messages in order", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "first" });
    await store.appendMessage(flowId, itemId, { role: "agent",    text: "second" });
    await store.appendMessage(flowId, itemId, { role: "operator", text: "third" });
    const { messages } = await store.getState(flowId, itemId);
    assert.equal(messages.length, 3);
    assert.equal(messages[0].text, "first");
    assert.equal(messages[1].text, "second");
    assert.equal(messages[2].text, "third");
  });

  test("preserves all optional fields (audioKey, proposedDraft, proposedMeta, error)", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, {
      role: "agent",
      text: "body",
      audioKey: "s3://bucket/key",
      proposedDraft: "draft text",
      proposedMeta: { budget: 100 },
      error: "something went wrong",
    });
    const { messages } = await store.getState(flowId, itemId);
    const m = messages[0];
    assert.equal(m.audioKey, "s3://bucket/key");
    assert.equal(m.proposedDraft, "draft text");
    assert.deepEqual(m.proposedMeta, { budget: 100 });
    assert.equal(m.error, "something went wrong");
  });

  test("sets optional fields to null when not provided", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "clean" });
    const { messages } = await store.getState(flowId, itemId);
    const m = messages[0];
    assert.equal(m.audioKey, null);
    assert.equal(m.proposedDraft, null);
    assert.equal(m.proposedMeta, null);
    assert.equal(m.error, null);
    assert.equal(m.turnId, null);
  });

  test("preserves turnId when provided", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "q", turnId });
    const { messages } = await store.getState(flowId, itemId);
    assert.equal(messages[0].turnId, turnId);
  });
});

// ─── stampMessageError ────────────────────────────────────────────────────────

describe("stampMessageError", () => {
  test("stamps error on the last operator message matching turnId", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "q", turnId });
    await store.appendMessage(flowId, itemId, { role: "operator", text: "q2", turnId: randomUUID() });
    await store.stampMessageError(flowId, itemId, turnId, "LLM timeout");
    const { messages } = await store.getState(flowId, itemId);
    // The first message (matching turnId) should have the error
    assert.equal(messages[0].error, "LLM timeout");
    // The second message (different turnId) should not
    assert.equal(messages[1].error, null);
  });

  test("stamps error on the last operator message when turnId is null", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "first op" });
    await store.appendMessage(flowId, itemId, { role: "agent",    text: "reply" });
    await store.appendMessage(flowId, itemId, { role: "operator", text: "second op" });
    await store.stampMessageError(flowId, itemId, null, "network error");
    const { messages } = await store.getState(flowId, itemId);
    // Last operator message (index 2) gets the error
    assert.equal(messages[2].error, "network error");
    assert.equal(messages[0].error, null);
  });

  test("is a no-op when no operator message exists", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "agent", text: "only agent" });
    // Must not throw
    await store.stampMessageError(flowId, itemId, null, "irrelevant");
    const { messages } = await store.getState(flowId, itemId);
    assert.equal(messages[0].error, null);
  });

  test("is a no-op when row does not exist (no throw)", async () => {
    const store = await makeStore();
    // No row — ensureRow creates it, then there are no operator messages
    await assert.doesNotReject(() => store.stampMessageError(randomUUID(), randomUUID(), null, "err"));
  });

  test("targets the LAST operator message with matching turnId, not the first", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "earlier", turnId });
    await store.appendMessage(flowId, itemId, { role: "agent",    text: "reply" });
    await store.appendMessage(flowId, itemId, { role: "operator", text: "later", turnId });
    await store.stampMessageError(flowId, itemId, turnId, "err");
    const { messages } = await store.getState(flowId, itemId);
    // The LAST one matching (index 2) gets the error
    assert.equal(messages[2].error, "err");
    assert.equal(messages[0].error, null);
  });
});

// ─── setTurnStatus ────────────────────────────────────────────────────────────

describe("setTurnStatus", () => {
  test("persists status and activeTurnId", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();
    await store.setTurnStatus(flowId, itemId, "running", turnId);
    const state = await store.getState(flowId, itemId);
    assert.equal(state.turnStatus, "running");
    assert.equal(state.activeTurnId, turnId);
  });

  test("activeTurnId defaults to null when omitted", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setTurnStatus(flowId, itemId, "idle");
    const state = await store.getState(flowId, itemId);
    assert.equal(state.activeTurnId, null);
  });

  test("can transition through idle → running → error → idle", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();

    await store.setTurnStatus(flowId, itemId, "running", turnId);
    assert.equal((await store.getState(flowId, itemId)).turnStatus, "running");

    await store.setTurnStatus(flowId, itemId, "error", null);
    assert.equal((await store.getState(flowId, itemId)).turnStatus, "error");

    await store.setTurnStatus(flowId, itemId, "idle", null);
    assert.equal((await store.getState(flowId, itemId)).turnStatus, "idle");
  });

  test("auto-creates the row via ensureRow", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    // No prior row — should not throw
    await store.setTurnStatus(flowId, itemId, "running", randomUUID());
    const state = await store.getState(flowId, itemId);
    assert.equal(state.turnStatus, "running");
  });
});

// ─── setPending ───────────────────────────────────────────────────────────────

describe("setPending", () => {
  test("writes text and draftId", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const draftId = randomUUID();
    await store.setPending(flowId, itemId, "draft text here", draftId);
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.text, "draft text here");
    assert.equal(state.pending.draftId, draftId);
  });

  test("sets draftId to null when omitted", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setPending(flowId, itemId, "some text");
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.draftId, null);
  });

  test("merges with existing fields — does not wipe prior mergePendingFields keys", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    // First set some fields via mergePendingFields
    await store.mergePendingFields(flowId, itemId, { budget: 500, title: "old title" });
    // Then call setPending — should not wipe the `fields` sub-object
    await store.setPending(flowId, itemId, "new draft text");
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.text, "new draft text");
    // fields should still be there from the prior merge
    assert.deepEqual(state.pending.fields, { budget: 500, title: "old title" });
  });

  test("overwrites text if called twice", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setPending(flowId, itemId, "first draft");
    await store.setPending(flowId, itemId, "second draft");
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.text, "second draft");
  });
});

// ─── mergePendingFields ───────────────────────────────────────────────────────

describe("mergePendingFields", () => {
  test("shallow-merges fields without overwriting unrelated keys", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.mergePendingFields(flowId, itemId, { budget: 100 });
    await store.mergePendingFields(flowId, itemId, { title: "my project" });
    const state = await store.getState(flowId, itemId);
    assert.deepEqual(state.pending.fields, { budget: 100, title: "my project" });
  });

  test("overwrites the same field key on repeated calls", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.mergePendingFields(flowId, itemId, { budget: 100 });
    await store.mergePendingFields(flowId, itemId, { budget: 200 });
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.fields.budget, 200);
  });

  test("auto-creates row via ensureRow", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await assert.doesNotReject(() => store.mergePendingFields(flowId, itemId, { x: 1 }));
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.fields.x, 1);
  });

  test("does not clobber the text field set by setPending", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setPending(flowId, itemId, "the draft");
    await store.mergePendingFields(flowId, itemId, { category: "web" });
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending.text, "the draft");
    assert.equal(state.pending.fields.category, "web");
  });
});

// ─── clearPending ─────────────────────────────────────────────────────────────

describe("clearPending", () => {
  test("sets pending to null", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setPending(flowId, itemId, "some draft");
    await store.clearPending(flowId, itemId);
    const state = await store.getState(flowId, itemId);
    assert.equal(state.pending, null);
  });

  test("is idempotent — clearing an already-null pending does not throw", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    // Ensure row exists
    await store.appendMessage(flowId, itemId, { role: "operator", text: "x" });
    await assert.doesNotReject(() => store.clearPending(flowId, itemId));
    await assert.doesNotReject(() => store.clearPending(flowId, itemId));
  });
});

// ─── setComponent ─────────────────────────────────────────────────────────────

describe("setComponent", () => {
  test("upserts by kind — last-write-wins", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, { kind: "shortlist", data: [1, 2] });
    await store.setComponent(flowId, itemId, { kind: "shortlist", data: [3, 4] });
    const comps = await store.getComponents(flowId, itemId);
    assert.equal(comps.length, 1);
    assert.deepEqual(comps[0].data, [3, 4]);
  });

  test("with no kind is a no-op", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, { data: "no kind here" });
    const comps = await store.getComponents(flowId, itemId);
    assert.deepEqual(comps, []);
  });

  test("with null component is a no-op", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, null);
    // No row created, no throw
    const result = await store.getState(flowId, itemId);
    assert.equal(result, null);
  });

  test("replaces existing component of same kind, keeping others", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, { kind: "shortlist", items: ["a"] });
    await store.setComponent(flowId, itemId, { kind: "budget",    amount: 500 });
    await store.setComponent(flowId, itemId, { kind: "shortlist", items: ["b", "c"] });
    const comps = await store.getComponents(flowId, itemId);
    assert.equal(comps.length, 2);
    const shortlist = comps.find((c) => c.kind === "shortlist");
    const budget    = comps.find((c) => c.kind === "budget");
    assert.deepEqual(shortlist.items, ["b", "c"]);
    assert.equal(budget.amount, 500);
  });

  test("auto-stamps ts when not provided", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const before = Date.now();
    await store.setComponent(flowId, itemId, { kind: "info" });
    const after = Date.now();
    const comps = await store.getComponents(flowId, itemId);
    assert.ok(comps[0].ts >= before && comps[0].ts <= after);
  });

  test("preserves caller-supplied ts", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, { kind: "x", ts: 9999 });
    const comps = await store.getComponents(flowId, itemId);
    assert.equal(comps[0].ts, 9999);
  });
});

// ─── getComponents ────────────────────────────────────────────────────────────

describe("getComponents", () => {
  test("returns empty array when no row exists", async () => {
    const store = await makeStore();
    const result = await store.getComponents(randomUUID(), randomUUID());
    assert.deepEqual(result, []);
  });

  test("returns empty array when row has empty components_json", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "x" });
    assert.deepEqual(await store.getComponents(flowId, itemId), []);
  });
});

// ─── getBulkSummaries ────────────────────────────────────────────────────────

describe("getBulkSummaries", () => {
  test("returns empty object for empty flowIds array", async () => {
    const store = await makeStore();
    const result = await store.getBulkSummaries([]);
    assert.deepEqual(result, {});
  });

  test("returns empty object when called with null/undefined", async () => {
    const store = await makeStore();
    assert.deepEqual(await store.getBulkSummaries(null), {});
    assert.deepEqual(await store.getBulkSummaries(undefined), {});
  });

  test("returns only flows with non-null pending_json", async () => {
    const store = await makeStore();
    const flowWithPending    = randomUUID();
    const flowWithoutPending = randomUUID();
    const itemId = randomUUID();

    await store.setPending(flowWithPending, itemId, "draft");
    await store.appendMessage(flowWithoutPending, itemId, { role: "operator", text: "no pending" });

    const result = await store.getBulkSummaries([flowWithPending, flowWithoutPending]);
    assert.ok(flowWithPending    in result, "pending flow should appear");
    assert.ok(!(flowWithoutPending in result), "non-pending flow should be absent");
  });

  test("aggregates multiple items under the same flowId", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const item1  = randomUUID();
    const item2  = randomUUID();
    const item3  = randomUUID();

    await store.setPending(flowId, item1, "draft 1");
    await store.setPending(flowId, item2, "draft 2");
    // item3 has no pending
    await store.appendMessage(flowId, item3, { role: "operator", text: "no pending" });

    const result = await store.getBulkSummaries([flowId]);
    assert.ok(result[flowId]);
    const { pendingItemIds } = result[flowId];
    assert.equal(pendingItemIds.length, 2);
    assert.ok(pendingItemIds.includes(item1));
    assert.ok(pendingItemIds.includes(item2));
    assert.ok(!pendingItemIds.includes(item3));
  });

  test("excludes unknown flowIds that have no rows", async () => {
    const store = await makeStore();
    const ghost = randomUUID();
    const result = await store.getBulkSummaries([ghost]);
    assert.deepEqual(result, {});
  });

  test("handles mix of known-pending, known-no-pending, and unknown flows", async () => {
    const store = await makeStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    const flowC = randomUUID(); // no rows at all
    const itemA = randomUUID();
    const itemB = randomUUID();

    await store.setPending(flowA, itemA, "draft");
    await store.appendMessage(flowB, itemB, { role: "agent", text: "reply" });

    const result = await store.getBulkSummaries([flowA, flowB, flowC]);
    assert.ok(flowA in result);
    assert.ok(!(flowB in result));
    assert.ok(!(flowC in result));
  });
});

// ─── ensureRow safety (ON CONFLICT DO NOTHING) ───────────────────────────────

describe("ensureRow — ON CONFLICT DO NOTHING", () => {
  test("repeated appendMessage calls on same (flowId, itemId) do not duplicate the row", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "a" });
    await store.appendMessage(flowId, itemId, { role: "agent",    text: "b" });
    await store.appendMessage(flowId, itemId, { role: "operator", text: "c" });
    const { messages } = await store.getState(flowId, itemId);
    // Should have exactly 3 messages in one row, not 3 rows
    assert.equal(messages.length, 3);
  });

  test("setTurnStatus on same (flowId, itemId) repeated does not create extra rows", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setTurnStatus(flowId, itemId, "running", randomUUID());
    await store.setTurnStatus(flowId, itemId, "idle");
    const state = await store.getState(flowId, itemId);
    assert.equal(state.turnStatus, "idle");
  });
});

// ─── tryParse edge cases (exercised via stored state) ────────────────────────

describe("tryParse — invalid JSON / null handling", () => {
  test("tryParse returns fallback for null input — pending returns null on fresh row", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "x" });
    const state = await store.getState(flowId, itemId);
    // pending_json is NULL in DB; tryParse(null, null) → null
    assert.equal(state.pending, null);
  });

  test("tryParse returns fallback [] for invalid messages_json if DB row corrupted", async () => {
    // We can't inject invalid JSON through the public API, so we write
    // directly to the underlying DB to simulate corruption.
    const dbPath = tmpDb();
    const store = await createDiscussStore({ dbPath });
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.appendMessage(flowId, itemId, { role: "operator", text: "seed" });

    // Access internal DB to corrupt messages_json
    const corruptClient = createClient({ url: `file:${dbPath}` });
    await corruptClient.execute({
      sql: "UPDATE discuss_state SET messages_json = ? WHERE flow_id = ? AND item_id = ?",
      args: ["NOT VALID JSON{{", flowId, itemId],
    });
    corruptClient.close();

    // Re-open store and read — tryParse should return []
    const store2 = await createDiscussStore({ dbPath });
    const state = await store2.getState(flowId, itemId);
    assert.deepEqual(state.messages, []);
  });

  test("tryParse returns fallback [] for invalid components_json if DB row corrupted", async () => {
    const dbPath = tmpDb();
    const store = await createDiscussStore({ dbPath });
    const flowId = randomUUID();
    const itemId = randomUUID();
    await store.setComponent(flowId, itemId, { kind: "x" });

    const corruptClient = createClient({ url: `file:${dbPath}` });
    await corruptClient.execute({
      sql: "UPDATE discuss_state SET components_json = ? WHERE flow_id = ? AND item_id = ?",
      args: ["{bad json", flowId, itemId],
    });
    corruptClient.close();

    const store2 = await createDiscussStore({ dbPath });
    assert.deepEqual(await store2.getComponents(flowId, itemId), []);
  });
});

// ─── cross-method integration ─────────────────────────────────────────────────

describe("cross-method integration", () => {
  test("full discuss lifecycle: operator message → running → agent reply → idle → clear pending", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();

    // Operator sends a message
    await store.appendMessage(flowId, itemId, { role: "operator", text: "Please review this proposal", turnId });
    await store.setTurnStatus(flowId, itemId, "running", turnId);

    // Mid-turn: agent proposes a draft and emits components
    await store.setPending(flowId, itemId, "Proposal draft v1");
    await store.mergePendingFields(flowId, itemId, { budget: 1500 });
    await store.setComponent(flowId, itemId, { kind: "shortlist", items: ["item a"] });

    // Agent reply lands
    await store.appendMessage(flowId, itemId, {
      role: "agent",
      text: "Here is my proposal",
      turnId,
      proposedDraft: "Proposal draft v1",
    });
    await store.setTurnStatus(flowId, itemId, "idle", null);
    await store.clearPending(flowId, itemId);

    const state = await store.getState(flowId, itemId);
    assert.equal(state.messages.length, 2);
    assert.equal(state.turnStatus, "idle");
    assert.equal(state.activeTurnId, null);
    assert.equal(state.pending, null);
    // Components should still be present (only cleared on gate resolve)
    assert.equal(state.components.length, 1);
  });

  test("error path: stampMessageError + setTurnStatus error", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();
    const turnId = randomUUID();

    await store.appendMessage(flowId, itemId, { role: "operator", text: "do something", turnId });
    await store.setTurnStatus(flowId, itemId, "running", turnId);
    await store.stampMessageError(flowId, itemId, turnId, "Agent timed out after 30s");
    await store.setTurnStatus(flowId, itemId, "error", null);

    const state = await store.getState(flowId, itemId);
    assert.equal(state.turnStatus, "error");
    assert.equal(state.activeTurnId, null);
    assert.equal(state.messages[0].error, "Agent timed out after 30s");
  });

  test("getBulkSummaries reflects setPending and clearPending changes", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const itemId = randomUUID();

    await store.setPending(flowId, itemId, "active draft");
    let result = await store.getBulkSummaries([flowId]);
    assert.ok(result[flowId]);
    assert.ok(result[flowId].pendingItemIds.includes(itemId));

    await store.clearPending(flowId, itemId);
    result = await store.getBulkSummaries([flowId]);
    assert.deepEqual(result, {});
  });

  test("multiple (flowId, itemId) pairs are isolated", async () => {
    const store = await makeStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    const item1 = randomUUID();
    const item2 = randomUUID();

    await store.appendMessage(flowA, item1, { role: "operator", text: "flow A item 1" });
    await store.appendMessage(flowB, item2, { role: "agent",    text: "flow B item 2" });
    await store.setPending(flowA, item1, "draft A");

    const stateA = await store.getState(flowA, item1);
    const stateB = await store.getState(flowB, item2);

    assert.equal(stateA.messages[0].text, "flow A item 1");
    assert.equal(stateB.messages[0].text, "flow B item 2");
    assert.equal(stateA.pending.text, "draft A");
    assert.equal(stateB.pending, null);
  });
});

// ─── listForFlow() ────────────────────────────────────────────────────────────

describe("listForFlow()", () => {
  test("returns every thread for the flow with parsed messages", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const item1 = randomUUID();
    const item2 = randomUUID();
    await store.appendMessage(flowId, item1, { role: "operator", text: "first" });
    await store.appendMessage(flowId, item1, { role: "agent", text: "reply" });
    await store.appendMessage(flowId, item2, { role: "operator", text: "other item" });
    await store.appendMessage(randomUUID(), randomUUID(), { role: "operator", text: "other flow" });
    const rows = await store.listForFlow(flowId);
    assert.equal(rows.length, 2);
    const byItem = new Map(rows.map((r) => [r.itemId, r]));
    assert.equal(byItem.get(item1).messages.length, 2);
    assert.equal(byItem.get(item1).messages[0].text, "first");
    assert.equal(byItem.get(item1).turnStatus, "idle");
    assert.equal(byItem.get(item2).messages.length, 1);
    assert.ok(typeof byItem.get(item1).updatedAt === "number");
  });

  test("unknown flow returns an empty list", async () => {
    const store = await makeStore();
    assert.deepEqual(await store.listForFlow(randomUUID()), []);
  });
});
