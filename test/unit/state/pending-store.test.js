// Unit tests for state/pending-store.js — readPending, setPendingText, mergePendingFields, clearPending.
// The store is synchronous better-sqlite3 over runtime.sqlite's pending_proposals table.

import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createPendingStore } from "../../../state/pending-store.js";

// ─── test store setup ─────────────────────────────────────────────────────────
// Each store gets its own tmp sqlite file so tests never touch the real store.

const _open = [];
function tmpDb() {
  return path.join(os.tmpdir(), `gf-pending-test-${randomUUID()}.sqlite`);
}

function makeStore(dbPath = tmpDb()) {
  const store = createPendingStore({ dbPath });
  _open.push({ store, dbPath });
  return store;
}

afterEach(() => {
  while (_open.length) {
    const { store, dbPath } = _open.pop();
    try { store.close(); } catch {}
    try { fs.unlinkSync(dbPath); } catch {}
    try { fs.unlinkSync(`${dbPath}-wal`); } catch {}
    try { fs.unlinkSync(`${dbPath}-shm`); } catch {}
    try { fs.unlinkSync(`${dbPath}-journal`); } catch {}
  }
});

// ─── readPending — null-guard paths ─────────────────────────────────────────

describe("readPending — null-guard paths", () => {
  test("returns null when flowId is falsy (null)", () => {
    assert.strictEqual(makeStore().readPending(null, "item-1"), null);
  });

  test("returns null when flowId is empty string", () => {
    assert.strictEqual(makeStore().readPending("", "item-1"), null);
  });

  test("returns null when itemId is falsy (null)", () => {
    assert.strictEqual(makeStore().readPending("flow-1", null), null);
  });

  test("returns null when itemId is empty string", () => {
    assert.strictEqual(makeStore().readPending("flow-1", ""), null);
  });

  test("returns null when both flowId and itemId are falsy", () => {
    assert.strictEqual(makeStore().readPending(null, null), null);
  });
});

// ─── readPending — unknown pair ─────────────────────────────────────────────

describe("readPending — unknown pair", () => {
  test("returns null when no row exists", () => {
    const store = makeStore();
    assert.strictEqual(store.readPending(randomUUID(), "no-such-item"), null);
  });
});

// ─── setPendingText ───────────────────────────────────────────────────────────

describe("setPendingText", () => {
  test("creates a new record when no prior row exists", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const result = store.setPendingText(flowId, "new-item", "proposal body");

    assert.strictEqual(result.flowId, flowId);
    assert.strictEqual(result.itemId, "new-item");
    assert.strictEqual(result.text, "proposal body");
    assert.strictEqual(result.draftId, null);
  });

  test("text write keeps existing fields (fields survive a text write)", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "overwrite-item";
    store.mergePendingFields(flowId, itemId, { x: 1 });

    const result = store.setPendingText(flowId, itemId, "second");

    assert.strictEqual(result.text, "second");
    assert.deepEqual(result.fields, { x: 1 });
  });

  test("stores draftId when provided as a non-null argument", () => {
    const result = makeStore().setPendingText(randomUUID(), "draft-id-item", "body", "slot-A");
    assert.strictEqual(result.draftId, "slot-A");
  });

  test("falls back to prior.draftId when draftId arg is null and prior has one", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "fallback-draft-item";
    store.setPendingText(flowId, itemId, "old", "prior-slot");

    const result = store.setPendingText(flowId, itemId, "new body", null);

    assert.strictEqual(result.draftId, "prior-slot");
  });

  test("draftId is null when not provided and no prior draftId exists", () => {
    const result = makeStore().setPendingText(randomUUID(), "no-draft-id-item", "text");
    assert.strictEqual(result.draftId, null);
  });

  test("draftId from arg takes precedence over prior.draftId", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "draftid-override-item";
    store.setPendingText(flowId, itemId, "old", "old-slot");

    const result = store.setPendingText(flowId, itemId, "new body", "new-slot");

    assert.strictEqual(result.draftId, "new-slot");
  });

  test("sets updatedAt to a recent timestamp (within 5 seconds)", () => {
    const store = makeStore();
    const before = Date.now();
    const result = store.setPendingText(randomUUID(), "timestamp-item", "ts test");
    const after = Date.now();

    assert.ok(result.updatedAt >= before, "updatedAt should be >= before");
    assert.ok(result.updatedAt <= after, "updatedAt should be <= after");
  });

  test("overwrites updatedAt even when a prior row exists", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "overwrite-ts-item";
    const first = store.setPendingText(flowId, itemId, "old");
    const result = store.setPendingText(flowId, itemId, "new");

    assert.ok(result.updatedAt >= first.updatedAt, "updatedAt should advance on rewrite");
  });
});

// ─── mergePendingFields ───────────────────────────────────────────────────────

describe("mergePendingFields", () => {
  test("merges new fields when no prior row exists", () => {
    const result = makeStore().mergePendingFields(randomUUID(), "merge-empty-item", { budget: 200 });
    assert.deepEqual(result.fields, { budget: 200 });
  });

  test("shallow-merges over existing fields — new keys win", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "merge-overwrite-item";
    store.mergePendingFields(flowId, itemId, { budget: 100, currency: "BRL" });

    const result = store.mergePendingFields(flowId, itemId, { budget: 500, deadline: "2026-07-01" });

    assert.strictEqual(result.fields.budget, 500, "new value wins");
    assert.strictEqual(result.fields.currency, "BRL", "untouched key preserved");
    assert.strictEqual(result.fields.deadline, "2026-07-01", "new key added");
  });

  test("fields round-trip as an object, including nested values", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "nested-fields-item";
    store.mergePendingFields(flowId, itemId, { budget: 500, currency: "USD", meta: { src: "agent", n: 2 } });

    const read = store.readPending(flowId, itemId);
    assert.deepEqual(read.fields, { budget: 500, currency: "USD", meta: { src: "agent", n: 2 } });
  });

  test("preserves text from a prior text write", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "merge-preserve-text-item";
    store.setPendingText(flowId, itemId, "keep me");

    const result = store.mergePendingFields(flowId, itemId, { x: 1 });

    assert.strictEqual(result.text, "keep me");
  });

  test("merging empty object leaves fields unchanged", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "merge-empty-obj-item";
    store.mergePendingFields(flowId, itemId, { x: 10 });

    const result = store.mergePendingFields(flowId, itemId, {});

    assert.deepEqual(result.fields, { x: 10 });
  });

  test("successive merges accumulate fields across calls", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "merge-successive-item";

    store.mergePendingFields(flowId, itemId, { a: 1 });
    store.mergePendingFields(flowId, itemId, { b: 2 });
    const result = store.mergePendingFields(flowId, itemId, { c: 3 });

    assert.deepEqual(result.fields, { a: 1, b: 2, c: 3 });
  });
});

// ─── clearPending ─────────────────────────────────────────────────────────────

describe("clearPending", () => {
  test("removes the row when it exists", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "clear-existing-item";
    store.setPendingText(flowId, itemId, "to be cleared");
    assert.ok(store.readPending(flowId, itemId) !== null, "pre-condition: row exists");

    store.clearPending(flowId, itemId);

    assert.strictEqual(store.readPending(flowId, itemId), null, "row should be removed");
  });

  test("does not throw when the row does not exist", () => {
    assert.doesNotThrow(() => makeStore().clearPending(randomUUID(), "clear-nonexistent-item"));
  });

  test("does nothing when flowId is falsy (null)", () => {
    assert.doesNotThrow(() => makeStore().clearPending(null, "some-item"));
  });

  test("does nothing when itemId is falsy (null)", () => {
    assert.doesNotThrow(() => makeStore().clearPending("some-flow", null));
  });

  test("does nothing when flowId is empty string", () => {
    assert.doesNotThrow(() => makeStore().clearPending("", "some-item"));
  });

  test("does nothing when itemId is empty string", () => {
    assert.doesNotThrow(() => makeStore().clearPending("some-flow", ""));
  });

  test("calling clearPending twice on same key is idempotent", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "clear-twice-item";
    store.setPendingText(flowId, itemId, "data");
    store.clearPending(flowId, itemId);
    assert.doesNotThrow(() => store.clearPending(flowId, itemId));
  });
});

// ─── store home isolation ─────────────────────────────────────────────────────

describe("store home isolation", () => {
  test("defaults to MANIFOLD_DATA_DIR runtime.sqlite when no dbPath is given", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gf-pending-home-"));
    const prev = process.env.MANIFOLD_DATA_DIR;
    process.env.MANIFOLD_DATA_DIR = dir;
    try {
      const store = createPendingStore();
      _open.push({ store, dbPath: path.join(dir, "runtime.sqlite") });
      const flowId = randomUUID();
      store.setPendingText(flowId, "home-item", "hello");
      assert.strictEqual(store.readPending(flowId, "home-item").text, "hello");
      assert.ok(fs.existsSync(path.join(dir, "runtime.sqlite")), "db file lands under MANIFOLD_DATA_DIR");
    } finally {
      if (prev === undefined) delete process.env.MANIFOLD_DATA_DIR;
      else process.env.MANIFOLD_DATA_DIR = prev;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  test("rows survive close and reopen of the same file", () => {
    const dbPath = tmpDb();
    const s1 = makeStore(dbPath);
    const flowId = randomUUID();
    s1.setPendingText(flowId, "persist-item", "saved", "slot-9");
    s1.mergePendingFields(flowId, "persist-item", { k: "v" });
    s1.close();
    _open.pop();

    const s2 = makeStore(dbPath);
    const read = s2.readPending(flowId, "persist-item");
    assert.strictEqual(read.text, "saved");
    assert.strictEqual(read.draftId, "slot-9");
    assert.deepEqual(read.fields, { k: "v" });
  });
});

// ─── round-trip integration ───────────────────────────────────────────────────

describe("round-trip integration", () => {
  test("setPendingText then readPending returns the same record", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "roundtrip-text-item";
    const written = store.setPendingText(flowId, itemId, "round-trip body", "slot-1");
    const read = store.readPending(flowId, itemId);
    assert.deepEqual(read, written);
  });

  test("mergePendingFields then readPending returns merged record", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "roundtrip-fields-item";
    const written = store.mergePendingFields(flowId, itemId, { k: "v" });
    const read = store.readPending(flowId, itemId);
    assert.deepEqual(read, written);
  });

  test("clearPending after setPendingText causes readPending to return null", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "roundtrip-clear-item";
    store.setPendingText(flowId, itemId, "ephemeral");
    store.clearPending(flowId, itemId);
    assert.strictEqual(store.readPending(flowId, itemId), null);
  });

  test("setPendingText + mergePendingFields interleave correctly", () => {
    const store = makeStore();
    const flowId = randomUUID();
    const itemId = "roundtrip-interleave-item";

    store.setPendingText(flowId, itemId, "initial text");
    store.mergePendingFields(flowId, itemId, { budget: 100 });
    const result = store.setPendingText(flowId, itemId, "updated text");

    assert.strictEqual(result.text, "updated text");
    assert.deepEqual(result.fields, { budget: 100 });
  });

  test("two different (flowId, itemId) pairs are stored independently", () => {
    const store = makeStore();
    const flowId = randomUUID();

    store.setPendingText(flowId, "pair-item-1", "body-one");
    store.setPendingText(flowId, "pair-item-2", "body-two");

    assert.strictEqual(store.readPending(flowId, "pair-item-1").text, "body-one");
    assert.strictEqual(store.readPending(flowId, "pair-item-2").text, "body-two");
  });

  test("clearing one pair does not affect a different pair", () => {
    const store = makeStore();
    const flowId = randomUUID();

    store.setPendingText(flowId, "isolation-item-1", "keep");
    store.setPendingText(flowId, "isolation-item-2", "remove");

    store.clearPending(flowId, "isolation-item-2");

    assert.strictEqual(store.readPending(flowId, "isolation-item-1").text, "keep");
    assert.strictEqual(store.readPending(flowId, "isolation-item-2"), null);
  });
});
