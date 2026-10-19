// Unit tests for state/pending-store.js — readPending, setPendingText, mergePendingFields, clearPending.
// Covers: null-guard paths, missing/malformed file handling, filename sanitization, create/overwrite
// semantics, draftId fallback, field merging, clearPending idempotency, and ensureDir transparency.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, unlinkSync, readFileSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createPendingStore } from "../../../state/pending-store.js";

// ─── test store setup ─────────────────────────────────────────────────────────
// Each test run uses its own isolated directory so tests never touch ~/.openclaw.

const PENDING_DIR = mkdtempSync(join(os.tmpdir(), "gf-pending-test-"));
const store = createPendingStore(PENDING_DIR);
const { readPending, setPendingText, mergePendingFields, clearPending } = store;

// ─── helpers ─────────────────────────────────────────────────────────────────

function safeSegment(s) {
  return String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
}

function expectedFile(flowId, itemId) {
  return join(PENDING_DIR, `${safeSegment(flowId)}__${safeSegment(itemId)}.json`);
}

function writeRaw(flowId, itemId, content) {
  const p = expectedFile(flowId, itemId);
  writeFileSync(p, content, "utf-8");
  return p;
}

function removeFile(flowId, itemId) {
  const p = expectedFile(flowId, itemId);
  try { unlinkSync(p); } catch { /* already gone */ }
}

// ─── readPending — null-guard paths ─────────────────────────────────────────

describe("readPending — null-guard paths", () => {
  test("returns null when flowId is falsy (null)", () => {
    assert.strictEqual(readPending(null, "item-1"), null);
  });

  test("returns null when flowId is empty string", () => {
    assert.strictEqual(readPending("", "item-1"), null);
  });

  test("returns null when itemId is falsy (null)", () => {
    assert.strictEqual(readPending("flow-1", null), null);
  });

  test("returns null when itemId is empty string", () => {
    assert.strictEqual(readPending("flow-1", ""), null);
  });

  test("returns null when both flowId and itemId are falsy", () => {
    assert.strictEqual(readPending(null, null), null);
  });
});

// ─── readPending — missing file ───────────────────────────────────────────────

describe("readPending — missing file", () => {
  test("returns null when file does not exist", () => {
    const flowId = randomUUID();
    const itemId = "no-such-item";
    removeFile(flowId, itemId);
    assert.strictEqual(readPending(flowId, itemId), null);
  });
});

// ─── readPending — malformed file ────────────────────────────────────────────

describe("readPending — malformed file handling", () => {
  test("returns null when file contains invalid JSON", () => {
    const flowId = randomUUID();
    const itemId = "bad-json-item";
    writeRaw(flowId, itemId, "{ not valid json ]]]");
    assert.strictEqual(readPending(flowId, itemId), null);
    removeFile(flowId, itemId);
  });

  test("returns null when file contains a JSON null", () => {
    const flowId = randomUUID();
    const itemId = "null-json-item";
    writeRaw(flowId, itemId, "null");
    assert.strictEqual(readPending(flowId, itemId), null);
    removeFile(flowId, itemId);
  });

  test("returns null when file contains a JSON number", () => {
    const flowId = randomUUID();
    const itemId = "number-json-item";
    writeRaw(flowId, itemId, "42");
    assert.strictEqual(readPending(flowId, itemId), null);
    removeFile(flowId, itemId);
  });

  test("returns the array when file contains a JSON array (guard only checks !obj and typeof)", () => {
    // The module checks `!obj || typeof obj !== "object"`, but arrays are typeof "object"
    // and truthy, so a JSON array passes the guard and is returned as-is.
    const flowId = randomUUID();
    const itemId = "array-json-item";
    writeRaw(flowId, itemId, "[1,2,3]");
    const result = readPending(flowId, itemId);
    assert.deepEqual(result, [1, 2, 3]);
    removeFile(flowId, itemId);
  });

  test("returns null when file is empty", () => {
    const flowId = randomUUID();
    const itemId = "empty-file-item";
    writeRaw(flowId, itemId, "");
    assert.strictEqual(readPending(flowId, itemId), null);
    removeFile(flowId, itemId);
  });
});

// ─── readPending — valid file ─────────────────────────────────────────────────

describe("readPending — valid file", () => {
  test("returns parsed object for a valid pending file", () => {
    const flowId = randomUUID();
    const itemId = "valid-item";
    const payload = { flowId, itemId, text: "Hello world", updatedAt: 1000 };
    writeRaw(flowId, itemId, JSON.stringify(payload, null, 2));
    const result = readPending(flowId, itemId);
    assert.deepEqual(result, payload);
    removeFile(flowId, itemId);
  });

  test("preserves all fields including nested fields object", () => {
    const flowId = randomUUID();
    const itemId = "fields-item";
    const payload = { flowId, itemId, text: "t", fields: { budget: 500, currency: "USD" }, updatedAt: 9999 };
    writeRaw(flowId, itemId, JSON.stringify(payload));
    const result = readPending(flowId, itemId);
    assert.deepEqual(result.fields, { budget: 500, currency: "USD" });
    removeFile(flowId, itemId);
  });
});

// ─── fileNameFor — sanitization (tested via readPending / setPendingText) ────

describe("fileNameFor — filename sanitization", () => {
  test("sanitizes special characters in flowId to underscores", () => {
    const flowId = "flow/with:special chars!";
    const itemId = "plain-item";
    setPendingText(flowId, itemId, "text");
    const p = join(PENDING_DIR, `${safeSegment(flowId)}__${safeSegment(itemId)}.json`);
    assert.ok(existsSync(p), "file should exist at sanitized path");
    clearPending(flowId, itemId);
  });

  test("sanitizes special characters in itemId to underscores", () => {
    const flowId = randomUUID();
    const itemId = "slug with spaces & symbols!";
    setPendingText(flowId, itemId, "body");
    const p = join(PENDING_DIR, `${safeSegment(flowId)}__${safeSegment(itemId)}.json`);
    assert.ok(existsSync(p), "file should exist at sanitized itemId path");
    clearPending(flowId, itemId);
  });

  test("preserves safe characters [A-Za-z0-9._-] in filename", () => {
    const flowId = "abc-123.UUID_test";
    const itemId = "item.slug-ok_1";
    setPendingText(flowId, itemId, "safe");
    const p = join(PENDING_DIR, `${flowId}__${itemId}.json`);
    assert.ok(existsSync(p), "safe characters should be preserved verbatim in filename");
    clearPending(flowId, itemId);
  });

  test("separates flowId and itemId with double underscore in filename", () => {
    const flowId = "flow1";
    const itemId = "item1";
    setPendingText(flowId, itemId, "sep-test");
    const p = join(PENDING_DIR, "flow1__item1.json");
    assert.ok(existsSync(p), "double-underscore separator must be present");
    clearPending(flowId, itemId);
  });

  test("replaces consecutive special characters with a single underscore", () => {
    const flowId = randomUUID();
    const itemId = "item!!!with###many%%%specials";
    setPendingText(flowId, itemId, "collapse");
    const sanitized = safeSegment(itemId);
    const p = join(PENDING_DIR, `${safeSegment(flowId)}__${sanitized}.json`);
    assert.ok(existsSync(p), "consecutive specials should collapse to single underscore");
    clearPending(flowId, itemId);
  });
});

// ─── setPendingText ───────────────────────────────────────────────────────────

describe("setPendingText", () => {
  test("creates a new record when no prior file exists", () => {
    const flowId = randomUUID();
    const itemId = "new-item";
    removeFile(flowId, itemId);

    const result = setPendingText(flowId, itemId, "proposal body");

    assert.strictEqual(result.flowId, flowId);
    assert.strictEqual(result.itemId, itemId);
    assert.strictEqual(result.text, "proposal body");
    assert.ok(existsSync(expectedFile(flowId, itemId)), "file should be created on disk");

    removeFile(flowId, itemId);
  });

  test("written file is valid JSON matching the returned payload", () => {
    const flowId = randomUUID();
    const itemId = "write-check-item";
    const result = setPendingText(flowId, itemId, "check me");
    const raw = readFileSync(expectedFile(flowId, itemId), "utf-8");
    const parsed = JSON.parse(raw);
    assert.deepEqual(parsed, result);
    removeFile(flowId, itemId);
  });

  test("overwrites text on an existing record while preserving other fields", () => {
    const flowId = randomUUID();
    const itemId = "overwrite-item";
    const initial = { flowId, itemId, text: "first", fields: { x: 1 }, updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(initial));

    const result = setPendingText(flowId, itemId, "second");

    assert.strictEqual(result.text, "second");
    assert.deepEqual(result.fields, { x: 1 });

    removeFile(flowId, itemId);
  });

  test("stores draftId when provided as a non-null argument", () => {
    const flowId = randomUUID();
    const itemId = "draft-id-item";
    const result = setPendingText(flowId, itemId, "body", "slot-A");
    assert.strictEqual(result.draftId, "slot-A");
    removeFile(flowId, itemId);
  });

  test("falls back to prior.draftId when draftId arg is null and prior has one", () => {
    const flowId = randomUUID();
    const itemId = "fallback-draft-item";
    const prior = { flowId, itemId, text: "old", draftId: "prior-slot", updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = setPendingText(flowId, itemId, "new body", null);

    assert.strictEqual(result.draftId, "prior-slot");
    removeFile(flowId, itemId);
  });

  test("draftId is null when not provided and no prior draftId exists", () => {
    const flowId = randomUUID();
    const itemId = "no-draft-id-item";
    removeFile(flowId, itemId);

    const result = setPendingText(flowId, itemId, "text");

    assert.strictEqual(result.draftId, null);
    removeFile(flowId, itemId);
  });

  test("draftId from arg takes precedence over prior.draftId", () => {
    const flowId = randomUUID();
    const itemId = "draftid-override-item";
    const prior = { flowId, itemId, text: "old", draftId: "old-slot", updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = setPendingText(flowId, itemId, "new body", "new-slot");

    assert.strictEqual(result.draftId, "new-slot");
    removeFile(flowId, itemId);
  });

  test("sets updatedAt to a recent timestamp (within 5 seconds)", () => {
    const flowId = randomUUID();
    const itemId = "timestamp-item";
    const before = Date.now();
    const result = setPendingText(flowId, itemId, "ts test");
    const after = Date.now();

    assert.ok(result.updatedAt >= before, "updatedAt should be >= before");
    assert.ok(result.updatedAt <= after, "updatedAt should be <= after");
    removeFile(flowId, itemId);
  });

  test("returns the written payload including all fields", () => {
    const flowId = randomUUID();
    const itemId = "return-check-item";
    const result = setPendingText(flowId, itemId, "returned");

    assert.ok(typeof result === "object" && result !== null);
    assert.strictEqual(result.flowId, flowId);
    assert.strictEqual(result.itemId, itemId);
    assert.strictEqual(result.text, "returned");
    assert.ok("updatedAt" in result);
    removeFile(flowId, itemId);
  });

  test("overwrites updatedAt even when prior record exists", () => {
    const flowId = randomUUID();
    const itemId = "overwrite-ts-item";
    const prior = { flowId, itemId, text: "old", updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = setPendingText(flowId, itemId, "new");

    assert.ok(result.updatedAt > 1, "updatedAt should be updated to current time");
    removeFile(flowId, itemId);
  });
});

// ─── mergePendingFields ───────────────────────────────────────────────────────

describe("mergePendingFields", () => {
  test("merges new fields into an empty prior.fields", () => {
    const flowId = randomUUID();
    const itemId = "merge-empty-fields-item";
    removeFile(flowId, itemId);

    const result = mergePendingFields(flowId, itemId, { budget: 200 });

    assert.deepEqual(result.fields, { budget: 200 });
    removeFile(flowId, itemId);
  });

  test("shallow-merges over existing fields — new keys win", () => {
    const flowId = randomUUID();
    const itemId = "merge-overwrite-item";
    const prior = { flowId, itemId, fields: { budget: 100, currency: "BRL" }, updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = mergePendingFields(flowId, itemId, { budget: 500, deadline: "2026-07-01" });

    assert.strictEqual(result.fields.budget, 500, "new value wins");
    assert.strictEqual(result.fields.currency, "BRL", "untouched key preserved");
    assert.strictEqual(result.fields.deadline, "2026-07-01", "new key added");
    removeFile(flowId, itemId);
  });

  test("preserves unrelated fields (e.g. text) from prior record", () => {
    const flowId = randomUUID();
    const itemId = "merge-preserve-text-item";
    const prior = { flowId, itemId, text: "keep me", fields: {}, updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = mergePendingFields(flowId, itemId, { x: 1 });

    assert.strictEqual(result.text, "keep me");
    removeFile(flowId, itemId);
  });

  test("creates a new record when no prior file exists", () => {
    const flowId = randomUUID();
    const itemId = "merge-new-record-item";
    removeFile(flowId, itemId);

    const result = mergePendingFields(flowId, itemId, { title: "Hello" });

    assert.deepEqual(result.fields, { title: "Hello" });
    assert.ok(existsSync(expectedFile(flowId, itemId)), "file should be created");
    removeFile(flowId, itemId);
  });

  test("sets updatedAt to a recent timestamp", () => {
    const flowId = randomUUID();
    const itemId = "merge-ts-item";
    const before = Date.now();
    const result = mergePendingFields(flowId, itemId, { k: "v" });
    const after = Date.now();

    assert.ok(result.updatedAt >= before && result.updatedAt <= after);
    removeFile(flowId, itemId);
  });

  test("written file on disk matches the returned payload", () => {
    const flowId = randomUUID();
    const itemId = "merge-disk-check-item";
    const result = mergePendingFields(flowId, itemId, { a: 1 });
    const raw = readFileSync(expectedFile(flowId, itemId), "utf-8");
    const parsed = JSON.parse(raw);
    assert.deepEqual(parsed, result);
    removeFile(flowId, itemId);
  });

  test("merging empty object leaves fields unchanged", () => {
    const flowId = randomUUID();
    const itemId = "merge-empty-obj-item";
    const prior = { flowId, itemId, fields: { x: 10 }, updatedAt: 1 };
    writeRaw(flowId, itemId, JSON.stringify(prior));

    const result = mergePendingFields(flowId, itemId, {});

    assert.deepEqual(result.fields, { x: 10 });
    removeFile(flowId, itemId);
  });

  test("successive merges accumulate fields across calls", () => {
    const flowId = randomUUID();
    const itemId = "merge-successive-item";
    removeFile(flowId, itemId);

    mergePendingFields(flowId, itemId, { a: 1 });
    mergePendingFields(flowId, itemId, { b: 2 });
    const result = mergePendingFields(flowId, itemId, { c: 3 });

    assert.deepEqual(result.fields, { a: 1, b: 2, c: 3 });
    removeFile(flowId, itemId);
  });
});

// ─── clearPending ─────────────────────────────────────────────────────────────

describe("clearPending", () => {
  test("removes the file when it exists", () => {
    const flowId = randomUUID();
    const itemId = "clear-existing-item";
    setPendingText(flowId, itemId, "to be cleared");
    assert.ok(existsSync(expectedFile(flowId, itemId)), "pre-condition: file exists");

    clearPending(flowId, itemId);

    assert.ok(!existsSync(expectedFile(flowId, itemId)), "file should be removed");
  });

  test("does not throw when the file does not exist", () => {
    const flowId = randomUUID();
    const itemId = "clear-nonexistent-item";
    removeFile(flowId, itemId);
    assert.doesNotThrow(() => clearPending(flowId, itemId));
  });

  test("does nothing when flowId is falsy (null)", () => {
    assert.doesNotThrow(() => clearPending(null, "some-item"));
  });

  test("does nothing when itemId is falsy (null)", () => {
    assert.doesNotThrow(() => clearPending("some-flow", null));
  });

  test("does nothing when flowId is empty string", () => {
    assert.doesNotThrow(() => clearPending("", "some-item"));
  });

  test("does nothing when itemId is empty string", () => {
    assert.doesNotThrow(() => clearPending("some-flow", ""));
  });

  test("does nothing when both flowId and itemId are falsy", () => {
    assert.doesNotThrow(() => clearPending(null, null));
  });

  test("calling clearPending twice on same key is idempotent", () => {
    const flowId = randomUUID();
    const itemId = "clear-twice-item";
    setPendingText(flowId, itemId, "data");
    clearPending(flowId, itemId);
    assert.doesNotThrow(() => clearPending(flowId, itemId));
  });
});

// ─── ensureDir — missing directory created transparently ─────────────────────

describe("ensureDir — directory creation", () => {
  test("creates the pending dir when it does not exist before a write", () => {
    // Point a fresh store at a dir that hasn't been created yet.
    const freshDir = join(os.tmpdir(), `gf-pending-ensuredir-${randomUUID()}`);
    assert.ok(!existsSync(freshDir), "pre-condition: directory must not exist");

    const freshStore = createPendingStore(freshDir);
    const flowId = randomUUID();
    freshStore.setPendingText(flowId, "item-a", "content");

    assert.ok(existsSync(freshDir), "ensureDir should have created the directory");
    freshStore.clearPending(flowId, "item-a");
  });

  test("ensureDir is called transparently — write succeeds even when dir pre-exists", () => {
    // PENDING_DIR already exists; setPendingText must still succeed (mkdirSync with recursive is idempotent).
    const flowId = randomUUID();
    const itemId = "ensuredir-exists-item";
    assert.doesNotThrow(() => setPendingText(flowId, itemId, "safe write"));
    removeFile(flowId, itemId);
  });
});

// ─── round-trip integration ───────────────────────────────────────────────────

describe("round-trip integration", () => {
  test("setPendingText then readPending returns the same record", () => {
    const flowId = randomUUID();
    const itemId = "roundtrip-text-item";
    const written = setPendingText(flowId, itemId, "round-trip body", "slot-1");
    const read = readPending(flowId, itemId);
    assert.deepEqual(read, written);
    clearPending(flowId, itemId);
  });

  test("mergePendingFields then readPending returns merged record", () => {
    const flowId = randomUUID();
    const itemId = "roundtrip-fields-item";
    const written = mergePendingFields(flowId, itemId, { k: "v" });
    const read = readPending(flowId, itemId);
    assert.deepEqual(read, written);
    clearPending(flowId, itemId);
  });

  test("clearPending after setPendingText causes readPending to return null", () => {
    const flowId = randomUUID();
    const itemId = "roundtrip-clear-item";
    setPendingText(flowId, itemId, "ephemeral");
    clearPending(flowId, itemId);
    assert.strictEqual(readPending(flowId, itemId), null);
  });

  test("setPendingText + mergePendingFields interleave correctly", () => {
    const flowId = randomUUID();
    const itemId = "roundtrip-interleave-item";

    setPendingText(flowId, itemId, "initial text");
    mergePendingFields(flowId, itemId, { budget: 100 });
    const result = setPendingText(flowId, itemId, "updated text");

    assert.strictEqual(result.text, "updated text");
    assert.deepEqual(result.fields, { budget: 100 });

    clearPending(flowId, itemId);
  });

  test("two different (flowId, itemId) pairs are stored independently", () => {
    const flowId = randomUUID();
    const itemId1 = "pair-item-1";
    const itemId2 = "pair-item-2";

    setPendingText(flowId, itemId1, "body-one");
    setPendingText(flowId, itemId2, "body-two");

    assert.strictEqual(readPending(flowId, itemId1).text, "body-one");
    assert.strictEqual(readPending(flowId, itemId2).text, "body-two");

    clearPending(flowId, itemId1);
    clearPending(flowId, itemId2);
  });

  test("clearing one pair does not affect a different pair", () => {
    const flowId = randomUUID();
    const itemId1 = "isolation-item-1";
    const itemId2 = "isolation-item-2";

    setPendingText(flowId, itemId1, "keep");
    setPendingText(flowId, itemId2, "remove");

    clearPending(flowId, itemId2);

    assert.strictEqual(readPending(flowId, itemId1).text, "keep");
    assert.strictEqual(readPending(flowId, itemId2), null);

    clearPending(flowId, itemId1);
  });
});
