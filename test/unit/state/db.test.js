/**
 * Unit tests for state/db.js's withSharedWrite()/isSqliteBusy() — the
 * shared write-serializer + busy-retry primitive extracted from
 * admission-queue-store.js's own proven serialize()/withWriteTx(),
 * generalized per-dbPath so every store sharing a connection via
 * getSharedDb() gets the same protection without reimplementing it.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { isSqliteBusy, withSharedWrite } from "../../../state/db.js";

function busyError() {
  const err = new Error("SQLITE_BUSY: database is locked");
  err.code = "SQLITE_BUSY";
  return err;
}

describe("isSqliteBusy", () => {
  test("true for an error with code SQLITE_BUSY", () => {
    assert.equal(isSqliteBusy({ code: "SQLITE_BUSY" }), true);
  });

  test("true for an error whose message contains SQLITE_BUSY (no code)", () => {
    assert.equal(isSqliteBusy({ message: "SQLITE_BUSY: database is locked" }), true);
  });

  test("false for an unrelated error", () => {
    assert.equal(isSqliteBusy(new Error("SQLITE_CONSTRAINT")), false);
  });

  test("false for undefined/null", () => {
    assert.equal(isSqliteBusy(undefined), false);
    assert.equal(isSqliteBusy(null), false);
  });
});

describe("withSharedWrite — serialization", () => {
  test("two concurrent calls for the same dbPath never interleave", async () => {
    const dbPath = `test-${randomUUID()}`;
    const order = [];
    const call = (label) => withSharedWrite(dbPath, async () => {
      order.push(`${label}:start`);
      await new Promise((r) => setTimeout(r, 5));
      order.push(`${label}:end`);
      return label;
    });

    const [a, b] = await Promise.all([call("a"), call("b")]);
    assert.equal(a, "a");
    assert.equal(b, "b");
    // Whichever ran first, its start/end must be adjacent — never
    // a:start, b:start, a:end, b:end (that would mean they overlapped).
    assert.ok(
      (order[0] === "a:start" && order[1] === "a:end" && order[2] === "b:start" && order[3] === "b:end") ||
      (order[0] === "b:start" && order[1] === "b:end" && order[2] === "a:start" && order[3] === "a:end"),
      `expected non-interleaved order, got ${JSON.stringify(order)}`,
    );
  });

  test("calls against DIFFERENT dbPaths do not serialize against each other", async () => {
    const dbPathA = `test-${randomUUID()}`;
    const dbPathB = `test-${randomUUID()}`;
    let bStartedWhileAWasRunning = false;

    const slowA = withSharedWrite(dbPathA, async () => {
      await new Promise((r) => setTimeout(r, 30));
      return "a";
    });
    // Give slowA a moment to actually start before firing b.
    await new Promise((r) => setTimeout(r, 5));
    const fastB = withSharedWrite(dbPathB, async () => {
      bStartedWhileAWasRunning = true;
      return "b";
    });

    await Promise.all([slowA, fastB]);
    assert.equal(bStartedWhileAWasRunning, true, "b should not have waited on a's unrelated dbPath queue");
  });

  test("a rejecting call does not poison the queue for calls after it", async () => {
    const dbPath = `test-${randomUUID()}`;
    const first = withSharedWrite(dbPath, async () => { throw new Error("boom"); }, { maxAttempts: 1 });
    await assert.rejects(first, /boom/);

    const second = await withSharedWrite(dbPath, async () => "still works");
    assert.equal(second, "still works");
  });
});

describe("withSharedWrite — retry on SQLITE_BUSY", () => {
  test("retries a transient SQLITE_BUSY and eventually succeeds", async () => {
    const dbPath = `test-${randomUUID()}`;
    let attempts = 0;
    const result = await withSharedWrite(dbPath, async () => {
      attempts++;
      if (attempts < 3) throw busyError();
      return "ok";
    });
    assert.equal(result, "ok");
    assert.equal(attempts, 3);
  });

  test("does not retry a non-BUSY error", async () => {
    const dbPath = `test-${randomUUID()}`;
    let attempts = 0;
    await assert.rejects(
      withSharedWrite(dbPath, async () => { attempts++; throw new Error("SQLITE_CONSTRAINT"); }),
      /SQLITE_CONSTRAINT/,
    );
    assert.equal(attempts, 1, "a non-busy error must not be retried");
  });

  test("gives up and rethrows after maxAttempts of persistent SQLITE_BUSY", async () => {
    const dbPath = `test-${randomUUID()}`;
    let attempts = 0;
    await assert.rejects(
      withSharedWrite(dbPath, async () => { attempts++; throw busyError(); }, { maxAttempts: 3 }),
      /SQLITE_BUSY/,
    );
    assert.equal(attempts, 3);
  });
});
