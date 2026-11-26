import { test, describe } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import {
  isCorruptClass,
  noteDbFailure,
  __setExitFn,
  __resetGuard,
  TRIP_THRESHOLD,
} from "../../../state/db-guard.js";
import { checkDb } from "../../../state/db-health.js";

describe("db-guard — classification", () => {
  test("corrupt-class matches, transient does not", () => {
    for (const msg of [
      "SQLITE_CORRUPT: database disk image is malformed",
      "SQLITE_IOERR: disk I/O error",
      "SQLITE_IOERR_SHORT_READ",
      "SQLITE_NOTADB: file is not a database",
    ]) {
      assert.equal(isCorruptClass(new Error(msg)), true, msg);
    }
    assert.equal(isCorruptClass({ code: "SQLITE_IOERR", message: "x" }), true);
    for (const msg of ["SQLITE_BUSY: database is locked", "timed out", "connection refused"]) {
      assert.equal(isCorruptClass(new Error(msg)), false, msg);
    }
    assert.equal(isCorruptClass(new Error("SQLITE_BUSY")), false);
  });
});

describe("db-guard — trip counting", () => {
  test("trips on the third corrupt-class failure, exits once", () => {
    __resetGuard();
    const exits = [];
    __setExitFn((code) => exits.push(code));
    try {
      const err = () => ({ code: "SQLITE_IOERR", message: "disk I/O error" });
      assert.equal(noteDbFailure(err()), false);
      assert.equal(noteDbFailure(err()), false);
      assert.equal(noteDbFailure(err()), true);
      assert.deepEqual(exits, [1]);
      assert.equal(TRIP_THRESHOLD, 3);
    } finally {
      __setExitFn((code) => process.exit(code));
      __resetGuard();
    }
  });

  test("transient failures never count toward the trip", () => {
    __resetGuard();
    const exits = [];
    __setExitFn((code) => exits.push(code));
    try {
      for (let i = 0; i < 10; i++) {
        assert.equal(noteDbFailure(new Error("SQLITE_BUSY")), false);
      }
      assert.deepEqual(exits, []);
    } finally {
      __setExitFn((code) => process.exit(code));
      __resetGuard();
    }
  });

  test("noteDbFailure never throws, even on hostile input", () => {
    __resetGuard();
    try {
      assert.equal(noteDbFailure(null), false);
      assert.equal(noteDbFailure(undefined), false);
      assert.equal(noteDbFailure("plain string"), false);
    } finally {
      __resetGuard();
    }
  });
});

describe("db-health — checkDb", () => {
  test("healthy file reports ok with latency", () => {
    const dir = path.join(os.tmpdir(), `db-health-${randomUUID()}`);
    fs.mkdirSync(dir, { recursive: true });
    const dbPath = path.join(dir, "t.sqlite");
    const db = new DatabaseSync(dbPath);
    db.exec("CREATE TABLE t (id TEXT)");
    db.close();
    const out = checkDb(dbPath);
    assert.equal(out.ok, true);
    assert.ok(out.latencyMs >= 0);
  });

  test("missing file reports missing without creating it", () => {
    const dbPath = path.join(os.tmpdir(), `db-health-${randomUUID()}`, "nope.sqlite");
    const out = checkDb(dbPath);
    assert.equal(out.ok, false);
    assert.match(out.error ?? "", /missing/);
    assert.equal(fs.existsSync(dbPath), false);
  });
});
