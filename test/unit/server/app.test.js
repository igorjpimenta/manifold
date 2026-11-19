import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { checkDatabases } from "../../../server/app.js";

const logger = { info: () => {}, warn: () => {}, error: () => {} };

function pathsIn(dir) {
  return { runtimeDb: path.join(dir, "runtime.sqlite"), opsDb: path.join(dir, "ops.sqlite") };
}

describe("checkDatabases", () => {
  it("accepts a fresh home — absent files are created on first write", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "manifold-app-check-"));
    try {
      assert.deepEqual(checkDatabases(pathsIn(dir), { logger }), { ok: true });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses an unreadable primary and names the file", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "manifold-app-check-"));
    try {
      const paths = pathsIn(dir);
      writeFileSync(paths.runtimeDb, "not a database", "utf8");
      const result = checkDatabases(paths, { logger });
      assert.equal(result.ok, false);
      assert.equal(result.dbPath, paths.runtimeDb);
      assert.ok(result.error);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
