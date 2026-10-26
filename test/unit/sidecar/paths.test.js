// paths.test.js — package-owned data dir + one-time legacy migration.
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import {
  defaultDataDir,
  resolveDataDir,
  runtimeDbPath,
  opsDbPath,
  ensureMigrated,
  readMigrationMarker,
} from "../../../sidecar/paths.js";

const require = createRequire(import.meta.url);

let home;
let dataDir;

beforeEach(() => {
  home = mkdtempSync(path.join(os.tmpdir(), "sidecar-home-"));
  dataDir = mkdtempSync(path.join(os.tmpdir(), "sidecar-data-"));
});

afterEach(() => {
  rmSync(home, { recursive: true, force: true });
  rmSync(dataDir, { recursive: true, force: true });
});

describe("resolveDataDir", () => {
  it("prefers MANIFOLD_DATA_DIR", () => {
    assert.equal(resolveDataDir({ env: { MANIFOLD_DATA_DIR: "/tmp/custom" }, home }), "/tmp/custom");
  });
  it("defaults to ~/.manifold", () => {
    assert.equal(resolveDataDir({ env: {}, home: "/fake/home" }), path.join("/fake/home", ".manifold"));
    assert.equal(defaultDataDir("/fake/home"), path.join("/fake/home", ".manifold"));
  });
});

describe("ensureMigrated", () => {
  it("fresh install: writes marker, copies nothing", () => {
    const r = ensureMigrated({ dataDir, home });
    assert.equal(r.migrated, false);
    const marker = readMigrationMarker(dataDir);
    assert.equal(marker?.fresh, true);
    assert.equal(existsSync(runtimeDbPath(dataDir)), false);
  });
  it("copies legacy DBs once, then idempotent", async () => {
    const { mkdirSync } = await import("node:fs");
    mkdirSync(path.join(home, ".openclaw"), { recursive: true });
    const Database = require("better-sqlite3");
    for (const name of ["runtime.sqlite", "ops.sqlite"]) {
      const db = new Database(path.join(home, ".openclaw", name));
      db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY);");
      db.close();
    }
    const r1 = ensureMigrated({ dataDir, home });
    assert.equal(r1.migrated, true);
    assert.ok(existsSync(runtimeDbPath(dataDir)));
    assert.ok(existsSync(opsDbPath(dataDir)));
    const marker = readMigrationMarker(dataDir);
    assert.equal(marker?.from?.length, 2);
    const r2 = ensureMigrated({ dataDir, home });
    assert.equal(r2.migrated, false);
  });
  it("does not overwrite existing package-owned DBs", () => {
    writeFileSync(runtimeDbPath(dataDir), "pinned");
    writeFileSync(opsDbPath(dataDir), "pinned");
    const r = ensureMigrated({ dataDir, home });
    assert.equal(r.migrated, false);
    assert.equal(readFileSync(runtimeDbPath(dataDir), "utf8"), "pinned");
  });
});
