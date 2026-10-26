// pipeline-dirs.test.js — per-harness pipeline dir overrides: upsert,
// dir-must-exist, resolution order table → env → config → default.
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createPipelineDirsStore } from "../../../state/pipeline-dirs.js";

let dir = null;
let store = null;
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), `pipeline-dirs-${randomUUID()}-`));
  store = createPipelineDirsStore({ dbPath: path.join(dir, "runtime.sqlite") });
});
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe("register", () => {
  it("upserts per harness", () => {
    const a = path.join(dir, "a");
    const b = path.join(dir, "b");
    fs.mkdirSync(a); fs.mkdirSync(b);
    store.register({ harness: "openclaw", dirPath: a });
    assert.equal(store.get("openclaw").path, a);
    store.register({ harness: "openclaw", dirPath: b });
    assert.equal(store.get("openclaw").path, b);
    assert.equal(store.list().length, 1);
  });

  it("throws when the directory does not exist", () => {
    assert.throws(
      () => store.register({ harness: "openclaw", dirPath: path.join(dir, "missing") }),
      /not a directory/,
    );
  });

  it("throws on empty harness", () => {
    assert.throws(() => store.register({ harness: "", dirPath: dir }), /harness/);
  });
});

describe("resolve", () => {
  it("prefers table over env over config over default, in order", () => {
    const t = path.join(dir, "t");
    fs.mkdirSync(t);
    store.register({ harness: "h", dirPath: t });
    const lookup = (h) => store.get(h);
    assert.equal(store.resolve({ harness: "h", env: { MANIFOLD_PIPELINES_DIR: "/e" }, configDir: "/c", dataDir: "/d", lookup }).source, "table");
    assert.equal(store.resolve({ harness: "other", env: { MANIFOLD_PIPELINES_DIR: "/e" }, configDir: "/c", dataDir: "/d", lookup }).dir, "/e");
    assert.equal(store.resolve({ harness: "other", env: {}, configDir: "/c", dataDir: "/d", lookup }).dir, "/c");
    const def = store.resolve({ harness: "other", env: {}, configDir: null, dataDir: "/d", lookup });
    assert.equal(def.dir, path.join("/d", "workspace", ".pipelines"));
    assert.equal(def.source, "default");
  });
});
