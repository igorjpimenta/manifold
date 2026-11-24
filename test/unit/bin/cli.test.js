import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm, readFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { resolveCliPipelinesDir } from "../../../bin/cli.js";
import { loadPipelines } from "../../../engine/loader.js";

const MINIMAL_PIPELINE = `
export default {
  id: "test_cli_dir",
  state: {},
  nodes: { step1: { kind: "code", run: async (state) => state } },
  edges: { __start__: "step1", step1: "__end__" },
};
`;

async function makeTempDir(prefix) {
  const dir = path.join(os.tmpdir(), `${prefix}-${randomUUID()}`);
  await mkdir(dir, { recursive: true });
  return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) };
}

describe("resolveCliPipelinesDir — seam-or-explicit, never the harness layout", () => {
  test("explicit flag wins over adapter, env, and store", () => {
    const r = resolveCliPipelinesDir({
      flagDir: "/flag/dir",
      resolvePipelinesDir: () => ({ dir: "/adapter/dir", source: "table" }),
      env: { MANIFOLD_DATA_DIR: "/data", MANIFOLD_PIPELINES_DIR: "/env/dir" },
    });
    assert.deepEqual(r, { dir: "/flag/dir", source: "flag" });
  });

  test("adapter seam wins over env and store", () => {
    const r = resolveCliPipelinesDir({
      resolvePipelinesDir: () => ({ dir: "/adapter/dir", source: "table" }),
      env: { MANIFOLD_DATA_DIR: "/data", MANIFOLD_PIPELINES_DIR: "/env/dir" },
    });
    assert.deepEqual(r, { dir: "/adapter/dir", source: "table" });
  });

  test("a throwing adapter falls through to the store seam", () => {
    const r = resolveCliPipelinesDir({
      resolvePipelinesDir: () => { throw new Error("store offline"); },
      env: { MANIFOLD_DATA_DIR: "/data" },
    });
    assert.equal(r.dir, path.join("/data", "pipelines"));
    assert.equal(r.source, "store");
  });

  test("MANIFOLD_PIPELINES_DIR env wins over the derived store default", () => {
    const r = resolveCliPipelinesDir({ env: { MANIFOLD_DATA_DIR: "/data", MANIFOLD_PIPELINES_DIR: "/env/dir" } });
    assert.deepEqual(r, { dir: "/env/dir", source: "env" });
  });

  test("store default derives from MANIFOLD_DATA_DIR", () => {
    const r = resolveCliPipelinesDir({ env: { MANIFOLD_DATA_DIR: "/data" } });
    assert.deepEqual(r, { dir: path.join("/data", "pipelines"), source: "store" });
  });

  test("loud refusal when nothing resolves names the flag and the env var", () => {
    assert.throws(
      () => resolveCliPipelinesDir({ storePaths: { pipelinesDir: "" }, env: {} }),
      (err) => {
        assert.match(err.message, /--pipelines-dir/);
        assert.match(err.message, /MANIFOLD_PIPELINES_DIR/);
        return true;
      },
    );
  });

  test("resolved explicit dir loads real pipelines from disk", async () => {
    const { dir, cleanup } = await makeTempDir("cli-pipelines-dir");
    try {
      await mkdir(path.join(dir, "team_pipe"), { recursive: true });
      await writeFile(path.join(dir, "team_pipe", "index.js"), MINIMAL_PIPELINE, "utf8");
      const resolved = resolveCliPipelinesDir({ flagDir: dir, env: {} });
      const loaded = await loadPipelines(resolved.dir, null);
      assert.ok(loaded.has("test_cli_dir"));
    } finally {
      await cleanup();
    }
  });

  test("bin/cli.js carries no harness-layout fallback", async () => {
    const src = await readFile(new URL("../../../bin/cli.js", import.meta.url), "utf8");
    assert.ok(!src.includes(".openclaw"), "cli.js must not reference the harness layout");
  });
});
