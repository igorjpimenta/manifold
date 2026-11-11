/**
 * Unit tests for engine/loader.js's createPipelinesLoader.
 *
 * Regression coverage for the 2026-08-04 boot race: manifold-flow-revival
 * (and executor.js's per-flow fallback) each independently check
 * `pipelinesRef.value.size > 0` and start their own loadPipelines() pass if
 * they observed it still empty — so a caller arriving while the
 * "manifold-loader" service's own load was still in flight would kick off a
 * second, uncoordinated load and could act before either completed,
 * intermittently failing orphan revival with "pipeline not loaded: <id>".
 * createPipelinesLoader replaces that heuristic with one memoized promise
 * every caller shares.
 *
 * Kept in its own file (not appended to loader.test.js) — that file's
 * describe blocks share large on-disk temp directories reloaded via
 * loadPipelines() on every test, and interleaving this file's real
 * timer/promise-based async tests with it destabilized unrelated,
 * already-passing tests there.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createPipelinesLoader } from "../../../engine/loader.js";

const VALID_MINIMAL_PIPELINE = `
export default {
  id: "test_minimal",
  state: { items: [] },
  nodes: {
    step1: { kind: "code", run: async (state) => state },
  },
  edges: {
    __start__: "step1",
    step1: "__end__",
  },
};
`;

async function makeTempPipelinesDir() {
  const dir = path.join(os.tmpdir(), `gf-pipelines-loader-test-${randomUUID()}`);
  await mkdir(dir, { recursive: true });
  return {
    dir,
    async cleanup() {
      await rm(dir, { recursive: true, force: true });
    },
  };
}

describe("createPipelinesLoader — memoized single load", () => {
  test("concurrent callers share one in-flight load; load() is invoked exactly once", async () => {
    let calls = 0;
    let resolveLoad;
    const loadPromise = new Promise((resolve) => { resolveLoad = resolve; });
    const fakeMap = new Map([["p1", { id: "p1" }]]);
    const load = async () => {
      calls += 1;
      return loadPromise;
    };
    const pipelinesRef = { value: new Map() };
    const ensurePipelinesLoaded = createPipelinesLoader("/fake/dir", null, pipelinesRef, { load });

    // Mirrors manifold-loader and manifold-flow-revival both calling
    // ensurePipelinesLoaded() before the underlying load has resolved.
    const p1 = ensurePipelinesLoaded();
    const p2 = ensurePipelinesLoaded();
    assert.equal(calls, 1, "load() must only be invoked once even with concurrent callers");

    resolveLoad(fakeMap);
    const [r1, r2] = await Promise.all([p1, p2]);

    assert.equal(r1, fakeMap);
    assert.equal(r2, fakeMap);
    assert.equal(pipelinesRef.value, fakeMap, "pipelinesRef must reflect the shared load's result");
  });

  test("a caller arriving after the load already resolved gets the cached result, no reload", async () => {
    let calls = 0;
    const fakeMap = new Map([["p1", { id: "p1" }]]);
    const load = async () => { calls += 1; return fakeMap; };
    const pipelinesRef = { value: new Map() };
    const ensurePipelinesLoaded = createPipelinesLoader("/fake/dir", null, pipelinesRef, { load });

    await ensurePipelinesLoaded();
    const second = await ensurePipelinesLoaded();

    assert.equal(calls, 1);
    assert.equal(second, fakeMap);
  });

  test("integrates with the real loadPipelines to populate pipelinesRef from disk", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await mkdir(path.join(dir, "team_pipe"), { recursive: true });
      await writeFile(path.join(dir, "team_pipe", "index.js"), VALID_MINIMAL_PIPELINE, "utf8");
      const pipelinesRef = { value: new Map() };
      const ensurePipelinesLoaded = createPipelinesLoader(dir, null, pipelinesRef);
      const loaded = await ensurePipelinesLoaded();
      assert.ok(loaded.has("test_minimal"));
      assert.equal(pipelinesRef.value, loaded);
    } finally {
      await cleanup();
    }
  });
});
