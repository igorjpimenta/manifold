import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createPipelinesWatcher } from "../../../engine/pipelines-watcher.js";

const MINIMAL_PIPELINE = (id) => `
export default {
  id: "${id}",
  state: { items: [] },
  nodes: { step1: { kind: "code", run: async (state) => state } },
  edges: { __start__: "step1", step1: "__end__" },
};
`;

const SUBFLOW_PARENT_PIPELINE = (id) => `
export default {
  id: "${id}",
  state: { items: [] },
  nodes: {
    NODE: {
      kind: "subflow",
      selector: () => [],
      mode: "any",
      flow: {
        nodes: { INNER: { kind: "code", run: async (state) => state } },
        edges: { __start__: "INNER", INNER: "__end__" },
      },
    },
  },
  edges: { __start__: "NODE", NODE: "__end__" },
};
`;

// Valid JS that throws at import — a transient reload failure, not a syntax error.
const BROKEN_PIPELINE = (id) => `
import { missing } from "./does-not-exist.js";
export default {
  id: "${id}",
  state: { items: [] },
  nodes: { step1: { kind: "code", run: async (state) => state } },
  edges: { __start__: "step1", step1: "__end__" },
};
`;

async function makeTempPipelinesDir() {
  const dir = path.join(os.tmpdir(), `gf-pipelines-watcher-test-${randomUUID()}`);
  await mkdir(dir, { recursive: true });
  return { dir, async cleanup() { await rm(dir, { recursive: true, force: true }); } };
}

async function writePipeline(dir, id, source = MINIMAL_PIPELINE(id)) {
  const pdir = path.join(dir, id);
  await mkdir(pdir, { recursive: true });
  await writeFile(path.join(pdir, "index.js"), source, "utf8");
}

describe("createPipelinesWatcher — onReload timing/ordering", () => {
  test("the first tick primes the fingerprint WITHOUT firing onReload (boot-time load already happened)", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      let fired = false;
      watcher.onReload(() => { fired = true; });
      await watcher._tick();
      assert.equal(fired, false, "first tick must not reload — see the source's own comment on this");
      assert.equal(pipelinesRef.value.size, 0, "pipelinesRef stays whatever it was before priming");
    } finally {
      await cleanup();
    }
  });

  test("a tick with no on-disk change does not fire onReload", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      await watcher._tick();
      let fired = false;
      watcher.onReload(() => { fired = true; });
      await watcher._tick();
      assert.equal(fired, false);
    } finally {
      await cleanup();
    }
  });

  test("a real on-disk change fires onReload with pipelinesRef.value ALREADY swapped to the new Map", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      await watcher._tick();

      let seenAtCallbackTime = null;
      watcher.onReload(() => {
        seenAtCallbackTime = pipelinesRef.value.has("test_p2");
      });

      await writePipeline(dir, "test_p2");
      await watcher._tick();

      assert.equal(seenAtCallbackTime, true, "pipelinesRef.value must already contain the new pipeline by the time the listener runs");
      assert.ok(pipelinesRef.value.has("test_p1"), "the original pipeline survives the reload too");
    } finally {
      await cleanup();
    }
  });

  test("multiple independent listeners all fire on the same reload", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      await watcher._tick();

      const calls = [];
      watcher.onReload(() => calls.push("a"));
      watcher.onReload(() => calls.push("b"));

      await writePipeline(dir, "test_p2");
      await watcher._tick();

      assert.deepEqual(calls.sort(), ["a", "b"], "both listeners must fire — index.js registers two independent ones");
    } finally {
      await cleanup();
    }
  });

  test("a listener that throws does not prevent other listeners from firing", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const warnings = [];
      const watcher = createPipelinesWatcher({
        pipelinesDir: dir, pipelinesRef, pollMs: 999999,
        logger: { warn: (m) => warnings.push(m), info: () => {} },
      });
      await watcher._tick();

      let secondFired = false;
      watcher.onReload(() => { throw new Error("boom"); });
      watcher.onReload(() => { secondFired = true; });

      await writePipeline(dir, "test_p2");
      await watcher._tick();

      assert.equal(secondFired, true, "one listener throwing must not stop the next one from running");
      assert.ok(warnings.some((w) => w.includes("boom")), "the throw should be logged, not silently swallowed");
    } finally {
      await cleanup();
    }
  });

  test("the unsubscribe function returned by onReload actually removes the listener", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_p1");
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      await watcher._tick();

      let fired = false;
      const unsubscribe = watcher.onReload(() => { fired = true; });
      unsubscribe();

      await writePipeline(dir, "test_p2");
      await watcher._tick();

      assert.equal(fired, false);
    } finally {
      await cleanup();
    }
  });
});

describe("createPipelinesWatcher — synthetic subflow-child pipelines survive a transient parent reload failure", () => {
  test("a synthetic child entry survives when its parent's index.js transiently fails to import but is still on disk", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_parent", SUBFLOW_PARENT_PIPELINE("test_parent"));
      const pipelinesRef = { value: new Map() };
      const warnings = [];
      const watcher = createPipelinesWatcher({
        pipelinesDir: dir, pipelinesRef, pollMs: 999999,
        logger: { warn: (m) => warnings.push(m), info: () => {} },
      });
      await watcher._tick();

      await writePipeline(dir, "test_other");
      await watcher._tick();
      assert.ok(pipelinesRef.value.has("test_parent"), "sanity: parent loaded");
      assert.ok(pipelinesRef.value.has("test_parent::NODE"), "sanity: synthetic child was minted");

      await writePipeline(dir, "test_parent", BROKEN_PIPELINE("test_parent"));
      await watcher._tick();

      assert.ok(pipelinesRef.value.has("test_parent"), "parent survives — its own index.js is still on disk");
      assert.ok(
        pipelinesRef.value.has("test_parent::NODE"),
        "synthetic child must survive too — its parent's index.js (not its own nonexistent directory) is what should be checked",
      );
      assert.ok(warnings.some((w) => w.includes('kept stale entry for "test_parent::NODE"')));
      assert.ok(pipelinesRef.value.has("test_other"), "unrelated pipeline unaffected");
    } finally {
      await cleanup();
    }
  });

  test("a synthetic child entry is dropped once its parent's index.js is genuinely removed from disk", async () => {
    const { dir, cleanup } = await makeTempPipelinesDir();
    try {
      await writePipeline(dir, "test_parent", SUBFLOW_PARENT_PIPELINE("test_parent"));
      const pipelinesRef = { value: new Map() };
      const watcher = createPipelinesWatcher({ pipelinesDir: dir, pipelinesRef, logger: null, pollMs: 999999 });
      await watcher._tick();
      await writePipeline(dir, "test_other");
      await watcher._tick();
      assert.ok(pipelinesRef.value.has("test_parent::NODE"), "sanity: synthetic child was minted");

      await rm(path.join(dir, "test_parent"), { recursive: true, force: true });
      await writePipeline(dir, "test_yet_another");
      await watcher._tick();

      assert.ok(!pipelinesRef.value.has("test_parent"), "parent correctly dropped — its index.js is genuinely gone");
      assert.ok(
        !pipelinesRef.value.has("test_parent::NODE"),
        "synthetic child must be dropped too once its parent is genuinely gone, not kept forever",
      );
    } finally {
      await cleanup();
    }
  });
});
