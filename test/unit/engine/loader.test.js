/**
 * Unit tests for engine/loader.js
 *
 * Covers validatePipeline (via loadPipelines with real temp-fs pipeline dirs)
 * and resolveRelativeImports (indirectly via loadPipelines source rewriting).
 *
 * validatePipeline is not exported, so we drive it through loadPipelines:
 * write a real index.js in a temp directory, call loadPipelines, and assert
 * whether the pipeline loaded successfully or was skipped with the expected
 * error logged.
 *
 * resolveRelativeImports is tested via loadPipelines by verifying that a
 * pipeline whose index.js uses relative imports can be loaded when the
 * imported module exists at the resolved absolute path.
 *
 * No SQLite is required — loader.js only touches the filesystem.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { loadPipelines } from "../../../engine/loader.js";

// ── helpers ──────────────────────────────────────────────────────────────────

/**
 * Create a fresh temp directory for one test run.
 * Returns { dir, pipelinesDir, cleanup }.
 */
async function makeTempPipelinesDir() {
  const dir = path.join(os.tmpdir(), `gf-loader-test-${randomUUID()}`);
  await mkdir(dir, { recursive: true });
  return {
    dir,
    async cleanup() {
      await rm(dir, { recursive: true, force: true });
    },
  };
}

/**
 * Write a pipeline folder `<pipelinesDir>/<name>/index.js` with the given source.
 */
async function writePipeline(pipelinesDir, name, source) {
  const pDir = path.join(pipelinesDir, name);
  await mkdir(pDir, { recursive: true });
  await writeFile(path.join(pDir, "index.js"), source, "utf8");
  return pDir;
}

/**
 * A logger that captures warn messages so tests can assert on them.
 */
function makeLogger() {
  const warns = [];
  const infos = [];
  return {
    warns,
    infos,
    logger: {
      info: (msg) => infos.push(msg),
      warn: (msg) => warns.push(msg),
      debug: () => {},
      error: () => {},
    },
  };
}

/**
 * Minimal valid pipeline source — passes all validation.
 */
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

// ── loadPipelines: missing / non-existent directory ──────────────────────────

describe("loadPipelines — missing directory", () => {
  test("returns empty map when dir does not exist (ENOENT)", async () => {
    const { logger } = makeLogger();
    const result = await loadPipelines("/tmp/__nonexistent_gf_test__", logger);
    assert.equal(result.size, 0);
  });

  test("logs info when dir does not exist", async () => {
    const { logger, infos } = makeLogger();
    await loadPipelines("/tmp/__nonexistent_gf_test__", logger);
    assert.ok(
      infos.some((m) => m.includes("not found")),
      "should log 'not found' info"
    );
  });
});

// ── loadPipelines: valid minimal pipeline ────────────────────────────────────

describe("loadPipelines — valid minimal pipeline", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
    await writePipeline(dir, "test_minimal", VALID_MINIMAL_PIPELINE);
  });

  after(async () => { await cleanup(); });

  test("loads the pipeline and returns a map with size 1", async () => {
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    assert.equal(result.size, 1);
  });

  test("loaded pipeline has the correct id", async () => {
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    assert.ok(result.has("test_minimal"));
  });

  test("loaded pipeline has _filepath and _dir attached", async () => {
    const result = await loadPipelines(dir);
    const p = result.get("test_minimal");
    assert.ok(p._filepath.endsWith("index.js"), "_filepath ends with index.js");
    assert.ok(p._dir.includes("test_minimal"), "_dir includes folder name");
  });
});

// ── validatePipeline: top-level shape ────────────────────────────────────────

describe("validatePipeline — top-level shape", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  async function expectSkipped(name, source, warnSnippet) {
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), source, "utf8");
    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    // Should not have loaded this pipeline
    const loaded = [...result.values()].find((p) => p._dir === pDir);
    assert.equal(loaded, undefined, `pipeline "${name}" should not have loaded`);
    if (warnSnippet) {
      assert.ok(
        warns.some((w) => w.includes(warnSnippet)),
        `expected warn containing "${warnSnippet}", got: ${JSON.stringify(warns)}`
      );
    }
    return warns;
  }

  test("throws when pipeline exports null", async () => {
    await expectSkipped(
      "null_export",
      `export default null;`,
      "failed to load"
    );
  });

  test("throws when pipeline exports a string", async () => {
    await expectSkipped(
      "string_export",
      `export default "not-an-object";`,
      "failed to load"
    );
  });

  test("throws when pipeline exports an array", async () => {
    await expectSkipped(
      "array_export",
      `export default [];`,
      "failed to load"
    );
  });

  test("throws when .id is missing", async () => {
    await expectSkipped(
      "missing_id",
      `export default { state: {}, nodes: {}, edges: { __start__: "x" } };`,
      "failed to load"
    );
  });

  test("throws when .id is not a string (number)", async () => {
    await expectSkipped(
      "numeric_id",
      `export default { id: 42, state: {}, nodes: {}, edges: { __start__: "x" } };`,
      "failed to load"
    );
  });

  test("throws when .state is missing", async () => {
    await expectSkipped(
      "missing_state",
      `export default { id: "p", nodes: {}, edges: { __start__: "x" } };`,
      "failed to load"
    );
  });

  test("throws when .state is not an object (string)", async () => {
    await expectSkipped(
      "state_string",
      `export default { id: "p", state: "bad", nodes: {}, edges: { __start__: "x" } };`,
      "failed to load"
    );
  });

  test("throws when .nodes is missing", async () => {
    await expectSkipped(
      "missing_nodes",
      `export default { id: "p", state: {}, edges: { __start__: "x" } };`,
      "failed to load"
    );
  });

  test("does not throw when .nodes is an empty array (Array is typeof object — validator allows it)", async () => {
    // The check is `!p.nodes || typeof p.nodes !== "object"`.
    // An array [] is truthy and typeof === "object", so it slips through.
    // This test documents the current behavior rather than asserting an exception.
    const name = "nodes_array";
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });
    await writeFile(
      path.join(pDir, "index.js"),
      `export default { id: "p_arr_nodes", state: {}, nodes: [], edges: { __start__: "x" } };`,
      "utf8"
    );
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    // Array has no own entries so Object.entries([]) = [] — loads fine with zero nodes
    const loaded = [...result.values()].find((p) => p._dir === pDir);
    assert.ok(loaded, "empty array nodes loads as zero-node pipeline");
  });

  test("throws when .edges is missing", async () => {
    await expectSkipped(
      "missing_edges",
      `export default { id: "p", state: {}, nodes: {} };`,
      "failed to load"
    );
  });

  test("throws when .edges is not an object", async () => {
    await expectSkipped(
      "edges_null",
      `export default { id: "p", state: {}, nodes: {}, edges: null };`,
      "failed to load"
    );
  });

  test("throws when edges.__start__ is absent", async () => {
    await expectSkipped(
      "no_start_edge",
      `export default { id: "p", state: {}, nodes: { n: { kind: "code", run: ()=>{} } }, edges: { n: "__end__" } };`,
      "failed to load"
    );
  });
});

// ── validatePipeline: node kind checks ───────────────────────────────────────

describe("validatePipeline — node kind", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  let counter = 0;
  function nextName() { return `kind_test_${counter++}`; }

  async function expectSkipped(source) {
    const name = nextName();
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), source, "utf8");
    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = [...result.values()].find((p) => p._dir === pDir);
    assert.equal(loaded, undefined, "should not have loaded");
    return warns;
  }

  async function expectLoaded(source) {
    const name = nextName();
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), source, "utf8");
    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = [...result.values()].find((p) => p._dir === pDir);
    assert.ok(loaded, `pipeline should have loaded (warns: ${JSON.stringify(warns)})`);
    return loaded;
  }

  test("throws for unknown node kind", async () => {
    await expectSkipped(`
      export default {
        id: "p_${Date.now()}_ukind",
        state: {},
        nodes: { n: { kind: "unknown-kind" } },
        edges: { __start__: "n" },
      };
    `);
  });

  test("code node: throws when .run is missing", async () => {
    await expectSkipped(`
      export default {
        id: "p_${Date.now()}_code_norun",
        state: {},
        nodes: { n: { kind: "code" } },
        edges: { __start__: "n" },
      };
    `);
  });

  test("code node: throws when .run is not a function", async () => {
    await expectSkipped(`
      export default {
        id: "p_${Date.now()}_code_run_string",
        state: {},
        nodes: { n: { kind: "code", run: "not a function" } },
        edges: { __start__: "n" },
      };
    `);
  });

  test("code node: valid .run function passes", async () => {
    await expectLoaded(`
      export default {
        id: "p_${Date.now()}_code_ok",
        state: {},
        nodes: { n: { kind: "code", run: async (s) => s } },
        edges: { __start__: "n" },
      };
    `);
  });
});

// ── validatePipeline: agent node ─────────────────────────────────────────────

describe("validatePipeline — agent node", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  function nextId() { return `agent_${Date.now()}_${counter++}`; }

  async function tryLoad(source) {
    const id = nextId();
    const name = id;
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), source, "utf8");
    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = [...result.values()].find((p) => p._dir === pDir);
    return { loaded, warns };
  }

  test("throws when agent node has neither .promptBuilder nor .skill", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_no_builder_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", apply: (s, o) => s } },
        edges: { __start__: "n" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when agent node has .promptBuilder but missing .apply and .applyResult", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_no_apply_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", promptBuilder: () => "p" } },
        edges: { __start__: "n" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("accepts agent node with .promptBuilder and .apply", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_pb_apply_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", promptBuilder: () => "p", apply: (s, o) => s } },
        edges: { __start__: "n" },
      };
    `);
    assert.ok(loaded, "should have loaded");
  });

  test("accepts agent node with .promptBuilder and .applyResult instead of .apply", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_apply_result_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", promptBuilder: () => "p", applyResult: (s, r) => s } },
        edges: { __start__: "n" },
      };
    `);
    assert.ok(loaded, "should have loaded with .applyResult");
  });

  test("accepts agent node with .skill string instead of .promptBuilder", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_skill_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", skill: "my-skill", apply: (s, o) => s } },
        edges: { __start__: "n" },
      };
    `);
    assert.ok(loaded, "should have loaded with .skill");
  });

  test("throws when .skill is a string but .apply and .applyResult are both missing", async () => {
    const { loaded } = await tryLoad(`
      export default {
        id: "agent_skill_no_apply_${Date.now()}",
        state: {},
        nodes: { n: { kind: "agent", skill: "my-skill" } },
        edges: { __start__: "n" },
      };
    `);
    assert.equal(loaded, undefined);
  });
});

// ── validatePipeline: human-gate node ────────────────────────────────────────

describe("validatePipeline — human-gate node", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  function nextId() { return `hgate_${Date.now()}_${counter++}`; }

  async function tryLoad(extra) {
    const id = nextId();
    const src = `
      export default {
        id: "${id}",
        state: {},
        nodes: { gate: { kind: "human-gate", ${extra} } },
        edges: { __start__: "gate" },
      };
    `;
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), src, "utf8");
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return [...result.values()].find((p) => p._dir === pDir);
  }

  test("throws when .pendingSelector is missing", async () => {
    const loaded = await tryLoad(`approvalRequestBuilder: (s) => ({})`)
    assert.equal(loaded, undefined);
  });

  test("throws when .approvalRequestBuilder is missing", async () => {
    const loaded = await tryLoad(`pendingSelector: (s) => []`);
    assert.equal(loaded, undefined);
  });

  test("throws when .pendingSelector is not a function", async () => {
    const loaded = await tryLoad(`pendingSelector: "not-fn", approvalRequestBuilder: (s) => ({})`);
    assert.equal(loaded, undefined);
  });

  test("accepts valid human-gate with both functions", async () => {
    const loaded = await tryLoad(`pendingSelector: (s) => [], approvalRequestBuilder: (s) => ({})`);
    assert.ok(loaded, "should load valid human-gate");
  });

  test("throws when .apply is present but not a function", async () => {
    const loaded = await tryLoad(`pendingSelector: (s) => [], approvalRequestBuilder: (s) => ({}), apply: "not-fn"`);
    assert.equal(loaded, undefined);
  });

  test("accepts valid human-gate with an .apply function", async () => {
    const loaded = await tryLoad(`pendingSelector: (s) => [], approvalRequestBuilder: (s) => ({}), apply: async (s, item) => {}`);
    assert.ok(loaded, "should load valid human-gate with apply");
  });
});

// ── validatePipeline: loop node (branch mode) ────────────────────────────────

// Stage 1.5 — .mode is required on every kind:"subflow" node, no default.
// Doesn't reuse the sibling
// describe blocks' tryLoad helpers below since those bake mode:"any" into
// their template on purpose (they're testing unrelated validation paths).
describe("validatePipeline — subflow node .mode (Stage 1.5)", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => { ({ dir, cleanup } = await makeTempPipelinesDir()); });
  after(async () => { await cleanup(); });

  function nextId() { return `subflow_mode_${Date.now()}_${counter++}`; }

  async function tryLoad(modeField) {
    const id = nextId();
    const src = `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          myloop: {
            kind: "subflow",
            selector: (s) => [],
            ${modeField}
            flow: { nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } },
          },
        },
        edges: { __start__: "myloop" },
      };
    `;
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), src, "utf8");
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return [...result.values()].find((p) => p._dir === pDir);
  }

  test("throws when .mode is missing entirely", async () => {
    const loaded = await tryLoad("");
    assert.equal(loaded, undefined, "a subflow node with no .mode must fail to load");
  });

  test('throws when .mode is an unrecognized value (not "any"/"all")', async () => {
    const loaded = await tryLoad('mode: "sometimes",');
    assert.equal(loaded, undefined);
  });

  test('accepts .mode: "any"', async () => {
    const loaded = await tryLoad('mode: "any",');
    assert.ok(loaded, 'mode:"any" should load');
    assert.equal(loaded.nodes.myloop.mode, "any");
  });

  test('accepts .mode: "all"', async () => {
    const loaded = await tryLoad('mode: "all",');
    assert.ok(loaded, 'mode:"all" should load');
    assert.equal(loaded.nodes.myloop.mode, "all");
  });
});

describe("validatePipeline — loop node (branch mode)", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  function nextId() { return `loop_branch_${Date.now()}_${counter++}`; }

  async function tryLoad(nodeBody) {
    const id = nextId();
    const src = `
      export default {
        id: "${id}",
        state: {},
        nodes: { myloop: { kind: "subflow", selector: (s) => [], mode: "any", ${nodeBody} } },
        edges: { __start__: "myloop" },
      };
    `;
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), src, "utf8");
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return [...result.values()].find((p) => p._dir === pDir);
  }

  test("throws when .flow is present but .flow.nodes is missing", async () => {
    const loaded = await tryLoad(`
      flow: {
        edges: { __start__: "s1" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when .flow is present but .flow.edges is missing", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { s1: { kind: "code", run: () => {} } },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when .flow.edges lacks __start__", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { s1: { kind: "code", run: () => {} } },
        edges: { s1: "__end__" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("accepts branch-mode loop without explicit groupId (uses nodeId as implicit key)", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { s1: { kind: "code", run: () => {} } },
        edges: { __start__: "s1" },
      }
    `);
    assert.ok(loaded, "branch-mode without groupId should load");
  });

  // Both "explicit custom groupId" collision scenarios that used to be
  // tested here are impossible now — childFlows:true (mandatory on every
  // loop node) forbids setting .groupId at all (see "throws when .childFlows
  // is true and a custom .groupId is also set" in the childFlows describe
  // block below), and an implicit-groupId collision between two loop nodes'
  // own ids can't happen since node ids are already unique JS object keys.

  test("throws when branch sub-node has unknown kind", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { s1: { kind: "not-a-thing" } },
        edges: { __start__: "s1" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when branch sub-node of kind agent is missing .promptBuilder", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { s1: { kind: "agent", apply: (s, o) => s } },
        edges: { __start__: "s1" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("branch sub-node human-gate: throws when missing .approvalRequestBuilder", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate" } },
        edges: { __start__: "g" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  // A branch sub-node human-gate under childFlows:true (mandatory on every
  // loop node) DOES require .pendingSelector — see "validatePipeline — loop
  // node childFlows"'s own "REQUIRES .pendingSelector" test below, which
  // supersedes what used to be tested here for the now-removed plain,
  // non-childFlows branch mode.

  test("branch sub-node human-gate: throws when .apply is present but not a function", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate", pendingSelector: (s) => [], approvalRequestBuilder: (item) => ({}), apply: "not-fn" } },
        edges: { __start__: "g" },
      }
    `);
    assert.equal(loaded, undefined);
  });

  test("branch sub-node human-gate: accepts a valid .apply function (GAP_GATE shape)", async () => {
    const loaded = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate", pendingSelector: (s) => [], approvalRequestBuilder: (item) => ({}), apply: async (ctx, item) => {} } },
        edges: { __start__: "g" },
      }
    `);
    assert.ok(loaded, "branch human-gate with a valid .apply should load");
  });

});

// ── validatePipeline: loop node childFlows (Phase 2) ────────────────────────

// childFlows is no longer a field validatePipeline reads — every subflow
// node's items become real child flows unconditionally — so this describe
// block covers what remains real: a .flow definition is required, groupId
// is always the node's own id, human-gate sub-nodes require
// .pendingSelector, and the subflow registers as an addressable child
// pipeline.
describe("validatePipeline — loop node (childFlows behavior, unconditional)", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  function nextId() { return `loop_childflows_${Date.now()}_${counter++}`; }

  async function tryLoad(nodeBody) {
    const id = nextId();
    const src = `
      export default {
        id: "${id}",
        state: {},
        nodes: { myloop: { kind: "subflow", selector: (s) => [], mode: "any", ${nodeBody} } },
        edges: { __start__: "myloop" },
      };
    `;
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), src, "utf8");
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return { id, result, loaded: [...result.values()].find((p) => p._dir === pDir && p.id === id) };
  }

  test("throws when .flow is missing", async () => {
    const { loaded } = await tryLoad(`
      groupId: "g",
      apply: async (s, item, out) => s, promptBuilder: (s) => "x"
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a custom .groupId is set", async () => {
    const { loaded } = await tryLoad(`
      groupId: "custom",
      flow: { nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } }
    `);
    assert.equal(loaded, undefined);
  });

  test("branch human-gate sub-node REQUIRES .pendingSelector", async () => {
    const { loaded } = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate", approvalRequestBuilder: (state, itemId) => ({}) } },
        edges: { __start__: "g" },
      }
    `);
    assert.equal(loaded, undefined, "human-gate without .pendingSelector must fail");
  });

  test("branch human-gate sub-node loads with a proper root-level shape", async () => {
    const { loaded } = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate", pendingSelector: (state) => [], approvalRequestBuilder: (state, itemId) => ({}) } },
        edges: { __start__: "g" },
      }
    `);
    assert.ok(loaded, "human-gate with pendingSelector + approvalRequestBuilder should load");
  });

  test("registers the branch as an addressable child pipeline (${parentId}::${nodeId})", async () => {
    const { id, result } = await tryLoad(`
      flow: { nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } }
    `);
    const synthetic = result.get(`${id}::myloop`);
    assert.ok(synthetic, "synthetic child pipeline should be registered");
    assert.equal(synthetic.nodes.s1.kind, "code");
    assert.equal(typeof synthetic.nodes.s1.run, "function");
    assert.equal(synthetic.edges.__start__, "s1");
    assert.equal(synthetic._synthetic, true);
    assert.equal(synthetic._parentPipelineId, id);
    assert.equal(synthetic._parentNodeId, "myloop");
  });

  test("recursively registers a nested subflow (a flow.nodes entry that is itself kind:\"subflow\") as its own addressable child pipeline", async () => {
    const { id, result } = await tryLoad(`
      flow: {
        nodes: {
          inner: {
            kind: "subflow",
            mode: "all",
            selector: (s) => [{ id: "only" }],
            flow: { nodes: { s2: { kind: "code", run: () => {} } }, edges: { __start__: "s2" } },
          },
        },
        edges: { __start__: "inner" },
      }
    `);
    const outer = result.get(`${id}::myloop`);
    assert.ok(outer, "outer synthetic child pipeline should be registered");
    const nested = result.get(`${id}::myloop::inner`);
    assert.ok(nested, "nested synthetic child pipeline should ALSO be registered — engine/executor-subflow-children.js computes a spawned child's pipelineId as ${state.pipelineId}::${nodeId}, so an unregistered nested id would fail at dispatch time, not load time");
    assert.equal(nested.nodes.s2.kind, "code");
    assert.equal(nested.edges.__start__, "s2");
    assert.equal(nested._synthetic, true);
    assert.equal(nested._parentPipelineId, `${id}::myloop`);
    assert.equal(nested._parentNodeId, "inner");
  });

  test("throws when a nested subflow (flow.nodes entry) is missing .mode — deep validation, not just kind recognition", async () => {
    const { loaded } = await tryLoad(`
      flow: {
        nodes: {
          inner: {
            kind: "subflow",
            selector: (s) => [],
            flow: { nodes: { s2: { kind: "code", run: () => {} } }, edges: { __start__: "s2" } },
          },
        },
        edges: { __start__: "inner" },
      }
    `);
    assert.equal(loaded, undefined, "a nested subflow node missing .mode must fail to load, same as a top-level one would");
  });

  test("propagates flow.kind onto the synthesized pipeline object", async () => {
    const { id, result } = await tryLoad(`
      flow: { kind: "cascade", nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } }
    `);
    const synthetic = result.get(`${id}::myloop`);
    assert.ok(synthetic, "synthetic child pipeline should be registered");
    assert.equal(synthetic.kind, "cascade");
  });

  test("synthesized pipeline's .kind is undefined when .flow.kind is not declared", async () => {
    const { id, result } = await tryLoad(`
      flow: { nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } }
    `);
    const synthetic = result.get(`${id}::myloop`);
    assert.ok(synthetic, "synthetic child pipeline should be registered");
    assert.equal(synthetic.kind, undefined, "a plain subflow with no declared .flow.kind stays inert");
  });

  test("a pipeline with an invalid branch fails to load entirely — no partial/corrupt state", async () => {
    // The pendingSelector requirement is checked identically during the
    // parent's own branch-nodes validation and during the synthetic
    // pipeline's validation (deliberately kept in lockstep), so an
    // invalid branch is rejected at the parent's own load step, before
    // synthesis ever runs — neither the parent nor a synthetic entry ends
    // up registered.
    const { id, result, loaded } = await tryLoad(`
      flow: {
        nodes: { g: { kind: "human-gate", approvalRequestBuilder: (state, itemId) => ({}) } },
        edges: { __start__: "g" },
      }
    `);
    assert.equal(loaded, undefined, "parent with invalid human-gate branch fails to load");
    assert.equal(result.get(`${id}::myloop`), undefined, "no synthetic entry registered either");
  });

  test("a second, independently-valid pipeline in the same directory still loads when another pipeline's branch is invalid", async () => {
    // Confirms the per-directory try/catch in loadPipelines() isolates
    // failures — one bad pipeline (or its branch) never takes down
    // sibling pipelines in the same scan.
    const badId = nextId();
    await mkdir(path.join(dir, badId), { recursive: true });
    await writeFile(path.join(dir, badId, "index.js"), `
      export default {
        id: "${badId}",
        state: {},
        nodes: { myloop: { kind: "subflow", selector: (s) => [], flow: {
          nodes: { g: { kind: "human-gate", approvalRequestBuilder: (s, i) => ({}) } },
          edges: { __start__: "g" },
        } } },
        edges: { __start__: "myloop" },
      };
    `, "utf8");

    const { loaded } = await tryLoad(`
      flow: { nodes: { s1: { kind: "code", run: () => {} } }, edges: { __start__: "s1" } }
    `);
    assert.ok(loaded, "the independently-valid pipeline should still load");
  });
});

// ── validatePipeline: groups ──────────────────────────────────────────────────

describe("validatePipeline — .groups", () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  function nextId() { return `groups_${Date.now()}_${counter++}`; }

  async function tryLoad(src) {
    const id = nextId();
    // Inject unique id
    const withId = src.replace("__ID__", id);
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), withId, "utf8");
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return [...result.values()].find((p) => p._dir === pDir);
  }

  test("throws when .groups is an array instead of an object", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: { n: { kind: "code", run: () => {} } },
        edges: { __start__: "n" },
        groups: [],
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a group has an empty .members array", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: { n: { kind: "code", run: () => {} } },
        edges: { __start__: "n" },
        groups: { g1: { members: [] } },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a group member string is not found in .nodes", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: { n: { kind: "code", run: () => {} } },
        edges: { __start__: "n" },
        groups: { g1: { members: ["nonexistent"] } },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a group member uses dot-notation but parent node is not a loop", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: { n: { kind: "code", run: () => {} } },
        edges: { __start__: "n" },
        groups: { g1: { members: ["n.child"] } },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a group member uses dot-notation and child is not in flow.nodes", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          myloop: {
            kind: "subflow",
            selector: (s) => [],
            flow: {
              nodes: { s1: { kind: "code", run: () => {} } },
              edges: { __start__: "s1" },
            },
          }
        },
        edges: { __start__: "myloop" },
        groups: { g1: { members: ["myloop.nonexistent"] } },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("throws when a group mixes branch-scoped members from different loop parents", async () => {
    // Mixing members from two different branch parents triggers the scope check.
    // The first branch member sets scopeParent = "loop1"; the second from "loop2"
    // triggers memberScope !== scopeParent → throws.
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          loop1: {
            kind: "subflow",
            selector: (s) => [],
            flow: {
              nodes: { s1: { kind: "code", run: () => {} } },
              edges: { __start__: "s1" },
            },
          },
          loop2: {
            kind: "subflow",
            selector: (s) => [],
            flow: {
              nodes: { s2: { kind: "code", run: () => {} } },
              edges: { __start__: "s2" },
            },
          }
        },
        edges: { __start__: "loop1", loop1: "loop2" },
        groups: { g1: { members: ["loop1.s1", "loop2.s2"] } },
      };
    `);
    assert.equal(loaded, undefined, "mixed branch parents should fail");
  });

  test("throws when a group has a branch-scoped member followed by a top-level member", async () => {
    // branch-scoped first → sets scopeParent = "myloop"
    // top-level second → memberScope = null, null !== "myloop" → throws
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          topNode: { kind: "code", run: () => {} },
          myloop: {
            kind: "subflow",
            selector: (s) => [],
            flow: {
              nodes: { s1: { kind: "code", run: () => {} } },
              edges: { __start__: "s1" },
            },
          }
        },
        edges: { __start__: "myloop", myloop: "topNode" },
        groups: { g1: { members: ["myloop.s1", "topNode"] } },
      };
    `);
    assert.equal(loaded, undefined, "branch-scoped then top-level in same group should fail");
  });

  test("throws when a node appears in multiple groups", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          n1: { kind: "code", run: () => {} },
          n2: { kind: "code", run: () => {} },
        },
        edges: { __start__: "n1", n1: "n2" },
        groups: {
          g1: { members: ["n1"] },
          g2: { members: ["n1", "n2"] },
        },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("accepts valid groups with top-level members", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          n1: { kind: "code", run: () => {} },
          n2: { kind: "code", run: () => {} },
        },
        edges: { __start__: "n1", n1: "n2" },
        groups: {
          g1: { members: ["n1"] },
          g2: { members: ["n2"] },
        },
      };
    `);
    assert.ok(loaded, "valid groups should load");
  });

  test("accepts valid groups with branch-scoped members", async () => {
    const loaded = await tryLoad(`
      export default {
        id: "__ID__",
        state: {},
        nodes: {
          myloop: {
            kind: "subflow",
            selector: (s) => [],
            mode: "any",
            flow: {
              nodes: {
                s1: { kind: "code", run: () => {} },
                s2: { kind: "code", run: () => {} },
              },
              edges: { __start__: "s1", s1: "s2" },
            },
          }
        },
        edges: { __start__: "myloop" },
        groups: {
          branchGroup: { members: ["myloop.s1", "myloop.s2"] },
        },
      };
    `);
    assert.ok(loaded, "valid branch-scoped group should load");
  });

});

// ── resolveRelativeImports — tested indirectly via loadPipelines ──────────────

describe("resolveRelativeImports — via loadPipelines", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  test("rewrites static from './x' to absolute file:// URL so sibling modules load", async () => {
    // Write a sibling helper module that the pipeline imports via relative path.
    const name = `rel_import_test_${Date.now()}`;
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });

    // Write the helper
    await writeFile(
      path.join(pDir, "helpers.js"),
      `export const HELPER_VALUE = "hello-from-helper";`,
      "utf8"
    );

    // Write the pipeline that imports from './helpers'
    await writeFile(
      path.join(pDir, "index.js"),
      `
import { HELPER_VALUE } from './helpers.js';
export default {
  id: "${name}",
  state: { helperLoaded: HELPER_VALUE },
  nodes: { n: { kind: "code", run: (s) => s } },
  edges: { __start__: "n" },
};
`,
      "utf8"
    );

    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = result.get(name);
    assert.ok(loaded, `should have loaded (warns: ${JSON.stringify(warns)})`);
    assert.equal(loaded.state.helperLoaded, "hello-from-helper");
  });

  test("rewrites dynamic import('./x') to absolute file:// URL so it resolves", async () => {
    const name = `dyn_import_test_${Date.now()}`;
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });

    await writeFile(
      path.join(pDir, "data.js"),
      `export const DATA = 42;`,
      "utf8"
    );

    await writeFile(
      path.join(pDir, "index.js"),
      `
const { DATA } = await import('./data.js');
export default {
  id: "${name}",
  state: { data: DATA },
  nodes: { n: { kind: "code", run: (s) => s } },
  edges: { __start__: "n" },
};
`,
      "utf8"
    );

    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = result.get(name);
    assert.ok(loaded, `should have loaded (warns: ${JSON.stringify(warns)})`);
    assert.equal(loaded.state.data, 42);
  });

  test("leaves bare specifiers (non-relative) unchanged — they fail naturally (not a rewrite bug)", async () => {
    // A bare specifier that doesn't exist should fail with an import error,
    // not a rewrite error. This tests that resolveRelativeImports doesn't
    // accidentally mangle bare specifiers.
    const name = `bare_specifier_test_${Date.now()}`;
    const pDir = path.join(dir, name);
    await mkdir(pDir, { recursive: true });

    await writeFile(
      path.join(pDir, "index.js"),
      `
// A non-relative import that won't be rewritten
// We use node:path which IS a valid bare specifier so it loads fine
import { join } from 'node:path';
export default {
  id: "${name}",
  state: {},
  nodes: { n: { kind: "code", run: (s) => s } },
  edges: { __start__: "n" },
};
`,
      "utf8"
    );

    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const loaded = result.get(name);
    assert.ok(loaded, `bare node: specifier should load fine (warns: ${JSON.stringify(warns)})`);
  });
});

// ── loadPipelines: duplicate pipeline id ─────────────────────────────────────

describe("loadPipelines — duplicate pipeline id", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  test("keeps the first when two folders export the same pipeline id", async () => {
    const sharedId = `dup_test_${Date.now()}`;
    const src = `export default {
      id: "${sharedId}",
      state: {},
      nodes: { n: { kind: "code", run: () => {} } },
      edges: { __start__: "n" },
    };`;
    await writePipeline(dir, "aaa_first", src);
    await writePipeline(dir, "zzz_second", src);

    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    assert.ok(result.has(sharedId), "pipeline should be in map");
    assert.ok(
      warns.some((w) => w.includes("duplicate")),
      `expected duplicate warn, got: ${JSON.stringify(warns)}`
    );
    // Only one entry with this id
    assert.equal(result.size, 1);
  });
});

// ── loadPipelines: skips .js flat files and hidden dirs ───────────────────────

describe("loadPipelines — filesystem edge cases", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  test("skips legacy flat .js files and logs a warn", async () => {
    await writeFile(path.join(dir, "legacy.js"), `export default {};`, "utf8");
    const { logger, warns } = makeLogger();
    await loadPipelines(dir, logger);
    assert.ok(
      warns.some((w) => w.includes("legacy")),
      `expected legacy flat-file warn, got: ${JSON.stringify(warns)}`
    );
  });

  test("skips directories without index.js and logs a warn", async () => {
    await mkdir(path.join(dir, "no_index_dir"), { recursive: true });
    await writeFile(path.join(dir, "no_index_dir", "something.js"), ``, "utf8");
    const { logger, warns } = makeLogger();
    await loadPipelines(dir, logger);
    assert.ok(
      warns.some((w) => w.includes("no index.js") || w.includes("skipping")),
      `expected no-index.js warn, got: ${JSON.stringify(warns)}`
    );
  });
});

// ── validatePipeline — "needs:" (Option B, SUBFLOW_PARENT_STATE_ACCESS.
// design.md's "needs: v3") ──────────────────────────────────────────────────
//
// Keyed by local variable name — { varName: {hops, nodeId} } — not an
// array, since ctx.needs(varName) resolves by that key, not by nodeId.
//
// A needs: violation anywhere in the tree — top-level or nested arbitrarily
// deep inside node.flow.nodes — fails the WHOLE pipeline's load, because
// validateFlowSubNodes/validateSubflowNode's recursion all happens inside
// the ONE validatePipeline(pipeline, ...) call loadPipelines makes before
// registerSubflowChildren ever runs (loadPipelines.js:110) — there is no
// partial-tree degradation for a structural validation error, same as the
// existing .mode tests above.
describe('validatePipeline — "needs:"', () => {
  let dir, cleanup;
  let counter = 0;

  before(async () => { ({ dir, cleanup } = await makeTempPipelinesDir()); });
  after(async () => { await cleanup(); });

  function nextId() { return `needs_${Date.now()}_${counter++}`; }

  async function load(srcBuilder) {
    const id = nextId();
    const src = srcBuilder(id);
    const pDir = path.join(dir, id);
    await mkdir(pDir, { recursive: true });
    await writeFile(path.join(pDir, "index.js"), src, "utf8");
    const { logger, warns } = makeLogger();
    const result = await loadPipelines(dir, logger);
    return { id, pDir, result, warns, loaded: [...result.values()].find((p) => p._dir === pDir) };
  }

  test("a top-level node's needs: is rejected outright — hop:1 from a root pipeline has no ancestor at all", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: 1, nodeId: "X" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined, "a root pipeline's own node can never have a real hop:1 ancestor");
  });

  test("needs: entry with a non-integer hops throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: "one", nodeId: "X" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: entry with a missing nodeId throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: 1 } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: as an array (the old v2 shape) throws — must be a keyed object now", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: [{ hops: 1, nodeId: "X" }] },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: as a bare {hops,nodeId} object (not keyed by varName) throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { hops: 1, nodeId: "X" } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("a human-gate node declaring needs: throws — ctx.needs() is not wired into pendingSelector/approvalRequestBuilder yet", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          GATE: {
            kind: "human-gate",
            needs: { x: { hops: 1, nodeId: "SCAN" } },
            pendingSelector: () => [],
            approvalRequestBuilder: () => ({ title: "", description: "", layout: [] }),
          },
        },
        edges: { __start__: "SCAN", SCAN: "GATE" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("a nested node's needs:{hops:1} referencing a nonexistent nodeId in its immediate parent fails the whole load", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, nodeId: "DOES_NOT_EXIST" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("a nested node's needs:{hops:1} referencing a REAL sibling of its own OUTER subflow node loads and registers the synthetic child", async () => {
    const { loaded, result, id } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { scan: { hops: 1, nodeId: "SCAN" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "SCAN", SCAN: "OUTER" },
      };
    `);
    assert.ok(loaded, "SCAN is a real top-level sibling of OUTER — hops:1 from INNER must resolve to it");
    assert.ok(result.has(`${id}::OUTER`), "the synthetic child pipeline must still register — needs: validation must not block registerSubflowChildren's own pass");
    assert.deepEqual(loaded.nodes.OUTER.flow.nodes.INNER.needs, { scan: { hops: 1, nodeId: "SCAN" } }, "the keyed declaration survives load intact for ctx.needs() to read at runtime");
  });

  test("hops:2 from a doubly-nested node resolves through two real ancestor levels", async () => {
    const { loaded, result, id } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          ROOT_ARGS: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: {
                MIDDLE: {
                  kind: "subflow", selector: () => [], mode: "any",
                  flow: {
                    nodes: { DEEP: { kind: "code", run: () => {}, needs: { rootArgs: { hops: 2, nodeId: "ROOT_ARGS" } } } },
                    edges: { __start__: "DEEP" },
                  },
                },
              },
              edges: { __start__: "MIDDLE" },
            },
          },
        },
        edges: { __start__: "ROOT_ARGS", ROOT_ARGS: "OUTER" },
      };
    `);
    assert.ok(loaded, "hops:2 from DEEP (inside MIDDLE, inside OUTER) must resolve past MIDDLE to the root pipeline's own ROOT_ARGS");
    assert.ok(result.has(`${id}::OUTER::MIDDLE`), "both nesting levels must still register");
  });

  test("needs:{fields} as a single-element array loads fine", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { listings: { hops: 1, nodeId: "SCAN", fields: ["listings"] } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "SCAN", SCAN: "OUTER" },
      };
    `);
    assert.ok(loaded);
    assert.deepEqual(loaded.nodes.OUTER.flow.nodes.INNER.needs.listings.fields, ["listings"]);
  });

  test("needs:{fields} as a bare string (not an array) throws — always an array, even for one field", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, nodeId: "SCAN", fields: "listings" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "SCAN", SCAN: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs:{fields} as an empty array throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, nodeId: "SCAN", fields: [] } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "SCAN", SCAN: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("hops:2 that overshoots the real nesting depth (only 1 real ancestor level) fails the whole load", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 2, nodeId: "ANYTHING" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined, "INNER only has ONE real ancestor level (OUTER's own pipeline) — hops:2 walks off the top statically");
  });

  test("needs:{entry} for a recognized bookkeeping field (runId) loads fine", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { runId: { hops: 1, entry: "runId" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.ok(loaded);
    assert.deepEqual(loaded.nodes.OUTER.flow.nodes.INNER.needs.runId, { hops: 1, entry: "runId" });
  });

  test("needs:{entry} for an unrecognized field name throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, entry: "notARealField" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs:{entry} for 'gates' throws — gates has its own dedicated accessor (gate_records), not this path", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, entry: "gates" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: declaring both .nodeId and .entry together throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1, nodeId: "SCAN", entry: "runId" } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "SCAN", SCAN: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: declaring neither .nodeId nor .entry throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: { INNER: { kind: "code", run: () => {}, needs: { x: { hops: 1 } } } },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs:{entry} at hops:2 (root's own runId from a doubly-nested node) loads fine — no node-map check needed for entry-type needs", async () => {
    const { loaded, id } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          OUTER: {
            kind: "subflow", selector: () => [], mode: "any",
            flow: {
              nodes: {
                MIDDLE: {
                  kind: "subflow", selector: () => [], mode: "any",
                  flow: {
                    nodes: { DEEP: { kind: "code", run: () => {}, needs: { runId: { hops: 2, entry: "runId" } } } },
                    edges: { __start__: "DEEP" },
                  },
                },
              },
              edges: { __start__: "MIDDLE" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    assert.ok(loaded, "hops:2 from DEEP reaches the root pipeline itself — no node named 'runId' needs to exist there, it's an entry field");
  });

  test("needs:{hops:0, nodeId} referencing a REAL sibling in the SAME root pipeline loads fine — no ancestor required at hops:0", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          A: { kind: "code", run: () => {}, needs: { x: { hops: 0, nodeId: "SCAN" } } },
        },
        edges: { __start__: "SCAN", SCAN: "A" },
      };
    `);
    assert.ok(loaded, "hops:0 resolves against this pipeline's own p.nodes, unlike hops:1+ which needs a real ancestor");
  });

  test("needs:{hops:0, nodeId} referencing a nonexistent node in the SAME pipeline throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: 0, nodeId: "DOES_NOT_EXIST" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: a negative hops throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: -1, nodeId: "A" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs:{hops:0, config} referencing this pipeline's own declared state — loads fine even in a root pipeline, no ancestor needed", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: { sourceFilter: { includeAgentSelfRediscovery: true } },
        nodes: {
          A: { kind: "code", run: () => {}, needs: { sourceFilter: { hops: 0, config: "sourceFilter" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.ok(loaded, "config-type needs never checks a node map — it reads the pipeline's own state: block at runtime, not validated against a schema at load time");
  });

  test("needs:{config} with an empty string throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: 0, config: "" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: declaring both .nodeId and .config together throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          SCAN: { kind: "code", run: () => {} },
          A: { kind: "code", run: () => {}, needs: { x: { hops: 0, nodeId: "SCAN", config: "sourceFilter" } } },
        },
        edges: { __start__: "SCAN", SCAN: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });

  test("needs: declaring both .entry and .config together throws", async () => {
    const { loaded } = await load((id) => `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          A: { kind: "code", run: () => {}, needs: { x: { hops: 0, entry: "runId", config: "sourceFilter" } } },
        },
        edges: { __start__: "A" },
      };
    `);
    assert.equal(loaded, undefined);
  });
});

// registerSubflowChildren's synthetic child pipelines only copy
// {id, kind, state, nodes, edges} off node.flow — a root-level field like
// proposalMeta (declared once on the parent's own index.js, e.g.
// freelance_freelancer-proposal's requiredDeliveryField opt-in) lives
// nowhere on node.flow. Without explicitly threading it through, a tool
// that resolves its own pipelineId to a per-item subflow's compound id
// (e.g. "freelance_freelancer-proposal::PROPOSE" — exactly what
// session_lineage records for a DRAFT-node dispatch) and looks it up via
// pipelinesRef.value.get(pipelineId) would silently miss the parent's
// config. Confirmed live 2026-09-17: this exact gap, stacked with
// submit-draft.js's pipelineId resolution gap (see that file's own test),
// made every Freelancer.com draft fall back to "timeframe" instead of the
// pipeline's declared "period".
describe("registerSubflowChildren — proposalMeta propagation to synthetic children", () => {
  let dir, cleanup;

  before(async () => {
    ({ dir, cleanup } = await makeTempPipelinesDir());
  });

  after(async () => { await cleanup(); });

  test("a synthetic subflow child inherits the root pipeline's proposalMeta", async () => {
    const id = `proposalmeta_${randomUUID().slice(0, 8)}`;
    await writePipeline(dir, id, `
      export default {
        id: "${id}",
        state: {},
        proposalMeta: { requiredDeliveryField: "period" },
        nodes: {
          PROPOSE: {
            kind: "subflow", mode: "any", selector: (s) => [],
            flow: {
              nodes: { DRAFT: { kind: "code", run: () => {} } },
              edges: { __start__: "DRAFT" },
            },
          },
        },
        edges: { __start__: "PROPOSE" },
      };
    `);
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const root = result.get(id);
    assert.equal(root.proposalMeta.requiredDeliveryField, "period");
    const synthetic = result.get(`${id}::PROPOSE`);
    assert.ok(synthetic, "synthetic child pipeline should be registered");
    assert.deepEqual(synthetic.proposalMeta, { requiredDeliveryField: "period" });
  });

  test("a nested (grandchild) subflow also inherits the root's proposalMeta", async () => {
    const id = `proposalmeta_nested_${randomUUID().slice(0, 8)}`;
    await writePipeline(dir, id, `
      export default {
        id: "${id}",
        state: {},
        proposalMeta: { requiredDeliveryField: "period" },
        nodes: {
          OUTER: {
            kind: "subflow", mode: "any", selector: (s) => [],
            flow: {
              nodes: {
                INNER: {
                  kind: "subflow", mode: "all", selector: (s) => [{ id: "only" }],
                  flow: { nodes: { s: { kind: "code", run: () => {} } }, edges: { __start__: "s" } },
                },
              },
              edges: { __start__: "INNER" },
            },
          },
        },
        edges: { __start__: "OUTER" },
      };
    `);
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const grandchild = result.get(`${id}::OUTER::INNER`);
    assert.ok(grandchild, "nested synthetic child pipeline should be registered");
    assert.deepEqual(grandchild.proposalMeta, { requiredDeliveryField: "period" });
  });

  test("a root pipeline with no proposalMeta leaves synthetic children without one (Workana today)", async () => {
    const id = `proposalmeta_absent_${randomUUID().slice(0, 8)}`;
    await writePipeline(dir, id, `
      export default {
        id: "${id}",
        state: {},
        nodes: {
          PROPOSE: {
            kind: "subflow", mode: "any", selector: (s) => [],
            flow: { nodes: { DRAFT: { kind: "code", run: () => {} } }, edges: { __start__: "DRAFT" } },
          },
        },
        edges: { __start__: "PROPOSE" },
      };
    `);
    const { logger } = makeLogger();
    const result = await loadPipelines(dir, logger);
    const synthetic = result.get(`${id}::PROPOSE`);
    assert.ok(synthetic);
    assert.equal(synthetic.proposalMeta, undefined);
  });
});

