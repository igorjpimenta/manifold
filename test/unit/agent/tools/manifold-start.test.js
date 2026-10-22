/**
 * Unit tests for agent/tools/manifold-start.js — registerGraphFlowTool (manifold_start).
 *
 * Verifies validation, ensurePipelinesLoaded, executor.start call, setImmediate
 * runFlow, and lineage recordTrigger wiring.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { registerGraphFlowTool } from "../../../../agent/tools/manifold-start.js";

// ── helpers ──────────────────────────────────────────────────────────────────

function createToolApi() {
  const tools = [];
  const logWarns = [];
  const logInfos = [];

  const api = {
    registerTool(defOrFactory) {
      tools.push(defOrFactory);
    },
    logger: {
      info: (msg) => logInfos.push(msg),
      warn: (msg) => logWarns.push(msg),
      debug: () => {},
      error: () => {},
    },
    runtime: {
      lineage: {
        recordTrigger: () => {},
      },
    },
  };

  return { api, tools, logWarns, logInfos };
}

function resolveFactoryTool(tools, ctx = {}) {
  const factory = tools[0];
  assert.equal(typeof factory, "function", "expected factory-form registration");
  return factory(ctx);
}

function flushSetImmediate() {
  return new Promise((resolve) => setImmediate(resolve));
}

// ── registerGraphFlowTool ────────────────────────────────────────────────────

describe("registerGraphFlowTool — manifold_start", () => {
  function makeStartDeps(overrides = {}) {
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: (pipelineId, args, triggeredBy) => ({
        flowId: "flow-1111-2222-3333",
        stateJson: { pipelineId, runId: "run-abc" },
      }),
      runFlow: async () => {},
    };
    const { api, tools, logWarns } = createToolApi();
    return { api, tools, logWarns, pipelinesRef, executor, ...overrides };
  }

  test("registers one tool via factory form", () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    assert.equal(tools.length, 1);
    assert.equal(typeof tools[0], "function");
  });

  test("registered tool is named manifold_start", () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const def = tools[0]({});
    assert.equal(def.name, "manifold_start");
  });

  test("returns toolErr when pipelineId is missing", async () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", {}, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("pipelineId is required"));
  });

  test("returns toolErr when pipelineId is not a string (number)", async () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: 42 }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("pipelineId is required"));
  });

  test("returns toolErr when pipelineId not in pipelinesRef", async () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "nonexistent_pipeline" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("unknown pipeline: nonexistent_pipeline"));
    assert.ok(result.content[0].text.includes("my_pipeline"), "known pipelines listed in error");
  });

  test("lists (none loaded) when pipelinesRef is empty", async () => {
    const { api, tools, executor } = makeStartDeps();
    const emptyPipelinesRef = { value: new Map() };
    registerGraphFlowTool({ api, executor, pipelinesRef: emptyPipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "any_pipe" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("(none loaded)"));
  });

  test("calls ensurePipelinesLoaded before checking pipelinesRef", async () => {
    let loaderCalled = false;
    const pipelinesRef = { value: new Map() };
    const ensurePipelinesLoaded = async () => {
      loaderCalled = true;
      pipelinesRef.value.set("lazy_pipeline", {});
    };
    const executor = {
      start: () => ({ flowId: "fid-lazy", stateJson: { runId: "rid-lazy" } }),
      runFlow: async () => {},
    };
    const { api, tools } = createToolApi();
    registerGraphFlowTool({ api, executor, pipelinesRef, ensurePipelinesLoaded });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "lazy_pipeline" }, null);
    assert.equal(loaderCalled, true, "ensurePipelinesLoaded was called");
    assert.equal(result.isError, undefined, "no toolErr for valid lazy pipeline");
  });

  test("swallows ensurePipelinesLoaded rejection and falls through to has check", async () => {
    const pipelinesRef = { value: new Map() };
    const ensurePipelinesLoaded = async () => { throw new Error("loader boom"); };
    const { api, tools } = createToolApi();
    const executor = { start: () => ({}), runFlow: async () => {} };
    registerGraphFlowTool({ api, executor, pipelinesRef, ensurePipelinesLoaded });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "any" }, null);
    assert.equal(result.isError, true, "falls through to unknown-pipeline error");
  });

  test("calls executor.start with triggeredBy kind=agent_tool", async () => {
    const startCalls = [];
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: (pId, args, triggeredBy) => {
        startCalls.push({ pId, args, triggeredBy });
        return { flowId: "fid-start", stateJson: { runId: "rid-start" } };
      },
      runFlow: async () => {},
    };
    const { api, tools } = createToolApi();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    await execute("tc1", { pipelineId: "my_pipeline" }, null);
    assert.equal(startCalls.length, 1);
    assert.equal(startCalls[0].triggeredBy.kind, "agent_tool");
    assert.equal(startCalls[0].pId, "my_pipeline");
  });

  test("passes args to executor.start", async () => {
    const startCalls = [];
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: (pId, args, triggeredBy) => {
        startCalls.push({ args });
        return { flowId: "fid-args", stateJson: { runId: "rid-args" } };
      },
      runFlow: async () => {},
    };
    const { api, tools } = createToolApi();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    await execute("tc1", { pipelineId: "my_pipeline", args: { mockMode: true } }, null);
    assert.deepEqual(startCalls[0].args, { mockMode: true });
  });

  test("returns flowId and runId in success text content", async () => {
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: () => ({ flowId: "flow-aabbccdd-xxxx", stateJson: { runId: "run-42" } }),
      runFlow: async () => {},
    };
    const { api, tools } = createToolApi();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "my_pipeline" }, null);
    assert.equal(result.isError, undefined);
    const text = result.content[0].text;
    assert.ok(text.includes("flow-aabbccdd-xxxx"), "text includes flowId");
    assert.ok(text.includes("run-42"), "text includes runId");
    assert.ok(text.includes("my_pipeline"), "text includes pipelineId");
  });

  test("fires executor.runFlow via setImmediate (non-blocking)", async () => {
    const runFlowCalls = [];
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: () => ({ flowId: "fid-run", stateJson: { runId: "rid-run" } }),
      runFlow: async (flowId) => { runFlowCalls.push(flowId); },
    };
    const { api, tools } = createToolApi();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", { pipelineId: "my_pipeline" }, null);
    // runFlow should NOT be called synchronously
    assert.equal(runFlowCalls.length, 0, "runFlow not called before setImmediate flush");
    await flushSetImmediate();
    assert.equal(runFlowCalls.length, 1, "runFlow called after setImmediate flush");
    assert.equal(runFlowCalls[0], "fid-run");
  });

  test("calls api.runtime.lineage.recordTrigger with flowId+pipelineId+parentSessionKey when ctx.sessionKey is present", async () => {
    const recordTriggerCalls = [];
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: () => ({ flowId: "fid-lineage", stateJson: { runId: "rid-lineage" } }),
      runFlow: async () => {},
    };
    const { api, tools } = createToolApi();
    api.runtime.lineage.recordTrigger = (args) => recordTriggerCalls.push(args);
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const ctx = { sessionKey: "sess-abc-123" };
    const def = tools[0](ctx);
    await def.execute("tc1", { pipelineId: "my_pipeline" }, null);
    assert.equal(recordTriggerCalls.length, 1);
    assert.equal(recordTriggerCalls[0].flowId, "fid-lineage");
    assert.equal(recordTriggerCalls[0].pipelineId, "my_pipeline");
    assert.equal(recordTriggerCalls[0].parentSessionKey, "sess-abc-123");
  });

  // recordTrigger is async in production (it walks the lineage chain, then
  // upserts session_lineage). Unawaited, the setImmediate(runFlow) below can
  // fire while that write is still suspended, so a single-LLM-call pipeline's
  // first node resolves lineage before the trigger edge exists. Asserts the
  // ordering rather than just the call, since the call alone passes either way.
  test("awaits recordTrigger before scheduling runFlow", async () => {
    const order = [];
    let releaseTrigger;
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: () => ({ flowId: "fid-order", stateJson: { runId: "rid-order" } }),
      runFlow: async () => { order.push("runFlow"); },
    };
    const { api, tools } = createToolApi();
    api.runtime.lineage.recordTrigger = () => {
      order.push("recordTrigger:start");
      // Suspends the way the real resolver walk does.
      return new Promise((resolve) => { releaseTrigger = () => { order.push("recordTrigger:done"); resolve({ ok: true }); }; });
    };
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const def = tools[0]({ sessionKey: "sess-order" });

    const toolDone = def.execute("tc1", { pipelineId: "my_pipeline" }, null);
    // setTimeout, not a single setImmediate flush: the tool suspends on its
    // first await (executor.start), so one flush can resolve before the tool
    // has even reached recordTrigger — which would make this pass either way.
    // A real timer lets every queued setImmediate (the runFlow scheduling)
    // actually run.
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.deepEqual(order, ["recordTrigger:start"], "runFlow must not run while recordTrigger is in flight");

    releaseTrigger();
    await toolDone;
    await flushSetImmediate();
    assert.deepEqual(
      order,
      ["recordTrigger:start", "recordTrigger:done", "runFlow"],
      "runFlow must land after the trigger edge is committed",
    );
  });

  test("skips lineage recordTrigger and warns when ctx.sessionKey is null", async () => {
    const recordTriggerCalls = [];
    const pipelinesRef = { value: new Map([["my_pipeline", {}]]) };
    const executor = {
      start: () => ({ flowId: "fid-noline", stateJson: { runId: "rid-noline" } }),
      runFlow: async () => {},
    };
    const { api, tools, logWarns } = createToolApi();
    api.runtime.lineage.recordTrigger = (args) => recordTriggerCalls.push(args);
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const ctx = { sessionKey: null };
    const def = tools[0](ctx);
    await def.execute("tc1", { pipelineId: "my_pipeline" }, null);
    assert.equal(recordTriggerCalls.length, 0, "recordTrigger not called");
    assert.ok(logWarns.some((w) => w.includes("lineage edge skipped")), "warning logged");
  });

  test("toolErr shape: isError true + content[0].type=text", async () => {
    const { api, tools, pipelinesRef, executor } = makeStartDeps();
    registerGraphFlowTool({ api, executor, pipelinesRef });
    const { execute } = resolveFactoryTool(tools);
    const result = await execute("tc1", {}, null);
    assert.equal(result.isError, true);
    assert.ok(Array.isArray(result.content));
    assert.equal(result.content[0].type, "text");
    assert.equal(typeof result.content[0].text, "string");
  });
});
