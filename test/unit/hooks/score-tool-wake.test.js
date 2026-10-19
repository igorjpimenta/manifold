// Unit tests for the after_tool_call hook that wakes a waiting SCORE_LLM
// dispatch the instant submit_score fires — see score-tool-wake.js's own
// header comment for the failure class this fixes (788 occurrences of
// SCORE_LLM's "parse error: no valid score" where the real submit_score
// call landed after the node had already been marked failed).

import { describe, it, mock, beforeEach } from "node:test";
import assert from "node:assert/strict";

const toolHandoffUrl = new URL("../../../state/tool-handoff.js", import.meta.url).href;

let notifyToolFiredCalls = [];

await mock.module(toolHandoffUrl, {
  exports: {
    notifyToolFired: (runId) => { notifyToolFiredCalls.push(runId); },
  },
});

const { registerScoreToolWakeHook } = await import("../../../hooks/score-tool-wake.js");

function makeMockApi() {
  const handlers = [];
  return {
    on: (event, handler, opts) => { handlers.push({ event, handler, opts }); },
    _find: (event) => handlers.find((h) => h.event === event)?.handler,
  };
}

beforeEach(() => {
  notifyToolFiredCalls = [];
});

describe("registerScoreToolWakeHook", () => {
  it("registers exactly one after_tool_call hook, no before_tool_call", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    assert.ok(api._find("after_tool_call"), "after_tool_call handler must be registered");
    assert.equal(api._find("before_tool_call"), undefined);
  });

  it("calls notifyToolFired(runId) when submit_score fires", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    api._find("after_tool_call")({ toolName: "submit_score", runId: "run-1" });
    assert.deepEqual(notifyToolFiredCalls, ["run-1"]);
  });

  it("ignores a different tool name entirely", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    api._find("after_tool_call")({ toolName: "edit", runId: "run-2" });
    assert.deepEqual(notifyToolFiredCalls, []);
  });

  it("does not call notifyToolFired when the event has no runId", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    api._find("after_tool_call")({ toolName: "submit_score" });
    assert.deepEqual(notifyToolFiredCalls, []);
  });

  it("does not throw on a null/undefined event", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    const handler = api._find("after_tool_call");
    assert.doesNotThrow(() => handler(null));
    assert.doesNotThrow(() => handler(undefined));
    assert.deepEqual(notifyToolFiredCalls, []);
  });

  it("does not wake on a rejected call (toolErr()'s isError:true return shape) — the model may still self-correct within the same turn", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    api._find("after_tool_call")({
      toolName: "submit_score",
      runId: "run-3",
      result: { isError: true, content: [{ type: "text", text: "score must be an integer 0-100" }] },
    });
    assert.deepEqual(notifyToolFiredCalls, []);
  });

  it("event.error alone (without result.isError) does not suppress the wake — openclaw core only sets .error on a thrown exception, which a registered tool's execute() never does for a validation rejection", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    api._find("after_tool_call")({
      toolName: "submit_score",
      runId: "run-4",
      result: { content: [{ type: "text", text: "score submitted: 30" }] },
      error: undefined,
    });
    assert.deepEqual(notifyToolFiredCalls, ["run-4"]);
  });

  it("wakes on a successful call with no result field at all (defensive — missing result must not throw)", () => {
    const api = makeMockApi();
    registerScoreToolWakeHook(api);
    assert.doesNotThrow(() => api._find("after_tool_call")({ toolName: "submit_score", runId: "run-5" }));
    assert.deepEqual(notifyToolFiredCalls, ["run-5"]);
  });
});
