// meta.test.js — pure-function tests for the openclaw companion hook; runs
// under `node --test plugins/` with no gateway runtime.
import { test } from "node:test";
import assert from "node:assert/strict";
import { isManifoldTool, buildManifoldMeta, registerManifoldMetaHook } from "./meta.js";

test("isManifoldTool matches the mcp__manifold__ naming and rejects other tools", () => {
  assert.equal(isManifoldTool("mcp__manifold__manifold_start"), true);
  assert.equal(isManifoldTool("mcp__manifold__manifold_status"), true);
  assert.equal(isManifoldTool("mcp__gbrain__query"), false);
  assert.equal(isManifoldTool("bash"), false);
  assert.equal(isManifoldTool(undefined), false);
});

test("non-manifold tool: undefined (call passes through untouched)", () => {
  assert.equal(
    buildManifoldMeta({ toolName: "bash", params: { command: "ls" } }, { agentId: "a", sessionKey: "s" }),
    undefined,
  );
});

test("manifold tool: stamps harness/agent/session from ctx fields", () => {
  const out = buildManifoldMeta(
    { toolName: "mcp__manifold__manifold_start", params: { pipelineId: "p" } },
    { agentId: "scout", sessionKey: "ses_42" },
  );
  assert.deepEqual(out.params._meta.manifold, { harness: "openclaw", agent: "scout", session: "ses_42" });
  assert.deepEqual(out.params.pipelineId, "p");
});

test("runId stands in for sessionKey — the detached-continuation fallback chain", () => {
  const out = buildManifoldMeta({ toolName: "mcp__manifold__manifold_status" }, { agentId: "scout", runId: "run_7" });
  assert.equal(out.params._meta.manifold.session, "run_7");
});

test("ctx.sessionId beats runId", () => {
  const out = buildManifoldMeta(
    { toolName: "mcp__manifold__manifold_start" },
    { agentId: "scout", sessionId: "ses_9", runId: "run_7" },
  );
  assert.equal(out.params._meta.manifold.session, "ses_9");
});

test("sessionKey still wins over sessionId — most stable first", () => {
  const out = buildManifoldMeta(
    { toolName: "mcp__manifold__manifold_start" },
    { agentId: "scout", sessionKey: "ses_live", sessionId: "ses_9", runId: "run_7" },
  );
  assert.equal(out.params._meta.manifold.session, "ses_live");
});

test("event.sessionId is read when ctx carries no session fields", () => {
  const out = buildManifoldMeta({ toolName: "manifold_x", sessionId: "ses_e" }, {});
  assert.equal(out.params._meta.manifold.session, "ses_e");
  assert.equal(out.params._meta.manifold.agent, null);
});

test("event.runId is read when ctx carries neither sessionKey nor runId", () => {
  const out = buildManifoldMeta({ toolName: "manifold_x", runId: "run_e" }, {});
  assert.equal(out.params._meta.manifold.session, "run_e");
  assert.equal(out.params._meta.manifold.agent, null);
});

test("no resolvable session or agent: undefined (server refuses the unstamped call)", () => {
  assert.equal(buildManifoldMeta({ toolName: "mcp__manifold__manifold_start", params: {} }, {}), undefined);
});

test("a model-supplied _meta.manifold is overwritten, sibling keys preserved", () => {
  const out = buildManifoldMeta(
    {
      toolName: "mcp__manifold__manifold_start",
      params: { _meta: { manifold: { harness: "evil", agent: "mallory", session: "s" }, progressToken: "p1" } },
    },
    { agentId: "scout", sessionKey: "ses_42" },
  );
  assert.deepEqual(out.params._meta.manifold, { harness: "openclaw", agent: "scout", session: "ses_42" });
  assert.equal(out.params._meta.progressToken, "p1");
});

test("params is optional — a call with no params still gets stamped", () => {
  const out = buildManifoldMeta({ toolName: "mcp__manifold__manifold_start" }, { agentId: "a", sessionKey: "s" });
  assert.equal(out.params._meta.manifold.harness, "openclaw");
});

test("registerManifoldMetaHook wires a before_tool_call stamper", () => {
  const seen = [];
  const api = { on: (event, handler) => seen.push([event, handler]) };
  registerManifoldMetaHook(api);
  assert.equal(seen.length, 1);
  assert.equal(seen[0][0], "before_tool_call");
  const out = seen[0][1]({ toolName: "mcp__manifold__manifold_start" }, { agentId: "a", sessionKey: "s" });
  assert.deepEqual(out.params._meta.manifold, { harness: "openclaw", agent: "a", session: "s" });
});
