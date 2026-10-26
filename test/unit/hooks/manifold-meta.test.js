// Exercises the adapter's before_tool_call identity stamper through the
// unit suite (the canonical suite lives at plugins/openclaw/manifold/
// meta.test.js, mirroring winch's; this file pins the wiring the gateway
// relies on: every manifold tool call leaves with _meta.manifold stamped).
import { test } from "node:test";
import assert from "node:assert/strict";
import { isManifoldTool, buildManifoldMeta, registerManifoldMetaHook } from "../../../plugins/openclaw/manifold/meta.js";

test("manifold tool calls are stamped; other tools pass through", () => {
  const out = buildManifoldMeta(
    { toolName: "mcp__manifold__manifold_start", params: { pipelineId: "p" } },
    { agentId: "scout", sessionKey: "ses_42" },
  );
  assert.deepEqual(out.params._meta.manifold, { harness: "openclaw", agent: "scout", session: "ses_42" });
  assert.equal(isManifoldTool("bash"), false);
  assert.equal(buildManifoldMeta({ toolName: "bash" }, { agentId: "a", sessionKey: "s" }), undefined);
});

test("hook registration stamps via the before_tool_call return channel", () => {
  const seen = [];
  registerManifoldMetaHook({ on: (event, handler) => seen.push([event, handler]) });
  assert.equal(seen[0][0], "before_tool_call");
  const out = seen[0][1]({ toolName: "mcp__manifold__manifold_start" }, { agentId: "a", sessionKey: "s" });
  assert.deepEqual(out.params._meta.manifold, { harness: "openclaw", agent: "a", session: "s" });
});
