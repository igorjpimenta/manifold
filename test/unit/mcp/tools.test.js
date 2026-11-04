// tools.test.js — manifold_deliver_result handler contract: the session
// comes from the stamped caller identity (never from args), payload is
// required, and the write goes through the injected deliverResult op.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createManifoldToolHandlers, TOOL_DEFS } from "../../../mcp/tools.js";

const identity = { harness: "test-harness", agent: "a1", session: "sess-9" };

function handlers(deliverCalls) {
  return createManifoldToolHandlers({
    deliverResult: async (args) => { deliverCalls.push(args); return { ok: true }; },
    logger: { info() {}, warn() {}, error() {} },
  });
}

describe("manifold_deliver_result", () => {
  test("is registered with a payload-only schema (no session arg)", () => {
    const def = TOOL_DEFS.find((d) => d.name === "manifold_deliver_result");
    assert.ok(def, "tool must be registered");
    assert.deepEqual(def.inputSchema.required, ["payload"]);
    assert.ok(!("session" in (def.inputSchema.properties ?? {})), "session must not be caller-supplied");
  });

  test("stamped call writes keyed by the identity session, not args", async () => {
    const deliverCalls = [];
    const { handle } = handlers(deliverCalls);
    const res = await handle("manifold_deliver_result", { payload: { score: 5 }, kind: "score", runId: "r1" }, identity);
    assert.deepEqual(res.details, { status: "ok", session: "sess-9" });
    assert.equal(deliverCalls.length, 1);
    assert.deepEqual(deliverCalls[0], {
      session: "sess-9",
      runId: "r1",
      agentId: "a1",
      kind: "score",
      payload: { score: 5 },
    });
  });

  test("missing payload is rejected, nothing written", async () => {
    const deliverCalls = [];
    const { handle } = handlers(deliverCalls);
    const res = await handle("manifold_deliver_result", { kind: "score" }, identity);
    assert.equal(res.details.status, "rejected");
    assert.equal(deliverCalls.length, 0);
  });

  test("a spoofed session in args is ignored — identity wins", async () => {
    const deliverCalls = [];
    const { handle } = handlers(deliverCalls);
    await handle("manifold_deliver_result", { payload: { v: 1 }, session: "attacker-key" }, identity);
    assert.equal(deliverCalls[0].session, "sess-9");
  });
});
