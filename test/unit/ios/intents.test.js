import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { parseActivityId, createIntentRouter } from "../../../ios/intents.js";

const UUID = "11111111-2222-3333-4444-555555555555";

function makeRouter({ invokeElevatedImpl, fetchImpl } = {}) {
  const api = {
    logger: { info: () => {}, warn: () => {} },
    runtime: {
      tasks: {
        managedFlows: {
          bindSession: () => ({ get: () => null, fail: () => ({ applied: false }) }),
        },
      },
    },
  };
  const invokeElevated = mock.fn(invokeElevatedImpl ?? (async () => ({
    ok: true, status: 200, body: JSON.stringify({ ok: true, status: "applied" }),
  })));
  if (fetchImpl) mock.method(globalThis, "fetch", fetchImpl);
  const router = createIntentRouter({ api, logger: api.logger, invokeElevated });
  return { router, invokeElevated };
}

describe("parseActivityId", () => {
  it("returns null for non-string", () => {
    assert.equal(parseActivityId(null), null);
    assert.equal(parseActivityId(42), null);
    assert.equal(parseActivityId(undefined), null);
  });

  it("returns null when prefix is wrong", () => {
    assert.equal(parseActivityId("pipeline-progress:some-id"), null);
    assert.equal(parseActivityId("agent-approval"), null);
    assert.equal(parseActivityId(""), null);
  });

  it("returns null for empty string after prefix", () => {
    assert.equal(parseActivityId("agent-approval:"), null);
  });

  it("parses needs-info shape (36-char UUID, no itemId)", () => {
    const r = parseActivityId(`agent-approval:${UUID}`);
    assert.deepEqual(r, { flowId: UUID, itemId: null });
  });

  it("parses gate-item shape (UUID:itemId)", () => {
    const r = parseActivityId(`agent-approval:${UUID}:my-item-id`);
    assert.deepEqual(r, { flowId: UUID, itemId: "my-item-id" });
  });

  it("itemId may itself contain colons", () => {
    const r = parseActivityId(`agent-approval:${UUID}:a:b:c`);
    assert.deepEqual(r, { flowId: UUID, itemId: "a:b:c" });
  });

  it("falls back to treating the whole rest as flowId for short non-UUID ids", () => {
    const r = parseActivityId("agent-approval:short-id");
    assert.equal(r?.flowId, "short-id");
    assert.equal(r?.itemId, null);
  });
});

describe("handleGateDecision (via handle) — edit intent draftPath branching", () => {
  it("no draft target: loops back via invokeElevated with decision:edit + editedText, no apply-draft HTTP call", async (t) => {
    t.after(() => mock.restoreAll());
    const { router, invokeElevated } = makeRouter({
      fetchImpl: mock.fn(async () => { throw new Error("fetch should not be called"); }),
    });
    const result = await router.handle({
      type: "live_activity.intent",
      activityId: `agent-approval:${UUID}:item-1`,
      intentId: "edit",
      text: "the operator's typed replacement",
    });
    assert.equal(invokeElevated.mock.calls.length, 1);
    const [path, call] = invokeElevated.mock.calls[0].arguments;
    assert.equal(path, "/pipeclaw/_elev/gate-resolve");
    assert.equal(call.decision, "edit");
    assert.equal(call.editedText, "the operator's typed replacement");
    assert.equal(call.itemId, "item-1");
    assert.equal(call.source, "ios");
    assert.equal(globalThis.fetch.mock.calls.length, 0);
    assert.equal(result.action, "edit");
  });

  it("invokeElevated failure (non-ok body) surfaces as a thrown error", async (t) => {
    t.after(() => mock.restoreAll());
    const { router } = makeRouter({
      invokeElevatedImpl: async () => ({
        ok: true, status: 409, body: JSON.stringify({ ok: false, code: "already-decided" }),
      }),
    });
    await assert.rejects(
      router.handle({
        type: "live_activity.intent",
        activityId: `agent-approval:${UUID}:item-3`,
        intentId: "approve",
      }),
      /already-decided/,
    );
  });
});
