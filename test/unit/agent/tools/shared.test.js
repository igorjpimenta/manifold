/**
 * Unit tests for agent/tools/shared.js — resolveGateContext.
 *
 * Verifies auto-resolve delegates to the telemetry lineage resolver
 * (getSharedResolver().resolve(sessionKey)) and degrades gracefully when
 * the resolver isn't ready or nothing resolves.
 *
 * resolveGateContext is async — resolver.resolve() has been async since the
 * libsql migration (dba2f6e1), and this call site was the one caller that
 * migration missed awaiting. Without the await, `row` was a pending Promise
 * (truthy but fieldless), so every one of these "returns null" cases used to
 * pass for the wrong reason: they'd have passed against the broken
 * implementation too. Each test now awaits the call so it actually exercises
 * the real async resolve path.
 *
 * telemetry/index.js is mocked at the module level so this test doesn't
 * need a real session_lineage sqlite store.
 */

import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";

let fakeResolver = null; // { resolve(sessionKey) => row|null } or null (not ready)

await mock.module("../../../../telemetry/index.js", {
  exports: {
    getSharedResolver: () => fakeResolver,
  },
});

const { resolveGateContext, resolveFlowIdOnly } = await import("../../../../agent/tools/shared.js");

describe("resolveGateContext", () => {
  test("returns null when ctx is missing", async () => {
    fakeResolver = { resolve: () => { throw new Error("should not be called"); } };
    const result = await resolveGateContext({ ctx: null });
    assert.equal(result, null);
  });

  test("returns null when ctx.sessionKey is missing", async () => {
    fakeResolver = { resolve: () => { throw new Error("should not be called"); } };
    const result = await resolveGateContext({ ctx: {} });
    assert.equal(result, null);
  });

  test("returns null when the resolver isn't ready yet", async () => {
    fakeResolver = null;
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:main:gf-discuss-a700a3ea-propose-some-item" } });
    assert.equal(result, null);
  });

  test("resolves flowId/itemId from the lineage row", async () => {
    fakeResolver = {
      // Real resolver.resolve() is async — mock it as such so this test
      // would have caught the missing-await regression.
      resolve: async (sessionKey) => {
        assert.equal(sessionKey, "agent:main:gf-discuss-a700a3ea-propose-some-item");
        return { flow_id: "a700a3ea-e3f9-4f4d-aade-e444e4850792", item_key: "some-item" };
      },
    };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:main:gf-discuss-a700a3ea-propose-some-item" } });
    assert.deepEqual(result, { flowId: "a700a3ea-e3f9-4f4d-aade-e444e4850792", itemId: "some-item", pipelineId: null });
  });

  test("also resolves pipelineId from the lineage row when present — read by submit_draft to pick timeframe vs period", async () => {
    fakeResolver = {
      resolve: async () => ({ flow_id: "a700a3ea-e3f9-4f4d-aade-e444e4850792", item_key: "some-item", pipeline_id: "freelance_freelancer-proposal" }),
    };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:main:gf-discuss-a700a3ea-propose-some-item" } });
    assert.deepEqual(result, { flowId: "a700a3ea-e3f9-4f4d-aade-e444e4850792", itemId: "some-item", pipelineId: "freelance_freelancer-proposal" });
  });

  test("subagent case: resolver inherits flowId/itemId from an ancestor frame", async () => {
    // Cyrano's own session carries no item context; resolve() is expected
    // to have already walked its parent_session_key chain internally and
    // inherited from the discuss session that spawned it.
    fakeResolver = {
      resolve: async (sessionKey) => {
        assert.equal(sessionKey, "agent:content-writer:subagent:78a6012a-34a0-4ee6-9977-780ba6c99016");
        return { flow_id: "a700a3ea-e3f9-4f4d-aade-e444e4850792", item_key: "some-item" };
      },
    };
    const result = await resolveGateContext({
      ctx: { sessionKey: "agent:content-writer:subagent:78a6012a-34a0-4ee6-9977-780ba6c99016", agentId: "content-writer" },
    });
    assert.deepEqual(result, { flowId: "a700a3ea-e3f9-4f4d-aade-e444e4850792", itemId: "some-item", pipelineId: null });
  });

  test("returns null when resolve() returns null (nothing found)", async () => {
    fakeResolver = { resolve: async () => null };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:content-writer:subagent:not-in-the-chain" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() returns a row missing flow_id", async () => {
    fakeResolver = { resolve: async () => ({ flow_id: null, item_key: "some-item" }) };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:content-writer:subagent:x" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() returns a row missing item_key", async () => {
    fakeResolver = { resolve: async () => ({ flow_id: "a700a3ea-e3f9-4f4d-aade-e444e4850792", item_key: null }) };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:content-writer:subagent:x" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() throws synchronously", async () => {
    fakeResolver = { resolve: () => { throw new Error("db closed"); } };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:content-writer:subagent:x" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() rejects", async () => {
    fakeResolver = { resolve: async () => { throw new Error("db closed"); } };
    const result = await resolveGateContext({ ctx: { sessionKey: "agent:content-writer:subagent:x" } });
    assert.equal(result, null);
  });
});

describe("resolveFlowIdOnly", () => {
  test("returns null when ctx is missing", async () => {
    fakeResolver = { resolve: () => { throw new Error("should not be called"); } };
    const result = await resolveFlowIdOnly({ ctx: null });
    assert.equal(result, null);
  });

  test("returns null when ctx.sessionKey is missing", async () => {
    fakeResolver = { resolve: () => { throw new Error("should not be called"); } };
    const result = await resolveFlowIdOnly({ ctx: {} });
    assert.equal(result, null);
  });

  test("returns null when the resolver isn't ready yet", async () => {
    fakeResolver = null;
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:flow:abc-node:SCORE_LLM" } });
    assert.equal(result, null);
  });

  // The whole point of this helper vs. resolveGateContext: a plain
  // kind:"agent" dispatch (like SCORE_LLM) never gets an itemKey stamped on
  // its lineage row — recordSpawn only passes flowId/nodeId/pipelineId for
  // these, so row.item_key is SQL NULL. resolveFlowIdOnly must still
  // resolve in that case, unlike resolveGateContext which requires item_key.
  test("resolves flowId even when item_key is null (plain agent dispatch, no itemId)", async () => {
    fakeResolver = {
      resolve: async (sessionKey) => {
        assert.equal(sessionKey, "agent:scorer:flow:a700a3ea-e3f9-4f4d-aade-e444e4850792:SCORE_LLM");
        return { flow_id: "a700a3ea-e3f9-4f4d-aade-e444e4850792", item_key: null, node_id: "SCORE_LLM", pipeline_id: "freelance_workana-proposal" };
      },
    };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:flow:a700a3ea-e3f9-4f4d-aade-e444e4850792:SCORE_LLM" } });
    assert.deepEqual(result, { flowId: "a700a3ea-e3f9-4f4d-aade-e444e4850792", nodeId: "SCORE_LLM", pipelineId: "freelance_workana-proposal" });
  });

  test("nodeId and pipelineId default to null when absent from the row", async () => {
    fakeResolver = { resolve: async () => ({ flow_id: "fid-1" }) };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:x" } });
    assert.deepEqual(result, { flowId: "fid-1", nodeId: null, pipelineId: null });
  });

  test("returns null when resolve() returns a row missing flow_id", async () => {
    fakeResolver = { resolve: async () => ({ flow_id: null, node_id: "SCORE_LLM" }) };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:x" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() returns null (nothing found)", async () => {
    fakeResolver = { resolve: async () => null };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:not-in-the-chain" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() throws synchronously", async () => {
    fakeResolver = { resolve: () => { throw new Error("db closed"); } };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:x" } });
    assert.equal(result, null);
  });

  test("returns null when resolve() rejects", async () => {
    fakeResolver = { resolve: async () => { throw new Error("db closed"); } };
    const result = await resolveFlowIdOnly({ ctx: { sessionKey: "agent:scorer:x" } });
    assert.equal(result, null);
  });
});
