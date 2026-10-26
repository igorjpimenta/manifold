/**
 * Unit tests for agent/tools/propose-draft.js — registerProposeDraftTool (propose_draft).
 *
 * Verifies validation, setPendingText, discussStore, componentEmitter, draftId
 * handling, trimming.
 *
 * pending-store is mocked at the module level so disk I/O is replaced with an
 * in-memory map.
 */

import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";

const pendingStoreUrl = new URL("../../../../state/pending-store.js", import.meta.url).href;
const telemetryUrl = new URL("../../../../telemetry/index.js", import.meta.url).href;

// resolveGateContext's auto-resolve delegates to getSharedResolver().resolve() —
// tests that exercise auto-resolve set this before calling execute().
let fakeResolver = null;

const _pendingStore = new Map(); // key: `${flowId}__${itemId}`

function _pendingKey(flowId, itemId) { return `${flowId}__${itemId}`; }

const setPendingTextCalls = [];

await mock.module(pendingStoreUrl, {
  exports: {
    setPendingText(flowId, itemId, text, draftId = null) {
      const key = _pendingKey(flowId, itemId);
      const prior = _pendingStore.get(key) ?? { flowId, itemId };
      const next = { ...prior, flowId, itemId, text, draftId: draftId ?? null, updatedAt: Date.now() };
      _pendingStore.set(key, next);
      setPendingTextCalls.push({ flowId, itemId, text, draftId });
      return next;
    },
    mergePendingFields(flowId, itemId, fields) {
      const key = _pendingKey(flowId, itemId);
      const prior = _pendingStore.get(key) ?? { flowId, itemId };
      const mergedFields = { ...(prior.fields ?? {}), ...fields };
      const next = { ...prior, flowId, itemId, fields: mergedFields, updatedAt: Date.now() };
      _pendingStore.set(key, next);
      return next;
    },
    readPending(flowId, itemId) {
      return _pendingStore.get(_pendingKey(flowId, itemId)) ?? null;
    },
    clearPending(flowId, itemId) {
      _pendingStore.delete(_pendingKey(flowId, itemId));
    },
  },
});

await mock.module(telemetryUrl, {
  exports: {
    getSharedResolver: () => fakeResolver,
  },
});

const { registerProposeDraftTool } = await import("../../../../agent/tools/propose-draft.js");

function createToolApi() {
  const tools = [];
  const api = {
    registerTool(defOrFactory) { tools.push(defOrFactory); },
    logger: { info: () => {}, warn: () => {}, debug: () => {}, error: () => {} },
  };
  return { api, tools };
}

function findTool(tools, name, ctx = {}) {
  const def = tools.find((t) => {
    if (typeof t === "function") return t({}).name === name;
    return t.name === name;
  });
  assert.ok(def, `tool '${name}' not registered`);
  return typeof def === "function" ? def(ctx) : def;
}

describe("registerProposeDraftTool — propose_draft", () => {
  function clearStore() {
    _pendingStore.clear();
    setPendingTextCalls.length = 0;
    fakeResolver = null;
  }

  function makeGateFlow(itemIdOverrides = {}) {
    return {
      flowId: "fid-gate-draft",
      status: "blocked",
      waitJson: {
        kind: "human-gate",
        items: [
          {
            itemId: "item-abc",
            requestPayload: { editableText: "Original text here" },
            ...itemIdOverrides,
          },
        ],
      },
    };
  }

  function makeDraftDeps({ readStoreFlow = makeGateFlow(), componentEmitter = null, discussStore = null, ctx = {} } = {}) {
    const readStore = { get: (id) => (id === readStoreFlow?.flowId ? readStoreFlow : null) };
    const { api, tools } = createToolApi();
    registerProposeDraftTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, readStore, componentEmitter, discussStore });
    const toolDef = findTool(tools, "propose_draft", ctx);
    return { toolDef, readStore };
  }

  test("returns toolErr when flowId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { itemId: "item-abc", text: "new text" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("returns toolErr when itemId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", text: "new text" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("returns toolErr when text is missing", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("required"));
  });

  test("returns toolErr when text trims to empty string", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "   " }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("required"));
  });

  test("returns toolErr when flow not found in readStore", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "ghost-flow", itemId: "item-abc", text: "new text" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("flow not found: ghost-flow"));
  });

  test("returns toolErr when flow.waitJson.kind is not human-gate", async () => {
    clearStore();
    const flow = {
      flowId: "fid-ni",
      waitJson: { kind: "needs-info", items: [{ itemId: "item-abc" }] },
    };
    const readStore = { get: () => flow };
    const { api, tools } = createToolApi();
    registerProposeDraftTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, readStore });
    const toolDef = findTool(tools, "propose_draft");
    const result = await toolDef.execute("tc", { flowId: "fid-ni", itemId: "item-abc", text: "new text" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("not paused on a gate"));
  });

  test("returns toolErr when itemId not found in flow.waitJson.items (string itemId)", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-nonexistent", text: "new text" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("gate item not found: item-nonexistent"));
    assert.ok(result.content[0].text.includes("item-abc"), "lists existing items in error");
  });

  test("supports itemId as object with .id (resolveItemId compat)", async () => {
    clearStore();
    const flow = {
      flowId: "fid-obj-item",
      waitJson: {
        kind: "human-gate",
        items: [
          { itemId: { id: "item-obj" }, requestPayload: { editableText: "old" } },
        ],
      },
    };
    const readStore = { get: () => flow };
    const { api, tools } = createToolApi();
    registerProposeDraftTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, readStore });
    const toolDef = findTool(tools, "propose_draft");
    const result = await toolDef.execute("tc", { flowId: "fid-obj-item", itemId: "item-obj", text: "new text" }, null);
    assert.equal(result.isError, undefined, "object-itemId should resolve correctly");
  });

  test("returns toolErr when proposed text is identical to currentItem.requestPayload.editableText", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "Original text here" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("identical to the current draft"));
  });

  test("calls setPendingText with flowId, itemId, text, draftId on success", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "new revised text" }, null);
    assert.equal(setPendingTextCalls.length, 1);
    assert.equal(setPendingTextCalls[0].flowId, "fid-gate-draft");
    assert.equal(setPendingTextCalls[0].itemId, "item-abc");
    assert.equal(setPendingTextCalls[0].text, "new revised text");
    assert.equal(setPendingTextCalls[0].draftId, null);
  });

  test("calls setPendingText with provided draftId when params.id is set", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "new text", id: "body" }, null);
    const call = setPendingTextCalls.find((c) => c.draftId === "body");
    assert.ok(call, "setPendingText called with draftId=body");
  });

  test("uses null draftId when params.id is absent", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "new text" }, null);
    assert.equal(setPendingTextCalls[0].draftId, null);
  });

  test("uses null draftId when params.id is blank string", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "new text", id: "   " }, null);
    assert.equal(setPendingTextCalls[0].draftId, null);
  });

  test("calls discussStore.setPending when discussStore is provided", async () => {
    clearStore();
    const setPendingCalls = [];
    const discussStore = {
      setPending: (...args) => setPendingCalls.push(args),
      mergePendingFields: () => {},
    };
    const { toolDef } = makeDraftDeps({ discussStore });
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "new text" }, null);
    assert.equal(setPendingCalls.length, 1);
    assert.equal(setPendingCalls[0][0], "fid-gate-draft");
    assert.equal(setPendingCalls[0][1], "item-abc");
    assert.equal(setPendingCalls[0][2], "new text");
  });

  test("calls componentEmitter with kind=proposed_draft when provided", async () => {
    clearStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeDraftDeps({ componentEmitter });
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "emitter text" }, null);
    assert.equal(emitterCalls.length, 1);
    assert.equal(emitterCalls[0].flowId, "fid-gate-draft");
    assert.equal(emitterCalls[0].itemId, "item-abc");
    assert.equal(emitterCalls[0].component.kind, "proposed_draft");
    assert.equal(emitterCalls[0].component.text, "emitter text");
    assert.equal(emitterCalls[0].component.draftId, null);
  });

  test("componentEmitter receives draftId when provided", async () => {
    clearStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeDraftDeps({ componentEmitter });
    await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "emitter text", id: "subject" }, null);
    assert.equal(emitterCalls[0].component.draftId, "subject");
  });

  test("trims flowId, itemId, text before processing", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    // "  fid-gate-draft  " should match after trim
    await toolDef.execute("tc", { flowId: "  fid-gate-draft  ", itemId: "  item-abc  ", text: "  new text  " }, null);
    const call = setPendingTextCalls[0];
    assert.equal(call.flowId, "fid-gate-draft");
    assert.equal(call.itemId, "item-abc");
    assert.equal(call.text, "new text");
  });

  test("success result mentions char count", async () => {
    clearStore();
    const { toolDef } = makeDraftDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "hello world" }, null);
    assert.equal(result.isError, undefined);
    assert.ok(result.content[0].text.includes("11 chars"), "char count in result");
  });

  // ── auto-resolve (telemetry lineage resolver) ─────────────────────────────

  test("auto-resolves flowId/itemId from ctx.sessionKey when omitted entirely", async () => {
    clearStore();
    fakeResolver = { resolve: () => ({ flow_id: "fid-gate-draft", item_key: "item-abc" }) };
    const { toolDef } = makeDraftDeps({
      ctx: { sessionKey: "agent:main:gf-discuss-deadbeef-touch-item-abc" },
    });
    const result = await toolDef.execute("tc", { text: "auto-resolved text" }, null);
    assert.equal(result.isError, undefined);
    assert.equal(setPendingTextCalls[0].flowId, "fid-gate-draft");
    assert.equal(setPendingTextCalls[0].itemId, "item-abc");
  });

  test("auto-resolve overrides a wrong/garbled explicit flowId (mirrors the real subagent bug)", async () => {
    clearStore();
    fakeResolver = { resolve: () => ({ flow_id: "fid-gate-draft", item_key: "item-abc" }) };
    const { toolDef } = makeDraftDeps({
      ctx: { sessionKey: "agent:main:gf-discuss-deadbeef-touch-item-abc" },
    });
    // Agent guessed a nonsense flowId/itemId, as Cyrano did in production.
    const result = await toolDef.execute("tc", { flowId: "auto-resolve", itemId: "auto-resolve", text: "revised text" }, null);
    assert.equal(result.isError, undefined);
    assert.equal(setPendingTextCalls[0].flowId, "fid-gate-draft");
    assert.equal(setPendingTextCalls[0].itemId, "item-abc");
  });

  test("does not attempt auto-resolve when the explicit flowId already resolves to a real flow", async () => {
    clearStore();
    fakeResolver = { resolve: () => ({ flow_id: "should-not-be-used", item_key: "wrong-item" }) };
    const { toolDef } = makeDraftDeps({
      ctx: { sessionKey: "agent:main:gf-discuss-baadf00d-touch-x" },
    });
    const result = await toolDef.execute("tc", { flowId: "fid-gate-draft", itemId: "item-abc", text: "explicit text" }, null);
    assert.equal(result.isError, undefined);
    assert.equal(setPendingTextCalls[0].flowId, "fid-gate-draft");
    assert.equal(setPendingTextCalls[0].itemId, "item-abc");
  });

  test("auto-resolve failure (resolver has nothing for this session) still surfaces a clear error, not a crash", async () => {
    clearStore();
    fakeResolver = { resolve: () => null };
    const { toolDef } = makeDraftDeps({
      ctx: { sessionKey: "agent:content-writer:subagent:some-random-uuid" },
    });
    const result = await toolDef.execute("tc", { text: "text with no ids" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });
});
