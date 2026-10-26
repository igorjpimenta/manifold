/**
 * Unit tests for agent/tools/propose-draft-meta.js — registerProposeDraftMetaTool
 * (propose_draft_meta): field whitelist, budget normalization, no-op guard,
 * mergePendingFields, normalizePendingFields emitter shape.
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

const mergePendingFieldsCalls = [];

await mock.module(pendingStoreUrl, {
  exports: {
    setPendingText(flowId, itemId, text, draftId = null) {
      const key = _pendingKey(flowId, itemId);
      const prior = _pendingStore.get(key) ?? { flowId, itemId };
      const next = { ...prior, flowId, itemId, text, draftId: draftId ?? null, updatedAt: Date.now() };
      _pendingStore.set(key, next);
      return next;
    },
    mergePendingFields(flowId, itemId, fields) {
      const key = _pendingKey(flowId, itemId);
      const prior = _pendingStore.get(key) ?? { flowId, itemId };
      const mergedFields = { ...(prior.fields ?? {}), ...fields };
      const next = { ...prior, flowId, itemId, fields: mergedFields, updatedAt: Date.now() };
      _pendingStore.set(key, next);
      mergePendingFieldsCalls.push({ flowId, itemId, fields });
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

const { registerProposeDraftMetaTool } = await import("../../../../agent/tools/propose-draft-meta.js");

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

describe("registerProposeDraftMetaTool — propose_draft_meta", () => {
  function clearMetaStore() {
    _pendingStore.clear();
    mergePendingFieldsCalls.length = 0;
    fakeResolver = null;
  }

  function makeGateFlowMeta(items = null) {
    return {
      flowId: "fid-meta-draft",
      status: "blocked",
      waitJson: {
        kind: "human-gate",
        items: items ?? [
          {
            itemId: "item-meta",
            requestPayload: {
              editableText: "Proposal body",
              editableFields: [
                { id: "budget", value: 5000, editable: true },
                { id: "timeframe", value: "4 semanas", editable: true },
                { id: "language", value: "pt-BR", editable: true },
              ],
            },
          },
        ],
      },
      stateJson: { pipelineId: "freelance_workana" },
    };
  }

  function makeMetaDeps({ readStoreFlow = makeGateFlowMeta(), componentEmitter = null, discussStore = null, ctx = {} } = {}) {
    const readStore = { get: (id) => (id === readStoreFlow?.flowId ? readStoreFlow : null) };
    const { api, tools } = createToolApi();
    registerProposeDraftMetaTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, readStore, componentEmitter, discussStore });
    const toolDef = findTool(tools, "propose_draft_meta", ctx);
    return { toolDef, readStore };
  }

  test("returns toolErr when flowId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", { itemId: "item-meta", fields: { budget: 6000 } }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("returns toolErr when itemId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-meta-draft", fields: { budget: 6000 } }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("returns toolErr when fields is missing", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-meta-draft", itemId: "item-meta" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("fields object is required"));
  });

  test("returns toolErr when fields is not an object (string)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", { flowId: "fid-meta-draft", itemId: "item-meta", fields: "budget=5000" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("fields object is required"));
  });

  test("returns toolErr for unknown field not in META_FIELD_WHITELIST", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { status: "draft" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("unknown field: status"));
    assert.ok(result.content[0].text.includes("budget"), "allowed fields listed");
  });

  test("returns toolErr for approval-token field (pipeline-owned, rejected)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { "approval-token": "tok123" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("unknown field"));
  });

  // The tool rejects any formatting rather than stripping it and guessing
  // what a caller meant — the range tests below pin that contract, and
  // these two assert rejection instead of normalization.
  test("rejects budget R$ 6.000 instead of stripping formatting", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "R$ 6.000" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("no currency symbols or formatting"));
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.equal(call, undefined, "must not have stashed a value derived from formatted input");
  });

  test("rejects budget $6,000 instead of stripping formatting", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "$6,000" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("no currency symbols or formatting"));
  });

  test("normalizes budget 6000 (plain string digits) to integer 6000", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "6000" },
    }, null);
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.equal(call.fields.budget, 6000);
  });

  test("returns toolErr when budget is zero", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: 0 },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("budget must be a positive integer"));
  });

  test("returns toolErr when budget is negative", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: -100 },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("budget must be a positive integer"));
  });

  test("returns toolErr when budget string has no digits", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "abc" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("no currency symbols or formatting"));
  });

  // Real incident: an agent passed a job listing's stated budget range
  // ("R$ 2.600 - 3.500") straight through. The digit-strip parse below
  // removed the "-" along with the currency formatting, concatenating both
  // numbers into 26003500 -- still finite and positive, so it silently
  // passed as a valid (wildly wrong) budget instead of erroring.
  test("rejects a budget range like 'R$ 2.600 - 3.500' instead of concatenating it into 26003500", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "R$ 2.600 - 3.500" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("not a range"));
    assert.ok(!result.content[0].text.includes("26003500"));
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.equal(call, undefined, "must not have stashed the mangled range as a budget");
  });

  test("rejects a budget range with an en-dash and no currency symbol", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "2600–3500" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("not a range"));
  });

  test("rejects a period range like '14-21'", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { period: "14-21" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("period must be a single number, not a range"));
  });

  test("returns toolErr when all supplied fields are empty/null (no valid fields)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { timeframe: "", language: null },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("no valid fields supplied"));
  });

  test("returns toolErr when flow not found", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "ghost-flow",
      itemId: "item-meta",
      fields: { budget: 8000 },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("flow not found: ghost-flow"));
  });

  test("returns toolErr when flow.waitJson.kind is not human-gate", async () => {
    clearMetaStore();
    const flow = {
      flowId: "fid-ni-meta",
      waitJson: { kind: "needs-info", items: [{ itemId: "item-meta" }] },
      stateJson: {},
    };
    const readStore = { get: () => flow };
    const { api, tools } = createToolApi();
    registerProposeDraftMetaTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, readStore });
    const toolDef = findTool(tools, "propose_draft_meta");
    const result = await toolDef.execute("tc", { flowId: "fid-ni-meta", itemId: "item-meta", fields: { budget: 7000 } }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("not paused on a gate"));
  });

  test("returns toolErr when gate item not found", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "nonexistent-item",
      fields: { budget: 7000 },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("gate item not found: nonexistent-item"));
  });

  test("no-op guard: returns toolErr when proposed fields identical to editableFields baseline", async () => {
    clearMetaStore();
    // budget in editableFields baseline = 5000; propose same value
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: 5000 },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("identical to the current draft values"));
  });

  test("no-op guard: budget comparison is numeric (5000 === '5000')", async () => {
    clearMetaStore();
    // editableFields has budget=5000 (number); propose "5000" (string after stripping = 5000)
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "5000" },
    }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("identical to the current draft values"));
  });

  test("no-op guard: allows change when field differs from baseline", async () => {
    clearMetaStore();
    // editableFields has budget=5000; propose 8000 → should succeed
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: 8000 },
    }, null);
    assert.equal(result.isError, undefined, "should not be a no-op when budget differs");
  });

  test("calls mergePendingFields on success (shallow merge, not replace)", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: 9000 },
    }, null);
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.ok(call, "mergePendingFields was called");
    assert.equal(call.itemId, "item-meta");
    assert.deepEqual(call.fields, { budget: 9000 });
  });

  test("calls discussStore.mergePendingFields when discussStore provided", async () => {
    clearMetaStore();
    const mergeDiscussCalls = [];
    const discussStore = {
      setPending: () => {},
      mergePendingFields: (...args) => mergeDiscussCalls.push(args),
    };
    const { toolDef } = makeMetaDeps({ discussStore });
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { timeframe: "6 weeks" },
    }, null);
    assert.equal(mergeDiscussCalls.length, 1);
    assert.equal(mergeDiscussCalls[0][0], "fid-meta-draft");
    assert.equal(mergeDiscussCalls[0][1], "item-meta");
    assert.deepEqual(mergeDiscussCalls[0][2], { timeframe: "6 weeks" });
  });

  test("calls componentEmitter with kind=proposed_meta and normalized fields", async () => {
    clearMetaStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeMetaDeps({ componentEmitter });
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: "9000" },
    }, null);
    assert.equal(emitterCalls.length, 1);
    assert.equal(emitterCalls[0].component.kind, "proposed_meta");
    assert.equal(emitterCalls[0].component.fields.budget, 9000);
  });

  test("componentEmitter receives timeframe and language as strings", async () => {
    clearMetaStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeMetaDeps({ componentEmitter });
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { timeframe: "8 semanas", language: "en" },
    }, null);
    assert.equal(emitterCalls[0].component.fields.timeframe, "8 semanas");
    assert.equal(emitterCalls[0].component.fields.language, "en");
  });

  test("accepts duration (Upwork-only whitelisted field) as a plain string", async () => {
    clearMetaStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeMetaDeps({ componentEmitter });
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { duration: "1 to 3 months" },
    }, null);
    assert.equal(result.isError, undefined);
    assert.equal(emitterCalls[0].component.fields.duration, "1 to 3 months");
  });

  test("accepts period (Freelancer.com-only whitelisted field) as a positive integer", async () => {
    clearMetaStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeMetaDeps({ componentEmitter });
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { period: 14 },
    }, null);
    assert.equal(result.isError, undefined);
    assert.equal(emitterCalls[0].component.fields.period, 14);
  });

  test("rejects a non-numeric period", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { period: "abc" },
    }, null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /period must be a plain integer number of days/);
  });

  test("success result contains proposed field summary", async () => {
    clearMetaStore();
    const { toolDef } = makeMetaDeps();
    const result = await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { budget: 12000 },
    }, null);
    assert.equal(result.isError, undefined);
    assert.ok(result.content[0].text.includes("budget=12000"), "summary in result");
  });

  test("normalizePendingFields: leaves non-budget string fields unchanged in emitter", async () => {
    clearMetaStore();
    const emitterCalls = [];
    const componentEmitter = async (payload) => { emitterCalls.push(payload); };
    const { toolDef } = makeMetaDeps({ componentEmitter });
    // Use "es" which differs from the baseline "pt-BR" to pass the no-op guard
    await toolDef.execute("tc", {
      flowId: "fid-meta-draft",
      itemId: "item-meta",
      fields: { language: "es" },
    }, null);
    assert.equal(emitterCalls.length, 1, "componentEmitter was called");
    // language is a string and should pass through unchanged by normalizePendingFields
    assert.equal(emitterCalls[0].component.fields.language, "es");
  });

  // ── auto-resolve (telemetry lineage resolver) ─────────────────────────────

  test("auto-resolves flowId/itemId from ctx.sessionKey when omitted entirely", async () => {
    clearMetaStore();
    fakeResolver = { resolve: () => ({ flow_id: "fid-meta-draft", item_key: "item-meta" }) };
    const { toolDef } = makeMetaDeps({
      ctx: { sessionKey: "agent:main:gf-discuss-deadbeef-touch-item-meta" },
    });
    const result = await toolDef.execute("tc", { fields: { budget: 9000 } }, null);
    assert.equal(result.isError, undefined);
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.ok(call, "mergePendingFields was called with the auto-resolved flowId");
    assert.equal(call.itemId, "item-meta");
  });

  test("auto-resolve overrides a wrong/garbled explicit flowId", async () => {
    clearMetaStore();
    fakeResolver = { resolve: () => ({ flow_id: "fid-meta-draft", item_key: "item-meta" }) };
    const { toolDef } = makeMetaDeps({
      ctx: { sessionKey: "agent:main:gf-discuss-deadbeef-touch-item-meta" },
    });
    const result = await toolDef.execute("tc", { flowId: "auto-resolve", itemId: "auto-resolve", fields: { budget: 9500 } }, null);
    assert.equal(result.isError, undefined);
    const call = mergePendingFieldsCalls.find((c) => c.flowId === "fid-meta-draft");
    assert.ok(call, "mergePendingFields was called with the auto-resolved flowId");
    assert.equal(call.itemId, "item-meta");
  });

  test("auto-resolve failure (resolver has nothing for this session) still surfaces a clear error, not a crash", async () => {
    clearMetaStore();
    fakeResolver = { resolve: () => null };
    const { toolDef } = makeMetaDeps({
      ctx: { sessionKey: "agent:content-writer:subagent:some-random-uuid" },
    });
    const result = await toolDef.execute("tc", { fields: { budget: 9000 } }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });
});
