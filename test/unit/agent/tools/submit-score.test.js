/**
 * Unit tests for agent/tools/submit-score.js — registerSubmitScoreTool (submit_score).
 *
 * state/tool-handoff.js is mocked at the module level (via mock.module) so
 * the tool's resolveInvocationId()/writePendingResult() calls never touch
 * the real in-memory registry — we just control what
 * resolveInvocationId(ticket) returns and record what
 * writePendingResult() was called with.
 */

import { test, mock, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

const toolHandoffUrl = new URL("../../../../state/tool-handoff.js", import.meta.url).href;

let invocationIdByTicket = {};
let writePendingResultCalls = [];

await mock.module(toolHandoffUrl, {
  exports: {
    resolveInvocationId: (ticket) => invocationIdByTicket[ticket] ?? null,
    writePendingResult: (invocationId, payload) => { writePendingResultCalls.push({ invocationId, payload }); },
    readPendingResult: () => null,
    clearPendingResult: () => {},
  },
});

const { registerSubmitScoreTool } = await import("../../../../plugins/openclaw/manifold/agent/tools/submit-score.js");

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

function makeTool(ctx = {}) {
  const { api, tools } = createToolApi();
  registerSubmitScoreTool({ api });
  return findTool(tools, "submit_score", ctx);
}

function baseParams(overrides = {}) {
  return {
    score: 78,
    reasons: ["stack overlap", "budget above floor"],
    fitFlags: { language: true, budget: true, domain: true, time: true },
    ticket: "tick-1",
    ...overrides,
  };
}

describe("registerSubmitScoreTool — submit_score", () => {
  beforeEach(() => {
    invocationIdByTicket = {};
    writePendingResultCalls = [];
  });

  test("returns toolErr when no ticket is passed", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ ticket: undefined }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("ticket is required"));
    assert.equal(writePendingResultCalls.length, 0);
  });

  test("returns toolErr when the ticket names no parked invocation", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("unknown ticket"));
    assert.equal(writePendingResultCalls.length, 0);
  });

  test("succeeds and writes to the pending-result map when the ticket resolves", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, undefined);
    assert.equal(writePendingResultCalls.length, 1);
    assert.equal(writePendingResultCalls[0].invocationId, "inv-1");
    assert.equal(writePendingResultCalls[0].payload.score, 78);
    assert.deepEqual(writePendingResultCalls[0].payload.reasons, ["stack overlap", "budget above floor"]);
    assert.deepEqual(writePendingResultCalls[0].payload.fitFlags, { language: true, budget: true, domain: true, time: true });
  });

  test("rejects a non-integer score", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ score: 78.5 }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /score must be an integer 0-100/);
  });

  test("rejects a score below 0", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ score: -1 }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /score must be an integer 0-100/);
  });

  test("rejects a score above 100", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ score: 101 }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /score must be an integer 0-100/);
  });

  test("rejects an empty reasons array", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ reasons: [] }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /reasons must be a non-empty array/);
  });

  test("filters out blank/non-string reasons entries and still requires at least one real reason", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ reasons: ["  ", "", 5, null] }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /reasons must be a non-empty array/);
  });

  test("rejects missing fitFlags", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ fitFlags: undefined }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /fitFlags is required/);
  });

  test("rejects a fitFlags key that isn't a boolean", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ fitFlags: { language: "yes", budget: true, domain: true, time: true } }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /fitFlags\.language must be a boolean/);
  });

  test("rejects a hard-gate-failed score above the 30 cap", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ score: 60, fitFlags: { language: true, budget: false, domain: true, time: true } }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /score must be ≤30 when a hard gate failed/);
    assert.equal(writePendingResultCalls.length, 0);
  });

  test("accepts a hard-gate-failed score at or below the 30 cap", async () => {
    invocationIdByTicket["tick-1"] = "inv-1";
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ score: 20, fitFlags: { language: true, budget: false, domain: true, time: true } }), null);
    assert.equal(result.isError, undefined);
    assert.equal(writePendingResultCalls.length, 1);
    assert.equal(writePendingResultCalls[0].payload.score, 20);
  });

  test("unknown ticket surfaces a clear error, not a crash", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ ticket: "tick-nope" }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("unknown ticket"));
  });
});
