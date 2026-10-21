/**
 * Unit tests for agent/tools/manifold-send-push.js — registerManifoldSendPushTool
 * (manifold_send_push): validation and APNs push delivery.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { registerManifoldSendPushTool } from "../../../../agent/tools/manifold-send-push.js";
import { configureApi } from "../../../../telemetry/digest-push.js";

function createToolApi(apns) {
  const tools = [];
  const api = {
    registerTool(defOrFactory) { tools.push(defOrFactory); },
    logger: { info: () => {}, warn: () => {}, debug: () => {}, error: () => {} },
    runtime: { apns },
  };
  return { api, tools };
}

function findTool(tools, name) {
  const def = tools.find((t) => {
    if (typeof t === "function") return t({}).name === name;
    return t.name === name;
  });
  assert.ok(def, `tool '${name}' not registered`);
  return typeof def === "function" ? def({}) : def;
}

describe("registerManifoldSendPushTool — manifold_send_push", () => {
  function makeSendDeps(apns) {
    const { api, tools } = createToolApi(apns);
    configureApi(api);
    registerManifoldSendPushTool({ api });
    const toolDef = findTool(tools, "manifold_send_push");
    return { toolDef };
  }

  test("returns toolErr when text is missing", async () => {
    const { toolDef } = makeSendDeps({ isConfigured: () => true, listTokens: () => [] });
    const result = await toolDef.execute("tc", {}, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("text is required"));
  });

  test("returns toolErr when text is empty", async () => {
    const { toolDef } = makeSendDeps({ isConfigured: () => true, listTokens: () => [] });
    const result = await toolDef.execute("tc", { text: "" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("text is required"));
  });

  test("returns toolErr when APNs is not configured", async () => {
    const { toolDef } = makeSendDeps({ isConfigured: () => false, listTokens: () => [] });
    const result = await toolDef.execute("tc", { text: "hi" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("not_configured"));
  });

  test("returns toolErr when there are no registered tokens", async () => {
    const { toolDef } = makeSendDeps({ isConfigured: () => true, listTokens: () => [], send: async () => ({ ok: true }) });
    const result = await toolDef.execute("tc", { text: "hi" }, null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("no_tokens"));
  });

  test("sends to every registered token and reports the count", async () => {
    const sends = [];
    const apns = {
      isConfigured: () => true,
      listTokens: () => [
        { apns_token: "tok-a", environment: "production" },
        { apns_token: "tok-b", environment: "production" },
      ],
      send: async (args) => { sends.push(args); return { ok: true }; },
    };
    const { toolDef } = makeSendDeps(apns);
    const result = await toolDef.execute("tc", { title: "Hey", text: "Howdy!" }, null);
    assert.equal(result.isError, undefined);
    assert.equal(sends.length, 2);
    assert.equal(sends[0].payload.aps.alert.body, "Howdy!");
    assert.equal(sends[0].payload.aps.alert.title, "Hey");
    assert.ok(result.content[0].text.includes("2/2"));
  });
});
