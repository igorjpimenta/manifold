// The adapter's telemetry half: openclaw hook payloads in, semantic events
// out. Core decides what the events mean.

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { translateHook, registerTelemetryHooks } from "../../../../../plugins/openclaw/manifold/telemetry.js";

describe("translateHook", () => {
  it("maps before_agent_start to a lineage.session event", () => {
    assert.deepEqual(
      translateHook("before_agent_start", { sessionKey: "s1", agentId: "a1" }, { requesterSessionKey: "p1" }),
      { kind: "lineage.session", source: "start", session: "s1", agentId: "a1", parentSession: "p1" },
    );
  });

  it("maps session_start to the session_start source", () => {
    const event = translateHook("session_start", { sessionKey: "s1" }, {});
    assert.equal(event.source, "session_start");
  });

  it("falls back to the context session key", () => {
    assert.equal(translateHook("before_agent_start", {}, { sessionKey: "ctx" }).session, "ctx");
  });

  it("maps subagent_spawned to lineage.spawn with the child session", () => {
    assert.deepEqual(
      translateHook("subagent_spawned", { childSessionKey: "child", agentId: "a1" }, { requesterSessionKey: "parent" }),
      { kind: "lineage.spawn", session: "child", parentSession: "parent", agentId: "a1" },
    );
  });

  it("maps before_message_write to cost.usage with the raw usage block", () => {
    const usage = { input: 10, output: 5, cost: { total: 0.01 } };
    const event = translateHook(
      "before_message_write",
      { sessionKey: "s1", message: { id: "m1", provider: "p", model: "m", timestamp: 1, usage } },
      { sessionId: "sid" },
    );
    assert.equal(event.kind, "cost.usage");
    assert.equal(event.session, "s1");
    assert.equal(event.sessionId, "sid");
    assert.equal(event.messageId, "m1");
    assert.deepEqual(event.usage, usage);
  });

  it("returns null when the hook carries nothing core can use", () => {
    assert.equal(translateHook("before_agent_start", {}, {}), null);
    assert.equal(translateHook("subagent_spawned", {}, {}), null);
    assert.equal(translateHook("before_message_write", { message: {} }, {}), null);
    assert.equal(translateHook("other_hook", {}, {}), null);
  });
});

describe("registerTelemetryHooks", () => {
  function makeApi() {
    const handlers = {};
    return { handlers, on: (name, fn) => { handlers[name] = fn; }, logger: { warn() {} } };
  }

  it("registers all four hooks", () => {
    const api = makeApi();
    registerTelemetryHooks(api, { ingestUrl: "http://x", token: "t" });
    assert.deepEqual(Object.keys(api.handlers).sort(), [
      "before_agent_start", "before_message_write", "session_start", "subagent_spawned",
    ]);
  });

  it("posts the translated event with the token", async () => {
    const seen = [];
    const api = makeApi();
    registerTelemetryHooks(api, {
      ingestUrl: "http://127.0.0.1:7431/manifold/telemetry/event",
      token: "secret",
      fetchImpl: async (url, init) => {
        seen.push({ url, init });
        return { ok: true, status: 202 };
      },
    });
    api.handlers.subagent_spawned({ childSessionKey: "child" }, { requesterSessionKey: "p" });
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].url, "http://127.0.0.1:7431/manifold/telemetry/event");
    assert.equal(seen[0].init.headers.authorization, "Bearer secret");
    assert.equal(JSON.parse(seen[0].init.body).kind, "lineage.spawn");
  });

  it("does not post without an ingest URL or token", () => {
    const seen = [];
    const api = makeApi();
    registerTelemetryHooks(api, { ingestUrl: null, token: "t", fetchImpl: async () => { seen.push(1); } });
    api.handlers.session_start({ sessionKey: "s" }, {});
    assert.equal(seen.length, 0);
  });

  it("swallows a failed post and logs it", async () => {
    const warns = [];
    const api = makeApi();
    api.logger = { warn: (m) => warns.push(m) };
    registerTelemetryHooks(api, {
      ingestUrl: "http://x", token: "t",
      fetchImpl: async () => { throw new Error("core down"); },
    });
    api.handlers.session_start({ sessionKey: "s" }, {});
    await new Promise((r) => setTimeout(r, 10));
    assert.ok(warns.some((m) => /core down/.test(m)));
  });
});
