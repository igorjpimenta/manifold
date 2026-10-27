// manifold-webhook.test.js — adapter wake: shape guard, receiver prompts
// that start work, result-reactive fresh→fallback chain, loud double failure.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  pushEventError, wakeSessionKey, wakeMessage, turnOk, registerPushWebhook,
} from "../../../plugins/openclaw/manifold/webhook.js";

function apiStub({ bearer = "s3cret", run = null } = {}) {
  const routes = {};
  const logs = { warn: [], error: [], info: [] };
  const subagent = run ? { run } : null;
  return {
    api: {
      pluginConfig: bearer == null ? {} : { webhookBearer: bearer },
      logger: {
        warn: (m) => logs.warn.push(String(m)),
        error: (m) => logs.error.push(String(m)),
        info: (m) => logs.info.push(String(m)),
      },
      runtime: subagent ? { subagent } : {},
      registerHttpRoute: (r) => { routes[r.path] = r; },
    },
    routes,
    logs,
  };
}

function reqRes({ method = "POST", headers = {}, body = null }) {
  const raw = body == null ? "" : JSON.stringify(body);
  const req = {
    method,
    headers,
    async *[Symbol.asyncIterator]() { if (raw) yield raw; },
  };
  const res = {
    statusCode: 200,
    headers: {},
    chunks: [],
    setHeader(k, v) { this.headers[k] = v; },
    end(c) { this.chunks.push(String(c ?? "")); this.ended = true; },
    json() { return JSON.parse(this.chunks.join("")); },
  };
  return { req, res };
}

function dispatched() {
  return {
    event: "agent.dispatched",
    at: Date.now(),
    flow: { id: "F1", pipelineId: "p1", nodeId: "n1" },
    session: { harness: "openclaw", agent: "a1", sessionKey: "orig-session" },
  };
}

test("shape guard: webhook.test passes, unknown/dispatched-without-agent refused", () => {
  assert.equal(pushEventError({ event: "webhook.test" }), null);
  assert.match(pushEventError({ event: "nope" }), /unsupported event/);
  assert.match(pushEventError({ event: "agent.dispatched", session: {} }), /session.agent required/);
  assert.equal(pushEventError(dispatched()), null);
});

test("wakeSessionKey namespaces manifold turns", () => {
  assert.equal(wakeSessionKey("F1", "n1"), "manifold-push-F1-n1");
});

test("wakeMessage starts work: manifold_start for dispatch, manifold_approve for gates", () => {
  const d = wakeMessage(dispatched());
  assert.match(d, /manifold_start/);
  assert.match(d, /F1/);
  assert.match(d, /n1/);
  const g = wakeMessage({ event: "gate.opened", flow: { id: "F2", nodeId: "n9", pipelineId: "p" }, session: {} });
  assert.match(g, /manifold_approve F2 <itemId> <approve\|reject\|edit>/);
});

test("turnOk reacts to the result, not its presence", () => {
  assert.equal(turnOk({ started: true }), true);
  assert.equal(turnOk("done"), true);
  assert.equal(turnOk(null), false);
  assert.equal(turnOk(undefined), false);
  assert.equal(turnOk({ ok: false }), false);
  assert.equal(turnOk({ error: "boom" }), false);
});

test("disarmed route (no bearer) answers 503 and never wakes", async () => {
  const { api, routes } = apiStub({ bearer: null });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: {}, body: dispatched() });
  await routes["/api/manifold/pushed"].handler(req, res);
  assert.equal(res.statusCode, 503);
});

test("fresh session wake carries the work-starting prompt", async () => {
  const seen = [];
  const { api, routes } = apiStub({ run: async (t) => { seen.push(t); return { started: true }; } });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body: dispatched() });
  await routes["/api/manifold/pushed"].handler(req, res);
  const out = res.json();
  assert.equal(out.woken, true);
  assert.equal(out.wokenSession, "manifold-push-F1-n1");
  assert.equal(seen.length, 1);
  assert.equal(seen[0].sessionKey, "manifold-push-F1-n1");
  assert.match(seen[0].message, /manifold_start/);
});

test("failed fresh turn falls back to the originating session", async () => {
  const seen = [];
  const { api, routes } = apiStub({
    run: async (t) => { seen.push(t.sessionKey); return t.sessionKey.startsWith("manifold-push") ? { ok: false } : { started: true }; },
  });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body: dispatched() });
  await routes["/api/manifold/pushed"].handler(req, res);
  const out = res.json();
  assert.equal(out.woken, true);
  assert.equal(out.wokenSession, "orig-session");
  assert.deepEqual(seen, ["manifold-push-F1-n1", "orig-session"]);
});

test("double failure pages loudly, still answers 200 (run unaffected)", async () => {
  const { api, routes, logs } = apiStub({ run: async () => { throw new Error("down"); } });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body: dispatched() });
  await routes["/api/manifold/pushed"].handler(req, res);
  const out = res.json();
  assert.equal(out.woken, false);
  assert.match(logs.error.join("\n"), /FAILED/);
});

test("gate.opened without an agent is report-only", async () => {
  const { api, routes } = apiStub({ run: async () => ({ started: true }) });
  registerPushWebhook({ api });
  const { req, res } = reqRes({
    headers: { authorization: "Bearer s3cret" },
    body: { event: "gate.opened", flow: { id: "F3" }, session: {} },
  });
  await routes["/api/manifold/pushed"].handler(req, res);
  assert.deepEqual(res.json(), { ok: true, event: "gate.opened", woken: false, reason: "no-agent" });
});
