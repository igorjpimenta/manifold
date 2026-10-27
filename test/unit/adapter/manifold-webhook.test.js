// manifold-webhook.test.js — adapter wake: shape guard, CLAIM receiver
// prompts, result-reactive fresh→fallback chain, loud double failure.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  pushEventError, wakeSessionKey, claimSessionKey, wakeMessage, claimMessage, turnOk, registerPushWebhook,
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

function ready() {
  return {
    event: "agent.ready",
    at: Date.now(),
    flow: { id: "F1", pipelineId: "p1", nodeId: "n1" },
    session: { harness: "openclaw", agent: "a1" },
    claim: { ticket: "tick-1", prompt: "Score these listings." },
  };
}

test("shape guard: webhook.test passes, unknown/ready-without-agent-or-ticket refused", () => {
  assert.equal(pushEventError({ event: "webhook.test" }), null);
  assert.match(pushEventError({ event: "nope" }), /unsupported event/);
  assert.match(pushEventError({ event: "agent.dispatched" }), /unsupported event/);
  assert.match(pushEventError({ event: "agent.ready", session: {} }), /session.agent required/);
  assert.match(pushEventError({ event: "agent.ready", session: { agent: "a1" } }), /claim.ticket required/);
  assert.equal(pushEventError(ready()), null);
});

test("claimSessionKey namespaces claim turns", () => {
  assert.equal(claimSessionKey("tick-1"), "manifold-claim-tick-1");
});

test("wakeSessionKey namespaces gate turns", () => {
  assert.equal(wakeSessionKey("F1", "n1"), "manifold-push-F1-n1");
});

test("claimMessage carries CLAIM instructions with the delivered prompt verbatim", () => {
  const m = claimMessage(ready());
  assert.match(m, /manifold_claim/);
  assert.match(m, /manifold_report/);
  assert.match(m, /tick-1/);
  assert.match(m, /Score these listings\./);
});

test("wakeMessage routes agent.ready to claim instructions, gate.opened to approve", () => {
  const c = wakeMessage(ready());
  assert.match(c, /manifold_claim/);
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
  const { req, res } = reqRes({ headers: {}, body: ready() });
  await routes["/api/manifold/pushed"].handler(req, res);
  assert.equal(res.statusCode, 503);
});

test("fresh session wake carries the CLAIM prompt", async () => {
  const seen = [];
  const { api, routes } = apiStub({ run: async (t) => { seen.push(t); return { started: true }; } });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body: ready() });
  await routes["/api/manifold/pushed"].handler(req, res);
  const out = res.json();
  assert.equal(out.woken, true);
  assert.equal(out.wokenSession, "manifold-claim-tick-1");
  assert.equal(seen.length, 1);
  assert.equal(seen[0].sessionKey, "manifold-claim-tick-1");
  assert.match(seen[0].message, /manifold_claim/);
});

test("failed fresh turn falls back to the originating session when one is attached", async () => {
  const seen = [];
  const { api, routes } = apiStub({
    run: async (t) => { seen.push(t.sessionKey); return t.sessionKey.startsWith("manifold-claim") ? { ok: false } : { started: true }; },
  });
  registerPushWebhook({ api });
  const body = { ...ready(), session: { harness: "openclaw", agent: "a1", sessionKey: "orig-session" } };
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body });
  await routes["/api/manifold/pushed"].handler(req, res);
  const out = res.json();
  assert.equal(out.woken, true);
  assert.equal(out.wokenSession, "orig-session");
  assert.deepEqual(seen, ["manifold-claim-tick-1", "orig-session"]);
});

test("double failure pages loudly, still answers 200 (run unaffected)", async () => {
  const { api, routes, logs } = apiStub({ run: async () => { throw new Error("down"); } });
  registerPushWebhook({ api });
  const { req, res } = reqRes({ headers: { authorization: "Bearer s3cret" }, body: ready() });
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
