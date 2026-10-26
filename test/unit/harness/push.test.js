// Unit tests for harness URL push delivery (harness/push.js,
// harness/openclaw.js autoregister, state/push-targets.js).
//
// Real node:http servers throughout — no fetch stubs. Store tests run
// against a tmp sqlite file.

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { mkdtempSync } from "node:fs";
import { resolvePushTarget, notifyPush, pushPayload } from "../../../harness/push.js";
import { autoregisterPushTarget } from "../../../harness/openclaw.js";
import { createPushTargetsStore } from "../../../state/push-targets.js";

const SAVED_ENV = {};
for (const k of ["MANIFOLD_PUSH_URL", "MANIFOLD_PUSH_BEARER", "MANIFOLD_PUSH_TIMEOUT_MS", "MANIFOLD_PUSH_BASE_URL"]) {
  SAVED_ENV[k] = process.env[k];
  delete process.env[k];
}
after(() => {
  for (const [k, v] of Object.entries(SAVED_ENV)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
});

function startServer(status = 200) {
  const requests = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      requests.push({ url: req.url, method: req.method, headers: req.headers, body });
      res.writeHead(status, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      resolve({
        server,
        url: `http://127.0.0.1:${server.address().port}`,
        requests,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

function tmpStore() {
  const dir = mkdtempSync(path.join(os.tmpdir(), "manifold-push-"));
  return createPushTargetsStore({ logger: null, dbPath: path.join(dir, "rt.sqlite") });
}

describe("pushPayload", () => {
  it("leads with event/at keys", () => {
    const p = pushPayload({ event: "agent.dispatched", harness: "openclaw", flowId: "f1" });
    assert.deepEqual(Object.keys(p).slice(0, 2), ["event", "at"]);
    assert.equal(p.event, "agent.dispatched");
    assert.equal(typeof p.at, "number");
    assert.equal(p.harness, "openclaw");
    assert.equal(p.flowId, "f1");
  });

  it("merges extra last", () => {
    const p = pushPayload({ event: "e", extra: { flowId: "over", z: 1 } , flowId: "base" });
    assert.equal(p.flowId, "over");
    assert.equal(p.z, 1);
  });
});

describe("resolvePushTarget precedence", () => {
  it("DB row beats config entries and single URL", async () => {
    const store = await tmpStore();
    try {
      await store.upsert("h1", { url: "http://127.0.0.1:9/db", bearer: "db-b" });
      const t = await resolvePushTarget({
        pushTargets: store, harnessId: "h1",
        config: { pushUrls: { h1: "http://127.0.0.1:9/cfg" }, pushUrl: "http://127.0.0.1:9/single" },
      });
      assert.equal(t.url, "http://127.0.0.1:9/db");
      assert.equal(t.bearer, "db-b");
    } finally { store.close(); }
  });

  it("per-harness config entry beats single URL; string and object forms", async () => {
    const store = await tmpStore();
    try {
      const a = await resolvePushTarget({
        pushTargets: store, harnessId: "h1",
        config: { pushUrls: { h1: "http://127.0.0.1:9/per" }, pushUrl: "http://127.0.0.1:9/single" },
      });
      assert.equal(a.url, "http://127.0.0.1:9/per");
      const b = await resolvePushTarget({
        pushTargets: store, harnessId: "h2",
        config: { pushUrls: { h2: { url: "http://127.0.0.1:9/obj", bearer: "tok", timeoutMs: 111 } }, pushUrl: "http://127.0.0.1:9/single" },
      });
      assert.deepEqual(b, { url: "http://127.0.0.1:9/obj", bearer: "tok", timeoutMs: 111 });
    } finally { store.close(); }
  });

  it("single config URL resolves; disabled without any URL", async () => {
    const store = await tmpStore();
    try {
      const t = await resolvePushTarget({ pushTargets: store, harnessId: "h9", config: { pushUrl: "http://127.0.0.1:9/x" } });
      assert.equal(t.url, "http://127.0.0.1:9/x");
      assert.equal(t.bearer, null);
      assert.equal(t.timeoutMs, 3000);
      assert.equal(await resolvePushTarget({ pushTargets: store, harnessId: "h9", config: {} }), null);
      assert.equal(await resolvePushTarget({ harnessId: "h9" }), null);
    } finally { store.close(); }
  });

  it("invalid URLs fall through to the next precedence level", async () => {
    const store = await tmpStore();
    try {
      await store.upsert("h1", { url: "nota-url" });
      const t = await resolvePushTarget({
        pushTargets: store, harnessId: "h1",
        config: { pushUrl: "http://127.0.0.1:9/fallback" },
      });
      assert.equal(t.url, "http://127.0.0.1:9/fallback");
      assert.equal(await resolvePushTarget({ pushTargets: store, harnessId: "h2", config: { pushUrl: "ftp://x/y" } }), null);
    } finally { store.close(); }
  });

  it("non-positive timeout falls back to 3000", async () => {
    const store = await tmpStore();
    try {
      await store.upsert("h1", { url: "http://127.0.0.1:9/u", timeoutMs: -5 });
      const t = await resolvePushTarget({ pushTargets: store, harnessId: "h1", config: { pushTimeoutMs: 0 } });
      assert.equal(t.timeoutMs, 3000);
    } finally { store.close(); }
  });
});

describe("notifyPush over real HTTP", () => {
  let srv;
  before(async () => { srv = await startServer(200); });
  after(async () => { await srv.close(); });

  it("2xx resolves ok with status; sends JSON + bearer", async () => {
    const payload = pushPayload({ event: "agent.dispatched", harness: "openclaw", flowId: "f1" });
    const r = await notifyPush({ url: `${srv.url}/hook`, bearer: "s3cr3t", timeoutMs: 3000 }, payload, null);
    assert.deepEqual(r, { ok: true, status: 200 });
    const last = srv.requests.at(-1);
    assert.equal(last.method, "POST");
    assert.equal(last.headers.authorization, "Bearer s3cr3t");
    assert.equal(last.headers["content-type"], "application/json");
    assert.deepEqual(JSON.parse(last.body), payload);
  });

  it("500 resolves {ok:false} without throwing", async () => {
    const bad = await startServer(500);
    try {
      const r = await notifyPush({ url: `${bad.url}/hook`, timeoutMs: 3000 }, { event: "x", at: 1 }, null);
      assert.deepEqual(r, { ok: false, reason: "http_500" });
    } finally { await bad.close(); }
  });

  it("refused connection resolves {ok:false} without throwing", async () => {
    const tmp = await startServer(200);
    const deadUrl = `${tmp.url}/hook`;
    await tmp.close();
    const r = await notifyPush({ url: deadUrl, timeoutMs: 1000 }, { event: "x", at: 1 }, null);
    assert.equal(r.ok, false);
    assert.equal(r.reason, "error");
  });

  it("missing URL is disabled, never throws", async () => {
    assert.deepEqual(await notifyPush(null, { event: "x" }, null), { ok: false, reason: "disabled" });
  });
});

describe("autoregisterPushTarget", () => {
  it("upserts idempotently and verifies with a test ping", async () => {
    const srv = await startServer(200);
    const store = await tmpStore();
    try {
      const base = { pushTargets: store, baseUrl: srv.url, harness: "openclaw", bearer: "sek", timeoutMs: 2000 };
      const first = await autoregisterPushTarget(base);
      assert.deepEqual(first, { ok: true });
      const second = await autoregisterPushTarget(base);
      assert.deepEqual(second, { ok: true });
      assert.equal((await store.getAll()).length, 1);
      const row = await store.get("openclaw");
      assert.equal(row.url, `${srv.url}/api/manifold/push`);
      assert.equal(row.bearer, "sek");
      const ping = srv.requests.at(-1);
      assert.equal(ping.url, "/api/manifold/push");
      assert.deepEqual(JSON.parse(ping.body).event, "push.test");
    } finally { store.close(); await srv.close(); }
  });

  it("is disarmed without bearer and writes nothing", async () => {
    const store = await tmpStore();
    try {
      const r = await autoregisterPushTarget({ pushTargets: store, baseUrl: "http://127.0.0.1:9" });
      assert.deepEqual(r, { ok: false, reason: "disarmed" });
      assert.equal(await store.get("openclaw"), null);
      const r2 = await autoregisterPushTarget({ pushTargets: store, bearer: "x" });
      assert.deepEqual(r2, { ok: false, reason: "no-base-url" });
    } finally { store.close(); }
  });

  it("never throws when the store or network fails", async () => {
    const broken = { upsert: async () => { throw new Error("db down"); } };
    const r = await autoregisterPushTarget({ pushTargets: broken, baseUrl: "http://127.0.0.1:9", bearer: "b" });
    assert.equal(r.ok, false);
    const store = await tmpStore();
    try {
      const r2 = await autoregisterPushTarget({ pushTargets: store, baseUrl: "http://127.0.0.1:9", bearer: "b", timeoutMs: 500 });
      assert.deepEqual(r2, { ok: false, reason: "test-failed" });
    } finally { store.close(); }
  });
});

describe("push-targets store CRUD", () => {
  it("upsert/get/getAll/remove round-trip on a tmp db", async () => {
    const store = await tmpStore();
    try {
      assert.equal(await store.get("nope"), null);
      await store.upsert("a", { url: "http://127.0.0.1:9/a", bearer: "t" });
      await store.upsert("b", { url: "http://127.0.0.1:9/b" });
      assert.equal((await store.get("a")).bearer, "t");
      assert.equal((await store.getAll()).length, 2);
      await store.upsert("a", { url: "http://127.0.0.1:9/a2" });
      assert.equal((await store.get("a")).url, "http://127.0.0.1:9/a2");
      assert.equal((await store.getAll()).length, 2);
      assert.deepEqual(await store.remove("a"), { removed: 1 });
      assert.equal(await store.get("a"), null);
      assert.deepEqual(await store.remove("a"), { removed: 0 });
    } finally { store.close(); }
  });
});
