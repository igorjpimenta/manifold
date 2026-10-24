/**
 * Unit tests for routes/admin-proxy.js
 *
 * The WS-RPC loopback is injected (rpc stub) — its own transport is
 * covered against a fake gateway below. Route tests assert registration,
 * method guards, auth gating, and envelope passthrough.
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { WebSocketServer } from "ws";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";
import { registerAdminProxyRoutes, rpcViaLoopback } from "../../../routes/admin-proxy.js";

const authUrl = new URL("../../../auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
  },
});

function fakeApiWith(rpc) {
  const { api, routes } = createMockApi();
  const seen = { rpc: [] };
  registerAdminProxyRoutes({
    api,
    rpc: async (call) => {
      seen.rpc.push(call);
      return rpc(call);
    },
  });
  return { routes, seen };
}

function postReq(path, body) {
  const text = typeof body === "string" ? body : JSON.stringify(body);
  return {
    ...createMockReq("POST", path),
    [Symbol.asyncIterator]: async function* () { yield text; },
  };
}

const byPath = (routes) => Object.fromEntries(routes.map((r) => [r.path, r]));

describe("registerAdminProxyRoutes — registration", () => {
  test("registers health, cron, devices, and apns routes", () => {
    const { routes } = fakeApiWith(async () => ({ ok: true }));
    const paths = routes.map((r) => r.path).sort();
    assert.ok(paths.includes("/manifold/health"));
    assert.ok(paths.includes("/manifold/cron/jobs"));
    assert.ok(paths.includes("/manifold/cron/run"));
    assert.ok(paths.includes("/manifold/cron/update"));
    assert.ok(paths.includes("/manifold/devices"));
    assert.ok(paths.includes("/manifold/devices/approve"));
    assert.ok(paths.includes("/manifold/devices/token/rotate"));
    assert.ok(paths.includes("/manifold/apns/register"));
    for (const r of routes) assert.equal(r.auth, "plugin");
  });

  test("health answers without RPC", async () => {
    const { routes, seen } = fakeApiWith(async () => { throw new Error("must not be called"); });
    const res = createMockRes();
    await byPath(routes)["/manifold/health"].handler(createMockReq("GET", "/manifold/health"), res);
    assert.equal(res.statusCode, 200);
    const out = JSON.parse(res.body);
    assert.equal(out.ok, true);
    assert.ok(typeof out.manifold === "string");
    assert.equal(seen.rpc.length, 0);
  });

  test("cron/devices proxy preserves the harness envelope", async () => {
    const { routes, seen } = fakeApiWith(async (call) => ({ ok: true, payload: { jobs: [] } }));
    const res = createMockRes();
    await byPath(routes)["/manifold/cron/jobs"].handler(
      { ...createMockReq("GET", "/manifold/cron/jobs?includeDisabled=false"), url: "/manifold/cron/jobs?includeDisabled=false" },
      res,
    );
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body), { ok: true, payload: { jobs: [] } });
    assert.equal(seen.rpc[0].method, "cron.list");
    assert.deepEqual(seen.rpc[0].params, { includeDisabled: false });

    const res2 = createMockRes();
    await byPath(routes)["/manifold/devices/approve"].handler(
      postReq("/manifold/devices/approve", { requestId: "r1" }), res2,
    );
    assert.equal(seen.rpc[1].method, "device.pair.approve");
    assert.deepEqual(seen.rpc[1].params, { requestId: "r1" });
  });

  test("apns/register validates and delegates to the apns runtime", async () => {
    const registered = [];
    const { api, routes } = createMockApi();
    api.runtime = {
      apns: {
        isConfigured: () => true,
        registerDevice: async (deviceId, token, env) => registered.push({ deviceId, token, env }),
      },
    };
    registerAdminProxyRoutes({ api, rpc: async () => ({ ok: true }) });
    const bad = createMockRes();
    await byPath(routes)["/manifold/apns/register"].handler(postReq("/manifold/apns/register", {}), bad);
    assert.equal(bad.statusCode, 400);
    const ok = createMockRes();
    await byPath(routes)["/manifold/apns/register"].handler(
      postReq("/manifold/apns/register", { deviceId: "d1", apnsToken: "t1", environment: "sandbox" }), ok,
    );
    assert.equal(ok.statusCode, 200);
    assert.deepEqual(registered, [{ deviceId: "d1", token: "t1", env: "sandbox" }]);
  });
});

describe("rpcViaLoopback — transport", () => {
  async function withFakeGateway() {
    const { WebSocketServer: Server } = await import("ws");
    const server = new Server({ port: 0 });
    await new Promise((r) => server.on("listening", r));
    const seen = [];
    server.on("connection", (socket) => {
      socket.on("message", (data) => {
        let frame;
        try {
          frame = JSON.parse(String(data));
        } catch {
          return;
        }
        seen.push(frame);
        if (frame?.method === "connect") {
          socket.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: {} }));
        } else {
          socket.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { echoed: frame.method } }));
        }
      });
    });
    return { port: server.address().port, seen, close: () => new Promise((r) => server.close(r)) };
  }

  test("connects, sends the method, returns the envelope", async () => {
    const gw = await withFakeGateway();
    const out = await rpcViaLoopback({ method: "cron.list", params: { a: 1 }, port: gw.port, token: "t" });
    assert.equal(out.ok, true);
    assert.equal(out.payload.echoed, "cron.list");
    const connect = gw.seen.find((f) => f.method === "connect");
    assert.equal(connect.params.minProtocol, 3);
    assert.equal(connect.params.maxProtocol, 4);
    assert.equal(connect.params.client.mode, "backend");
    await gw.close();
  });

  test("unreachable gateway synthesizes {ok:false}, never throws", async () => {
    const out = await rpcViaLoopback({ method: "cron.list", port: 1, token: "t" });
    assert.equal(out.ok, false);
    assert.ok(typeof out.error?.message === "string");
  });
});
