// index.test.js — companion wiring: routes, ping capture, proxy decisions.
import { describe, it, before } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";

let entry = null;

before(async () => {
  // companion/auth.js degrades to env-token auth without the harness SDK.
  process.env.OPENCLAW_GATEWAY_TOKEN = "gw-test-token";
  entry = (await import("../../../companion/index.js")).default;
});

function fakeApi({ pluginConfig = {} } = {}) {
  const routes = [];
  const services = [];
  const methods = new Map();
  return {
    api: {
      pluginConfig,
      logger: { info: () => {}, warn: () => {}, error: () => {} },
      registerHttpRoute: (d) => routes.push(d),
      registerService: (d) => services.push(d),
      registerGatewayMethod: (name, handler, opts) => methods.set(name, { handler, opts }),
    },
    routes,
    services,
    methods,
  };
}

function fakeRes() {
  return {
    statusCode: 200,
    headers: {},
    body: "",
    setHeader(k, v) {
      this.headers[k.toLowerCase()] = v;
    },
    end(data) {
      if (data) this.body += data;
    },
  };
}

describe("companion entry", () => {
  it("registers proxy routes, link service, and ping method", () => {
    const f = fakeApi({ pluginConfig: { sidecarUrl: "http://127.0.0.1:9", sidecarToken: "svc" } });
    entry.register(f.api);
    assert.equal(f.routes.length, 2);
    assert.equal(f.services.length, 1);
    assert.equal(f.services[0].id, "manifold-companion-link");
    assert.ok(f.methods.has("plugin.manifold.ping"));
  });

  it("ping captures the broadcaster and responds", () => {
    const f = fakeApi({ pluginConfig: { sidecarToken: "svc" } });
    entry.register(f.api);
    let responded = null;
    const broadcast = () => {};
    f.methods.get("plugin.manifold.ping").handler({ context: { broadcast }, respond: (ok, body) => { responded = { ok, body }; } });
    assert.deepEqual(responded, { ok: true, body: { ok: true } });
  });

  it("denies sidecar internals and passes sibling namespaces through", async () => {
    const f = fakeApi({ pluginConfig: { sidecarToken: "svc" } });
    entry.register(f.api);
    const handler = f.routes[0].handler;
    const deny = fakeRes();
    assert.equal(await handler({ method: "GET", url: "/manifold/internal/health", headers: {} }, deny), true);
    assert.equal(deny.statusCode, 404);
    const pass = fakeRes();
    assert.equal(await handler({ method: "GET", url: "/manifold/ios/state", headers: {} }, pass), false);
  });

  it("proxies authorized calls with the service token", async () => {
    let seenAuth = null;
    const upstream = http.createServer((req, res) => {
      seenAuth = req.headers.authorization;
      req.resume();
      req.on("end", () => {
        res.end(JSON.stringify({ proxied: true }));
      });
    });
    await new Promise((r) => upstream.listen(0, "127.0.0.1", r));
    try {
      const f = fakeApi({ pluginConfig: { sidecarUrl: `http://127.0.0.1:${upstream.address().port}`, sidecarToken: "svc" } });
      entry.register(f.api);
      const { Writable } = await import("node:stream");
      const chunks = [];
      const res = new Writable({
        write(c, _e, cb) {
          chunks.push(c.toString());
          cb();
        },
      });
      const done = new Promise((resolve) => res.on("finish", resolve));
      const req = (await import("node:stream")).Readable.from([""]);
      req.method = "GET";
      req.url = "/manifold/health";
      req.headers = { authorization: "Bearer gw-test-token" };
      await f.routes[0].handler(req, res);
      await done;
      assert.equal(seenAuth, "Bearer svc");
      assert.equal(JSON.parse(chunks.join("")).proxied, true);
    } finally {
      await new Promise((r) => upstream.close(r));
    }
  });
});
