import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createRouteRegistry } from "../../../server/registry.js";

function fakeReq(url, method = "GET") {
  return { url, method, headers: { host: "127.0.0.1:7431" } };
}

function fakeRes() {
  return {
    statusCode: 200,
    headers: {},
    body: "",
    writableEnded: false,
    setHeader(k, v) { this.headers[k] = v; },
    end(body = "") { this.body = body; this.writableEnded = true; },
  };
}

describe("createRouteRegistry", () => {
  it("registers routes with exact matching by default", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    registry.registerHttpRoute({ path: "/manifold/health", handler: async (req, res) => { res.end("ok"); return true; } });
    const res = fakeRes();
    await registry.handle(fakeReq("/manifold/health"), res);
    assert.equal(res.body, "ok");
  });

  it("does not match an exact route on a prefix", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    registry.registerHttpRoute({ path: "/manifold/runs", handler: async (req, res) => { res.end("runs"); return true; } });
    const res = fakeRes();
    await registry.handle(fakeReq("/manifold/runs/abc"), res);
    assert.equal(res.statusCode, 404);
  });

  it("matches prefix routes", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    registry.registerHttpRoute({
      path: "/manifold/runs/", match: "prefix",
      handler: async (req, res) => { res.end(req.url); return true; },
    });
    const res = fakeRes();
    await registry.handle(fakeReq("/manifold/runs/abc"), res);
    assert.equal(res.body, "/manifold/runs/abc");
  });

  it("answers 404 JSON when nothing matches", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    const res = fakeRes();
    await registry.handle(fakeReq("/nope"), res);
    assert.equal(res.statusCode, 404);
    assert.match(res.body, /not found/);
  });

  it("turns a throwing handler into a 500 instead of a dead socket", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    registry.registerHttpRoute({ path: "/boom", handler: async () => { throw new Error("kaput"); } });
    const res = fakeRes();
    await registry.handle(fakeReq("/boom"), res);
    assert.equal(res.statusCode, 500);
    assert.match(res.body, /kaput/);
  });

  it("ends the response when a handler returns without ending it", async () => {
    const registry = createRouteRegistry({ logger: { warn() {} } });
    registry.registerHttpRoute({ path: "/empty", handler: async () => undefined });
    const res = fakeRes();
    await registry.handle(fakeReq("/empty"), res);
    assert.equal(res.writableEnded, true);
  });
});
