// proxy.test.js — companion routing decisions + loopback forwarding.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { createRequire } from "node:module";
import { proxyDecision, forwardToSidecar } from "../../../companion/proxy.js";

const require = createRequire(import.meta.url);

describe("proxyDecision", () => {
  it("proxies core paths, passes sibling namespaces through, denies internals", () => {
    assert.equal(proxyDecision("/manifold/graphql"), "proxy");
    assert.equal(proxyDecision("/manifold/health"), "proxy");
    assert.equal(proxyDecision("/manifold/ios/state"), "passthrough");
    assert.equal(proxyDecision("/manifold/ios/"), "passthrough");
    assert.equal(proxyDecision("/manifold/internal/dispatch"), "deny");
    assert.equal(proxyDecision("/manifold/internal/"), "deny");
  });
});

describe("forwardToSidecar", () => {
  let upstream;
  let upstreamPort;
  let seen = null;
  before(async () => {
    upstream = http.createServer((req, res) => {
      let raw = "";
      req.on("data", (c) => { raw += c; });
      req.on("end", () => {
        seen = { method: req.method, url: req.url, auth: req.headers.authorization, body: raw };
        res.statusCode = 201;
        res.setHeader("x-echo", "yes");
        res.end(JSON.stringify({ ok: true }));
      });
    });
    await new Promise((r) => upstream.listen(0, "127.0.0.1", r));
    upstreamPort = upstream.address().port;
  });
  after(async () => {
    await new Promise((r) => upstream.close(r));
  });

  it("forwards method/path/body, replaces bearer, passes status through", async () => {
    const forward = forwardToSidecar({ sidecarUrl: `http://127.0.0.1:${upstreamPort}`, serviceToken: "svc" });
    const { req, res, done } = fakePair({ method: "POST", url: "/manifold/graphql?x=1", auth: "Bearer caller-token", body: '{"q":1}' });
    await forward(req, res);
    await done;
    assert.equal(seen.method, "POST");
    assert.equal(seen.url, "/manifold/graphql?x=1");
    assert.equal(seen.auth, "Bearer svc");
    assert.equal(seen.body, '{"q":1}');
    assert.equal(res.statusCode, 201);
    assert.equal(res.headers["x-echo"], "yes");
    assert.equal(JSON.parse(res.body).ok, true);
  });

  it("degrades to 502 when the sidecar is down", async () => {
    const forward = forwardToSidecar({ sidecarUrl: "http://127.0.0.1:9", serviceToken: "svc", logger: null });
    const { req, res, done } = fakePair({ method: "GET", url: "/manifold/health", auth: "Bearer x", body: "" });
    await forward(req, res);
    await done;
    assert.equal(res.statusCode, 502);
    assert.equal(JSON.parse(res.body).error, "sidecar unavailable");
  });
});

function fakePair({ method, url, auth, body }) {
  const { Readable, Writable } = require("node:stream");
  const req = Readable.from([body]);
  req.method = method;
  req.url = url;
  req.headers = { authorization: auth, host: "x" };
  const chunks = [];
  const res = new Writable({
    write(c, _e, cb) {
      chunks.push(c.toString());
      cb();
    },
  });
  res.statusCode = 200;
  res.headers = {};
  res.setHeader = (k, v) => {
    res.headers[k.toLowerCase()] = v;
  };
  Object.defineProperty(res, "body", { get: () => chunks.join("") });
  const done = new Promise((resolve) => res.on("finish", resolve));
  // Writable has no .end(body) flow from pipe unless ended: end it after pipe.
  const origEnd = res.end.bind(res);
  res.end = (data) => {
    if (data) chunks.push(data.toString());
    origEnd();
  };
  return { req, res, done };
}
