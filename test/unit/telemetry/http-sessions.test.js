import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { registerSessionRoutes } = await import("../../../telemetry/http-sessions.js");

function makeApi() {
  const routes = new Map();
  return {
    _routes: routes,
    registerHttpRoute: (def) => routes.set(def.path, def),
  };
}

function makeJson() {
  const json = (res, body, status = 200) => {
    res.statusCode = status;
    res._body = JSON.stringify(body);
  };
  const bad = (res, msg, status = 503) => json(res, { error: msg }, status);
  return { json, bad };
}

function makeRes() {
  return { statusCode: 200, _body: null };
}

function makeReq(path) {
  return { method: "GET", url: `http://localhost${path}` };
}

function okSources(overrides = {}) {
  return {
    querySessionParts: async ({ sessionKey }) => ({
      parts: [{ seq: 1, role: "user", session_key: sessionKey }],
      hasMore: false,
      hasMoreNewer: false,
      firstSeq: 1,
      lastSeq: 1,
      ...overrides,
    }),
  };
}

describe("registerSessionRoutes", () => {
  it("registers /sessions/parts route", () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: null, json, bad, sources: okSources() });
    assert.ok(api._routes.has("/manifold/telemetry/sessions/parts"));
  });

  it("returns 503 when session_key is missing", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: null, json, bad, sources: okSources() });

    const def = api._routes.get("/manifold/telemetry/sessions/parts");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/sessions/parts"), res);
    assert.equal(res.statusCode, 503);
    assert.ok(JSON.parse(res._body).error.includes("session_key"));
  });

  it("returns 503 when the adapter reports lcm.db missing", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    const missing = new Error("lcm.db not found");
    missing.code = "LCM_DB_MISSING";
    registerSessionRoutes({
      api, logger: { warn: () => {} }, json, bad,
      sources: { querySessionParts: async () => { throw missing; } },
    });

    const def = api._routes.get("/manifold/telemetry/sessions/parts");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/sessions/parts?session_key=agent%3Atest%3Ax"), res);
    assert.equal(res.statusCode, 503);
    assert.ok(JSON.parse(res._body).error.includes("lcm.db"));
  });

  it("returns 503 without injected sources (same as missing lcm.db)", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: { warn: () => {} }, json, bad, sources: null });

    const def = api._routes.get("/manifold/telemetry/sessions/parts");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/sessions/parts?session_key=k"), res);
    assert.equal(res.statusCode, 503);
    assert.ok(JSON.parse(res._body).error.includes("lcm.db"));
  });

  it("returns the adapter payload with the sessionKey attached", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: null, json, bad, sources: okSources() });

    const def = api._routes.get("/manifold/telemetry/sessions/parts");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/sessions/parts?session_key=agent%3Atest%3Ax"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.equal(body.sessionKey, "agent:test:x");
    assert.equal(body.parts.length, 1);
    assert.equal(body.hasMore, false);
  });

  it("forwards cursor params and clamped limit to the adapter", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    let seen = null;
    registerSessionRoutes({
      api, logger: null, json, bad,
      sources: { querySessionParts: async (args) => { seen = args; return { parts: [], hasMore: false, hasMoreNewer: false, firstSeq: null, lastSeq: null }; } },
    });

    const def = api._routes.get("/manifold/telemetry/sessions/parts");
    await def.handler(
      makeReq("/manifold/telemetry/sessions/parts?session_key=k&before_seq=42&limit=99999"),
      makeRes(),
    );
    assert.equal(seen.beforeSeq, 42);
    assert.equal(seen.afterSeq, null);
    assert.equal(seen.limit, 2000);
  });
});
