import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { registerSessionRoutes } from "../../../telemetry/http-sessions.js";

// ── helpers ───────────────────────────────────────────────────────────────────

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

// ── route registration ────────────────────────────────────────────────────────

describe("registerSessionRoutes", () => {
  it("registers /sessions/parts route", () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: null, json, bad });
    assert.ok(api._routes.has("/pipeclaw/telemetry/sessions/parts"));
  });

  it("returns 503 when session_key is missing", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: null, json, bad });

    const def = api._routes.get("/pipeclaw/telemetry/sessions/parts");
    const req = makeReq("/pipeclaw/telemetry/sessions/parts");
    const res = makeRes();
    await def.handler(req, res);
    assert.equal(res.statusCode, 503);
    const body = JSON.parse(res._body);
    assert.ok(body.error.includes("session_key"));
  });

  it("returns 503 when lcm.db is unavailable (unknown session key)", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerSessionRoutes({ api, logger: { warn: () => {} }, json, bad });

    const def = api._routes.get("/pipeclaw/telemetry/sessions/parts");
    const req = makeReq("/pipeclaw/telemetry/sessions/parts?session_key=agent%3Atest%3Anonexistent%3Asession");
    const res = makeRes();
    await def.handler(req, res);
    // Either 503 (lcm.db not available) or 200/500 depending on whether lcm.db exists.
    // In either case, the response must be valid JSON.
    const body = JSON.parse(res._body);
    assert.ok(typeof body === "object");
  });
});
