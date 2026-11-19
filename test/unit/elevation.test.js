// Unit tests for elevation.js — the loopback POST that re-enters a route
// registered with auth:"gateway" using the env-var Bearer, so the handler
// runs with CLI_DEFAULT_OPERATOR_SCOPES ambient.
//
// GATEWAY_PORT and the token are read at module load, so each test imports a
// fresh instance (ESM query-string cache busting) configured for the port it
// is about to bind. That keeps one test's port from leaking into the next.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";

// GATEWAY_PORT is module-level, so it must be set only for the import; the
// token is read per call, so it stays set for the whole file.
const TOKEN = "sekret";
process.env.OPENCLAW_GATEWAY_TOKEN = TOKEN;

let seq = 0;

async function loadElevation({ port }) {
  const prevPort = process.env.OPENCLAW_GATEWAY_PORT;
  process.env.OPENCLAW_GATEWAY_PORT = String(port);
  try {
    return await import(`../../elevation.js?i=${seq++}`);
  } finally {
    if (prevPort === undefined) delete process.env.OPENCLAW_GATEWAY_PORT;
    else process.env.OPENCLAW_GATEWAY_PORT = prevPort;
  }
}

async function withoutToken(fn) {
  const prev = process.env.OPENCLAW_GATEWAY_TOKEN;
  delete process.env.OPENCLAW_GATEWAY_TOKEN;
  try {
    return await fn();
  } finally {
    process.env.OPENCLAW_GATEWAY_TOKEN = prev;
  }
}

function startServer(handler) {
  const server = http.createServer(handler);
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve({ server, port: server.address().port }));
  });
}

function stopServer(server) {
  server.closeAllConnections?.();
  return new Promise((resolve) => server.close(() => resolve()));
}

describe("invokeElevated — token gate", () => {
  test("no OPENCLAW_GATEWAY_TOKEN: refuses before any connection with 503 no_gateway_token", async () => {
    const { server, port } = await startServer((_req, res) => res.end("should not be reached"));
    try {
      const { invokeElevated } = await loadElevation({ port });
      const out = await withoutToken(() => invokeElevated("/x", {}));
      assert.equal(out.ok, false);
      assert.equal(out.status, 503);
      assert.equal(out.error, "no_gateway_token");
      assert.equal(JSON.parse(out.body).code, "no_gateway_token");
    } finally {
      await stopServer(server);
    }
  });
});

describe("invokeElevated — request shape", () => {
  test("POSTs JSON to the loopback port with the env-var Bearer and resolves the raw body", async () => {
    let seen;
    const { server, port } = await startServer((req, res) => {
      let body = "";
      req.on("data", (c) => { body += c; });
      req.on("end", () => {
        seen = { method: req.method, path: req.url, headers: req.headers, body };
        res.statusCode = 200;
        res.end("pong");
      });
    });
    try {
      const { invokeElevated } = await loadElevation({ port });
      const out = await invokeElevated("/manifold/_elev/start", { pipelineId: "p", n: 2 });
      assert.deepEqual(out, { ok: true, status: 200, body: "pong" });
      assert.equal(seen.method, "POST");
      assert.equal(seen.path, "/manifold/_elev/start");
      assert.equal(seen.headers.authorization, "Bearer sekret");
      assert.equal(seen.headers["content-type"], "application/json");
      assert.equal(seen.headers["content-length"], String(Buffer.byteLength('{"pipelineId":"p","n":2}')));
      assert.deepEqual(JSON.parse(seen.body), { pipelineId: "p", n: 2 });
    } finally {
      await stopServer(server);
    }
  });

  test("a string payload is sent verbatim, not re-serialized", async () => {
    let raw;
    const { server, port } = await startServer((req, res) => {
      let body = "";
      req.on("data", (c) => { body += c; });
      req.on("end", () => { raw = body; res.end("ok"); });
    });
    try {
      const { invokeElevated } = await loadElevation({ port });
      await invokeElevated("/x", "already-json");
      assert.equal(raw, "already-json");
    } finally {
      await stopServer(server);
    }
  });

  test("a null payload becomes {}", async () => {
    let raw;
    const { server, port } = await startServer((req, res) => {
      let body = "";
      req.on("data", (c) => { body += c; });
      req.on("end", () => { raw = body; res.end("ok"); });
    });
    try {
      const { invokeElevated } = await loadElevation({ port });
      await invokeElevated("/x", null);
      assert.equal(raw, "{}");
    } finally {
      await stopServer(server);
    }
  });
});

describe("invokeElevated — failure surfaces", () => {
  test("non-2xx keeps the status and body, and reports ok:false", async () => {
    const { server, port } = await startServer((_req, res) => {
      res.statusCode = 500;
      res.end('{"ok":false,"error":"boom"}');
    });
    try {
      const { invokeElevated } = await loadElevation({ port });
      const out = await invokeElevated("/x", {});
      assert.equal(out.ok, false);
      assert.equal(out.status, 500);
      assert.equal(out.body, '{"ok":false,"error":"boom"}');
    } finally {
      await stopServer(server);
    }
  });

  test("a closed port resolves (never throws) with status 0 and code loopback_failed", async () => {
    const { server, port } = await startServer(() => {});
    await stopServer(server);
    const { invokeElevated } = await loadElevation({ port });
    const out = await invokeElevated("/x", {});
    assert.equal(out.ok, false);
    assert.equal(out.status, 0);
    assert.equal(JSON.parse(out.body).code, "loopback_failed");
    assert.ok(out.error, "error carries the underlying message for logging");
  });

  test("opts.timeoutMs destroys a hanging request and resolves with the timeout error", async () => {
    const { server, port } = await startServer(() => {});
    try {
      const { invokeElevated } = await loadElevation({ port });
      const out = await invokeElevated("/x", {}, { timeoutMs: 50 });
      assert.equal(out.ok, false);
      assert.equal(out.status, 0);
      assert.equal(out.error, "loopback timeout");
    } finally {
      await stopServer(server);
    }
  });
});
