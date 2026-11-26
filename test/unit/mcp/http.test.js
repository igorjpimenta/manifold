
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { startMcpHttpServer } from "../../../mcp/http.js";

const TOKEN = "test-bearer";

const ops = {
  startRun: async () => ({}),
  runStatus: async () => null,
  listRuns: async () => [],
  approveGate: async () => ({}),
  cancelFlow: async () => ({}),
};

let server;

before(async () => {
  process.env.MANIFOLD_BEARER = TOKEN;
  server = await startMcpHttpServer({ ops, port: 0, host: "127.0.0.1" });
});

after(async () => {
  delete process.env.MANIFOLD_BEARER;
  await server?.close();
});

function base(pathname = "/mcp", headers = {}) {
  return `http://${server.host}:${server.port}${pathname}`;
}

describe("startMcpHttpServer — bearer gate", () => {
  test("refuses to start without MANIFOLD_BEARER", async () => {
    const prev = process.env.MANIFOLD_BEARER;
    delete process.env.MANIFOLD_BEARER;
    try {
      await assert.rejects(
        () => startMcpHttpServer({ ops, port: 0 }),
        /MANIFOLD_BEARER/,
      );
    } finally {
      process.env.MANIFOLD_BEARER = prev;
    }
  });
});

describe("startMcpHttpServer — routing", () => {
  test("GET /health is unauthenticated", async () => {
    const res = await fetch(base("/health"));
    assert.equal(res.status, 200);
    assert.deepEqual(await res.json(), { ok: true });
  });

  test("any other path is a 404", async () => {
    const res = await fetch(base("/nope"));
    assert.equal(res.status, 404);
    assert.match(await res.text(), /POST \/mcp only/);
  });
});

describe("startMcpHttpServer — auth and identity", () => {
  test("POST /mcp without a bearer is 401", async () => {
    const res = await fetch(base("/mcp"), { method: "POST", body: "{}" });
    assert.equal(res.status, 401);
    assert.match((await res.json()).error, /unauthorized/);
  });

  test("a wrong bearer is 401", async () => {
    const res = await fetch(base("/mcp"), {
      method: "POST",
      headers: { authorization: "Bearer wrong" },
      body: "{}",
    });
    assert.equal(res.status, 401);
  });

  test("bearer without X-Manifold-Harness is 400", async () => {
    const res = await fetch(base("/mcp"), {
      method: "POST",
      headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
      body: "{}",
    });
    assert.equal(res.status, 400);
    assert.match((await res.json()).error, /X-Manifold-Harness/);
  });

  test("an initialize request with bearer + harness reaches the MCP transport", async () => {
    const res = await fetch(base("/mcp"), {
      method: "POST",
      headers: {
        authorization: `Bearer ${TOKEN}`,
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "x-manifold-harness": "claude-code",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "wire-test", version: "1.0.0" },
        },
      }),
    });
    assert.equal(res.status, 200);
    const text = await res.text();
    assert.match(text, /manifold/, "the server identifies itself in the initialize result");
  });
});
