// http.test.js — MCP over Streamable HTTP, live transport. Mirrors the stdio
// contract (declared harness, per-call agent/session, unstamped refused)
// plus the HTTP-only doors: bearer gate, harness header gate, /health.
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startMcpHttpServer } from "../../../mcp/http.js";

const BEARER = "test-bearer-token";
const HARNESS = "test-harness";
let savedBearer;
let base;
let closeServer = null;
let client = null;

const ops = {
  async startRun({ pipelineId, args }) {
    return { flowId: "FLOW-1", pipelineId, status: "running", revision: 0, args };
  },
  async runStatus({ flowId }) {
    if (flowId !== "FLOW-1") return null;
    return { flowId, pipelineId: "demo", status: "running", revision: 3 };
  },
  async listRuns() {
    return [{ flowId: "FLOW-1", pipelineId: "demo", status: "running" }];
  },
  async approveGate({ flowId, itemId, decision }) {
    return { ok: true, flowId, itemId, decision };
  },
  async cancelFlow({ flowId }) {
    return flowId === "FLOW-1" ? { cancelled: true, flowId } : { cancelled: false, reason: "not found" };
  },
};

const quiet = { info() {}, warn() {}, error() {} };

before(async () => {
  savedBearer = process.env.MANIFOLD_BEARER;
  process.env.MANIFOLD_BEARER = BEARER;
  const started = await startMcpHttpServer({ ops, port: 0, host: "127.0.0.1", logger: quiet });
  base = `http://127.0.0.1:${started.port}`;
  closeServer = started.close;
});

after(async () => {
  if (client) { try { await client.close(); } catch {} client = null; }
  if (closeServer) { await closeServer(); closeServer = null; }
  if (savedBearer === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = savedBearer;
});

function connect(extraHeaders = {}) {
  const transport = new StreamableHTTPClientTransport(new URL(`${base}/mcp`), {
    requestInit: {
      headers: {
        Authorization: `Bearer ${BEARER}`,
        "X-Manifold-Harness": HARNESS,
        ...extraHeaders,
      },
    },
  });
  const c = new Client({ name: "test-client", version: "0.0.0" });
  return c.connect(transport).then(() => {
    client?.close?.().catch(() => {});
    client = c;
    return c;
  });
}

describe("mcp/http.js — transport doors", () => {
  test("GET /health answers without a bearer", async () => {
    const r = await fetch(`${base}/health`);
    assert.equal(r.status, 200);
    assert.deepEqual(await r.json(), { ok: true });
  });

  test("POST /mcp without bearer is 401", async () => {
    const r = await fetch(`${base}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json", "X-Manifold-Harness": HARNESS },
      body: "{}",
    });
    assert.equal(r.status, 401);
  });

  test("POST /mcp with bearer but no harness header is 400", async () => {
    const r = await fetch(`${base}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json", Authorization: `Bearer ${BEARER}` },
      body: "{}",
    });
    assert.equal(r.status, 400);
  });

  test("GET /unknown is 404", async () => {
    const r = await fetch(`${base}/nope`, { headers: { Authorization: `Bearer ${BEARER}` } });
    assert.equal(r.status, 404);
  });
});

describe("mcp/http.js — tool contract over the wire", () => {
  test("lists the same 5 tools as stdio", async () => {
    const c = await connect();
    const { tools } = await c.listTools();
    assert.deepEqual(tools.map((t) => t.name).sort(), [
      "manifold_approve",
      "manifold_cancel",
      "manifold_runs",
      "manifold_start",
      "manifold_status",
    ]);
  });

  test("stamped call works over HTTP", async () => {
    const c = await connect();
    const r = await c.callTool({
      name: "manifold_status",
      arguments: { flowId: "FLOW-1" },
      _meta: { manifold: { agent: "a1", session: "s1" } },
    });
    assert.equal(r.isError ?? false, false);
    assert.equal(JSON.parse(r.content[0].text).flowId, "FLOW-1");
  });

  test("unstamped call refused over HTTP", async () => {
    const c = await connect();
    const r = await c.callTool({ name: "manifold_status", arguments: { flowId: "FLOW-1" } });
    assert.equal(r.isError, true);
    assert.match(JSON.parse(r.content[0].text).error, /refused caller identity|unresolved agent and session/);
  });
});
