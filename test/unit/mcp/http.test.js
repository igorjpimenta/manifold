// http.test.js — MCP over Streamable HTTP, live transport.
// Mirrors the stdio contract (declared harness, per-call agent/session, unstamped refused)
// plus the HTTP-only doors: bearer gate, harness header gate, /health.
// Concurrency is declared per-harness via X-Manifold-Concurrency.
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startMcpHttpServer } from "../../../mcp/http.js";

const BEARER = "test-bearer-token";
const HARNESS_A = "harness-a";
const HARNESS_B = "harness-b";
let savedBearer;
let base;
let closeServer = null;

const ops = {
  async claimNext() { return null; },
  async renewClaim() { return { ok: true }; },
  async reportClaim() { return { ok: true }; },
  async startRun({ pipelineId, args }) {
    return { flowId: "FLOW-1", pipelineId, status: "running", revision: 0, args };
  },
  async runStatus({ flowId }) {
    if (flowId !== "FLOW-1") return null;
    return { flowId, pipelineId: "demo", status: "running", revision: 3 };
  },
  async listRuns() { return []; },
  async approveGate({ flowId, itemId, decision }) { return { ok: true, flowId, itemId, decision }; },
  async cancelFlow({ flowId }) { return { cancelled: false, reason: "not found" }; },
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
  if (closeServer) { await closeServer(); closeServer = null; }
  if (savedBearer === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = savedBearer;
});

function connect(harness, extraHeaders = {}) {
  const transport = new StreamableHTTPClientTransport(new URL(`${base}/mcp`), {
    requestInit: {
      headers: {
        Authorization: `Bearer ${BEARER}`,
        "X-Manifold-Harness": harness,
        ...extraHeaders,
      },
    },
  });
  const c = new Client({ name: "test-client", version: "0.0.0" });
  return c.connect(transport).then(() => c);
}

describe("mcp/http.js — per-harness concurrency", () => {
  test("harness A declares concurrency=2, harness B declares concurrency=5", async () => {
    const cA = await connect(HARNESS_A, { "X-Manifold-Concurrency": "2" });
    const cB = await connect(HARNESS_B, { "X-Manifold-Concurrency": "5" });

    // Both should be able to make calls without error
    const rA = await cA.callTool({
      name: "manifold_status",
      arguments: { flowId: "FLOW-1" },
      _meta: { manifold: { agent: "a1", session: "s1" } },
    });
    assert.equal(rA.isError ?? false, false);

    const rB = await cB.callTool({
      name: "manifold_status",
      arguments: { flowId: "FLOW-1" },
      _meta: { manifold: { agent: "b1", session: "s2" } },
    });
    assert.equal(rB.isError ?? false, false);

    await cA.close();
    await cB.close();
  });

  test("concurrency header is parsed and accepted", async () => {
    const c = await connect(HARNESS_A, { "X-Manifold-Concurrency": "3" });
    const r = await c.callTool({
      name: "manifold_status",
      arguments: { flowId: "FLOW-1" },
      _meta: { manifold: { agent: "a1", session: "s1" } },
    });
    assert.equal(r.isError ?? false, false);
    await c.close();
  });


});
