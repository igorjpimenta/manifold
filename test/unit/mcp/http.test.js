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

const pull = { ready: [{ ticket: "T-1", flowId: "FLOW-9", nodeId: "work" }], claims: new Map() };
function resetPull() {
  pull.ready = [{ ticket: "T-1", flowId: "FLOW-9", nodeId: "work" }];
  pull.claims = new Map();
}

const ops = {
  async claimNext({ harness, agent, session }) {
    const next = pull.ready[0] ?? null;
    if (!next) return null;
    pull.ready.shift();
    const leaseExpiresAt = Date.now() + 60000;
    pull.claims.set(next.ticket, { ...next, harness, agent, session, leaseExpiresAt });
    return { ...next, claimedAt: Date.now(), leaseExpiresAt };
  },
  async renewClaim({ ticket, harness, agent, session }) {
    const c = pull.claims.get(ticket);
    if (!c || c.harness !== harness || c.agent !== agent || c.session !== session) {
      throw new Error(`pull-claim: renewClaim refused — claimant binding mismatch on ticket ${ticket}`);
    }
    c.leaseExpiresAt += 60000;
    return { ok: true, ticket, leaseExpiresAt: c.leaseExpiresAt };
  },
  async reportClaim({ ticket, harness, agent, session, result }) {
    const c = pull.claims.get(ticket);
    if (!c || c.harness !== harness || c.agent !== agent || c.session !== session) {
      throw new Error(`pull-claim: reportClaim refused — claimant binding mismatch on ticket ${ticket}`);
    }
    pull.claims.delete(ticket);
    return { ok: true, ticket, flowId: c.flowId, nodeId: c.nodeId, result };
  },
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
  test("lists the same 8 tools as stdio", async () => {
    const c = await connect();
    const { tools } = await c.listTools();
    assert.deepEqual(tools.map((t) => t.name).sort(), [
      "manifold_approve",
      "manifold_cancel",
      "manifold_claim",
      "manifold_heartbeat",
      "manifold_report",
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

const stamp = (agent, session) => ({ _meta: { manifold: { agent, session } } });

describe("mcp/http.js — pull claims over the wire", () => {
  test("claim → report round trip; empty hands after", async () => {
    resetPull();
    const c = await connect();
    const claimed = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
    assert.equal(claimed.isError ?? false, false);
    const won = JSON.parse(claimed.content[0].text);
    assert.equal(won.ticket, "T-1");
    const reported = await c.callTool({
      name: "manifold_report", arguments: { ticket: won.ticket, result: { ok: true } }, ...stamp("a1", "s1"),
    });
    assert.equal(reported.isError ?? false, false);
    assert.equal(JSON.parse(reported.content[0].text).ticket, "T-1");
    const empty = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
    assert.equal(empty.isError ?? false, false);
    assert.equal(empty.content[0].text, "manifold_claim: no work ready");
  });

  test("report by another caller is refused loudly", async () => {
    resetPull();
    const c = await connect();
    const claimed = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
    const ticket = JSON.parse(claimed.content[0].text).ticket;
    const r = await c.callTool({
      name: "manifold_report", arguments: { ticket, result: { ok: true } }, ...stamp("a2", "s1"),
    });
    assert.match(r.content[0].text, /refused.*binding mismatch/);
  });

  test("heartbeat extends the lease", async () => {
    resetPull();
    const c = await connect();
    const claimed = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
    const first = JSON.parse(claimed.content[0].text);
    const beat = await c.callTool({
      name: "manifold_heartbeat", arguments: { ticket: first.ticket }, ...stamp("a1", "s1"),
    });
    assert.equal(beat.isError ?? false, false);
    assert.ok(JSON.parse(beat.content[0].text).leaseExpiresAt > first.leaseExpiresAt);
  });

  test("unstamped claim refused over HTTP", async () => {
    const c = await connect();
    const r = await c.callTool({ name: "manifold_claim", arguments: {} });
    assert.equal(r.isError, true);
    assert.match(JSON.parse(r.content[0].text).error, /refused caller identity|unresolved agent and session/);
  });
});
