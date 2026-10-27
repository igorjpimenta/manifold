// stdio.test.js — MCP round-trip over a real stdio transport: declared
// harness ok (stamped call invokes the injected op stub), unstamped calls
// refused loudly, undeclared server fails fast.
import { test, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const fixture = path.join(import.meta.dirname, "serve-fixture.js");
let client = null;

afterEach(async () => {
  if (client) {
    try { await client.close(); } catch {}
    client = null;
  }
});

async function connect(env = {}) {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fixture],
    env: { ...process.env, MANIFOLD_HARNESS: "test-harness", ...env },
    stderr: "pipe",
  });
  const c = new Client({ name: "test-client", version: "0.0.0" });
  await c.connect(transport);
  client = c;
  return c;
}

test("declared harness ok: tools listed, stamped call invokes op stub", async () => {
  const c = await connect();
  const { tools } = await c.listTools();
  assert.deepEqual(tools.map((t) => t.name).sort(), [
    "manifold_approve", "manifold_cancel", "manifold_claim", "manifold_heartbeat",
    "manifold_report", "manifold_runs", "manifold_start", "manifold_status",
  ]);
  const r = await c.callTool(
    { name: "manifold_runs", arguments: {}, _meta: { manifold: { agent: "a1", session: "s1" } } }
  );
  assert.equal(r.isError ?? false, false);
  const rows = JSON.parse(r.content[0].text);
  assert.equal(rows[0].flowId, "FLOW-1");
});

test("unstamped call refused loudly (isError, no default identity)", async () => {
  const c = await connect();
  const r = await c.callTool({ name: "manifold_status", arguments: { flowId: "FLOW-1" } });
  assert.equal(r.isError, true);
  assert.match(JSON.parse(r.content[0].text).error, /refused caller identity/);
});

test("half-stamped call refused (pairs never splice)", async () => {
  const c = await connect();
  const r = await c.callTool({
    name: "manifold_status",
    arguments: { flowId: "FLOW-1", manifold_agent: "a1" },
  });
  assert.equal(r.isError, true);
  assert.match(JSON.parse(r.content[0].text).error, /refused caller identity/);
});

test("stamped args channel works (manifold_agent/manifold_session)", async () => {
  const c = await connect();
  const r = await c.callTool({
    name: "manifold_status",
    arguments: { flowId: "FLOW-1", manifold_agent: "a1", manifold_session: "s1" },
  });
  assert.equal(r.isError ?? false, false);
  assert.equal(JSON.parse(r.content[0].text).flowId, "FLOW-1");
});

test("undeclared server fails fast without MANIFOLD_HARNESS", async () => {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fixture],
    env: { ...process.env, MANIFOLD_HARNESS: "" },
    stderr: "pipe",
  });
  const c = new Client({ name: "test-client", version: "0.0.0" });
  await assert.rejects(() => c.connect(transport), /missing: harness|closed|terminated/i);
  try { await c.close(); } catch {}
});

const stamp = (agent, session) => ({ _meta: { manifold: { agent, session } } });

test("claim → report round trip advances; second claim is empty hands, not an error", async () => {
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
  const c = await connect();
  const claimed = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
  const ticket = JSON.parse(claimed.content[0].text).ticket;
  const r = await c.callTool({
    name: "manifold_report", arguments: { ticket, result: { ok: true } }, ...stamp("a2", "s1"),
  });
  assert.equal(r.isError ?? false, false);
  assert.match(r.content[0].text, /refused.*binding mismatch/);
});

test("heartbeat extends the lease; foreign heartbeat refused", async () => {
  const c = await connect();
  const claimed = await c.callTool({ name: "manifold_claim", arguments: {}, ...stamp("a1", "s1") });
  const first = JSON.parse(claimed.content[0].text);
  const beat = await c.callTool({
    name: "manifold_heartbeat", arguments: { ticket: first.ticket }, ...stamp("a1", "s1"),
  });
  assert.equal(beat.isError ?? false, false);
  assert.ok(JSON.parse(beat.content[0].text).leaseExpiresAt > first.leaseExpiresAt);
  const alien = await c.callTool({
    name: "manifold_heartbeat", arguments: { ticket: first.ticket }, ...stamp("a2", "s1"),
  });
  assert.match(alien.content[0].text, /refused.*binding mismatch/);
});

test("unstamped claim refused loudly (no default identity)", async () => {
  const c = await connect();
  const r = await c.callTool({ name: "manifold_claim", arguments: {} });
  assert.equal(r.isError, true);
  assert.match(JSON.parse(r.content[0].text).error, /refused caller identity/);
});
