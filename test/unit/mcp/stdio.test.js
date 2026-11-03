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
    "manifold_approve", "manifold_cancel", "manifold_deliver_result", "manifold_runs", "manifold_start", "manifold_status",
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
