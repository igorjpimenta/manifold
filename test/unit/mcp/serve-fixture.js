// serve-fixture.js — stdio MCP server with stub ops, spawned by stdio.test.js
// over a real transport. Not a test itself (no .test.js assertions run here).
import { startMcpServer } from "../../../mcp/stdio.js";

const calls = [];
const pull = { ready: [{ ticket: "T-1", flowId: "FLOW-9", nodeId: "work" }], claims: new Map() };
const ops = {
  async startRun({ pipelineId, args }) {
    calls.push(["startRun", pipelineId]);
    return { flowId: "FLOW-1", pipelineId, status: "running", revision: 0, args };
  },
  async runStatus({ flowId }) {
    calls.push(["runStatus", flowId]);
    return flowId === "FLOW-1" ? { flowId, pipelineId: "demo", status: "running", revision: 3 } : null;
  },
  async listRuns(q) {
    calls.push(["listRuns", q]);
    return [{ flowId: "FLOW-1", pipelineId: "demo", status: "running" }];
  },
  async approveGate({ flowId, itemId, decision }) {
    calls.push(["approveGate", flowId, itemId, decision]);
    return { ok: true, flowId, itemId, decision };
  },
  async cancelFlow({ flowId }) {
    calls.push(["cancelFlow", flowId]);
    return flowId === "FLOW-1" ? { cancelled: true, flowId } : { cancelled: false, reason: "not found" };
  },
  // Single-ticket pull stub: one ready ticket per process. Each stdio test
  // spawns a fresh fixture, so every test starts with T-1 ready.
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
  logger: { info() {}, warn() {}, error() {} },
};

await startMcpServer({ ops, logger: { info() {}, warn() {}, error() {} } });
