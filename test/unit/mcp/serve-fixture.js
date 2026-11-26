import { startMcpServer } from "../../../mcp/stdio.js";

const calls = [];
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
  logger: { info() {}, warn() {}, error() {} },
};

await startMcpServer({ ops, logger: { info() {}, warn() {}, error() {} } });
