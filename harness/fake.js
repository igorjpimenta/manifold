import { createHarnessPort } from "./Harness.js";
import { formatIdentity, stampCallMeta } from "./identity.js";

export function createFakeHarness({ id = "fake", dataDir = "/tmp/manifold-fake" } = {}) {
  const runs = new Map();
  const events = [];
  const tools = new Map();
  const hooks = [];
  let seq = 0;
  const harness = createHarnessPort({
    id,
    getSubagent() {
      return {
        run: (args) => harness.spawn(args),
        waitForRun: (args) => harness.waitForRun(args),
        getSessionMessages: (args) => harness.getSessionMessages(args),
      };
    },
    async spawn({ sessionKey, message, skill, agentId }) {
      seq += 1;
      const runId = `fake-run-${seq}`;
      runs.set(runId, { runId, sessionKey, message, skill, agentId, status: "ok" });
      return { runId };
    },
    async waitForRun({ runId }) {
      const run = runs.get(runId);
      if (!run) return { status: "not-found" };
      return { status: "ok", runId };
    },
    async getSessionMessages({ sessionKey, limit = 5 } = {}) {
      const msgs = [...runs.values()]
        .filter((r) => r.sessionKey === sessionKey)
        .slice(-limit)
        .map((r) => ({ role: "assistant", content: r.message ?? "" }));
      return { messages: msgs };
    },
    emit(evt) {
      events.push(evt);
    },
    registerTool(name, def) {
      tools.set(name, def);
    },
    registerHook(kind, fn) {
      hooks.push([kind, fn]);
    },
    storePaths() {
      return { dataDir, runtimeDb: `${dataDir}/runtime.sqlite`, opsDb: `${dataDir}/ops.sqlite` };
    },
    sessionKey({ agent, session, flowId, nodeId, itemKey }) {
      const seed = itemKey ? `${String(nodeId).toLowerCase()}-${itemKey}` : String(nodeId).toLowerCase();
      const stamped = stampCallMeta({}, { harness: id, agent, session: session ?? `${String(flowId).slice(0, 8)}-${seed}` });
      return formatIdentity(stamped);
    },
  });
  return { harness, runs, events, tools, hooks };
}
