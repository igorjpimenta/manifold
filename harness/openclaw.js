import os from "node:os";
import path from "node:path";
import { createHarnessPort } from "./Harness.js";
import { formatIdentity, stampCallMeta } from "./identity.js";
import { defaultAgentSessionKey } from "../engine/executor-helpers.js";
import { GatewayHandoffNeeded } from "../engine/executor-errors.js";
import { createGatewaySubagentGate } from "../engine/gateway-subagent.js";

export function createOpenClawHarness({ api, getWorkingSubagent, whenGatewayReady, sessionIndex, id = "openclaw", emit } = {}) {
  void sessionIndex;
  const gate = createGatewaySubagentGate({ whenGatewayReady });
  const tools = new Map();
  const hooks = [];
  const emitFn = typeof emit === "function" ? emit : () => {};

  function resolveSubagent() {
    return getWorkingSubagent?.() ?? api?.runtime?.subagent;
  }

  function requireSubagent() {
    const subagent = resolveSubagent();
    if (!subagent?.run) throw new GatewayHandoffNeeded("subagent runtime unavailable on this api instance");
    return subagent;
  }

  return createHarnessPort({
    id,
    getSubagent() {
      return gate(resolveSubagent());
    },
    async spawn(args) {
      return requireSubagent().run(args);
    },
    async waitForRun(args) {
      return requireSubagent().waitForRun(args);
    },
    async getSessionMessages(args) {
      return requireSubagent().getSessionMessages(args);
    },
    emit(evt) {
      emitFn(evt);
    },
    registerTool(name, def) {
      if (typeof api?.registerTool === "function") return api.registerTool(def ?? name);
      tools.set(name, def);
    },
    registerHook(kind, fn) {
      if (typeof api?.on === "function") return api.on(kind, fn);
      hooks.push([kind, fn]);
    },
    storePaths() {
      const dataDir = path.join(os.homedir(), ".openclaw");
      return {
        dataDir,
        runtimeDb: process.env.MANIFOLD_RUNTIME_DB || path.join(dataDir, "runtime.sqlite"),
        opsDb: process.env.MANIFOLD_OPS_DB || path.join(dataDir, "ops.sqlite"),
      };
    },
    sessionKey({ agent, session, flowId, nodeId, itemKey }) {
      const seed = defaultAgentSessionKey(agent, flowId, nodeId, itemKey);
      return formatIdentity(stampCallMeta({}, { harness: id, agent, session: session ?? seed }));
    },
  });
}
