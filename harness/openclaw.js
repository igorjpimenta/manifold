import os from "node:os";
import path from "node:path";
import { createHarnessPort } from "./Harness.js";
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
    registerHttpRoute(route) {
      if (typeof api?.registerHttpRoute === "function") return api.registerHttpRoute(route);
    },
    registerService(def) {
      if (typeof api?.registerService === "function") return api.registerService(def);
    },
    storePaths() {
      const dataDir = path.join(os.homedir(), ".openclaw");
      return {
        dataDir,
        runtimeDb: process.env.MANIFOLD_RUNTIME_DB || path.join(dataDir, "runtime.sqlite"),
        opsDb: process.env.MANIFOLD_OPS_DB || path.join(dataDir, "ops.sqlite"),
        pendingDir: process.env.MANIFOLD_PENDING_DIR || path.join(dataDir, "discuss-pending"),
        locksDir: process.env.MANIFOLD_LOCKS_DIR || path.join(dataDir, "locks"),
        pipelinesDir: process.env.MANIFOLD_PIPELINES_DIR || path.join(dataDir, "workspace", ".pipelines"),
      };
    },
    sessionKey({ agent, session, flowId, nodeId, itemKey }) {
      if (!agent) throw new Error("manifold identity: unstamped call refused (agent must be stamped by the calling harness)");
      if (session) return session;
      return defaultAgentSessionKey(agent, flowId, nodeId, itemKey);
    },
  });
}
