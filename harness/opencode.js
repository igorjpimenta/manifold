import os from "node:os";
import path from "node:path";
import { createHarnessPort } from "./Harness.js";
import { defaultAgentSessionKey } from "../engine/executor-helpers.js";

export const OPENCODE_SPAWN_UNSUPPORTED =
  "opencode harness: spawn unsupported (no run slot) — read-only/dry-run mode";

export function createOpencodeHarness({ id = "opencode", dataDir, emit, sessionKeyPrefix = "agent" } = {}) {
  void sessionKeyPrefix;
  const events = [];
  const tools = new Map();
  const hooks = [];
  const emitFn = typeof emit === "function" ? emit : null;

  return createHarnessPort({
    id,
    getSubagent() {
      return null;
    },
    async spawn() {
      throw new Error(OPENCODE_SPAWN_UNSUPPORTED);
    },
    async waitForRun() {
      return { status: "unsupported" };
    },
    async getSessionMessages() {
      return { messages: [] };
    },
    emit(evt) {
      events.push(evt);
      emitFn?.(evt);
    },
    registerTool(name, def) {
      tools.set(name, def);
    },
    registerHook(kind, fn) {
      hooks.push([kind, fn]);
    },
    storePaths() {
      const root = dataDir ?? path.join(os.homedir(), ".manifold");
      return {
        dataDir: root,
        runtimeDb: process.env.MANIFOLD_RUNTIME_DB || path.join(root, "runtime.sqlite"),
        opsDb: process.env.MANIFOLD_OPS_DB || path.join(root, "ops.sqlite"),
      };
    },
    sessionKey({ agent, session, flowId, nodeId, itemKey }) {
      if (!agent) throw new Error("manifold identity: unstamped call refused (agent must be stamped by the calling harness)");
      if (session) return session;
      return defaultAgentSessionKey(agent, flowId, nodeId, itemKey);
    },
  });
}
