import os from "node:os";
import path from "node:path";
import { createHarnessPort } from "./Harness.js";
import { notifyPush, pushPayload } from "./push.js";
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
      if (!agent) throw new Error("manifold identity: unstamped call refused (agent must be stamped by the calling harness)");
      if (session) return session;
      return defaultAgentSessionKey(agent, flowId, nodeId, itemKey);
    },
  });
}

// Self-registers this harness's push target at boot, mirroring winch's
// plugins/openclaw autoregister: idempotent upsert + verifying test ping.
// Disarmed without a bearer or base URL. Never throws — boot must not fail
// because a push target couldn't register. runner is injectable
// (url, init) => Response; defaults to global fetch via notifyPush.
export async function autoregisterPushTarget({ pushTargets, baseUrl, harness = "openclaw", bearer, timeoutMs, runner, logger } = {}) {
  const resolvedBearer = bearer ?? process.env.MANIFOLD_PUSH_BEARER ?? "";
  const resolvedBase = baseUrl ?? process.env.MANIFOLD_PUSH_BASE_URL ?? null;
  if (!resolvedBearer) return { ok: false, reason: "disarmed" };
  if (typeof resolvedBase !== "string" || !/^https?:\/\//i.test(resolvedBase.trim())) {
    return { ok: false, reason: "no-base-url" };
  }
  const url = `${resolvedBase.trim().replace(/\/$/, "")}/api/manifold/push`;
  try {
    await pushTargets?.upsert?.(harness, { url, bearer: resolvedBearer, timeoutMs: timeoutMs ?? null });
  } catch (err) {
    logger?.warn?.(`manifold: push self-registration failed for harness ${harness}: ${err?.message ?? err}`);
    return { ok: false, reason: "upsert-failed" };
  }
  const verify = await notifyPush(
    { url, bearer: resolvedBearer, timeoutMs },
    pushPayload({ event: "push.test", harness }),
    logger,
    { runner },
  );
  if (!verify.ok) {
    logger?.warn?.(`manifold: push target registered for harness ${harness} -> ${url} but test ping failed (${verify.reason})`);
    return { ok: false, reason: "test-failed" };
  }
  logger?.info?.(`manifold: push target self-registered for harness ${harness} -> ${url}`);
  return { ok: true };
}
