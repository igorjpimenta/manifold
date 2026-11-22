// serve.js — `manifold serve`: the core application as its own process.
//
// Everything harness-specific arrives through the host this file builds:
//   - agent turns go to MANIFOLD_TURN_URL (the harness adapter's endpoint)
//   - discuss session ids are minted locally (no harness to ask)
//   - elevation loops back to this process's own API port
//
// Loopback only by default; the routes authenticate with MANIFOLD_BEARER.

import http from "node:http";
import { createRouteRegistry } from "./registry.js";
import { createManifoldApp, DEFAULT_DASHBOARD_HOST, DEFAULT_DASHBOARD_PORT } from "./app.js";
import { createPipelineDirsStore } from "../state/pipeline-dirs.js";
import { resolveStorePaths } from "../state/paths.js";

export const DEFAULT_API_PORT = 44301;
export const DEFAULT_API_HOST = "127.0.0.1";

function createLogger(prefix) {
  const line = (level, msg) => {
    const text = `[${prefix}] ${msg}`;
    if (level === "info") console.log(text);
    else console.error(text);
  };
  return {
    info: (msg) => line("info", msg),
    warn: (msg) => line("warn", msg),
    error: (msg) => line("error", msg),
    debug: (msg) => line("debug", msg),
  };
}

// Deterministic discuss thread ids — core names the thread, the harness would
// have minted it. Same flow/gate/item always maps to the same thread.
export function localSessions() {
  return {
    newDiscussSessionId({ agentId, flowId, gateNodeId, itemId }) {
      return `manifold:discuss:${flowId}:${gateNodeId ?? "flow"}:${itemId ?? "0"}:${agentId ?? "agent"}`;
    },
    // No harness model table: the caller's modelOverride wins, else default.
    async resolveAgentEffectiveModelPrimary() { return null; },
    isProviderCooldown() { return false; },
  };
}

export function createHttpRunTurn({ url, token, timeoutMs = 600_000, logger, fetchImpl = globalThis.fetch }) {
  return async (args) => {
    if (!url) {
      throw new Error("manifold: agent turn requested but MANIFOLD_TURN_URL is unset (agent nodes need a harness)");
    }
    const res = await fetchImpl(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok || payload?.ok === false) {
      const detail = payload?.error ?? `HTTP ${res.status}`;
      logger?.warn?.(`manifold: agent turn failed: ${detail}`);
      throw new Error(`manifold: agent turn failed: ${detail}`);
    }
    return { text: payload.text ?? "", runId: payload.runId ?? null, usage: payload.usage ?? null };
  };
}

// Elevation loopback against this process: the `_elev` handlers are mounted on
// the same server, so the operator-scope crossing the gateway needed collapses
// to a localhost call carrying the shared bearer.
export function createLocalElevation({ port, host, token, fetchImpl = globalThis.fetch }) {
  return async function invokeElevated(path, payload, opts = {}) {
    const timeoutMs = opts.timeoutMs ?? 30_000;
    if (!token) {
      return { ok: false, status: 503, body: JSON.stringify({ ok: false, code: "no_bearer", error: "MANIFOLD_BEARER unset" }), error: "MANIFOLD_BEARER unset" };
    }
    try {
      const res = await fetchImpl(`http://${host}:${port}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
        body: JSON.stringify(payload ?? {}),
        signal: AbortSignal.timeout(timeoutMs),
      });
      return { ok: res.ok, status: res.status, body: await res.text() };
    } catch (err) {
      return { ok: false, status: 0, body: "", error: err?.message ?? String(err) };
    }
  };
}

export function resolveServeConfig({ env = process.env } = {}) {
  const storePaths = resolveStorePaths({ env });
  const pipelineDirsStore = createPipelineDirsStore({ dbPath: storePaths.runtimeDb, logger: createLogger("manifold") });
  let pipelinesDir = env.MANIFOLD_PIPELINES_DIR ?? null;
  if (!pipelinesDir) {
    pipelinesDir = pipelineDirsStore.resolveDir({
      harness: env.MANIFOLD_HARNESS ?? null,
      env,
      dataDir: storePaths.dataDir,
      configDir: null,
    }).dir;
  }
  return {
    storePaths,
    config: {
      pipelinesDir,
      ownerSessionKey: env.MANIFOLD_OWNER_SESSION_KEY || undefined,
      discussDefaultAgent: env.MANIFOLD_DISCUSS_DEFAULT_AGENT || null,
      dashboardEnabled: env.MANIFOLD_DASHBOARD_ENABLED !== "false",
      dashboardPort: Number(env.MANIFOLD_DASHBOARD_PORT) || DEFAULT_DASHBOARD_PORT,
      dashboardHost: env.MANIFOLD_DASHBOARD_HOST || DEFAULT_DASHBOARD_HOST,
      globalConcurrency: Number(env.MANIFOLD_GLOBAL_CONCURRENCY) || undefined,
    },
    apiPort: Number(env.MANIFOLD_API_PORT) || DEFAULT_API_PORT,
    apiHost: env.MANIFOLD_API_HOST || DEFAULT_API_HOST,
    turnUrl: env.MANIFOLD_TURN_URL || null,
    turnToken: env.MANIFOLD_TURN_TOKEN || env.MANIFOLD_BEARER || null,
    bearer: env.MANIFOLD_BEARER || null,
  };
}

export async function serve({ env = process.env, logger: injectedLogger = null } = {}) {
  const cfg = resolveServeConfig({ env });
  const logger = injectedLogger ?? createLogger("manifold");

  const registry = createRouteRegistry({ logger });
  const host = {
    logger,
    registerHttpRoute: registry.registerHttpRoute,
    runTurn: createHttpRunTurn({ url: cfg.turnUrl, token: cfg.turnToken, logger }),
    invokeElevated: createLocalElevation({ port: cfg.apiPort, host: cfg.apiHost, token: cfg.bearer }),
    sessions: localSessions(),
    getBroadcast: () => null,
  };

  const app = createManifoldApp({ host, config: cfg.config });
  const server = http.createServer((req, res) => registry.handle(req, res));

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(cfg.apiPort, cfg.apiHost, resolve);
  });
  logger.info(`manifold serve: api on http://${cfg.apiHost}:${cfg.apiPort} (${registry.routes.length} route(s))`);

  await app.start({ logger });
  logger.info("manifold serve: ready");

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    logger.info(`manifold serve: ${signal}, stopping`);
    await app.stop({ logger }).catch(() => {});
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  };
  process.once("SIGINT", () => { shutdown("SIGINT"); });
  process.once("SIGTERM", () => { shutdown("SIGTERM"); });

  return { app, registry, server, cfg, shutdown };
}
