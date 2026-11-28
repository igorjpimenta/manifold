// Openclaw adapter for the manifold pipeline engine. The engine runs as its
// own process (`manifold serve`, spawned here); this adapter holds the only
// harness-specific pieces: supervising the core process, POST
// /api/manifold/turn (the core's agent-turn endpoint — the gateway's agent
// runtime never leaves this process), POST /api/manifold/session (mints
// dispatch session ids — ids are harness-owned), and stamping manifold
// session metadata on openclaw sessions. Nothing in core imports this file;
// this file imports core only through the published package.

import crypto from "node:crypto";
import path from "node:path";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { wrapSubagentKeys } from "./session-keys.js";
import { newNodeSessionId } from "./sessions.js";
import { invokeElevated } from "@igorjpimenta/manifold/elevation";
import { createRunTurn } from "./run-turn.js";
import { createToolResults } from "./tool-results.js";
import { registerManifoldMetaHook } from "./meta.js";
import { registerTelemetryHooks } from "./telemetry.js";
import { registerGatewayReadiness } from "./gateway-readiness.js";

const DEFAULT_CORE_API_PORT = 44301;
const DEFAULT_DASHBOARD_PORT = 44300;
const DEFAULT_DASHBOARD_HOST = "127.0.0.1";
const DEFAULT_GATEWAY_PORT = 18789;
const TURN_PATH = "/api/manifold/turn";
const ELEV_TURN_PATH = "/api/manifold/_elev/turn";
// A turn ends when its own run ends, not on socket silence: the elevation
// loopback's timeout is an idle timer, so it must outlast any single turn.
const ELEV_TURN_TIMEOUT_MS = 3_600_000;
const SESSION_PATH = "/api/manifold/session";
const MAX_TURN_BODY_BYTES = 1024 * 1024;
const RESTART_BACKOFF_MS = [2000, 5000, 15000];

const require = createRequire(import.meta.url);

// The elevated sibling answers with its own JSON error envelope; reuse its
// message rather than wrapping it again.
function parseErrorOf(raw) {
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed?.error === "string" ? parsed.error : null;
  } catch {
    return null;
  }
}

// Missing coordinates are refused rather than defaulted: the id labels a
// real transcript.
export function sessionIdFor(body) {
  const { agentId, flowId, nodeId } = body ?? {};
  if (!agentId || !flowId || !nodeId) return { error: "agentId, flowId and nodeId are required" };
  return { session: newNodeSessionId({ agentId, flowId, nodeId }) };
}

function resolveCoreEntry() {
  try {
    const pkg = require.resolve("@igorjpimenta/manifold/package.json");
    return path.join(path.dirname(pkg), "bin", "manifold.js");
  } catch {
    // Repo checkout: the core sits three levels up from this package.
    return fileURLToPath(new URL("../../../bin/manifold.js", import.meta.url));
  }
}

// The environment the core process inherits. Everything manifold-specific
// travels as MANIFOLD_* so the child needs no config file of its own; the
// rest of the environment passes through untouched (PATH, HOME, the store
// home env vars an operator set).
export function buildCoreEnv({
  userCfg = {}, coreApiPort, turnUrl, turnToken, sessionUrl, dashboardPort, dashboardHost, baseEnv = process.env,
}) {
  const env = {
    ...baseEnv,
    MANIFOLD_HARNESS: "openclaw",
    MANIFOLD_API_PORT: String(coreApiPort),
    MANIFOLD_TURN_URL: turnUrl,
    MANIFOLD_SESSION_URL: sessionUrl,
    MANIFOLD_TURN_TOKEN: turnToken,
    MANIFOLD_INGEST_TOKEN: turnToken,
    MANIFOLD_DASHBOARD_ENABLED: userCfg.dashboardEnabled === false ? "false" : "true",
    MANIFOLD_DASHBOARD_PORT: String(dashboardPort),
    MANIFOLD_DASHBOARD_HOST: dashboardHost,
  };
  if (userCfg.pipelinesDir) env.MANIFOLD_PIPELINES_DIR = userCfg.pipelinesDir;
  if (userCfg.ownerSessionKey) env.MANIFOLD_OWNER_SESSION_KEY = userCfg.ownerSessionKey;
  if (userCfg.discuss?.defaultAgent) env.MANIFOLD_DISCUSS_DEFAULT_AGENT = userCfg.discuss.defaultAgent;
  if (userCfg.globalConcurrency) env.MANIFOLD_GLOBAL_CONCURRENCY = String(userCfg.globalConcurrency);
  return env;
}

function bearerMatches(header, expected) {
  if (!expected) return false;
  const given = String(header ?? "");
  if (!given.startsWith("Bearer ")) return false;
  const a = Buffer.from(given.slice(7), "utf8");
  const b = Buffer.from(expected, "utf8");
  if (a.length !== b.length) { crypto.timingSafeEqual(a, a); return false; }
  return crypto.timingSafeEqual(a, b);
}

function readJsonBody(req) {
  return (async () => {
    let size = 0;
    const chunks = [];
    for await (const chunk of req) {
      size += chunk.length;
      if (size > MAX_TURN_BODY_BYTES) return { error: "body too large", status: 413 };
      chunks.push(chunk);
    }
    if (!chunks.length) return { value: {} };
    try {
      return { value: JSON.parse(Buffer.concat(chunks).toString("utf8")) };
    } catch {
      return { error: "invalid json body", status: 400 };
    }
  })();
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(body));
}

export default definePluginEntry({
  id: "manifold",
  name: "Manifold Pipeline Engine",
  description:
    "LangGraph-style pipeline executor. The engine runs as its own process (`manifold serve`) with its own MCP server and HTTP API; this plugin starts it, supervises it, and bridges agent turns back into the gateway.",
  configSchema: () => ({
    type: "object",
    additionalProperties: false,
    properties: {
      pipelinesDir: {
        type: "string",
        description: "Directory to scan for pipeline definition files. Default: <dataDir>/pipelines (MANIFOLD_DATA_DIR or ~/.manifold).",
      },
      ownerSessionKey: {
        type: "string",
        description: "Session key used to scope flow_run ownership. Default: agent:main:explicit:manifold-owner",
      },
      dashboardEnabled: {
        type: "boolean",
        description: "Whether the core's read-only dashboard service should boot. Default: true.",
      },
      dashboardPort: {
        type: "integer",
        minimum: 1024,
        maximum: 65535,
        description: `Port the dashboard listens on. Default: ${DEFAULT_DASHBOARD_PORT}.`,
      },
      dashboardHost: {
        type: "string",
        description: "Host/interface to bind. Default: 127.0.0.1 (loopback only).",
      },
      apiPort: {
        type: "integer",
        minimum: 1024,
        maximum: 65535,
        description: `Port the core HTTP API listens on. Default: ${DEFAULT_CORE_API_PORT}.`,
      },
      coreBinary: {
        type: "string",
        description: "Override the manifold CLI entry the plugin spawns.",
      },
      turnUrl: {
        type: "string",
        description: "Override the URL the core posts agent turns to (defaults to this gateway's /api/manifold/turn).",
      },
      discuss: {
        type: "object",
        additionalProperties: false,
        properties: {
          defaultAgent: {
            type: "string",
            description: "Default agent for gate discuss sessions when the gate declares none.",
          },
        },
      },
      globalConcurrency: {
        type: "integer",
        minimum: 1,
        description: "Shared ceiling for every flow-run concurrency mechanism. Default: 8.",
      },
    },
  }),
  register: (api) => {
    const userCfg = api.pluginConfig ?? {};
    const coreApiPort = Number.isInteger(userCfg.apiPort) ? userCfg.apiPort : DEFAULT_CORE_API_PORT;
    const dashboardPort = Number.isInteger(userCfg.dashboardPort) ? userCfg.dashboardPort : DEFAULT_DASHBOARD_PORT;
    const dashboardHost = userCfg.dashboardHost || DEFAULT_DASHBOARD_HOST;
    const coreEntry = userCfg.coreBinary || resolveCoreEntry();

    // Per-gateway-boot secret for the turn bridge: only the child process
    // learns it, and the route accepts nothing else.
    const turnToken = crypto.randomBytes(32).toString("hex");
    const gatewayPort = Number(process.env.OPENCLAW_GATEWAY_PORT) || DEFAULT_GATEWAY_PORT;
    const turnUrl = userCfg.turnUrl || `http://127.0.0.1:${gatewayPort}${TURN_PATH}`;
    const sessionUrl = `http://127.0.0.1:${gatewayPort}${SESSION_PATH}`;

    registerManifoldMetaHook(api);
    registerTelemetryHooks(api, {
      ingestUrl: `http://127.0.0.1:${coreApiPort}/manifold/telemetry/event`,
      token: turnToken,
      logger: api.logger,
    });

    const gatewayReadiness = registerGatewayReadiness(api, { logger: api.logger });
    const moduleState = { workingSubagent: null };
    // The turn buffer lives with the turns it serves: a tool posts here, the
    // turn reads it, and it dies with this process like the session does. The
    // durable copy is the flow state apply() writes afterwards.
    const toolResults = createToolResults();
    // One ingress for anything harness-bound: a tool posts its result to the
    // turn URL, so tools need the coordinate the core already has.
    process.env.MANIFOLD_TURN_URL = turnUrl;
    process.env.MANIFOLD_TURN_TOKEN = turnToken;
    const { runTurn } = createRunTurn({
      getRuntime: () => moduleState.workingSubagent ?? (api.runtime?.subagent ? wrapSubagentKeys(api.runtime.subagent) : null),
      whenGatewayReady: () => gatewayReadiness.whenReady(),
      logger: api.logger,
      toolResults,
    });

    // Two routes for one turn. The gateway refuses `runtime.subagent.run`
    // outside an operator-scoped request ("missing scope: operator.write"),
    // and an `auth: "plugin"` handler carries no scope at all — so the public
    // route, which the core calls with the per-boot token, loops back through
    // the env-var Bearer to the `auth: "gateway"` sibling, whose handler runs
    // with CLI_DEFAULT_OPERATOR_SCOPES ambient.
    api.registerHttpRoute({
      path: ELEV_TURN_PATH,
      auth: "gateway",
      match: "exact",
      handler: async (req, res) => {
        if (req.method !== "POST") {
          res.setHeader("allow", "POST");
          sendJson(res, 405, { ok: false, error: "method not allowed" });
          return true;
        }
        const body = await readJsonBody(req);
        if (body.error) { sendJson(res, body.status, { ok: false, error: body.error }); return true; }
        try {
          const result = await runTurn(body.value ?? {});
          sendJson(res, 200, { ok: true, text: result.text ?? "", runId: result.runId ?? null, usage: result.usage ?? null });
        } catch (err) {
          api.logger.warn(`manifold: turn failed: ${err?.message ?? err}`);
          sendJson(res, 502, { ok: false, error: err?.message ?? String(err) });
        }
        return true;
      },
    });

    api.registerHttpRoute({
      path: TURN_PATH,
      auth: "plugin",
      match: "exact",
      handler: async (req, res) => {
        if (req.method !== "POST") {
          res.setHeader("allow", "POST");
          sendJson(res, 405, { ok: false, error: "method not allowed" });
          return true;
        }
        if (!bearerMatches(req.headers?.authorization, turnToken)) {
          sendJson(res, 401, { ok: false, error: "unauthorized" });
          return true;
        }
        const body = await readJsonBody(req);
        if (body.error) { sendJson(res, body.status, { ok: false, error: body.error }); return true; }
        // A tool has no channel to the node that spawned it, so it posts its
        // result to the same ingress the core uses, discriminated by `kind`.
        if (body.value?.kind === "tool-result") {
          const stored = toolResults.write(body.value);
          if (!stored) { sendJson(res, 400, { ok: false, error: "session and payload required" }); return true; }
          sendJson(res, 200, { ok: true });
          return true;
        }
        const elevated = await invokeElevated(ELEV_TURN_PATH, body.value ?? {}, { timeoutMs: ELEV_TURN_TIMEOUT_MS });
        if (!elevated.ok) {
          sendJson(res, elevated.status || 502, { ok: false, error: parseErrorOf(elevated.body) ?? "turn failed" });
          return true;
        }
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(elevated.body);
        return true;
      },
    });

    // Dispatch ids belong to this harness (see sessions.js), so the core asks
    // here instead of inventing a shape the gateway may not accept.
    api.registerHttpRoute({
      path: SESSION_PATH,
      auth: "plugin",
      match: "exact",
      handler: async (req, res) => {
        if (req.method !== "POST") {
          res.setHeader("allow", "POST");
          sendJson(res, 405, { ok: false, error: "method not allowed" });
          return true;
        }
        if (!bearerMatches(req.headers?.authorization, turnToken)) {
          sendJson(res, 401, { ok: false, error: "unauthorized" });
          return true;
        }
        const body = await readJsonBody(req);
        if (body.error) { sendJson(res, body.status, { ok: false, error: body.error }); return true; }
        const minted = sessionIdFor(body.value);
        if (minted.error) { sendJson(res, 400, { ok: false, error: minted.error }); return true; }
        sendJson(res, 200, { ok: true, session: minted.session });
        return true;
      },
    });

    let child = null;
    let restartTimer = null;
    let stopping = false;
    let attempt = 0;

    function spawnCore() {
      const env = buildCoreEnv({
        userCfg, coreApiPort, turnUrl, turnToken, sessionUrl, dashboardPort, dashboardHost,
      });

      const proc = spawn(process.execPath, [coreEntry, "serve"], {
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      proc.stdout?.on("data", (d) => api.logger.info(String(d).trimEnd()));
      proc.stderr?.on("data", (d) => api.logger.warn(String(d).trimEnd()));
      proc.on("error", (err) => api.logger.error(`manifold core: spawn failed: ${err?.message ?? err}`));
      proc.on("exit", (code, signal) => {
        if (child === proc) child = null;
        if (stopping) return;
        const delay = RESTART_BACKOFF_MS[Math.min(attempt, RESTART_BACKOFF_MS.length - 1)];
        attempt += 1;
        api.logger.warn(`manifold core: exited (code ${code ?? "null"}, signal ${signal ?? "none"}); restarting in ${delay}ms`);
        restartTimer = setTimeout(() => { spawnCore(); }, delay);
        restartTimer.unref?.();
      });
      child = proc;
      return proc;
    }

    api.registerService({
      id: "manifold-core",
      start: async () => {
        attempt = 0;
        stopping = false;
        const proc = spawnCore();
        // The core binds its API port synchronously on boot; give it a beat
        // so a port conflict surfaces here instead of as a mysterious 502.
        await new Promise((resolve) => {
          const done = () => { proc.off("exit", done); resolve(); };
          proc.on("exit", done);
          setTimeout(done, 1500);
        });
        api.logger.info(`manifold: core spawned (${coreEntry} serve)`);
      },
      stop: async () => {
        stopping = true;
        if (restartTimer) { clearTimeout(restartTimer); restartTimer = null; }
        const proc = child;
        child = null;
        if (!proc || proc.exitCode !== null) return;
        await new Promise((resolve) => {
          const kill = setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} resolve(); }, 5000);
          kill.unref?.();
          proc.once("exit", () => { clearTimeout(kill); resolve(); });
          try { proc.kill("SIGTERM"); } catch { clearTimeout(kill); resolve(); }
        });
      },
    });

    // Capture once: the dispatcher path has no bound subagent runtime.
    api.registerService({
      id: "manifold-runtime-capture",
      start: async () => {
        if (!moduleState.workingSubagent && api.runtime?.subagent) {
          moduleState.workingSubagent = wrapSubagentKeys(api.runtime.subagent);
          api.logger.info("manifold: captured working agent runtime for dispatch use");
        }
      },
      stop: async () => {},
    });
  },
});
