// sidecar/server.js — the manifold sidecar: package-owned process serving
// the full manifold HTTP surface from the package-owned data dir.
//
// Wiring reuse: index.js's register() runs here against a shimmed gateway
// `api` (sidecar/shim.js), so every route module, store, service, and the
// engine boot byte-identical code to the gateway-hosted manifold. The only
// divergences are structural and live in this file:
//   - DB paths redirect via MANIFOLD_RUNTIME_DB/_OPS_DB (state/db.js).
//   - api.runtime.subagent is the remote subagent (sidecar/dispatch.js):
//     agent execution happens in a harness; the engine awaits outcomes.
//   - gateway broadcast is the SSE hub (sidecar/hub.js): harnesses
//     subscribe to /manifold/events and react; nothing calls into the
//     gateway process.
//   - the manifold-gateway-readiness service is skipped (no gateway_start
//     will ever fire here; skipping keeps whenReady() resolving at once).
//   - tool-call hooks (api.on) collect but never fire: correct, since this
//     process serves no tool calls.
// A dead or absent harness degrades dispatch to fast failure; state,
// queries, and events keep serving.
import http from "node:http";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import {
  resolveDataDir,
  runtimeDbPath,
  opsDbPath,
  apiTokenPath,
  ensureMigrated,
} from "./paths.js";
import { createHub } from "./hub.js";
import { createDispatchHub, createRemoteSubagent } from "./dispatch.js";
import { createSidecarApi } from "./shim.js";
import { checkDb } from "../state/db-health.js";
import { writeCorruptionAlert } from "../state/db-guard.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER, OPS_MIGRATIONS_FOLDER } from "../state/migrate.js";

const DEFAULT_PORT = 7431;
const DEFAULT_HOST = "127.0.0.1";
const SKIP_SERVICES = new Set(["manifold-gateway-readiness"]);

function serviceToken(dataDir, logger) {
  const p = apiTokenPath(dataDir);
  if (existsSync(p)) return readFileSync(p, "utf8").trim();
  const token = randomBytes(32).toString("hex");
  writeFileSync(p, token + "\n", { mode: 0o600 });
  logger?.info?.("sidecar: minted service token");
  return token;
}

function readJsonBody(req, limitBytes = 1 << 20) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > limitBytes) {
        reject(new Error("body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("invalid json"));
      }
    });
    req.on("error", reject);
  });
}

function unauthorized(res) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "unauthorized" }));
}

export async function bootSidecar({ env = process.env, logger = console } = {}) {
  const dataDir = resolveDataDir({ env });
  mkdirSync(dataDir, { recursive: true });
  const runtime = runtimeDbPath(dataDir);
  const ops = opsDbPath(dataDir);

  const migration = ensureMigrated({ dataDir, logger });
  if (migration.migrated) logger?.info?.(`sidecar: first boot migrated store from ${migration.from.join(", ")}`);

  // DB paths must be fixed before index.js (or any store module) evaluates
  // its module-level defaults. MANIFOLD_SIDECAR marks harness-only branches.
  env.MANIFOLD_RUNTIME_DB = runtime;
  env.MANIFOLD_OPS_DB = ops;
  env.MANIFOLD_SIDECAR = "1";
  const token = serviceToken(dataDir, logger);
  env.MANIFOLD_API_TOKEN = token;

  // Migrate-first ordering (winch-exact): snapshots before any libsql open,
  // once per file per process.
  migrateDatabase({ dbPath: runtime, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger });
  migrateDatabase({ dbPath: ops, migrationsFolder: OPS_MIGRATIONS_FOLDER, logger });

  // Boot gate: refuse on unreadable primaries, same as the gateway entry.
  for (const dbPath of [runtime, ops]) {
    const check = checkDb(dbPath);
    if (!check.ok) {
      logger?.error?.(`sidecar: refusing boot — unreadable database ${dbPath}: ${check.error}`);
      writeCorruptionAlert({ dbPath, phase: "boot", error: check.error });
      process.exit(1);
    }
  }

  const hub = createHub({ logger });
  const dispatchHub = createDispatchHub({ publish: hub.publish.bind(hub), logger });
  const remoteSubagent = createRemoteSubagent({ dispatchHub, logger });
  const { api, services, routes, cliRegistrations, tools, gatewayMethods, eventHandlers, findRoute } =
    createSidecarApi({
      logger,
      pluginConfig: {
        ...(env.MANIFOLD_PIPELINES_DIR ? { pipelinesDir: env.MANIFOLD_PIPELINES_DIR } : {}),
      },
      runtime: { subagent: remoteSubagent },
    });

  const entry = (await import("../index.js")).default;
  entry.register(api);
  logger?.info?.(
    `sidecar: wired ${routes.length} route(s), ${services.length} service(s), ` +
      `${tools.length} tool(s) collected (harness-side), ` +
      `${cliRegistrations.length} cli registration(s) collected (harness-side)`,
  );
  for (const [event, handlers] of eventHandlers) {
    logger?.info?.(`sidecar: ${handlers.size} hook(s) on ${event} inert (no tool calls in this process)`);
  }

  // Capture the WS broadcaster the ping method stashes: point it at the SSE
  // hub so every canonical emit lands on /manifold/events unchanged.
  const ping = gatewayMethods.get("plugin.manifold.ping");
  if (ping) {
    await ping.handler({ context: { broadcast: hub.broadcast.bind(hub) }, respond: () => {} });
    logger?.info?.("sidecar: broadcast captured to SSE hub");
  } else {
    logger?.warn?.("sidecar: ping method not registered; WS-bound emits will skip");
  }

  // Wire gate lifecycle (canonical open/close payloads) into the hub too —
  // the gateway entry leaves these on a dead broadcast path.
  const { gateEmitter } = await import("../events.js");
  gateEmitter.on("gate.opened", (payload) => hub.broadcast("manifold.gate.opened", payload ?? {}));
  gateEmitter.on("gate.closed", (payload) => hub.broadcast("manifold.gate.closed", payload ?? {}));

  const started = [];
  for (const svc of services) {
    if (SKIP_SERVICES.has(svc.id)) {
      logger?.info?.(`sidecar: skipping gateway-only service ${svc.id}`);
      continue;
    }
    try {
      await svc.start?.({ logger });
      started.push(svc);
      logger?.info?.(`sidecar: service started: ${svc.id}`);
    } catch (err) {
      logger?.error?.(`sidecar: service failed: ${svc.id}: ${err?.message ?? err}`);
    }
  }

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const pathname = url.pathname;

      if (pathname === "/manifold/events" && req.method === "GET") {
        if (req.headers.authorization !== `Bearer ${token}`) return unauthorized(res);
        res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
        const unsub = hub.subscribe("events", res);
        req.on("close", unsub);
        return;
      }
      if (pathname === "/manifold/internal/dispatch" && req.method === "GET") {
        if (req.headers.authorization !== `Bearer ${token}`) return unauthorized(res);
        res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
        const unsub = hub.subscribe("dispatch", res);
        req.on("close", unsub);
        return;
      }
      if (pathname === "/manifold/internal/dispatch/ack" && req.method === "POST") {
        if (req.headers.authorization !== `Bearer ${token}`) return unauthorized(res);
        const body = await readJsonBody(req);
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: dispatchHub.ack(body?.id) }));
        return;
      }
      if (pathname === "/manifold/internal/dispatch/result" && req.method === "POST") {
        if (req.headers.authorization !== `Bearer ${token}`) return unauthorized(res);
        const body = await readJsonBody(req);
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: dispatchHub.fulfill(body?.id, body) }));
        return;
      }
      if (pathname === "/manifold/internal/health" && req.method === "GET") {
        if (req.headers.authorization !== `Bearer ${token}`) return unauthorized(res);
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({
          ok: true,
          dataDir,
          uptimeMs: Math.round(process.uptime() * 1000),
          subscribers: { events: hub.subscriberCount("events"), dispatch: hub.subscriberCount("dispatch") },
          pendingDispatch: dispatchHub.pendingCount(),
        }));
        return;
      }

      const route = findRoute(pathname);
      if (!route) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "not found" }));
        return;
      }
      await route.handler(req, res);
    } catch (err) {
      logger?.error?.(`sidecar: request failed: ${err?.message ?? err}`);
      try {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "internal error" }));
      } catch {}
    }
  });

  const port = Number(env.MANIFOLD_PORT) > 0 ? Number(env.MANIFOLD_PORT) : DEFAULT_PORT;
  const host = env.MANIFOLD_HOST || DEFAULT_HOST;
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  logger?.info?.(`sidecar: listening on ${host}:${port} (data ${dataDir})`);

  const shutdown = async () => {
    logger?.info?.("sidecar: shutting down");
    for (const svc of [...started].reverse()) {
      try {
        await svc.stop?.();
      } catch (err) {
        logger?.warn?.(`sidecar: service stop failed: ${svc.id}: ${err?.message ?? err}`);
      }
    }
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref?.();
  };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);

  return { server, api, hub, dispatchHub, dataDir, port, host };
}
