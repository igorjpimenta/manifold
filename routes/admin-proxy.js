// admin-proxy.js — manifold as the app's only contact for admin surfaces.
//
// Cron control, device management, health, and apns registration ride
// here so the app never calls the harness (or sibling plugins) directly;
// transport handshake, pairing identity, and gateway restart stay on the
// host. Harness WS-RPC methods (cron.*, device.*) have no in-process
// dispatch handle, so the proxy uses a WS loopback client (connect per
// call with the operator token — admin actions are infrequent; no
// connection state to reconcile). apns/register and health are served
// without RPC: the former via the in-process apns runtime, the latter
// from manifold's own process. Every proxy preserves the harness
// {ok, payload?, error?} envelope so callers map trivially.
import os from "node:os";
import { randomUUID } from "node:crypto";
import WebSocket from "ws";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const MANIFOLD_VERSION = require("../package.json").version;

const RPC_TIMEOUT_MS = 15000;

// One WS-RPC round trip against the local gateway. Connects, sends,
// awaits the matching res frame, closes. Returns the harness envelope
// verbatim on success; synthesizes {ok:false} on transport failure.
export async function rpcViaLoopback({ method, params = {}, port, token, timeoutMs = RPC_TIMEOUT_MS }) {
  const url = `ws://127.0.0.1:${port}/`;
  const fail = (message) => ({ ok: false, error: { message } });
  let ws = null;
  try {
    ws = new WebSocket(url);
  } catch (err) {
    return fail(`ws open failed: ${err?.message ?? err}`);
  }
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("connect timeout")), 10000);
      const connectId = randomUUID();
      ws.on("open", () => {
        ws.send(JSON.stringify({
          type: "req",
          id: connectId,
          method: "connect",
          params: {
            minProtocol: 3,
            maxProtocol: 4,
            client: { id: "gateway-client", version: MANIFOLD_VERSION, platform: "node", mode: "backend" },
            role: "operator",
            auth: token ? { token } : undefined,
          },
        }));
      });
      const onHello = (data) => {
        let frame;
        try {
          frame = JSON.parse(String(data));
        } catch {
          return;
        }
        if (frame?.type !== "res" || frame?.id !== connectId) return;
        ws.off("message", onHello);
        clearTimeout(timer);
        if (frame.ok === true) resolve();
        else reject(new Error(frame?.error?.message ?? "connect rejected"));
      };
      ws.on("message", onHello);
      ws.on("error", () => {});
      ws.on("close", () => reject(new Error("ws closed before connect completed")));
    });
    const id = randomUUID();
    const out = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("rpc timeout")), timeoutMs);
      const onMessage = (data) => {
        let frame;
        try {
          frame = JSON.parse(String(data));
        } catch {
          return;
        }
        if (frame?.type === "res" && frame?.id === id) {
          clearTimeout(timer);
          ws.off("message", onMessage);
          resolve(frame);
        }
      };
      ws.on("message", onMessage);
      ws.send(JSON.stringify({ type: "req", id, method, params }));
    });
    return { ok: out.ok === true, payload: out.payload, error: out.error };
  } catch (err) {
    return fail(err?.message ?? String(err));
  } finally {
    try { ws?.close(); } catch {}
  }
}

function json(res, status, obj) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(obj));
}

async function readBody(req, res) {
  let raw = "";
  try {
    for await (const chunk of req) raw += chunk;
  } catch {
    res.statusCode = 400;
    res.end("unreadable body");
    return null;
  }
  try {
    return raw ? JSON.parse(raw) : {};
  } catch {
    res.statusCode = 400;
    res.end("invalid json");
    return null;
  }
}

export function registerAdminProxyRoutes({ api, rpc = rpcViaLoopback }) {
  const port = () => Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;
  const token = () => process.env.OPENCLAW_GATEWAY_TOKEN ?? null;

  async function authorize(req, res) {
    const { authorizeBridgeRequest, sendUnauthorized } = await import("../auth/bridge.js");
    const authz = await authorizeBridgeRequest(req);
    if (!authz.ok) {
      sendUnauthorized(res, authz.reason);
      return null;
    }
    return authz;
  }

  // ── GET /manifold/health ─────────────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/health",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      if (!await authorize(req, res)) return true;
      json(res, 200, { ok: true, manifold: MANIFOLD_VERSION, uptimeMs: Math.round(process.uptime() * 1000) });
      return true;
    },
  });

  // ── Cron passthrough ─────────────────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/cron/jobs",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      if (!await authorize(req, res)) return true;
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const includeDisabled = url.searchParams.get("includeDisabled") !== "false";
      json(res, 200, await rpc({ method: "cron.list", params: { includeDisabled }, port: port(), token: token() }));
      return true;
    },
  });

  for (const [subpath, method] of [["run", "cron.run"], ["update", "cron.update"]]) {
    api.registerHttpRoute({
      path: `/manifold/cron/${subpath}`,
      auth: "plugin",
      match: "exact",
      handler: async (req, res) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("Allow", "POST");
          res.end("method not allowed");
          return true;
        }
        if (!await authorize(req, res)) return true;
        const body = await readBody(req, res);
        if (!body) return true;
        json(res, 200, await rpc({ method, params: body, port: port(), token: token() }));
        return true;
      },
    });
  }

  // ── Device passthrough ───────────────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/devices",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      if (!await authorize(req, res)) return true;
      json(res, 200, await rpc({ method: "device.pair.list", params: {}, port: port(), token: token() }));
      return true;
    },
  });

  const deviceWrites = [
    ["approve", "device.pair.approve"],
    ["reject", "device.pair.reject"],
    ["remove", "device.pair.remove"],
    ["token/rotate", "device.token.rotate"],
    ["token/revoke", "device.token.revoke"],
  ];
  for (const [subpath, method] of deviceWrites) {
    api.registerHttpRoute({
      path: `/manifold/devices/${subpath}`,
      auth: "plugin",
      match: "exact",
      handler: async (req, res) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("Allow", "POST");
          res.end("method not allowed");
          return true;
        }
        if (!await authorize(req, res)) return true;
        const body = await readBody(req, res);
        if (!body) return true;
        json(res, 200, await rpc({ method, params: body, port: port(), token: token() }));
        return true;
      },
    });
  }

  // ── POST /manifold/apns/register ─────────────────────────────────
  // Same contract as the apns plugin's route, served via its in-process
  // runtime instead of a second HTTP hop.
  api.registerHttpRoute({
    path: "/manifold/apns/register",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405;
        res.setHeader("Allow", "POST");
        res.end("method not allowed");
        return true;
      }
      const authz = await authorize(req, res);
      if (!authz) return true;
      const body = await readBody(req, res);
      if (!body) return true;
      const deviceId = typeof body.deviceId === "string" ? body.deviceId : authz.deviceId;
      const apnsToken = typeof body.apnsToken === "string" ? body.apnsToken : "";
      const env = typeof body.environment === "string" ? body.environment : undefined;
      if (!deviceId || !apnsToken) {
        json(res, 400, { ok: false, error: "deviceId + apnsToken required" });
        return true;
      }
      try {
        const apns = api.runtime?.apns;
        if (!apns || apns.isConfigured?.() !== true) {
          json(res, 503, { ok: false, error: "apns not configured" });
          return true;
        }
        await apns.registerDevice(deviceId, apnsToken, env);
        json(res, 200, { ok: true });
      } catch (err) {
        api.logger.warn(`manifold apns/register error: ${err?.message ?? err}`);
        json(res, 500, { ok: false, error: err?.message ?? String(err) });
      }
      return true;
    },
  });
}
