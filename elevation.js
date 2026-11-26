// Plugin-side helper for routes that need ambient operator scope but are
// exposed via `auth: "plugin"` (so they accept device tokens instead of the
// env-var token).
//
// Plugin-auth HTTP routes get NO scope propagation: anything in the handler
// that scope-checks against operator.* (notably subagent.run, which requires
// operator.write) fails with `missing scope: operator.write` even when the
// device token has it — including async work scheduled via setImmediate from
// the handler. The gateway's `auth: "gateway"` middleware DOES set ambient
// operator scope (CLI_DEFAULT_OPERATOR_SCOPES) when a request authenticates
// via the shared-secret env-var token (OPENCLAW_GATEWAY_TOKEN).
//
// So a plugin-auth route validates its device token, then loops back over
// localhost to a sibling `auth: "gateway"` route using the env-var Bearer to
// perform the operator-scoped work: device-token auth in front, env-var auth
// inside, no token leakage to clients.
//
// Do NOT use for purely read-only operations (keep auth: "plugin") or for
// routes whose auth is acceptable as env-var only (register auth: "gateway"
// directly and skip the loopback).

import http from "node:http";

const GATEWAY_PORT = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;
const GATEWAY_HOST = "127.0.0.1";

/**
 * POST to a gateway-auth route on loopback with the env-var Bearer, so the
 * target handler runs with CLI_DEFAULT_OPERATOR_SCOPES ambient. Returns
 * { ok, status, body, error? } — body is the raw response string.
 *
 * `opts.timeoutMs` defaults to 30s — fine for routes that schedule work and
 * return quickly. Routes that block on inner work (e.g. /_elev/discuss
 * waiting on an agent reply) MUST pass a timeout above that inner wait, or
 * the loopback gives up while the work is still in flight and surfaces a
 * confusing 500.
 */
export async function invokeElevated(path, payload, opts = {}) {
  const timeoutMs = Number.isFinite(opts.timeoutMs) ? opts.timeoutMs : 30_000;
  const token = process.env.OPENCLAW_GATEWAY_TOKEN;
  if (!token) {
    return {
      ok: false,
      status: 503,
      body: JSON.stringify({ ok: false, code: "no_gateway_token", error: "OPENCLAW_GATEWAY_TOKEN unset" }),
      error: "no_gateway_token",
    };
  }
  const body = typeof payload === "string" ? payload : JSON.stringify(payload ?? {});
  return new Promise((resolve) => {
    const req = http.request({
      hostname: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "POST",
      path,
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "Content-Length": Buffer.byteLength(body),
      },
    }, (res) => {
      let raw = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => { raw += chunk; });
      res.on("end", () => {
        resolve({
          ok: res.statusCode >= 200 && res.statusCode < 300,
          status: res.statusCode ?? 0,
          body: raw,
        });
      });
    });
    req.on("error", (err) => {
      resolve({
        ok: false,
        status: 0,
        body: JSON.stringify({ ok: false, code: "loopback_failed", error: err?.message ?? String(err) }),
        error: err?.message ?? String(err),
      });
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error("loopback timeout"));
    });
    req.write(body);
    req.end();
  });
}
