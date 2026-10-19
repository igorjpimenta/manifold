// elevation.js — plugin-side helper for routes that need ambient operator
// scope but are exposed via `auth: "plugin"` (so they accept device tokens
// instead of the env-var token).
//
// The problem this fixes:
//   Plugin-auth HTTP routes don't get an operator scope on the request —
//   they get "the plugin handles auth" with NO scope propagation. Anything
//   inside the handler that scope-checks against operator.* (notably
//   subagent.run, which gateway-internally requires operator.write) fails
//   with `missing scope: operator.write` even when the device token has
//   operator.write declared. This includes async work scheduled via
//   setImmediate from inside the handler — the orphaned context has no
//   ambient operator scope either.
//
// The mechanism:
//   The gateway's `auth: "gateway"` middleware DOES set ambient operator
//   scope: when an HTTP request authenticates via the shared-secret
//   env-var token (OPENCLAW_GATEWAY_TOKEN), the auth path attaches
//   CLI_DEFAULT_OPERATOR_SCOPES (admin/write/read/approvals/pairing/
//   talk.secrets) — see http-auth-utils-Chseq2ys.js. Routes registered
//   with `auth: "gateway"` inherit that scope ambient through their
//   handler execution.
//
//   So a plugin-auth route can validate its device token, then loopback
//   over localhost to a sibling `auth: "gateway"` route using the env-var
//   Bearer to perform the operator-scoped work. The elevation crosses
//   the auth boundary cleanly: device-token auth in front, env-var auth
//   inside, no token leakage to clients.
//
// Usage pattern:
//   In your external (auth: "plugin") route:
//     const { ok, status, body } = await invokeElevated("/pipeclaw/_elev/start", payload);
//     res.statusCode = status;
//     res.end(body);
//   In your internal (auth: "gateway") route:
//     handler: async (req, res) => {
//       // Body already validated by the caller (or re-validate). Scope is
//       // ambient operator.*. Do the work that needs it.
//     }
//
// When NOT to use this:
//   - For purely read-only operations, just keep auth: "plugin" — they
//     don't need operator.write.
//   - For routes whose auth is acceptable as env-var only (e.g. internal
//     gateway-to-gateway), register directly with auth: "gateway" and
//     skip the loopback.

import http from "node:http";

const GATEWAY_PORT = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;
const GATEWAY_HOST = "127.0.0.1";

/**
 * POST to a gateway-auth route on loopback with the env-var Bearer, so the
 * target handler runs with CLI_DEFAULT_OPERATOR_SCOPES ambient.
 *
 * Returns { ok, status, body, error? } — body is the raw response string.
 * Caller is responsible for content-type interpretation.
 *
 * `opts.timeoutMs` defaults to 30s — fine for routes that schedule work
 * and return quickly (e.g. /pipeclaw/_elev/start kicks off a flow via
 * setImmediate). Routes that block on subagent.run (e.g. /_elev/discuss
 * waits up to 5 min for Herculano to compose a reply) MUST pass a
 * timeout above the inner wait, or the loopback gives up while the
 * inner work is still in flight and surfaces a confusing 500.
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
