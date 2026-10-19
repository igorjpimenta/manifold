// auth.js — bearer-token check for the bridge's HTTP routes. Accepts EITHER
// the shared env-var gateway token (legacy / bootstrap / break-glass) OR a
// per-device token issued by `openclaw devices approve`. Used by every
// route the bridge registers + by the /pipeclaw/gate/resolve route in
// the pipeclaw plugin (sibling import).
//
// Why we own auth here instead of using `auth: "gateway"`: the gateway's
// built-in token check validates against process.env.OPENCLAW_GATEWAY_TOKEN
// only. Adding device-token acceptance there would mean modifying openclaw
// core; doing it plugin-side keeps the change contained and matches the
// `auth: "plugin"` contract (handler is responsible for auth).
//
// Scope enforcement: deferred. Today every paired device on this host has
// `operator.admin` scope (see `openclaw devices list --json`), and the
// env-var token has implicit admin. So accepting any non-revoked device
// token == today's security baseline. Add per-route scope checks once the
// first reduced-scope device (e.g. operator.read only) actually exists.

import { listDevicePairing } from "openclaw/plugin-sdk/device-bootstrap";
import { timingSafeEqual } from "node:crypto";

// Paired-device list is on-disk — cache the parse for one TTL window so every incoming
// bridge request (each of these routes is auth-checked per-call, including
// hot ones like /pipeclaw/ios/state that the app can poll frequently) doesn't
// re-read and re-parse the JSON file from scratch. Revocation is visible
// within one TTL window; acceptable for an operator-grade tool.
const PAIRED_CACHE_TTL_MS = 30_000;
let pairedCache = { ts: 0, devices: [] };

async function refreshPairedDevicesIfStale() {
  if (Date.now() - pairedCache.ts < PAIRED_CACHE_TTL_MS) return;
  try {
    const list = await listDevicePairing();
    pairedCache = { ts: Date.now(), devices: list?.paired ?? [] };
  } catch {
    // Leave the previous snapshot in place; the existing devices will
    // still authenticate. A logger call is the caller's job (we don't
    // have one here).
  }
}

// Constant-time bearer compare. Mismatched-length tokens still do a fake
// compare to keep timing flat.
function tokensEqual(a, b) {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) {
    timingSafeEqual(ab, ab); // burn the cycles
    return false;
  }
  return timingSafeEqual(ab, bb);
}

/**
 * Returns {ok, via?, reason?, deviceId?, scopes?}.
 *   via:     "env-var" | "device:<clientId>/<role>" — for log lines
 *   reason:  one of "no-bearer", "empty-token", "no-match"
 */
export async function authorizeBridgeRequest(req) {
  const raw = req.headers?.authorization || "";
  if (!raw.toLowerCase().startsWith("bearer ")) {
    return { ok: false, reason: "no-bearer" };
  }
  const token = raw.slice(7).trim();
  if (!token) return { ok: false, reason: "empty-token" };

  // Env-var path. Compared in constant time. Skipped silently if unset.
  const envToken = process.env.OPENCLAW_GATEWAY_TOKEN || "";
  if (envToken && tokensEqual(token, envToken)) {
    return { ok: true, via: "env-var" };
  }

  // Device-token path. Iterate every paired-device + role pair.
  await refreshPairedDevicesIfStale();
  for (const dev of pairedCache.devices) {
    const tokens = dev?.tokens ?? {};
    for (const [role, entry] of Object.entries(tokens)) {
      if (!entry || entry.revokedAtMs) continue;
      const stored = entry.token;
      if (!stored) continue;
      if (tokensEqual(token, stored)) {
        return {
          ok: true,
          via: `device:${dev.clientId ?? "?"}/${role}`,
          deviceId: dev.deviceId,
          scopes: Array.isArray(entry.scopes) ? entry.scopes : [],
        };
      }
    }
  }

  return { ok: false, reason: "no-match" };
}

/** Standard 401 response for an auth-rejected request. */
export function sendUnauthorized(res, reason) {
  res.statusCode = 401;
  res.setHeader("WWW-Authenticate", `Bearer realm="live-activity-bridge", error="${reason ?? "unauthorized"}"`);
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "unauthorized", reason: reason ?? "unauthorized" }));
}
