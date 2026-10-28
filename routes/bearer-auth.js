// routes/bearer-auth.js — winch-style bearer guard for manifold HTTP routes.
//
// One shared secret, no pairing, no device tokens: `Authorization: Bearer
// <MANIFOLD_BEARER>` — the same env the MCP HTTP server requires before it
// starts. Timing-safe compare; TLS terminates in front (reverse proxy or
// ingress). An unset/empty secret fails every check (fail-closed); the
// first rejection logs once so a missing env is loud instead of mysterious.
//
// The harness declares itself per call with X-Manifold-Harness where
// identity matters (MCP sessions); these operator routes authenticate the
// caller, not the harness.
import { timingSafeEqual } from "node:crypto";

let warnedNoSecret = false;

function tokensEqual(a, b) {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) {
    timingSafeEqual(ab, ab); // burn the cycles to keep timing flat
    return false;
  }
  return timingSafeEqual(ab, bb);
}

/**
 * Returns {ok: true} or {ok: false, reason}. Reasons: "no-bearer"
 * (missing/non-Bearer scheme), "empty-token", "no-secret" (MANIFOLD_BEARER
 * unset — fail-closed), "no-match".
 */
export function authorizeBearerRequest(req, { logger = null } = {}) {
  const raw = req?.headers?.authorization ?? "";
  if (typeof raw !== "string" || !raw.toLowerCase().startsWith("bearer ")) {
    return { ok: false, reason: "no-bearer" };
  }
  const token = raw.slice(7).trim();
  if (!token) return { ok: false, reason: "empty-token" };
  const secret = process.env.MANIFOLD_BEARER ?? "";
  if (!secret) {
    if (!warnedNoSecret) {
      warnedNoSecret = true;
      try { logger?.warn?.("manifold: MANIFOLD_BEARER is unset — every bearer check fails closed"); } catch {}
    }
    return { ok: false, reason: "no-secret" };
  }
  if (!tokensEqual(token, secret)) return { ok: false, reason: "no-match" };
  return { ok: true };
}

/** Standard 401 for a bearer-rejected request. */
export function sendUnauthorized(res, reason) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "unauthorized", reason: reason ?? "unauthorized" }));
}
