// Bearer guard: `Authorization: Bearer <MANIFOLD_BEARER>`. An unset/empty
// secret fails every check (fail-closed); TLS terminates in front.
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

export function authorizeBearerRequest(req, { logger = null, secret: secretOverride = null } = {}) {
  const raw = req?.headers?.authorization ?? "";
  if (typeof raw !== "string" || !raw.toLowerCase().startsWith("bearer ")) {
    return { ok: false, reason: "no-bearer" };
  }
  const token = raw.slice(7).trim();
  if (!token) return { ok: false, reason: "empty-token" };
  const secret = secretOverride ?? process.env.MANIFOLD_BEARER ?? "";
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

export function sendUnauthorized(res, reason) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "unauthorized", reason: reason ?? "unauthorized" }));
}
