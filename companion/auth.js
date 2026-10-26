// companion/auth.js — gateway-side bearer check for the proxied surface.
// Accepts the env gateway token or a paired-device token (same baseline as
// the edge). Gateway-resident: the device SDK import is static here, unlike
// the sidecar's tolerant copy. Service-token auth to the SIDECAR is separate
// (Authorization header is replaced, never forwarded).
import { timingSafeEqual } from "node:crypto";

// Device-pairing SDK lives in the harness (gateway). Lazy so this module
// also loads standalone (tests, sidecar-adjacent tooling), where the
// device path is skipped and only token auth applies.
let _listDevicePairing = null;
let _deviceSdkMissing = false;
async function listPairedDevices() {
  if (_deviceSdkMissing) return { paired: [] };
  if (!_listDevicePairing) {
    try {
      ({ listDevicePairing: _listDevicePairing } = await import("openclaw/plugin-sdk/device-bootstrap"));
    } catch {
      _deviceSdkMissing = true;
      return { paired: [] };
    }
  }
  return _listDevicePairing();
}

const PAIRED_CACHE_TTL_MS = 30_000;
let pairedCache = { ts: 0, devices: [] };

function tokensEqual(a, b) {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) {
    timingSafeEqual(ab, ab);
    return false;
  }
  return timingSafeEqual(ab, bb);
}

export async function authorizeCompanionRequest(req) {
  const raw = req.headers?.authorization || "";
  if (!raw.toLowerCase().startsWith("bearer ")) return { ok: false, reason: "no-bearer" };
  const token = raw.slice(7).trim();
  if (!token) return { ok: false, reason: "empty-token" };
  const envToken = process.env.OPENCLAW_GATEWAY_TOKEN || "";
  if (envToken && tokensEqual(token, envToken)) return { ok: true, via: "env-var" };
  if (Date.now() - pairedCache.ts >= PAIRED_CACHE_TTL_MS) {
    try {
      const list = await listPairedDevices();
      pairedCache = { ts: Date.now(), devices: list?.paired ?? [] };
    } catch {}
  }
  for (const dev of pairedCache.devices) {
    const tokens = dev?.tokens ?? {};
    for (const [role, entry] of Object.entries(tokens)) {
      if (!entry || entry.revokedAtMs || !entry.token) continue;
      if (tokensEqual(token, entry.token)) {
        return { ok: true, via: `device:${dev.clientId ?? "?"}/${role}` };
      }
    }
  }
  return { ok: false, reason: "no-match" };
}

export function sendUnauthorized(res, reason) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "unauthorized", reason: reason ?? "unauthorized" }));
}
