// device.js — dashboard device identity and pairing.
//
// The dashboard authenticates to the gateway as a genuinely PAIRED DEVICE,
// the same model the iOS app and the CLI itself use — not the shared
// OPENCLAW_GATEWAY_TOKEN master secret. This matters because paired
// devices are individually revocable and (eventually) individually
// scoped; the shared env-var token is neither.
//
// The public plugin-sdk deliberately does NOT export a "create a new
// pairing request" function (`requestDevicePairing` is gateway-internal,
// called only as a side effect of a real WebSocket client connecting —
// see message-handler internals). So pairing means actually attempting a
// real WS connection with our own device keypair via the exported
// `GatewayClient` (the same class the CLI/`openclaw node` use), not
// calling an SDK function directly.
//
// Auth on that connect: a bare deviceIdentity with no other credential is
// flatly rejected (AUTH_TOKEN_MISSING) before the server even looks at
// pairing — some already-authenticated caller must vouch for a new device
// identity. OPENCLAW_GATEWAY_TOKEN (already available to this process) is
// what's supplied for that. (A self-issued `issueDeviceBootstrapToken()`
// does NOT work here — that mints tokens for a separate silent/QR
// "setup code" handoff flow with its own role/scope allowlist; role
// "operator" isn't on it, so the token gets its scopes stripped and then
// rejected as invalid once they don't match the connect request.)
//
// Client identity matters too: `clientName:"gateway-client"` combined
// with `mode:"backend"` hits a DIFFERENT gateway code path
// (`shouldSkipLocalBackendSelfPairing`) that grants the requested
// role/scopes for the session but explicitly skips device registration —
// no persisted token, nothing in `paired.json`. Using `mode:"ui"` instead
// avoids that path, which lets a normal local-trust rule apply instead:
// a loopback connection authenticated with the real shared secret gets
// silently paired with a real, persisted, individually-revocable device
// token — no separate `openclaw devices approve` step, since it's
// already gated behind possession of the master token plus being on the
// same host (the same trust bar the master token itself already had).
//
// Device identity: a real Ed25519 keypair (the server verifies a
// signature, a hash alone isn't sufficient), persisted at
// ~/.openclaw/identity/manifold-dashboard-device.json — a DIFFERENT file
// from the CLI's own ~/.openclaw/identity/device.json, so pairing the
// dashboard never disturbs the CLI's identity or cached token.

import { createHash, generateKeyPairSync, createPublicKey } from "node:crypto";
// Harness SDK (pairing store, gateway WS client) is gateway-resident.
// Standalone (sidecar/tests) these degrade: no paired devices listed,
// pairing attempts report failure — both already handled by callers.
let listDevicePairing = async () => ({ paired: [], pending: [] });
let GatewayClient = class {
  constructor() {
    throw new Error("GatewayClient unavailable outside the harness");
  }
};
try {
  ({ listDevicePairing } = await import("openclaw/plugin-sdk/device-bootstrap"));
} catch {}
try {
  ({ GatewayClient } = await import("openclaw/plugin-sdk/gateway-runtime"));
} catch {}
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

const IDENTITY_PATH = path.join(os.homedir(), ".openclaw", "identity", "manifold-dashboard-device.json");
const OPERATOR_SCOPES = ["operator.read", "operator.write", "operator.approvals", "operator.admin"];

// Ed25519 SPKI DER prefix — stripping it yields the raw 32-byte public key,
// same convention openclaw's own device-identity module uses so our
// deviceId (sha256 of the raw key) matches what the server independently
// re-derives from the publicKey we send it.
const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");

function rawPublicKey(publicKeyPem) {
  const spki = createPublicKey(publicKeyPem).export({ type: "spki", format: "der" });
  if (spki.length === ED25519_SPKI_PREFIX.length + 32 && spki.subarray(0, ED25519_SPKI_PREFIX.length).equals(ED25519_SPKI_PREFIX)) {
    return spki.subarray(ED25519_SPKI_PREFIX.length);
  }
  return spki;
}

function fingerprintPublicKey(publicKeyPem) {
  return createHash("sha256").update(rawPublicKey(publicKeyPem)).digest("hex");
}

function generateIdentity() {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicKeyPem = publicKey.export({ type: "spki", format: "pem" });
  const privateKeyPem = privateKey.export({ type: "pkcs8", format: "pem" });
  return { deviceId: fingerprintPublicKey(publicKeyPem), publicKeyPem, privateKeyPem };
}

function loadOrCreateDeviceIdentity(filePath = IDENTITY_PATH) {
  try {
    if (fs.existsSync(filePath)) {
      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
      if (parsed?.version === 1 && typeof parsed.publicKeyPem === "string" && typeof parsed.privateKeyPem === "string") {
        return { deviceId: fingerprintPublicKey(parsed.publicKeyPem), publicKeyPem: parsed.publicKeyPem, privateKeyPem: parsed.privateKeyPem };
      }
    }
  } catch { /* fall through to generate fresh */ }
  const identity = generateIdentity();
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify({ version: 1, ...identity, createdAtMs: Date.now() }, null, 2)}\n`, { mode: 0o600 });
  try { fs.chmodSync(filePath, 0o600); } catch { /* best-effort */ }
  return identity;
}

export async function loadDashboardToken() {
  try {
    const identity = loadOrCreateDeviceIdentity();
    const list = await listDevicePairing();
    const device = (list?.paired ?? []).find((d) => d.deviceId === identity.deviceId);
    if (!device) return null;
    const entry = device.tokens?.operator;
    if (!entry || entry.revokedAtMs) return null;
    return entry.token ?? null;
  } catch {
    return null;
  }
}

// Attempts a real WS connect as a genuinely new device. Resolves (never
// rejects) with either:
//   { alreadyPaired: true, token }        — connected and got a device token
//   { alreadyPaired: false, requestId }   — a pending request exists (e.g.
//                                            connecting from a non-local
//                                            host, where silent local
//                                            trust doesn't apply); operator
//                                            must `openclaw devices approve
//                                            <requestId>`
//   { alreadyPaired: false, requestId: null } — attempt failed for some
//                                            other reason (network,
//                                            missing token, timeout);
//                                            caller falls back
export async function pairDashboardDevice({ logger, gatewayUrl } = {}) {
  const already = await loadDashboardToken();
  if (already) return { alreadyPaired: true, token: already };

  const identity = loadOrCreateDeviceIdentity();

  // Reuse an already-pending request from a prior boot instead of creating
  // a new pending entry on every restart.
  const list = await listDevicePairing();
  const existingPending = (list?.pending ?? []).find((r) => r.deviceId === identity.deviceId);
  if (existingPending) return { alreadyPaired: false, requestId: existingPending.requestId };

  const gatewayToken = process.env.OPENCLAW_GATEWAY_TOKEN || undefined;
  const url = gatewayUrl ?? `ws://127.0.0.1:${Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789}`;

  return await new Promise((resolve) => {
    let settled = false;
    let client;
    const settle = (result) => {
      if (settled) return;
      settled = true;
      try { client?.stop(); } catch { /* ignore */ }
      resolve(result);
    };

    try {
      client = new GatewayClient({
        url,
        deviceIdentity: identity,
        token: gatewayToken,
        clientName: "gateway-client",
        clientDisplayName: "Manifold Dashboard",
        mode: "ui",
        role: "operator",
        scopes: OPERATOR_SCOPES,
        onHelloOk: (hello) => {
          const token = hello?.auth?.deviceToken ?? null;
          settle(token ? { alreadyPaired: true, token } : { alreadyPaired: false, requestId: null });
        },
        onConnectError: (err) => {
          const requestId = err?.details?.requestId ?? null;
          if (!requestId) logger?.warn?.(`manifold dashboard: device pairing connect error: ${err?.message ?? err}`);
          settle({ alreadyPaired: false, requestId });
        },
      });
    } catch (err) {
      logger?.warn?.(`manifold dashboard: device pairing setup failed: ${err?.message ?? err}`);
      settle({ alreadyPaired: false, requestId: null });
      return;
    }

    try {
      client.start();
    } catch (err) {
      logger?.warn?.(`manifold dashboard: device pairing connect failed to start: ${err?.message ?? err}`);
      settle({ alreadyPaired: false, requestId: null });
    }
    setTimeout(() => settle({ alreadyPaired: false, requestId: null }), 10_000);
  });
}
