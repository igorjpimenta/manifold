// events/push.js — push event delivery to harness webhooks. Mirrors winch's
// src/claim-webhook.js contract: when an agent turn is dispatched or a
// human gate opens, core POSTs the handoff the moment it lands so the
// harness can wake the receiver instead of waiting for a poll tick.
//
// Delivery is best-effort and never fails the run: bounded timeout,
// network/HTTP errors become a logger warning. No target = no-op.
const DEFAULT_TIMEOUT_MS = 3000;

function validHttpUrl(url) {
  if (typeof url !== "string") return false;
  const trimmed = url.trim();
  if (!trimmed) return false;
  try {
    const parsed = new URL(trimmed);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function timeoutOf(entry, config) {
  if (Number.isInteger(entry?.timeoutMs) && entry.timeoutMs > 0) return entry.timeoutMs;
  if (Number.isInteger(config?.pushTimeoutMs) && config.pushTimeoutMs > 0) return config.pushTimeoutMs;
  return DEFAULT_TIMEOUT_MS;
}

// Resolve the push target for a harness. Precedence: DB-registered target
// (a deliberate admin act on the live system) → per-harness config entry
// (`pushUrls[harness]`) → the single pushUrl/pushBearer fallback → null.
// Each harness gets exactly its own events — no fan-out, no cross-harness
// leak. Returns null when disabled or misconfigured (never throws).
export function resolvePushTarget({ pushTargets = null, harnessId = null, config = null } = {}) {
  if (pushTargets && typeof pushTargets.url === "string" && pushTargets.url.trim() !== "") {
    if (!validHttpUrl(pushTargets.url)) return null;
    const bearer = typeof pushTargets.bearer === "string" && pushTargets.bearer !== "" ? pushTargets.bearer : null;
    return { url: pushTargets.url.trim(), bearer, timeoutMs: timeoutOf(pushTargets, config) };
  }
  const table = config?.pushUrls;
  if (harnessId && table && typeof table === "object") {
    const entry = table[harnessId];
    if (entry && typeof entry.url === "string" && entry.url.trim() !== "" && validHttpUrl(entry.url)) {
      const bearer = typeof entry.bearer === "string" && entry.bearer !== "" ? entry.bearer : null;
      return { url: entry.url.trim(), bearer, timeoutMs: timeoutOf(entry, config) };
    }
  }
  const url = typeof config?.pushUrl === "string" ? config.pushUrl.trim() : "";
  if (!url || !validHttpUrl(url)) return null;
  const bearer = typeof config?.pushBearer === "string" && config.pushBearer !== "" ? config.pushBearer : null;
  return { url, bearer, timeoutMs: timeoutOf(null, config) };
}

export function pushPayload({ event, flowId = null, nodeId = null, pipelineId = null, sessionKey = null, agent = null, harnessId = null, extra = null }) {
  return {
    event,
    at: Date.now(),
    flow: flowId ? { id: flowId, pipelineId: pipelineId ?? null, nodeId: nodeId ?? null } : null,
    session: { harness: harnessId ?? null, agent: agent ?? null, sessionKey: sessionKey ?? null },
    ...(extra && typeof extra === "object" ? extra : null),
  };
}

// POST the payload as JSON. Resolves {ok:true,status} on 2xx,
// {ok:false,…} otherwise — never throws, so callers can await it inline
// in the dispatch path without risking the run itself.
export async function notifyPush({ target, payload, logger = null } = {}) {
  if (!target || !validHttpUrl(target.url)) return { ok: false, reason: "disabled" };
  const timeoutMs = Number.isInteger(target.timeoutMs) && target.timeoutMs > 0 ? target.timeoutMs : DEFAULT_TIMEOUT_MS;
  try {
    const headers = { "content-type": "application/json" };
    if (target.bearer) headers.authorization = `Bearer ${target.bearer}`;
    const res = await fetch(target.url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) {
      logger?.warn?.(`push: ${target.url} -> ${res.status}`);
      return { ok: false, reason: `http_${res.status}` };
    }
    return { ok: true, status: res.status };
  } catch (err) {
    logger?.warn?.(`push: ${target.url} failed: ${err?.message ?? err}`);
    return { ok: false, reason: "error" };
  }
}
