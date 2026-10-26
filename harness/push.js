// push.js — URL push delivery for harness lifecycle events.
//
// In-process sibling of harness/events.js's createEventStream: where the
// stream keeps subscribers inside this process, push POSTs JSON to a
// per-harness URL so an external harness wakes without polling. Semantics
// mirror winch's src/claim-webhook.js (precedence + never-throwing notify).
// No harness imports — store and config are injected by the caller.

export const DEFAULT_PUSH_TIMEOUT_MS = 3000;

function timeoutOr(value, fallback) {
  if (Number.isInteger(value) && value > 0) return value;
  if (Number.isInteger(fallback) && fallback > 0) return fallback;
  return DEFAULT_PUSH_TIMEOUT_MS;
}

function validTarget({ url, bearer, timeoutMs }, fallbackTimeoutMs) {
  if (typeof url !== "string") return null;
  const trimmed = url.trim();
  if (!/^https?:\/\//i.test(trimmed)) return null;
  return {
    url: trimmed,
    bearer: typeof bearer === "string" && bearer !== "" ? bearer : null,
    timeoutMs: timeoutOr(timeoutMs, fallbackTimeoutMs),
  };
}

function envTimeout() {
  const n = Number(process.env.MANIFOLD_PUSH_TIMEOUT_MS);
  return Number.isInteger(n) && n > 0 ? n : null;
}

// Precedence: explicit DB row → per-harness config entry → single
// configured URL (config.pushUrl, else MANIFOLD_PUSH_URL) → disabled.
// Each harness gets exactly its own target — no fan-out. Never throws:
// a failing store read degrades to the config chain.
export async function resolvePushTarget({ pushTargets, harnessId, config } = {}) {
  const fallbackTimeoutMs = timeoutOr(config?.pushTimeoutMs ?? envTimeout(), DEFAULT_PUSH_TIMEOUT_MS);
  try {
    const row = await pushTargets?.get?.(harnessId);
    if (row?.url) {
      const t = validTarget({ url: row.url, bearer: row.bearer, timeoutMs: row.timeoutMs }, fallbackTimeoutMs);
      if (t) return t;
    }
  } catch {}
  const table = config?.pushUrls;
  if (harnessId && table && typeof table === "object") {
    const entry = table[harnessId];
    if (entry) {
      const cand = typeof entry === "string" ? { url: entry } : entry;
      const t = validTarget({ url: cand?.url, bearer: cand?.bearer, timeoutMs: cand?.timeoutMs }, fallbackTimeoutMs);
      if (t) return t;
    }
  }
  const singleUrl = typeof config?.pushUrl === "string" && config.pushUrl.trim() !== ""
    ? config.pushUrl
    : (typeof process.env.MANIFOLD_PUSH_URL === "string" ? process.env.MANIFOLD_PUSH_URL : "");
  if (singleUrl.trim() !== "") {
    const t = validTarget({
      url: singleUrl,
      bearer: config?.pushBearer ?? process.env.MANIFOLD_PUSH_BEARER ?? null,
      timeoutMs: fallbackTimeoutMs,
    }, fallbackTimeoutMs);
    if (t) return t;
  }
  return null;
}

// event/at lead; harness + caller fields follow; extra merges last.
export function pushPayload({ event, harness = null, extra, ...fields } = {}) {
  return { event, at: Date.now(), harness, ...fields, ...(extra ?? {}) };
}

// POST the payload as JSON. Resolves {ok:true,status} on 2xx,
// {ok:false,…} otherwise — never throws. runner is injectable
// (url, init) => Response; defaults to global fetch.
export async function notifyPush(target, payload, logger = null, { runner } = {}) {
  if (!target?.url) return { ok: false, reason: "disabled" };
  const run = runner ?? ((url, init) => fetch(url, init));
  try {
    const headers = { "content-type": "application/json" };
    if (target.bearer) headers.authorization = `Bearer ${target.bearer}`;
    const res = await run(target.url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(timeoutOr(target.timeoutMs, DEFAULT_PUSH_TIMEOUT_MS)),
    });
    if (!res?.ok) {
      logger?.warn?.(`manifold: push ${target.url} -> ${res?.status}`);
      return { ok: false, reason: `http_${res?.status}` };
    }
    return { ok: true, status: res.status };
  } catch (err) {
    logger?.warn?.(`manifold: push ${target?.url} failed: ${err?.message ?? err}`);
    return { ok: false, reason: "error" };
  }
}
