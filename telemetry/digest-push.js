// digest-push.js — APNs delivery for one-way pipeline digests
// (weekly-stats, reply-sla, ...). Pipeline `code` nodes run as plain
// imported modules with no `api` in scope, so this module holds its own
// `configureApi()`-set singleton to reach api.runtime.apns (wired in
// registerTelemetry). Kind namespace is manifold.*, matching what the
// app routes on.
let SHARED_API = null;
export function configureApi(api) { SHARED_API = api; }

function digestPayload({ title, body }) {
  return {
    aps: {
      alert: { title, body },
      sound: "default",
      "content-available": 1,
      category: "manifold.digest",
      "thread-id": "digest",
    },
    kind: "manifold.digest",
  };
}

/** Push a title/body digest alert to every registered device. Returns
 *  {ok, sent, total, reason} — same shape as budget-alert's fireAPNs, so
 *  callers can log a clear reason when nothing was delivered. */
export async function sendDigestPush({ title, body, logger }) {
  const apns = SHARED_API?.runtime?.apns;
  if (!apns || typeof apns.send !== "function") return { ok: false, reason: "not_configured" };
  if (typeof apns.isConfigured === "function" && !apns.isConfigured()) return { ok: false, reason: "not_configured" };
  const tokens = typeof apns.listTokens === "function" ? apns.listTokens() : [];
  if (!tokens.length) return { ok: false, reason: "no_tokens" };

  const payload = digestPayload({ title, body });
  let sent = 0;
  for (const t of tokens) {
    let result;
    try {
      result = await apns.send({
        deviceToken: t.apns_token,
        environment: t.environment,
        payload,
        pushType: "alert",
        priority: 10,
      });
    } catch (err) {
      logger?.warn?.(`digest-push: APNs threw: ${err?.message ?? err}`);
      result = { ok: false, reason: "exception" };
    }
    if (result?.ok) {
      sent++;
      continue;
    }
    logger?.warn?.(`digest-push: send failed status=${result?.status} reason=${result?.reason ?? "?"}`);
    if (result?.reason === "BadDeviceToken"
        || result?.reason === "Unregistered"
        || result?.reason === "BadEnvironmentKeyInToken") {
      try { apns.dropToken?.({ apnsToken: t.apns_token }); } catch {}
    }
  }

  if (sent > 0) return { ok: true, sent, total: tokens.length };
  return { ok: false, reason: "all_failed", total: tokens.length };
}
