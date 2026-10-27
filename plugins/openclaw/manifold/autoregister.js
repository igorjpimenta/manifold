// autoregister.js — self-wiring: on gateway boot the adapter registers its
// own push target in core, so no operator hand-registers a webhook URL.
//
// Preconditions (else it logs the exact config snippet and skips):
//   - webhookBearer configured — the shared secret core sends as
//     Authorization: Bearer on POST /api/manifold/pushed;
//   - the gateway's public base URL (pluginConfig webhookPublicBaseUrl,
//     else http://127.0.0.1:<gateway port>/api/manifold/pushed).
// Registration is idempotent (upsert) and verified with a test ping.
// Never throws: boot must not fail because a push target couldn't register.
import { notifyPush } from "../../../events/push.js";

export function autoregisterConfig(pluginConfig = {}, apiConfig = {}) {
  const bearer = pluginConfig.webhookBearer ?? "";
  const baseUrl =
    pluginConfig.webhookPublicBaseUrl ??
    (apiConfig?.gateway?.port ? `http://127.0.0.1:${apiConfig.gateway.port}` : null);
  const harness = pluginConfig.webhookHarness ?? "openclaw";
  const enabled = pluginConfig.webhookAutoregister ?? true;
  return { bearer, baseUrl, harness, enabled };
}

export function manualConfig({ baseUrl, harness, bearer }) {
  const url = `${baseUrl?.replace(/\/$/, "") ?? "<gateway-url>"}/api/manifold/pushed`;
  return `set plugins.entries.manifold.pushUrls = { "${harness}": { "url": "${url}", "bearer": "${bearer ? "<same-as-webhookBearer>" : "<secret>"}" } }`;
}

// pushTargetsStore is core's state/push-targets.js store (upsert/get).
// Returns {ok:true} | {ok:false, reason, manual}.
export async function autoregisterPushTarget({ pushTargetsStore, pluginConfig = {}, apiConfig = {}, logger = null } = {}) {
  try {
    const cfg = autoregisterConfig(pluginConfig, apiConfig);
    if (!cfg.enabled) return { ok: false, reason: "disabled" };
    if (!cfg.bearer) {
      return { ok: false, reason: "disarmed", manual: "set webhookBearer in the manifold plugin config first" };
    }
    if (!cfg.baseUrl) {
      return { ok: false, reason: "no-base-url", manual: manualConfig({ ...cfg, baseUrl: null }) };
    }
    const url = `${cfg.baseUrl.replace(/\/$/, "")}/api/manifold/pushed`;
    await pushTargetsStore.upsert({ harness: cfg.harness, url, bearer: cfg.bearer });
    const tested = await notifyPush({
      target: { url, bearer: cfg.bearer, timeoutMs: 3000 },
      payload: { event: "webhook.test", at: Date.now() },
      logger,
    });
    if (!tested.ok) {
      logger?.warn?.(`manifold: push target registered but test ping failed: ${tested.reason}`);
      return { ok: false, reason: "test-failed", manual: manualConfig({ ...cfg, baseUrl: cfg.baseUrl }) };
    }
    logger?.info?.(`manifold: push target self-registered for harness ${cfg.harness} -> ${url}`);
    return { ok: true };
  } catch (err) {
    logger?.warn?.(`manifold: push target self-registration failed: ${err?.message ?? err}`);
    return { ok: false, reason: "error" };
  }
}
