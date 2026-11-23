// model-health — takes failing models out of an agent's rotation.
//
// Host-side by nature: model choice belongs to the harness, which manifold
// only delegates to (core never calls out). It lives with the openclaw adapter
// because that is the module that owns this host's hooks, and because
// manifold's runtime schema already carries model_health.

import fs from "fs";
import path from "path";
import { DEFAULTS, classifyFailureText, classifyProbe, createHealthTracker } from "./lib.js";
import { createModelHealthStore } from "./store.js";

// How often the background probe re-checks a blocked ref for recovery.
const DEFAULT_PROBE_INTERVAL_MS = 5 * 60_000;
// Hard ceiling on a single symbolic probe call.
const PROBE_TIMEOUT_MS = 15_000;

// Module-level, so every register() call shares one tracker and one probe
// chain: the adapter registers on each gateway load, and a per-register chain
// would leak a permanent timer (each firing a real fetch) per load.
let tracker = null;
// Single in-flight hydration across repeated register() calls on the same
// generation (a second call would otherwise re-open the store and re-seed).
let hydratePromise = null;
let probeChainStarted = false;
let probeStopped = false;

function resolveAgentChain(cfg, agentId) {
  const list = cfg?.agents?.list ?? [];
  const agent = list.find((a) => a?.id === agentId);
  const model = agent?.model ?? cfg?.agents?.defaults?.model;
  if (!model) return [];
  if (typeof model === "string") return [model];
  const fallbacks = Array.isArray(model.fallbacks) ? model.fallbacks : [];
  return [model.primary, ...fallbacks].filter((r) => typeof r === "string" && r.length > 0);
}

function splitRef(ref) {
  const [provider, ...rest] = ref.split("/");
  return { provider, modelId: rest.join("/") };
}

function pickHealthyFromChain(chain, agentId, now) {
  for (const ref of chain) {
    const { provider } = splitRef(ref);
    if (tracker.isHealthy(ref, now) && !isAuthStateCooldown(provider, agentId)) return ref;
  }
  return null;
}

// Returns true when openclaw's own auth-state.json marks the provider's
// profile in a billing cooldown — this catches the case where the call
// never reaches llm_output because auth resolution fails first ("no
// available auth profile … all in cooldown"). Without this check the
// circuit breaker's before_model_resolve would keep sending calls to a
// provider whose profile the runtime has already disabled.
function isAuthStateCooldown(provider, agentId) {
  const home = process.env.HOME || process.env.USERPROFILE || "/root";
  const candidates = agentId
    ? [
        path.join(home, `.openclaw/agents/${agentId}/agent/auth-state.json`),
        path.join(home, ".openclaw/agents/main/agent/auth-state.json"),
      ]
    : [path.join(home, ".openclaw/agents/main/agent/auth-state.json")];
  for (const filePath of candidates) {
    try {
      const state = JSON.parse(fs.readFileSync(filePath, "utf8"));
      const stats = state?.usageStats ?? {};
      for (const [profileId, entry] of Object.entries(stats)) {
        // Match profiles owned by this provider (e.g. "anthropic:default")
        if (!profileId.startsWith(`${provider}:`)) continue;
        const until = entry?.disabledUntil;
        if (typeof until === "number" && Date.now() < until) return true;
      }
      break; // found a readable file for this agent; don't fall through to main
    } catch {
      // file missing or unreadable — continue to next candidate
    }
  }
  return false;
}

// Resolve a provider's apiKey from config (`${ENV}` placeholder) or the
// conventional <PROVIDER>_API_KEY env var (e.g. openrouter has no inline key).
function resolveApiKey(provider, raw) {
  if (typeof raw === "string" && raw.length > 0) {
    const m = raw.match(/^\$\{([A-Z0-9_]+)\}$/);
    if (m) return process.env[m[1]] || null;
    if (!raw.includes("${")) return raw; // literal inline key
  }
  const envName = provider.toUpperCase().replace(/[^A-Z0-9]+/g, "_") + "_API_KEY";
  return process.env[envName] || null;
}

// Build the minimal HTTP call needed to ask a specific model "are you back?".
// Targets the exact blocked model: a provider entry commonly lists several
// models, and probing a sibling says nothing about the one that's down. Falls
// back to models[0] only if the exact model isn't listed in config. Returns
// null when the ref can't be probed (missing baseUrl/key/model) — it then
// stays blocked until a manual clear().
function resolveProbeCall(cfg, ref) {
  const { provider, modelId } = splitRef(ref);
  const def = cfg?.models?.providers?.[provider];
  if (!def || !def.baseUrl) return null;
  const apiKey = resolveApiKey(provider, def.apiKey);
  if (!apiKey) return null;
  const modelList = Array.isArray(def.models) ? def.models : [];
  const model = modelList.find((m) => m?.id === modelId)?.id ?? modelList[0]?.id;
  if (!model) return null;
  return { baseUrl: String(def.baseUrl).replace(/\/+$/, ""), apiKey, model, api: def.api || "openai" };
}

async function symbolicProbe(call) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), PROBE_TIMEOUT_MS);
  try {
    let url, headers, body;
    if (call.api === "anthropic-messages") {
      url = `${call.baseUrl}/messages`;
      headers = { "content-type": "application/json", "x-api-key": call.apiKey, "anthropic-version": "2023-06-01" };
      body = JSON.stringify({ model: call.model, max_tokens: 1, messages: [{ role: "user", content: "." }] });
    } else if (call.api === "openai-completions") {
      url = `${call.baseUrl}/completions`;
      headers = { "content-type": "application/json", authorization: `Bearer ${call.apiKey}` };
      body = JSON.stringify({ model: call.model, max_tokens: 1, prompt: "." });
    } else {
      url = `${call.baseUrl}/chat/completions`;
      headers = { "content-type": "application/json", authorization: `Bearer ${call.apiKey}` };
      body = JSON.stringify({ model: call.model, max_tokens: 1, messages: [{ role: "user", content: "." }] });
    }
    const res = await fetch(url, { method: "POST", headers, body, signal: ctrl.signal });
    const text = res.ok ? "" : await res.text().catch(() => "");
    return classifyProbe(res.status, res.ok, text);
  } catch {
    return "inconclusive"; // network/timeout — unknown, keep the block
  } finally {
    clearTimeout(t);
  }
}

// Named exports for offline unit testing of the pure/probe helpers.
export { classifyFailureText, classifyProbe, resolveApiKey, resolveProbeCall, symbolicProbe };

function intOr(v, d) {
  return Number.isInteger(v) ? v : d;
}

export function registerModelHealth(api, userCfg = {}) {
  const cfg = userCfg ?? {};
  if (cfg.enabled === false) {
    api.logger.info("model-health: disabled in config; skipping registration");
    return;
  }
  const probeIntervalMs = intOr(cfg.probeIntervalMs, DEFAULT_PROBE_INTERVAL_MS);

    // Mutable box, because register() is synchronous: the store is built and
    // drained asynchronously, and onChange is handed to the tracker before the
    // store exists. A transition landing in that window is simply not
    // persisted — in-memory state is still correct for this process, and the
    // next failure re-trips it.
    const storeBox = { store: null };
    const persist = (ref, entry, strike) => {
      const st = storeBox.store;
      if (!st) return;
      if (entry == null) { if (ref) st.remove(ref); else st.clearAll(); return; }
      st.put(ref, entry, strike);
    };

    tracker ??= createHealthTracker({
      onChange: persist,
      failureWindowMs: intOr(cfg.failureWindowMs, DEFAULTS.failureWindowMs),
      failureThreshold: intOr(cfg.failureThreshold, DEFAULTS.failureThreshold),
      rateLimitThreshold: intOr(cfg.rateLimitThreshold, DEFAULTS.rateLimitThreshold),
      rateLimitCooldownMs: intOr(cfg.rateLimitCooldownMs, DEFAULTS.rateLimitCooldownMs),
      rateLimitMaxCooldownMs: intOr(cfg.rateLimitMaxCooldownMs, DEFAULTS.rateLimitMaxCooldownMs),
      logger: api.logger,
    });

    // Durable state is the whole point: without it a restart returns every
    // quarantined model to "healthy" and the provider re-throttles it. Runs on
    // service start, so merely registering (unit tests, a config reload that
    // never boots the gateway) touches no database and arms no timer.
    const hydrate = () => (hydratePromise ??= (async () => {
      try {
        const st = await createModelHealthStore({ logger: api.logger });
        const now = Date.now();
        const live = await st.load(now);
        // Only seed a tracker that has taken no decision yet, so a failure
        // observed during this await is never overwritten by a stale row.
        if (tracker.list(now).length === 0 && live.length > 0) {
          tracker.restore(live);
          api.logger.info(
            `model-health: restored ${live.length} down model(s) across restarts ` +
            `(${live.filter((e) => e.entry.until != null).length} still within quarantine)`,
          );
        }
        storeBox.store = st;
      } catch (err) {
        // Fail open: the breaker is an optimisation, and losing durability
        // must not stop it working in-process.
        api.logger.warn(
          `model-health: durable state unavailable (${err?.message ?? err}); ` +
          `quarantines will not survive a restart this generation`,
        );
      }
    })());

    if (typeof api.on !== "function") {
      api.logger.warn("model-health: api.on unavailable; not registering hooks");
      return;
    }

    // Detection: llm_output is the only hook that surfaces the raw provider
    // error string (the runtime classifies billing 402s at the semantic layer,
    // so model_call_ended/agent_end look "successful"). It fires once per
    // model attempt, so in-provider retries each count toward a threshold.
    // Keyed by event.resolvedRef, which disambiguates two models sharing a
    // provider, falling back to provider/model, then the bare provider.
    api.on("llm_output", (event /*, ctx */) => {
      try {
        const provider = event?.provider;
        if (!provider) return;
        const ref = event?.resolvedRef ?? (event?.model ? `${provider}/${event.model}` : provider);
        const last = event?.lastAssistant;
        if (!last || last.stopReason !== "error") {
          if (Array.isArray(event?.assistantTexts) && event.assistantTexts.length > 0) {
            tracker.noteSuccess(ref);
          }
          return;
        }
        const errorMessage = typeof last.errorMessage === "string" ? last.errorMessage : "";
        tracker.noteFailure(ref, classifyFailureText(null, errorMessage), Date.now());
      } catch (err) {
        api.logger?.warn?.(`model-health: llm_output handler threw: ${err?.message ?? err}`);
      }
    });

    // Routing: redirect an agent whose primary is blocked or quarantined to the
    // first healthy entry in its chain. Respects explicit caller overrides (a
    // pinned model is a deliberate choice, not the default fallback ladder).
    api.on("before_model_resolve", (event, ctx) => {
      try {
        const agentId = ctx?.agentId;
        if (!agentId) return;
        const chain = resolveAgentChain(api.config, agentId);
        if (chain.length < 2) return;
        const { provider: chainProvider, modelId: chainModel } = splitRef(chain[0]);
        const ctxProvider = ctx?.modelProviderId ?? ctx?.provider;
        const ctxModel = ctx?.modelId ?? ctx?.model;
        if (ctxProvider && ctxModel && (ctxProvider !== chainProvider || ctxModel !== chainModel)) {
          return;
        }
        const selected = pickHealthyFromChain(chain, agentId, Date.now());
        if (!selected || selected === chain[0]) return;
        const { provider, modelId } = splitRef(selected);
        if (!provider || !modelId) return;
        api.logger.info(`model-health: agent=${agentId} routing ${chain[0]} → ${selected} (primary out of rotation)`);
        return { providerOverride: provider, modelOverride: modelId };
      } catch (err) {
        api.logger?.warn?.(`model-health: before_model_resolve threw: ${err?.message ?? err}`);
      }
    });

    // Recovery for terminal blocks: a single self-rescheduling loop. Each tick
    // fires one symbolic call at every blocked ref; a clean response is the
    // ONLY thing that lifts a block. Quarantines are not probed — they expire
    // on their own. runtime.setTimer is one-shot (setTimeout-equivalent), so
    // we re-arm at the end of each tick — no overlapping ticks, survives a
    // slow probe. Probes run off the request path, so no real call ever eats
    // a recovery 402.
    const schedule = api.runtime?.setTimer
      ? (fn, ms) => api.runtime.setTimer(fn, ms)
      // unref: the probe loop must never be the reason a process stays alive
      // (a loaded-but-never-stopped plugin would otherwise pin the event loop).
      : (fn, ms) => {
          const t = setTimeout(fn, ms);
          t.unref?.();
          return t;
        };

    const probeTick = async () => {
      if (probeStopped) return;
      for (const [ref, entry] of tracker.blockedEntries()) {
        if (entry.probing) continue;
        const call = resolveProbeCall(api.config, ref);
        if (!call) {
          api.logger?.warn?.(`model-health: ${ref} blocked but NOT probeable (no baseUrl/key/model) — clear() it manually once funded`);
          continue;
        }
        entry.probing = true;
        try {
          const verdict = await symbolicProbe(call);
          if (verdict === "ok") {
            tracker.recover(ref);
            api.logger.info(`model-health: ${ref} RECOVERED (symbolic probe ok) → back in rotation`);
          } else {
            api.logger.info(`model-health: ${ref} still blocked (probe=${verdict})`);
          }
        } catch (err) {
          api.logger?.warn?.(`model-health: probe for ${ref} threw: ${err?.message ?? err}`);
        } finally {
          entry.probing = false;
        }
      }
      if (!probeStopped) schedule(probeTick, probeIntervalMs);
    };

    const startProber = () => {
      if (probeChainStarted) return;
      probeChainStarted = true;
      probeStopped = false;
      schedule(probeTick, probeIntervalMs);
    };
    const stopProber = () => {
      probeStopped = true;
      probeChainStarted = false;
    };

    if (typeof api.registerService === "function") {
      api.registerService({
        id: "manifold-model-health-prober",
        start: () => { hydrate(); startProber(); },
        stop: stopProber,
      });
    } else {
      hydrate();
      startProber();
    }

    // Runtime surface — inspection + manual override from sibling plugins or a
    // CLI shim. Keyed by the full "provider/modelId" ref. `until` is null for
    // a probe-gated block, an epoch-ms expiry for a rate-limit quarantine.
    api.runtime ??= {};
    api.runtime.modelCircuitBreaker = {
      listDown() {
        return tracker.list(Date.now()).map((e) => ({ ...e, ...splitRef(e.ref) }));
      },
      markDown(ref, reason = "manual") {
        tracker.markDown(ref, reason, Date.now());
      },
      clear(ref) {
        tracker.clear(ref);
      },
      _classifyProbe: classifyProbe,
      _classifyFailureText: classifyFailureText,
      _symbolicProbe: symbolicProbe,
      _resolveProbeCall: (ref) => resolveProbeCall(api.config, ref),
    };

    api.logger.info(`model-health: registered (terminal errors block until probe, probe every ${Math.round(probeIntervalMs / 60_000)}min; rate limits quarantine with doubling cooldown)`);
}
