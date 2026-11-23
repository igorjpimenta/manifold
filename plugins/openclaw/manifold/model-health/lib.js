// Pure health-tracking core of the circuit breaker — no plugin-SDK imports, so
// it loads outside the gateway (test.js).

export const DEFAULTS = Object.freeze({
  // Sliding window over which failures of one class count toward tripping.
  failureWindowMs: 60_000,
  // Terminal failures (billing / unavailable) within the window that block a
  // ref. 1 = the very next call after a billing error is already rerouted.
  failureThreshold: 1,
  // Rate-limit/overload failures within the window that quarantine a ref.
  // Above 1 so a single throttled call doesn't pull a model out of rotation.
  rateLimitThreshold: 3,
  // First quarantine length; doubles on each consecutive quarantine.
  rateLimitCooldownMs: 120_000,
  // Ceiling for the doubling, and how long after a quarantine ends before the
  // doubling resets to the base cooldown.
  rateLimitMaxCooldownMs: 30 * 60_000,
});

// Classifies raw provider error text (and an HTTP status when the caller has
// one) into the failure class that decides how a ref is taken out of rotation:
//   "terminal_billing"     402 / out of credits      → blocked until a probe clears it
//   "terminal_unavailable" 404 / model retired       → blocked until a probe clears it
//   "rate_limited"         429 / 503 / overloaded    → timed quarantine, self-expiring
//   null                   anything else (network blips, 5xx, bad request) → ignored
// Billing is tested first: a 429 whose body says the balance is exhausted must
// block, not quarantine — waiting it out never recovers.
export function classifyFailureText(status, bodyText) {
  const text = bodyText || "";
  if (status === 402 || /\b(?:insufficient|credit|billing|top up|out of credits?)\b/i.test(text)) {
    return "terminal_billing";
  }
  if (status === 404 || /\b(?:testing period|no longer available|discontinued|decommissioned|deprecated)\b/i.test(text)) {
    return "terminal_unavailable";
  }
  if (
    status === 429 || status === 503 || status === 529 ||
    /\brate[ _-]?limit|\btoo many requests\b|\boverloaded\b|\bservice unavailable\b|\btemporarily unavailable\b|\b(?:429|503|529)\b/i.test(text)
  ) {
    return "rate_limited";
  }
  return null;
}

// Probe verdict → "ok" | "billing" | "unavailable" | "inconclusive". A
// rate-limited probe is inconclusive: it says nothing about the balance or
// model availability the block is waiting on.
export function classifyProbe(status, ok, bodyText) {
  if (ok) return "ok";
  const kind = classifyFailureText(status, bodyText);
  if (kind === "terminal_billing") return "billing";
  if (kind === "terminal_unavailable") return "unavailable";
  return "inconclusive";
}

export function isTerminal(kind) {
  return kind === "terminal_billing" || kind === "terminal_unavailable";
}

// State is keyed by the full "provider/modelId" ref, never the bare provider:
// an agent chain routinely holds two models of one provider, and each must be
// taken out of rotation independently.
//
// Two kinds of entry in `health`:
//   blocked      { until: null }  — terminal; only a probe (or clear()) lifts it
//   quarantined  { until: ms }    — rate limit; expires on its own at `until`,
//                                   no probe, since a probe would spend the
//                                   same quota the provider is throttling.
// A terminal failure during a quarantine upgrades it to a block; rate-limit
// failures never downgrade a block.
export function createHealthTracker(opts = {}) {
  const o = { ...DEFAULTS, ...opts };
  const log = opts.logger;
  const health = new Map();
  // `${ref}|${class}` → timestamps of recent failures; one bucket per class
  // because the classes trip at different thresholds.
  const buckets = new Map();
  // ref → { level, endedAt } of the latest quarantine, for the doubling.
  const strikes = new Map();
  // ref → count of failures that arrived while the ref was ALREADY out of
  // rotation. Each one is a retry inside an already-running agent session:
  // before_model_resolve reroutes whole runs, so a new session never lands on
  // a quarantined model — only a session that started healthy can, and it then
  // stays pinned there for the rest of its retry ladder. Counted rather than
  // acted on, because no hook can interrupt an in-flight retry.
  const retryWhileDown = new Map();

  const bucketKey = (ref, cls) => `${ref}|${cls}`;

  function countFailure(ref, cls, now) {
    const key = bucketKey(ref, cls);
    const fresh = (buckets.get(key) ?? []).filter((t) => t >= now - o.failureWindowMs);
    fresh.push(now);
    buckets.set(key, fresh);
    return fresh.length;
  }

  function clearBuckets(ref) {
    buckets.delete(bucketKey(ref, "terminal"));
    buckets.delete(bucketKey(ref, "rate_limited"));
  }

  // Drops an expired quarantine; returns the live entry or undefined.
  function liveEntry(ref, now) {
    const e = health.get(ref);
    if (!e) return undefined;
    if (e.until != null && now >= e.until) {
      health.delete(ref);
      retryWhileDown.delete(ref);
      publish(ref);
      log?.info?.(`model-circuit-breaker: ${ref} quarantine expired → back in rotation`);
      return undefined;
    }
    return e;
  }

  // Every mutation of the durable state funnels through here. Persistence is
  // attached as a callback rather than reached for directly, so the tracker
  // stays a pure in-memory state machine and its tests need no filesystem.
  function publish(ref) {
    try {
      o.onChange?.(ref, health.get(ref), strikes.get(ref));
    } catch (err) {
      log?.warn?.(`model-circuit-breaker: onChange threw for ${ref}: ${err?.message ?? err}`);
    }
  }

  function block(ref, reason, now) {
    health.set(ref, { downSince: now, reason, until: null, probing: false });
    clearBuckets(ref);
    publish(ref);
  }

  // Returns what happened: "blocked" | "quarantined" | null.
  function noteFailure(ref, kind, now) {
    if (!kind) return null;
    const e = liveEntry(ref, now);
    if (e && e.until == null) return null;

    if (isTerminal(kind)) {
      const n = countFailure(ref, "terminal", now);
      if (n < o.failureThreshold) return null;
      block(ref, kind, now);
      log?.warn?.(`model-circuit-breaker: ${ref} BLOCKED (${n} non-transient errors in ${Math.round(o.failureWindowMs / 1000)}s; reason=${kind}); stays blocked until a symbolic probe confirms recovery`);
      return "blocked";
    }

    if (e) {
      // Already out of rotation and this session still hit it. No hook can
      // redirect a retry mid-ladder, so the only thing available is to make the
      // cost visible — and to log it once per episode rather than per attempt.
      const n = (retryWhileDown.get(ref) ?? 0) + 1;
      retryWhileDown.set(ref, n);
      if (n === 1) {
        log?.warn?.(
          `model-circuit-breaker: ${ref} was hit again while already ${e.until == null ? "blocked" : "quarantined"} — ` +
          `a session that started before this will keep retrying it for the rest of its ladder; ` +
          `new sessions are already routed around it`,
        );
      }
      return null;
    }
    const n = countFailure(ref, "rate_limited", now);
    if (n < o.rateLimitThreshold) return null;
    const prev = strikes.get(ref);
    const level = prev && now - prev.endedAt <= o.rateLimitMaxCooldownMs ? prev.level + 1 : 0;
    const cooldownMs = Math.min(o.rateLimitCooldownMs * 2 ** level, o.rateLimitMaxCooldownMs);
    const until = now + cooldownMs;
    health.set(ref, { downSince: now, reason: kind, until, probing: false });
    strikes.set(ref, { level, endedAt: until });
    clearBuckets(ref);
    publish(ref);
    log?.warn?.(`model-circuit-breaker: ${ref} QUARANTINED for ${Math.round(cooldownMs / 1000)}s (${n} rate-limit/overload errors in ${Math.round(o.failureWindowMs / 1000)}s; strike ${level + 1})`);
    return "quarantined";
  }

  // A clean output resets the failure counts but never lifts a block or a
  // quarantine: it can come from a different model the call was rerouted to.
  function noteSuccess(ref) {
    clearBuckets(ref);
  }

  return {
    noteFailure,
    noteSuccess,
    isHealthy: (ref, now) => !liveEntry(ref, now),
    has: (ref, now) => !!liveEntry(ref, now),
    // Probe targets: terminal blocks only.
    blockedEntries: () => [...health.entries()].filter(([, e]) => e.until == null),
    recover(ref) {
      health.delete(ref);
      clearBuckets(ref);
      retryWhileDown.delete(ref);
      publish(ref);
    },
    // Seed from durable storage at boot. Restores entries verbatim — a
    // quarantine keeps the deadline it was given rather than restarting its
    // cooldown, and a strike keeps its level so the doubling resumes instead
    // of resetting to the base cooldown on every restart.
    restore(entries = []) {
      for (const { ref: r, entry: e, strike } of entries) {
        if (!r || !e) continue;
        health.set(r, { ...e });
        if (strike) strikes.set(r, strike);
      }
    },
    // ref → failures seen while already down. Non-zero means in-flight
    // retries were pinned to an unhealthy model for the remainder of their
    // ladder; it is a cost measurement, not a lever.
    retryWhileDownStats() {
      return Object.fromEntries(retryWhileDown);
    },
    list(now) {
      const out = [];
      for (const ref of [...health.keys()]) {
        const e = liveEntry(ref, now);
        if (e) out.push({ ref, reason: e.reason, downSince: e.downSince, until: e.until, probing: e.probing });
      }
      return out;
    },
    markDown(ref, reason, now) {
      block(ref, reason, now);
    },
    clear(ref) {
      if (ref) {
        health.delete(ref);
        clearBuckets(ref);
        strikes.delete(ref);
        retryWhileDown.delete(ref);
        publish(ref);
      } else {
        health.clear();
        buckets.clear();
        strikes.clear();
        retryWhileDown.clear();
        publish(null);
      }
    },
  };
}
