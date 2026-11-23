import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DEFAULTS, classifyFailureText, classifyProbe, createHealthTracker } from "../../../../../plugins/openclaw/manifold/model-health/lib.js";

const ZAI = "zai/glm-4.5-flash";
const DS = "openrouter/deepseek/deepseek-v4-flash";

function silentLogger() {
  const lines = [];
  return { lines, info: (m) => lines.push(m), warn: (m) => lines.push(m) };
}

function trip(t, ref, kind, now, n) {
  let last = null;
  for (let i = 0; i < n; i++) last = t.noteFailure(ref, kind, now + i);
  return last;
}

describe("classifyFailureText", () => {
  it("classifies the runtime's rate-limit texts as rate_limited", () => {
    assert.equal(classifyFailureText(null, "⚠️ API rate limit reached. Please try again later."), "rate_limited");
    assert.equal(classifyFailureText(null, "429 Rate limit reached for requests"), "rate_limited");
    assert.equal(classifyFailureText(null, "Too Many Requests"), "rate_limited");
    assert.equal(classifyFailureText(null, "rate_limit_error"), "rate_limited");
  });

  it("classifies overload / 503 as rate_limited", () => {
    assert.equal(classifyFailureText(null, "Overloaded"), "rate_limited");
    assert.equal(classifyFailureText(null, "503 Service Unavailable"), "rate_limited");
    assert.equal(classifyFailureText(529, ""), "rate_limited");
  });

  it("billing wins over a 429 status", () => {
    assert.equal(classifyFailureText(429, "Insufficient balance or no resource package"), "terminal_billing");
  });

  it("keeps the terminal classes", () => {
    assert.equal(classifyFailureText(402, ""), "terminal_billing");
    assert.equal(classifyFailureText(null, "out of credits"), "terminal_billing");
    assert.equal(classifyFailureText(404, ""), "terminal_unavailable");
    assert.equal(classifyFailureText(null, "Thank you for participating in the testing period"), "terminal_unavailable");
  });

  it("ignores unrelated errors", () => {
    assert.equal(classifyFailureText(null, "socket hang up"), null);
    assert.equal(classifyFailureText(500, "Internal Server Error"), null);
    assert.equal(classifyFailureText(null, ""), null);
  });

  it("a rate-limited probe is inconclusive, never ok", () => {
    assert.equal(classifyProbe(429, false, "rate limit"), "inconclusive");
    assert.equal(classifyProbe(200, true, ""), "ok");
    assert.equal(classifyProbe(402, false, ""), "billing");
  });
});

describe("rate-limit quarantine", () => {
  it("quarantines after rateLimitThreshold failures in the window, not before", () => {
    const t = createHealthTracker();
    const now = 1_000_000;
    assert.equal(trip(t, ZAI, "rate_limited", now, DEFAULTS.rateLimitThreshold - 1), null);
    assert.equal(t.isHealthy(ZAI, now + 10), true);
    assert.equal(t.noteFailure(ZAI, "rate_limited", now + 10), "quarantined");
    assert.equal(t.isHealthy(ZAI, now + 11), false);
    assert.equal(t.isHealthy(DS, now + 11), true, "other refs unaffected");
  });

  it("failures older than the window don't count", () => {
    const t = createHealthTracker({ rateLimitThreshold: 2 });
    t.noteFailure(ZAI, "rate_limited", 0);
    assert.equal(t.noteFailure(ZAI, "rate_limited", DEFAULTS.failureWindowMs + 1), null);
  });

  it("expires on its own after the cooldown — no probe", () => {
    const log = silentLogger();
    const t = createHealthTracker({ logger: log });
    const now = 5_000_000;
    trip(t, ZAI, "rate_limited", now, 3);
    assert.deepEqual(t.blockedEntries(), [], "quarantines are never probe targets");
    const end = now + 2 + DEFAULTS.rateLimitCooldownMs;
    assert.equal(t.isHealthy(ZAI, end - 1), false);
    assert.equal(t.isHealthy(ZAI, end), true);
    assert.ok(log.lines.some((l) => l.includes("quarantine expired")));
  });

  it("failures during a quarantine (in-flight retries) don't extend it", () => {
    const t = createHealthTracker();
    const now = 0;
    trip(t, ZAI, "rate_limited", now, 3);
    const [{ until }] = t.list(now + 5);
    assert.equal(trip(t, ZAI, "rate_limited", now + 100, 10), null);
    assert.equal(t.list(now + 200)[0].until, until);
  });

  it("doubles on consecutive quarantines, capped, and resets after a quiet period", () => {
    const base = 1000, max = 4000;
    const t = createHealthTracker({ rateLimitThreshold: 1, rateLimitCooldownMs: base, rateLimitMaxCooldownMs: max });
    const lengths = [];
    let now = 0;
    for (let i = 0; i < 4; i++) {
      t.noteFailure(ZAI, "rate_limited", now);
      const [{ until }] = t.list(now);
      lengths.push(until - now);
      now = until; // re-trips the moment it's back
    }
    assert.deepEqual(lengths, [1000, 2000, 4000, 4000]);

    now += max + 1; // quiet for longer than the max cooldown
    t.noteFailure(ZAI, "rate_limited", now);
    assert.equal(t.list(now)[0].until - now, base);
  });

  it("a terminal failure upgrades a quarantine to a probe-gated block", () => {
    const t = createHealthTracker();
    trip(t, ZAI, "rate_limited", 0, 3);
    assert.equal(t.noteFailure(ZAI, "terminal_billing", 10), "blocked");
    const [entry] = t.list(10);
    assert.equal(entry.until, null);
    assert.equal(entry.reason, "terminal_billing");
    assert.equal(t.isHealthy(ZAI, 10 * DEFAULTS.rateLimitMaxCooldownMs), false, "never expires");
    assert.equal(t.blockedEntries().length, 1);
  });

  it("rate limits never downgrade a block", () => {
    const t = createHealthTracker();
    t.noteFailure(ZAI, "terminal_billing", 0);
    assert.equal(trip(t, ZAI, "rate_limited", 1, 5), null);
    assert.equal(t.list(2)[0].until, null);
  });

  it("a success resets the failure count", () => {
    const t = createHealthTracker();
    trip(t, ZAI, "rate_limited", 0, 2);
    t.noteSuccess(ZAI);
    assert.equal(t.noteFailure(ZAI, "rate_limited", 3), null);
  });
});

describe("terminal block", () => {
  it("blocks on the first terminal failure by default and stays until recover()", () => {
    const t = createHealthTracker();
    assert.equal(t.noteFailure(DS, "terminal_unavailable", 0), "blocked");
    assert.equal(t.isHealthy(DS, Number.MAX_SAFE_INTEGER), false);
    t.recover(DS);
    assert.equal(t.isHealthy(DS, 1), true);
  });

  it("ignores null (unclassified) failures", () => {
    const t = createHealthTracker({ failureThreshold: 1, rateLimitThreshold: 1 });
    assert.equal(t.noteFailure(DS, null, 0), null);
    assert.equal(t.isHealthy(DS, 1), true);
  });

  it("clear() lifts blocks and quarantines", () => {
    const t = createHealthTracker();
    t.markDown(DS, "manual", 0);
    trip(t, ZAI, "rate_limited", 0, 3);
    t.clear();
    assert.deepEqual(t.list(1), []);
  });
});
