// Durability of model-routing quarantine. The in-memory tracker is unchanged
// and already covered in test.js; what matters here is that a quarantine
// survives a process restart, that an expired one does not come back, and that
// the failure buckets stay in memory because their window makes them useless
// to persist.
import { test, describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { createModelHealthStore } from "../../../../../plugins/openclaw/manifold/model-health/store.js";
import { createHealthTracker, DEFAULTS } from "../../../../../plugins/openclaw/manifold/model-health/lib.js";

function tmpDb() {
  return path.join(os.tmpdir(), `model-health-store-test-${randomUUID()}.sqlite`);
}
const dbs = new Set();
function db() { const p = tmpDb(); dbs.add(p); return p; }

after(() => { for (const p of dbs) { try { rmSync(p, { force: true }); } catch {} } });

function tracker(opts = {}) {
  const changes = [];
  const t = createHealthTracker({
    ...opts,
    onChange: (ref, entry, strike) => changes.push({ ref, entry, strike }),
  });
  return { t, changes };
}

const RATE = "rate_limited";

describe("model-health-store — quarantine survives a restart", () => {
  it("a rate-limit quarantine written by one generation is restored by the next", async () => {
    const dbPath = db();
    const first = tracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 10 * 60_000 });
    first.t.noteFailure("zai/glm-4.5-flash", RATE, Date.now());
    assert.ok(first.t.has("zai/glm-4.5-flash", Date.now()), "quarantined in-process");

    const w = await createModelHealthStore({ dbPath });
    for (const c of first.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();
    w.remove; // no-op guard for API symmetry

    // A second generation: fresh tracker, same store file.
    const loaded = await w.load(Date.now());
    assert.equal(loaded.length, 1, "the row was readable after the writer flushed");
    assert.equal(loaded[0].ref, "zai/glm-4.5-flash");
    assert.ok(loaded[0].entry.until != null, "restored as a quarantine, not a block");

    const second = tracker({});
    second.t.restore(loaded);
    assert.equal(second.t.has("zai/glm-4.5-flash", Date.now()), true,
      "the model is still out of rotation after a restart");
  });

  it("restores the quarantine's ORIGINAL deadline, not a fresh cooldown", async () => {
    const dbPath = db();
    const t0 = 1_000_000;
    const t = tracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 5 * 60_000 });
    t.t.noteFailure("p/m", RATE, t0);
    const expectedUntil = t0 + 5 * 60_000;

    const w = await createModelHealthStore({ dbPath });
    for (const c of t.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();

    const [row] = await w.load(expectedUntil - 1000);
    assert.equal(row.entry.until, expectedUntil, "deadline survives verbatim");
    assert.equal(t.t.has("p/m", expectedUntil - 1000), true, "still quarantined one second before expiry");
    assert.equal(t.t.has("p/m", expectedUntil + 1), false, "back in rotation one second after");
  });

  it("a terminal block restores as a block (no deadline, probe-only)", async () => {
    const dbPath = db();
    const t = tracker({ failureThreshold: 1 });
    t.t.noteFailure("p/dead", "terminal_billing", Date.now());
    const w = await createModelHealthStore({ dbPath });
    for (const c of t.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();

    const [row] = await w.load(Date.now());
    assert.equal(row.entry.until, null, "a block carries no deadline");
    assert.equal(row.entry.reason, "terminal_billing", "and keeps its reason for the probe/operator");
  });

  it("a quarantine that expired while the process was down is dropped, not restored", async () => {
    const dbPath = db();
    const t = tracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 60_000 });
    const then = Date.now() - 10 * 60_000; // quarantined 10 min ago, 9 min past deadline
    t.t.noteFailure("p/stale", RATE, then);
    const w = await createModelHealthStore({ dbPath });
    for (const c of t.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();

    const live = await w.load(Date.now());
    assert.equal(live.length, 0, "an expired quarantine must not come back and re-block the model");
    const rows = new DatabaseSync(dbPath, { readOnly: true })
      .prepare("SELECT count(*) n FROM model_health").get().n;
    assert.equal(rows, 0, "and the stale row is cleaned up rather than left to be re-checked forever");
  });

  it("a block is never dropped by age, since it has no deadline", async () => {
    const dbPath = db();
    const t = tracker({ failureThreshold: 1 });
    t.t.noteFailure("p/old-block", "terminal_billing", Date.now() - 90 * 24 * 3600_000);
    const w = await createModelHealthStore({ dbPath });
    for (const c of t.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();
    const live = await w.load(Date.now());
    assert.equal(live.length, 1, "a 90-day-old block is still a block until a probe or an explicit clear");
  });

  it("the strike level is persisted, so cooldown doubling resumes instead of resetting", async () => {
    const dbPath = db();
    const t0 = Date.now();
    const t = tracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 1000, rateLimitMaxCooldownMs: 60_000 });
    // Two quarantine episodes -> level 1, i.e. a doubled cooldown.
    t.t.noteFailure("p/m", RATE, t0);
    t.t.noteFailure("p/m", RATE, t0 + 5_000);
    const last = t.changes[t.changes.length - 1];
    assert.equal(last.strike.level, 1, "precondition: the second episode doubled");

    const w = await createModelHealthStore({ dbPath });
    w.put(last.ref, last.entry, last.strike);
    await w.flush();

    const [row] = await w.load(Date.now());
    assert.equal(row.strike.level, 1, "strike level round-trips, so the next episode doubles again");
  });

  it("one active row per ref — a repeat write does not stack duplicates", async () => {
    const dbPath = db();
    const t = tracker({ rateLimitThreshold: 1 });
    t.t.noteFailure("p/m", RATE, Date.now());
    const w = await createModelHealthStore({ dbPath });
    const c = t.changes[0];
    w.put(c.ref, c.entry, c.strike);
    w.put(c.ref, c.entry, c.strike);
    w.put(c.ref, c.entry, c.strike);
    await w.flush();
    const rows = new DatabaseSync(dbPath, { readOnly: true })
      .prepare("SELECT count(*) n FROM model_health WHERE ref='p/m'").get().n;
    assert.equal(rows, 1);
  });

  it("failure buckets are NOT persisted — their window outlives their usefulness", async () => {
    // failureWindowMs defaults to 60s, so every bucket entry is stale by the
    // time a restart could read it. Persisting them would restore nothing and
    // risk a spurious trip, so only health + strikes reach the store.
    const dbPath = db();
    const t = tracker({ rateLimitThreshold: 5, failureWindowMs: 60_000 });
    t.t.noteFailure("p/m", RATE, Date.now()); // 1 of 5: counts, but does not trip
    const w = await createModelHealthStore({ dbPath });
    for (const c of t.changes) w.put(c.ref, c.entry, c.strike);
    await w.flush();
    const live = await w.load(Date.now());
    assert.equal(live.length, 0, "an untripped threshold writes nothing durable");
  });
});

// The residual gap: before_model_resolve reroutes a whole run, so a session
// that starts healthy is the only thing that can land on an already-quarantined
// model — and no hook can interrupt its retry ladder. These pin the counter
// that makes that cost visible.
describe("retryWhileDown — the cost of an in-flight retry on a quarantined model", () => {
  it("counts a failure that arrives while the ref is already quarantined", () => {
    const warns = [];
    const t = createHealthTracker({
      rateLimitThreshold: 1, rateLimitCooldownMs: 60_000, rateLimitMaxCooldownMs: 600_000,
      logger: { info: () => {}, warn: (m) => warns.push(m) },
    });
    const now = Date.now();
    t.noteFailure("p/m", RATE, now);          // trips the quarantine
    t.noteFailure("p/m", RATE, now + 100);    // a retry inside the same session
    t.noteFailure("p/m", RATE, now + 200);
    assert.equal(t.retryWhileDownStats()["p/m"], 2, "two retries landed on the quarantined ref");
    assert.equal(t.has("p/m", now + 300), true, "and it stays out of rotation");
  });

  it("logs the condition once per episode, not once per attempt", () => {
    const warns = [];
    const t = createHealthTracker({
      rateLimitThreshold: 1, rateLimitCooldownMs: 60_000, rateLimitMaxCooldownMs: 600_000,
      logger: { info: () => {}, warn: (m) => warns.push(m) },
    });
    const now = Date.now();
    t.noteFailure("p/m", RATE, now);
    for (let i = 0; i < 5; i++) t.noteFailure("p/m", RATE, now + 100 + i);
    const stuck = warns.filter((m) => /already (blocked|quarantined)/.test(m));
    assert.equal(stuck.length, 1, "one warning for five retries — an in-flight retry is expected, not novel");
    assert.match(stuck[0], /new sessions are already routed around it/);
  });

  it("does NOT count the failure that trips the quarantine in the first place", () => {
    const t = createHealthTracker({ rateLimitThreshold: 2, rateLimitCooldownMs: 60_000 });
    const now = Date.now();
    t.noteFailure("p/m", RATE, now);
    assert.equal(t.retryWhileDownStats()["p/m"], undefined, "the tripping attempt is not a retry-while-down");
    t.noteFailure("p/m", RATE, now + 50);    // second one trips it
    assert.equal(t.retryWhileDownStats()["p/m"], undefined);
    t.noteFailure("p/m", RATE, now + 100);   // now it is a retry
    assert.equal(t.retryWhileDownStats()["p/m"], 1);
  });

  it("resets the count when the model returns to rotation", () => {
    const t = createHealthTracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 1_000 });
    const now = Date.now();
    t.noteFailure("p/m", RATE, now);
    t.noteFailure("p/m", RATE, now + 10);
    assert.equal(t.retryWhileDownStats()["p/m"], 1);
    // Let the quarantine expire, then trip it again in a later episode.
    assert.equal(t.has("p/m", now + 2_000), false);
    t.noteFailure("p/m", RATE, now + 3_000);
    assert.equal(t.retryWhileDownStats()["p/m"], undefined, "a new episode counts from zero");
  });

  it("clear() wipes the counter alongside the rest of the state", () => {
    const t = createHealthTracker({ rateLimitThreshold: 1, rateLimitCooldownMs: 60_000 });
    const now = Date.now();
    t.noteFailure("p/m", RATE, now);
    t.noteFailure("p/m", RATE, now + 10);
    t.clear();
    assert.deepEqual(t.retryWhileDownStats(), {});
  });
});
