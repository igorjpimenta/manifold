// Daily-budget guard. Called from the before_message_write hook after each
// cost insert. When today's total exceeds CAP_USD, fires an APNs push
// alert to the operator's device(s) (once per day — debounced via
// lastAlertedLocalDay).

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { displayRootLabel } from "./labels.js";
import { execGet } from "./http-helpers.js";
import { NotificationKinds } from "../ios/notifications.js";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import { createMigrator } from "../state/migrator.js";

const DEFAULT_CAP_USD = 3.0;
const DEFAULT_LOCK_THRESHOLD = 0.8;  // fraction of cap; operator-configurable via iOS

// Thresholds — fraction of cap. Each fires at most once per local day.
// 50% lets the operator intervene before damage is done; 80% is the
// "approaching" warning; 100% means we've crossed the line.
const THRESHOLDS = [0.1, 0.5, 0.8, 1.0];

// Persisted across gateway restarts so we don't re-alert on every reload of
// the same already-over-budget day. The once-per-day-per-threshold debounce
// is the only silencing mechanism — no operator-facing snooze (removed:
// redundant with the debounce, no clear product job).
const STATE_FILE = path.join(os.homedir(), ".openclaw", "lineage", ".alerted-thresholds.json");
// Legacy single-day flag from the v1 single-threshold alert; migrated on read.
const LEGACY_DAY_FILE = path.join(os.homedir(), ".openclaw", "lineage", ".last-alerted-day");
// v1 cap/threshold store — flat file, single current value, no history. Kept
// only as a one-time seed source for cap_versions on first-ever init so
// upgrading installs keep their operator-set cap instead of reverting to
// DEFAULT_CAP_USD. No longer written to.
const LEGACY_CAP_FILE = path.join(os.homedir(), ".openclaw", "lineage", ".cap.json");
// Same physical file budget/index.js's gate_state lives in — each module
// owns its own tables there via its own migrator `name`, same convention.
const DB_PATH = path.join(os.homedir(), ".openclaw", "ops.sqlite");

const CAP_MIGRATIONS = [
  `CREATE TABLE IF NOT EXISTS cap_versions (
     id             TEXT    NOT NULL PRIMARY KEY,
     cap_usd        REAL    NOT NULL,
     lock_threshold REAL    NOT NULL,
     registered_at  INTEGER NOT NULL,
     retired_at     INTEGER
   );
   CREATE INDEX IF NOT EXISTS idx_cv_lookup ON cap_versions (registered_at);
   CREATE UNIQUE INDEX IF NOT EXISTS idx_cv_active ON cap_versions ((1)) WHERE retired_at IS NULL`,
  // Durable log of threshold crossings — the .alerted-thresholds.json debounce
  // file below only ever answers "did we already fire threshold X today" and
  // is overwritten every local day, so it can't answer "when did we last
  // cross 80%". This table is the queryable history that backs that question.
  `CREATE TABLE IF NOT EXISTS budget_alert_events (
     id         TEXT    NOT NULL PRIMARY KEY,
     threshold  REAL    NOT NULL,
     total_usd  REAL    NOT NULL,
     cap_usd    REAL    NOT NULL,
     fired_at   INTEGER NOT NULL
   );
   CREATE INDEX IF NOT EXISTS idx_bae_fired_at ON budget_alert_events (fired_at)`,
];

let state = readState();           // { day: "YYYY-MM-DD", fired: [0.5, 0.8, 1.0] }
let lastObservedTotal = 0;

// In-memory cache — read on the hot checkBudget path with zero I/O, same as
// the old flat-file version. Populated by initCapStore() at service start;
// updated optimistically (before the DB write lands) by the setters below.
let configuredCapUSD = DEFAULT_CAP_USD;
let configuredLockThreshold = DEFAULT_LOCK_THRESHOLD;

let _dbPath = DB_PATH;
let _client = null;
async function _getClient() {
  if (!_client) {
    const { client } = await getSharedDb(_dbPath);
    await createMigrator(client, { name: "budget-cap", migrations: CAP_MIGRATIONS });
    _client = client;
  }
  return _client;
}

function _readLegacyCapFile(legacyCapFile) {
  try { return JSON.parse(fs.readFileSync(legacyCapFile, "utf8")); } catch { return {}; }
}

/**
 * Loads the active cap_versions row into the in-memory cache. On a bare
 * table (first-ever run after this migration), seeds one row from the
 * legacy .cap.json — falling back to the hardcoded defaults if that file
 * never existed — so upgrading installs keep their operator-set cap.
 * Must complete before checkBudget()/the HTTP cap routes see traffic;
 * registered as its own api.registerService start hook.
 *
 * dbPath/legacyCapFile overrides are test-only (isolated per-test files) —
 * production always uses the ~/.openclaw defaults.
 */
export async function initCapStore({ dbPath = DB_PATH, legacyCapFile = LEGACY_CAP_FILE } = {}) {
  _dbPath = dbPath;
  _client = null; // re-resolve against the (possibly new) path
  const client = await _getClient();
  const { rows } = await client.execute(
    `SELECT cap_usd AS capUSD, lock_threshold AS lockThreshold FROM cap_versions WHERE retired_at IS NULL`
  );
  if (rows.length > 0) {
    configuredCapUSD = rows[0].capUSD;
    configuredLockThreshold = rows[0].lockThreshold;
    return;
  }
  const legacy = _readLegacyCapFile(legacyCapFile);
  const seedCap = (typeof legacy.capUSD === "number" && legacy.capUSD > 0 && Number.isFinite(legacy.capUSD))
    ? legacy.capUSD : DEFAULT_CAP_USD;
  const seedThreshold = (typeof legacy.lockThreshold === "number" && legacy.lockThreshold > 0 && legacy.lockThreshold <= 1 && Number.isFinite(legacy.lockThreshold))
    ? legacy.lockThreshold : DEFAULT_LOCK_THRESHOLD;
  await client.execute({
    sql: `INSERT INTO cap_versions (id, cap_usd, lock_threshold, registered_at, retired_at) VALUES (?, ?, ?, ?, NULL)`,
    args: [randomUUID(), seedCap, seedThreshold, Date.now()],
  });
  configuredCapUSD = seedCap;
  configuredLockThreshold = seedThreshold;
}

/**
 * SCD2 write — retires the current active row and inserts a new one in a
 * single atomic batch, same pattern as pipeline_versions. A no-op when the
 * patch doesn't actually change anything, so re-PUTting the same value
 * doesn't churn out a spurious history row.
 */
async function _writeCapVersion(patch) {
  const client = await _getClient();
  const { rows } = await client.execute(
    `SELECT id, cap_usd AS capUSD, lock_threshold AS lockThreshold FROM cap_versions WHERE retired_at IS NULL`
  );
  const active = rows[0] ?? null;
  const nextCap = patch.capUSD ?? active?.capUSD ?? configuredCapUSD;
  const nextThreshold = patch.lockThreshold ?? active?.lockThreshold ?? configuredLockThreshold;
  if (active && active.capUSD === nextCap && active.lockThreshold === nextThreshold) return;
  const now = Date.now();
  const queries = [];
  if (active) queries.push({ sql: `UPDATE cap_versions SET retired_at = ? WHERE id = ?`, args: [now, active.id] });
  queries.push({
    sql: `INSERT INTO cap_versions (id, cap_usd, lock_threshold, registered_at, retired_at) VALUES (?, ?, ?, ?, NULL)`,
    args: [randomUUID(), nextCap, nextThreshold, now],
  });
  await withSharedWrite(_dbPath, () => client.batch(queries));
}

/** Set cap, persist as a new cap_versions row. Only ever called from the
 *  (already-async) PUT /budget/cap route handler — awaited there so the
 *  response only confirms ok:true once the write actually landed, same
 *  durable-before-response guarantee the old synchronous fs.writeFileSync
 *  gave for free. */
export async function setCapAndPersist(usd) {
  if (typeof usd !== "number" || usd <= 0 || !Number.isFinite(usd)) return;
  configuredCapUSD = usd;
  await _writeCapVersion({ capUSD: usd });
}

export function currentCapUSD() { return configuredCapUSD; }

/** Fraction of daily cap at which the gate auto-locks (0 < x ≤ 1). */
export function currentLockThreshold() { return configuredLockThreshold; }

/** Set gate-lock threshold, persist as a new cap_versions row. Same
 *  await-before-respond contract as setCapAndPersist above. */
export async function setLockThresholdAndPersist(fraction) {
  if (typeof fraction !== "number" || fraction <= 0 || fraction > 1 || !Number.isFinite(fraction)) return;
  configuredLockThreshold = fraction;
  await _writeCapVersion({ lockThreshold: fraction });
}

/** Full cap/threshold history, oldest first — e.g. for rendering the actual
 *  cap in place at each point on a spend-over-time chart, rather than
 *  assuming today's cap applied retroactively. */
export async function getCapHistory() {
  const client = await _getClient();
  const { rows } = await client.execute(
    `SELECT cap_usd AS capUSD, lock_threshold AS lockThreshold, registered_at AS registeredAt, retired_at AS retiredAt
     FROM cap_versions ORDER BY registered_at ASC`
  );
  return rows;
}

/** Records one threshold crossing. Fire-and-forget from checkBudget — a
 *  failed write here should never block the alert push itself. */
async function _recordAlertEvent({ threshold, total, cap }) {
  const client = await _getClient();
  await withSharedWrite(_dbPath, () => client.execute({
    sql: `INSERT INTO budget_alert_events (id, threshold, total_usd, cap_usd, fired_at) VALUES (?, ?, ?, ?, ?)`,
    args: [randomUUID(), threshold, total, cap, Date.now()],
  }));
}

/** Threshold-crossing history, newest first — powers the iOS cap-hit chart's
 *  tap-to-detail (which threshold, what time), not just "was that day bad". */
export async function getAlertHistory({ limit = 60 } = {}) {
  const client = await _getClient();
  const { rows } = await client.execute({
    sql: `SELECT threshold, total_usd AS totalUSD, cap_usd AS capUSD, fired_at AS firedAt
          FROM budget_alert_events ORDER BY fired_at DESC LIMIT ?`,
    args: [limit],
  });
  return rows;
}

function readState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, "utf8");
    const j = JSON.parse(raw);
    if (j && typeof j === "object" && typeof j.day === "string" && Array.isArray(j.fired)) {
      // We have a JSON state; the v1 legacy file is obsolete (cleanup any
      // stragglers from an earlier in-progress migration).
      try { fs.unlinkSync(LEGACY_DAY_FILE); } catch {}
      return { day: j.day, fired: j.fired.filter((x) => typeof x === "number") };
    }
  } catch { /* fall through */ }
  // Migrate v1 legacy single-day flag — if it matches today, mark 100% fired
  try {
    const legacyDay = fs.readFileSync(LEGACY_DAY_FILE, "utf8").trim();
    if (legacyDay) {
      return { day: legacyDay, fired: [1.0] };
    }
  } catch {}
  return { day: null, fired: [] };
}

function writeState(next) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(next), "utf8");
    // Migrated forward — clean up the v1 legacy single-day file
    try { fs.unlinkSync(LEGACY_DAY_FILE); } catch {}
  } catch { /* best-effort */ }
}

const _GW_PORT = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;

async function _lockGate(logger) {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN ?? "";
  try {
    await fetch(`http://127.0.0.1:${_GW_PORT}/pipeclaw/budget/gate/lock`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: "{}",
      signal: AbortSignal.timeout(3000),
    });
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert] gate lock failed: ${err?.message ?? err}`);
  }
}

async function _unlockGate(logger) {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN ?? "";
  try {
    await fetch(`http://127.0.0.1:${_GW_PORT}/pipeclaw/budget/gate/unlock`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: "{}",
      signal: AbortSignal.timeout(3000),
    });
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert] gate unlock failed: ${err?.message ?? err}`);
  }
}

function localDayKey(d = new Date()) {
  // Local-time YYYY-MM-DD (the spend rolls over at local midnight, not UTC).
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Returns today's total spend in USD by summing cost_by_call rows whose
 * ts falls within the local-time day boundaries. Uses sqlite's localtime
 * modifier so the boundary is consistent regardless of timezone.
 */
async function todaysTotalUSD(store) {
  const row = await execGet(store._client, `
    SELECT COALESCE(SUM(cost_total), 0) AS total
    FROM cost_by_call
    WHERE date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')
  `);
  return Number(row?.total ?? 0);
}

/**
 * Should be called after every successful cost insert. Cheap — one indexed
 * SELECT and an in-memory threshold check. No-op when already alerted for
 * every threshold today.
 *
 * Behavior:
 * - On a NEW local day with prior history, silently mark already-crossed
 *   thresholds as fired (so a mid-day gateway restart doesn't retroactively
 *   re-fire pushes for thresholds the operator already saw).
 * - For each threshold (50% / 80% / 100%), fire the alert at most once
 *   per local day. Crossing two thresholds in one cost insert (rare) fires
 *   both notifications.
 */
export async function checkBudget({ store, logger, justWroteUsd = 0 }) {
  try {
    const today = localDayKey();

    // New-day rollover: reset fired list. To avoid spamming pushes after a
    // mid-day restart of an already-over-budget day, silently mark any
    // already-crossed thresholds as fired without dispatching.
    if (state.day !== today) {
      state = { day: today, fired: [] };
      const totalNow = await todaysTotalUSD(store);
      const fracNow = configuredCapUSD > 0 ? totalNow / configuredCapUSD : 0;
      for (const t of THRESHOLDS) if (fracNow >= t) state.fired.push(t);
      writeState(state);
      lastObservedTotal = totalNow;
      // New day: unlock the gate so pipelines run freely again
      _unlockGate(logger).catch(() => {});
      // Don't fire on the first call of a new day even if we crossed —
      // alerts only fire on transitions observed during this process.
      return;
    }

    if (state.fired.length === THRESHOLDS.length) return; // all fired today

    const total = await todaysTotalUSD(store);
    lastObservedTotal = total;
    const frac = configuredCapUSD > 0 ? total / configuredCapUSD : 0;

    for (const t of THRESHOLDS) {
      if (frac < t) continue;
      if (state.fired.includes(t)) continue;
      state.fired.push(t);
      writeState(state);
      _recordAlertEvent({ threshold: t, total, cap: configuredCapUSD })
        .catch((err) => logger?.warn?.(`lineage[budget-alert] record(${t}) error: ${err?.message ?? err}`));
      // Fire-and-forget so the hook isn't blocked on push round-trip.
      fireAlert({ threshold: t, total, cap: configuredCapUSD, store, logger })
        .catch((err) => logger?.warn?.(`lineage[budget-alert] fire(${t}) error: ${err?.message ?? err}`));
    }
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert]: ${err?.message ?? err}`);
  }
}

// ─── alert composition + dispatch ──────────────────────────────────────────

async function fireAlert({ threshold, total, cap, store, logger }) {
  const text = await composeText({ threshold, total, cap, store });
  const title = composeTitle({ threshold, total, cap });
  if (threshold >= configuredLockThreshold) {
    _lockGate(logger).catch(() => {});
    logger?.info?.(`lineage[budget-alert] approval gate locked (threshold=${threshold} >= lock=${configuredLockThreshold})`);
  }
  // APNs is the only delivery surface (iOS Lock Screen). The
  // budget_alert_events row above is written unconditionally, so the
  // threshold crossing is never lost even if nothing gets pushed here.
  const apnsResult = await fireAPNs({ title, body: text.body, threshold, total, cap, logger });
  if (apnsResult.ok && apnsResult.sent > 0) {
    logger?.info?.(`lineage[budget-alert] APNs fired threshold=${threshold} sent=${apnsResult.sent}/${apnsResult.total}`);
    return;
  }
  logger?.warn?.(`lineage[budget-alert] no delivery channel available (${apnsResult.reason ?? "?"}) — threshold=${threshold} alert NOT delivered`);
}

function composeTitle({ threshold }) {
  if (threshold >= 1.0)  return "🚨 LLM budget exceeded";
  if (threshold >= 0.8)  return "⚠️ LLM budget at 80%";
  if (threshold >= 0.5)  return "📊 LLM budget at 50%";
  return "LLM budget alert";
}

async function composeText({ threshold, total, cap, store }) {
  const pct = Math.round((total / Math.max(cap, 0.0001)) * 100);
  const top = await topDriverToday(store);
  let body = `$${total.toFixed(2)} of $${cap.toFixed(2)} today (${pct}%)`;
  if (top) body += `\nTop: ${top.label} $${top.cost.toFixed(2)}`;
  return { body };
}

/** Highest-spending root in today's data — used to point the operator at
 *  the driver without having to open the app first. Best-effort. */
async function topDriverToday(store) {
  try {
    const row = await execGet(store._client, `
      SELECT root_label, root_kind, SUM(cost_total) AS cost
      FROM cost_by_call
      WHERE date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')
        AND root_label IS NOT NULL
      GROUP BY root_label, root_kind ORDER BY cost DESC LIMIT 1
    `);
    if (!row || !row.cost) return null;
    return { label: displayRootLabel(row.root_kind, row.root_label), cost: row.cost };
  } catch { return null; }
}

// ─── APNs (primary) ────────────────────────────────────────────────────────

async function fireAPNs({ title, body, threshold, total, cap, logger }) {
  // Goes through the apns extension's own HTTP route instead of
  // api.runtime.apns — that in-process handle is a Proxy over a
  // PluginRuntime object created fresh on every loadOpenClawPlugins() pass
  // (gateway boot, plugin install, status/CLI, reload). This file's
  // module-level SHARED_API is set once at this plugin's own original
  // register() call and never updated, so if the runtime registry is ever
  // rebuilt after boot, apns's later api.runtime.apns write lands on a new,
  // disconnected object this module can no longer see — apns_present=false
  // with the extension itself fully healthy. /apns/push already works
  // end-to-end (it's what Deploy pushes use) and doesn't depend on two
  // plugins sharing an in-process object's identity.
  const token = process.env.OPENCLAW_GATEWAY_TOKEN ?? "";
  try {
    const res = await fetch(`http://127.0.0.1:${_GW_PORT}/apns/push`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ title, body, category: NotificationKinds.BUDGET, threadId: "budget" }),
      signal: AbortSignal.timeout(5000),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      logger?.warn?.(`lineage[budget-alert] APNs push failed (status=${res.status}, error=${data.error ?? "?"}, delivered=${data.delivered ?? 0}/${data.total ?? 0})`);
      return { ok: false, reason: data.error ?? `http_${res.status}`, total: data.total ?? 0 };
    }
    return { ok: true, sent: data.delivered, total: data.total };
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert] APNs push request threw: ${err?.message ?? err}`);
    return { ok: false, reason: "request_failed" };
  }
}

/** Fire a one-off alert through the normal APNs path, ignoring
 *  the once-per-day debounce. For operator validation only — does not mutate
 *  the persistent threshold-fired state. */
export async function testFireAlert({ store, logger, threshold = 0.5 }) {
  const total = await todaysTotalUSD(store);
  await fireAlert({ threshold, total, cap: configuredCapUSD, store, logger });
}

