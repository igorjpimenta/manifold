// Daily-budget guard. Called from the before_message_write hook after each
// cost insert. When today's total exceeds CAP_USD, writes the
// budget_alert_events row and emits a manifold.budget.alert event for a
// consumer to deliver (once per day per threshold — debounced via
// lastAlertedLocalDay). Delivery is not manifold's job.

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { displayRootLabel } from "./labels.js";
import { execGet } from "./http-helpers.js";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER } from "../state/migrate.js";

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
// Same physical file budget/index.js's gate_state lives in — each module
// owns its own tables there.
const DB_PATH = path.join(os.homedir(), ".openclaw", "ops.sqlite");

let state = readState();           // { day: "YYYY-MM-DD", fired: [0.5, 0.8, 1.0] }
let lastObservedTotal = 0;

// In-memory cache — read on the hot checkBudget path with zero I/O.
// Populated by initCapStore() at service start; updated optimistically
// (before the DB write lands) by the setters below.
let configuredCapUSD = DEFAULT_CAP_USD;
let configuredLockThreshold = DEFAULT_LOCK_THRESHOLD;

let _dbPath = DB_PATH;
let _client = null;
async function _getClient() {
  if (!_client) {
    // Generated snapshots are the sole schema path (see state/migrate.js).
    await migrateDatabase({ dbPath: _dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER });
    const { client } = await getSharedDb(_dbPath);
    _client = client;
  }
  return _client;
}

/**
 * Loads the active cap_versions row into the in-memory cache, seeding one
 * row from the hardcoded defaults on a bare table. Must complete before
 * checkBudget()/the HTTP cap routes see traffic; registered as its own
 * api.registerService start hook.
 *
 * dbPath override is test-only (isolated per-test files) — production
 * always uses the ~/.openclaw default.
 */
export async function initCapStore({ dbPath = DB_PATH } = {}) {
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
  await client.execute({
    sql: `INSERT INTO cap_versions (id, cap_usd, lock_threshold, registered_at, retired_at) VALUES (?, ?, ?, ?, NULL)`,
    args: [randomUUID(), DEFAULT_CAP_USD, DEFAULT_LOCK_THRESHOLD, Date.now()],
  });
  configuredCapUSD = DEFAULT_CAP_USD;
  configuredLockThreshold = DEFAULT_LOCK_THRESHOLD;
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
      return { day: j.day, fired: j.fired.filter((x) => typeof x === "number") };
    }
  } catch { /* fall through */ }
  return { day: null, fired: [] };
}

function writeState(next) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(next), "utf8");
  } catch { /* best-effort */ }
}

const _GW_PORT = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;

async function _lockGate(logger) {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN ?? "";
  try {
    await fetch(`http://127.0.0.1:${_GW_PORT}/manifold/budget/gate/lock`, {
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
    await fetch(`http://127.0.0.1:${_GW_PORT}/manifold/budget/gate/unlock`, {
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
export async function checkBudget({ store, logger, justWroteUsd = 0, notify }) {
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
      fireAlert({ threshold: t, total, cap: configuredCapUSD, store, logger, notify })
        .catch((err) => logger?.warn?.(`lineage[budget-alert] fire(${t}) error: ${err?.message ?? err}`));
    }
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert]: ${err?.message ?? err}`);
  }
}

// ─── alert composition + dispatch ──────────────────────────────────────────

async function fireAlert({ threshold, total, cap, store, logger, notify }) {
  const text = await composeText({ threshold, total, cap, store });
  const title = composeTitle({ threshold, total, cap });
  if (threshold >= configuredLockThreshold) {
    _lockGate(logger).catch(() => {});
    logger?.info?.(`lineage[budget-alert] approval gate locked (threshold=${threshold} >= lock=${configuredLockThreshold})`);
  }
  // The budget_alert_events row is written unconditionally, so the threshold
  // crossing is never lost even when no consumer is attached. Delivery
  // belongs to whoever subscribes (app bridge, notifier); manifold only
  // states the event.
  if (typeof notify === "function") {
    try {
      notify({ title, body: text.body, threshold, total, cap });
      logger?.info?.(`lineage[budget-alert] emitted manifold.budget.alert threshold=${threshold}`);
      return;
    } catch (err) {
      logger?.warn?.(`lineage[budget-alert] budget.alert emit failed: ${err?.message ?? err}`);
    }
  }
  logger?.warn?.(`lineage[budget-alert] no consumer attached — threshold=${threshold} alert not delivered`);
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

/** Fire a one-off alert through the normal event path, ignoring the
 *  once-per-day debounce. For operator validation only — does not mutate the
 *  persistent threshold-fired state. */
export async function testFireAlert({ store, logger, threshold = 0.5, notify }) {
  const total = await todaysTotalUSD(store);
  await fireAlert({ threshold, total, cap: configuredCapUSD, store, logger, notify });
}

