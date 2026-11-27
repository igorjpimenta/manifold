// Daily-budget guard: on a threshold crossing, write a budget_alert_events
// row and emit manifold.budget.alert for a consumer to deliver. Delivery is
// not manifold's job.

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { displayRootLabel } from "./labels.js";
import { execGet } from "./http-helpers.js";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER } from "../state/migrate.js";
import { resolveDataDir, opsDbPath } from "../state/paths.js";

const DEFAULT_CAP_USD = 3.0;
const DEFAULT_LOCK_THRESHOLD = 0.8;

const THRESHOLDS = [0.1, 0.5, 0.8, 1.0];

// Persisted so a restart doesn't re-alert on an already-over-budget day.
const STATE_FILE = path.join(resolveDataDir(), "lineage", ".alerted-thresholds.json");
// Same physical file budget/index.js's gate_state lives in; each module owns
// its own tables there.
const DB_PATH = opsDbPath(resolveDataDir());

let state = readState();
let lastObservedTotal = 0;

let configuredCapUSD = DEFAULT_CAP_USD;
let configuredLockThreshold = DEFAULT_LOCK_THRESHOLD;

let _dbPath = DB_PATH;
let _client = null;
async function _getClient() {
  if (!_client) {
    await migrateDatabase({ dbPath: _dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER });
    const { client } = await getSharedDb(_dbPath);
    _client = client;
  }
  return _client;
}

/**
 * Seeds the in-memory cap from the active cap_versions row (or the hardcoded
 * defaults on a bare table). Must complete before checkBudget() and the HTTP
 * cap routes see traffic. dbPath override is test-only.
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

/** SCD2: retire the active row and insert its replacement in one batch —
 *  no-op when the patch changes nothing, so a repeat PUT doesn't churn
 *  history. */
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

/** Awaited before responding, so ok:true means the write landed. */
export async function setCapAndPersist(usd) {
  if (typeof usd !== "number" || usd <= 0 || !Number.isFinite(usd)) return;
  configuredCapUSD = usd;
  await _writeCapVersion({ capUSD: usd });
}

export function currentCapUSD() { return configuredCapUSD; }

export function currentLockThreshold() { return configuredLockThreshold; }

/** Same await-before-respond contract as setCapAndPersist. */
export async function setLockThresholdAndPersist(fraction) {
  if (typeof fraction !== "number" || fraction <= 0 || fraction > 1 || !Number.isFinite(fraction)) return;
  configuredLockThreshold = fraction;
  await _writeCapVersion({ lockThreshold: fraction });
}

export async function getCapHistory() {
  const client = await _getClient();
  const { rows } = await client.execute(
    `SELECT cap_usd AS capUSD, lock_threshold AS lockThreshold, registered_at AS registeredAt, retired_at AS retiredAt
     FROM cap_versions ORDER BY registered_at ASC`
  );
  return rows;
}

/** Fire-and-forget from checkBudget: a failed write must never block the
 *  alert push itself. */
async function _recordAlertEvent({ threshold, total, cap }) {
  const client = await _getClient();
  await withSharedWrite(_dbPath, () => client.execute({
    sql: `INSERT INTO budget_alert_events (id, threshold, total_usd, cap_usd, fired_at) VALUES (?, ?, ?, ?, ?)`,
    args: [randomUUID(), threshold, total, cap, Date.now()],
  }));
}

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

// gate_state is this module's to write as much as budget/index.js's: same
// physical file (DB_PATH above), each module owning its own rows. Written
// in-process — a threshold crossing must not depend on an HTTP round-trip to
// a route that only this process serves.
async function _setGateLocked(locked, logger) {
  try {
    const client = await _getClient();
    const today = localDayKey();
    await withSharedWrite(_dbPath, () => client.execute(
      "INSERT OR IGNORE INTO gate_state (id, locked) VALUES ('singleton', 0)"
    ));
    await withSharedWrite(_dbPath, () => client.execute({
      sql: "UPDATE gate_state SET locked=?, day=? WHERE id='singleton'",
      args: [locked ? 1 : 0, today],
    }));
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert] gate ${locked ? "lock" : "unlock"} failed: ${err?.message ?? err}`);
  }
}

function _lockGate(logger) { return _setGateLocked(true, logger); }

function _unlockGate(logger) { return _setGateLocked(false, logger); }

function localDayKey(d = new Date()) {
  // Local-time YYYY-MM-DD: spend rolls over at local midnight, not UTC.
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function todaysTotalUSD(store) {
  const row = await execGet(store._client, `
    SELECT COALESCE(SUM(cost_total), 0) AS total
    FROM cost_by_call
    WHERE date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')
  `);
  return Number(row?.total ?? 0);
}

/**
 * Cheap: one indexed SELECT plus an in-memory threshold check; no-op once
 * every threshold has fired today. On a NEW local day, thresholds already
 * crossed are marked fired without dispatching, so a mid-day restart doesn't
 * re-push alerts the operator already saw. Two thresholds crossed by one
 * insert both fire.
 */
export async function checkBudget({ store, logger, justWroteUsd = 0, notify }) {
  try {
    const today = localDayKey();

    if (state.day !== today) {
      state = { day: today, fired: [] };
      const totalNow = await todaysTotalUSD(store);
      const fracNow = configuredCapUSD > 0 ? totalNow / configuredCapUSD : 0;
      for (const t of THRESHOLDS) if (fracNow >= t) state.fired.push(t);
      writeState(state);
      lastObservedTotal = totalNow;
      _unlockGate(logger).catch(() => {});
      return;
    }

    if (state.fired.length === THRESHOLDS.length) return;

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
      // Fire-and-forget: the hook must not block on the push round-trip.
      fireAlert({ threshold: t, total, cap: configuredCapUSD, store, logger, notify })
        .catch((err) => logger?.warn?.(`lineage[budget-alert] fire(${t}) error: ${err?.message ?? err}`));
    }
  } catch (err) {
    logger?.warn?.(`lineage[budget-alert]: ${err?.message ?? err}`);
  }
}

async function fireAlert({ threshold, total, cap, store, logger, notify }) {
  const text = await composeText({ threshold, total, cap, store });
  const title = composeTitle({ threshold, total, cap });
  if (threshold >= configuredLockThreshold) {
    _lockGate(logger).catch(() => {});
    logger?.info?.(`lineage[budget-alert] approval gate locked (threshold=${threshold} >= lock=${configuredLockThreshold})`);
  }
  // The budget_alert_events row is written unconditionally, so a crossing is
  // never lost even with no consumer attached; delivery belongs to whoever
  // subscribes.
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

/** Ignores the once-per-day debounce and does not mutate threshold state —
 *  operator validation only. */
export async function testFireAlert({ store, logger, threshold = 0.5, notify }) {
  const total = await todaysTotalUSD(store);
  await fireAlert({ threshold, total, cap: configuredCapUSD, store, logger, notify });
}

