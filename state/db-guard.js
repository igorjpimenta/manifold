// db-guard.js — fail-closed corruption handling.
//
// Transient storage hiccups (BUSY/locked/timeout) keep existing retry and
// degrade paths. Corruption-class failures (CORRUPT/IOERR/NOTADB/MALFORMED)
// are different: running blind mints wrong gate decisions, drops pushes,
// and loses failure records. So they count, and a short burst trips the
// breaker: corruption alert on disk, loud log, process exit. Launchd
// respawns into the boot probe, which refuses until the files are
// healthy — a down gateway with an alert beats a blind one taking
// operator decisions.
//
// The counter is process-wide across all callers; a single blip never
// trips it. Tunables live here (not config) because flapping the
// breaker open from config would defeat it.
import fs from "node:fs";
import path from "node:path";
import { resolveStorePaths } from "./db.js";

export const CORRUPT_CLASS = /(CORRUPT|IOERR|NOTADB|MALFORMED|database disk image is malformed)/i;
export const TRIP_THRESHOLD = 3;
export const TRIP_WINDOW_MS = 60_000;

let failures = [];
let exitFn = (code) => process.exit(code);

export function __setExitFn(fn) { exitFn = fn; }
export function __resetGuard() { failures = []; }

export function isCorruptClass(err) {
  const hay = `${err?.code ?? ""} ${err?.message ?? err ?? ""}`;
  return CORRUPT_CLASS.test(hay);
}

function writeAlert(detail) {
  const dir = path.join(resolveStorePaths().dataDir, "logs");
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, "SQLITE_CORRUPTION_ALERT"),
      JSON.stringify({ at: new Date().toISOString(), ...detail }) + "\n",
    );
  } catch {}
}

export { writeAlert as writeCorruptionAlert };

// Returns true when this failure tripped the breaker (caller has
// already been told to stop via exit). Never throws.
export function noteDbFailure(err, { dbPath = "unknown", logger = null } = {}) {
  if (!isCorruptClass(err)) return false;
  const now = Date.now();
  failures = failures.filter((t) => now - t < TRIP_WINDOW_MS);
  failures.push(now);
  logger?.warn?.(`db-guard: corrupt-class failure ${failures.length}/${TRIP_THRESHOLD} on ${dbPath}: ${err?.message ?? err}`);
  if (failures.length < TRIP_THRESHOLD) return false;
  const detail = { dbPath, failuresInWindow: failures.length, error: String(err?.message ?? err) };
  logger?.error?.(`db-guard: TRIPPING — ${failures.length} corrupt-class failures in 60s on ${dbPath}; writing alert and exiting`);
  writeAlert(detail);
  try {
    exitFn(1);
  } catch {}
  return true;
}
