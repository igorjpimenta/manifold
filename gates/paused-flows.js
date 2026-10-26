// paused-flows.js — persistent set of flow IDs whose before_dispatch claim
// is temporarily disabled, letting the operator talk to the main agent
// without the priming hook intercepting every message.
//
// State lives at ~/.openclaw/state/manifold-paused.json so it survives
// gateway restarts and is visible to both the hook (gateway process) and
// the CLI (separate process).

import fs from "node:fs";
import path from "node:path";
import { resolveStorePaths } from "../state/db.js";

// State file is harness-owned: every entry point takes an explicit path,
// defaulting via resolveStorePaths so direct callers keep working.
function defaultStateFile() {
  return path.join(resolveStorePaths().dataDir, "state", "manifold-paused.json");
}

export function listPaused(stateFile = defaultStateFile()) {
  try {
    const raw = fs.readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
}

export function isPaused(flowId, stateFile = defaultStateFile()) {
  return listPaused(stateFile).has(flowId);
}

export function pause(flowId, stateFile = defaultStateFile()) {
  const set = listPaused(stateFile);
  set.add(flowId);
  write(set, stateFile);
}

export function resume(flowId, stateFile = defaultStateFile()) {
  const set = listPaused(stateFile);
  set.delete(flowId);
  write(set, stateFile);
}

function write(set, stateFile) {
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });
  fs.writeFileSync(stateFile, JSON.stringify([...set]), "utf8");
}
