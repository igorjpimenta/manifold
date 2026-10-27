// paused-flows.js — persistent set of flow IDs whose before_dispatch claim
// is temporarily disabled, letting the operator talk to the main agent
// without the priming hook intercepting every message.
//
// State lives under the core-owned data dir so it survives gateway
// restarts and is visible to both the hook (gateway process) and the CLI
// (separate process).

import fs from "node:fs";
import path from "node:path";
import { resolveStorePaths, stateFile } from "../state/paths.js";

function defaultStateFile() {
  return stateFile(resolveStorePaths().dataDir);
}
// Adapter-owned override: the openclaw adapter calls setStateFile once
// with its resolved store paths.
let _stateFileOverride = null;
export function setStateFile(file) {
  _stateFileOverride = file ?? null;
}
function stateFilePath() {
  return _stateFileOverride ?? defaultStateFile();
}

export function listPaused() {
  try {
    const raw = fs.readFileSync(stateFilePath(), "utf8");
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
}

export function isPaused(flowId) {
  return listPaused().has(flowId);
}

export function pause(flowId) {
  const set = listPaused();
  set.add(flowId);
  write(set);
}

export function resume(flowId) {
  const set = listPaused();
  set.delete(flowId);
  write(set);
}

function write(set) {
  fs.mkdirSync(path.dirname(stateFilePath()), { recursive: true });
  fs.writeFileSync(stateFilePath(), JSON.stringify([...set]), "utf8");
}
