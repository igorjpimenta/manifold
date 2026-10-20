// paused-flows.js — persistent set of flow IDs whose before_dispatch claim
// is temporarily disabled, letting the operator talk to the main agent
// without the priming hook intercepting every message.
//
// State lives at ~/.openclaw/state/pipeclaw-paused.json so it survives
// gateway restarts and is visible to both the hook (gateway process) and
// the CLI (separate process).

import fs from "node:fs";
import path from "node:path";
import os from "node:os";

const STATE_FILE = path.join(os.homedir(), ".openclaw", "state", "pipeclaw-paused.json");

export function listPaused() {
  try {
    const raw = fs.readFileSync(STATE_FILE, "utf8");
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
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify([...set]), "utf8");
}
