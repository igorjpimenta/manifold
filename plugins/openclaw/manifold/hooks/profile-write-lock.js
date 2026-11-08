// hooks/profile-write-lock.js — before_tool_call/
// after_tool_call pair: when an agent calls the `edit` tool against
// PROFILE.md, hold a real lock around JUST that write.
//
// Deliberately scoped to the edit call itself, not the whole agent turn
// that leads up to it (read → think → edit) — the underlying lock
// primitive (the plugin's lock.js) lets a real holder keep a lock as it
// needs, but every other consumer of this same lock (gap-answers.js's
// settleGapAnswer, prime-nodes.js's applyTbdFills) is a fast,
// deterministic, millisecond-scale critical section — holding it for
// several seconds of agent reasoning would make every one of those
// contend against a single slow Thoth turn instead of each other. Scoping
// to just the edit call keeps this lock's hold time in the same class as
// every other PROFILE.md writer, at the cost of a narrow (not
// zero) staleness window between whatever the agent read and what it
// edits — the `edit` tool's own old-text matching turns that into a clean
// tool-call failure on a genuine race, never silent corruption.
//
// Pairs with hooks/profile-read-gate.js — that one is a barrier for `read`
// (acquire-then-release-immediately, just delays a read behind any
// in-flight writer). This one is a real held lock for `edit`.
//
// Records the write's outcome (success/error) keyed by the turn's runId —
// a kind:"agent" pipeline node's apply(state, output) only ever sees
// `{text, runId}` (see report-draft-blocked.js's header for why every
// agent-side signal in this codebase flows through a file side-channel,
// never direct tool-call introspection). output.runId and this hook's
// event.runId are the same identifier for a given agent dispatch: the
// in-repo half is engine/agent-dispatch.js's dispatchAgentWithRetry
// (output.runId = runResult.runId from subagent.run()); the other half is
// the gateway's embedded-run harness, outside this tree.

import { acquireLock, lockNameForFile } from "../lock.js";
import { writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import os from "node:os";

const EDIT_TOOL_NAME = "edit";
// How long to wait to ACQUIRE the lock (lock.js's timeoutMs semantics —
// this is wait-to-acquire, not how-long-a-holder-may-keep-it). Generous
// relative to an edit call's real duration (milliseconds) since a queued
// writer here is rare but should fail loudly rather than hang forever.
const ACQUIRE_TIMEOUT_MS = 30_000;
// Backstop only — see lock.js's own staleMs docs (PID-reuse safety, not a
// "long holds are abandoned" floor). A genuinely wedged before_tool_call
// (acquired, then the process died before after_tool_call could fire)
// self-heals via PID-liveness reaping well before this; this floor exists
// for the same-process, non-crash case (e.g. a bug leaks a toolCallId
// entry) so a leaked lock doesn't block PROFILE.md for the full 6h
// lock.js default.
const STALE_MS = 3 * 60_000;

// Defaults reproduce the live deployment byte-for-byte: PROFILE.md in the
// workspace .shared tree, edit-result markers under ~/.openclaw/locks.
// Both are parameters so tests and non-default installs can point them at
// fixtures without mocking os.homedir for this module.
const DEFAULT_PROFILE_PATH = path.join(os.homedir(), ".openclaw", "workspace", ".shared", "freelance", "profile", "PROFILE.md");
const DEFAULT_RESULTS_DIR = path.join(os.homedir(), ".openclaw", "locks", "profile-edit-results");

// Exported for SETTLE_GAP_ANSWER.apply() (workspace
// freelance-propose-nodes.js) to read+consume — same path convention on
// both sides by construction; the optional resultsDir must match whatever
// registerProfileWriteLock was called with.
export function profileEditResultPathFor(runId, { resultsDir = DEFAULT_RESULTS_DIR } = {}) {
  return path.join(resultsDir, `${runId}.json`);
}

// toolCallId -> the lock object acquired for that specific call, so
// after_tool_call releases the exact lock before_tool_call acquired (not
// a name-only lookup — matters if calls ever overlap across sessions).
const heldLocks = new Map();

export function registerProfileWriteLock(api, { profilePath = DEFAULT_PROFILE_PATH, resultsDir = DEFAULT_RESULTS_DIR } = {}) {
  api.on("before_tool_call", async (event) => {
    if (event?.toolName !== EDIT_TOOL_NAME) return;
    if (event?.params?.path !== profilePath) return;
    api?.logger?.info?.(`profile-write-lock: acquiring for PROFILE.md edit (runId=${event.runId ?? "?"}, toolCallId=${event.toolCallId ?? "?"})`);
    const lock = await acquireLock(lockNameForFile(profilePath), { timeoutMs: ACQUIRE_TIMEOUT_MS, staleMs: STALE_MS });
    if (event.toolCallId) heldLocks.set(event.toolCallId, lock);
    api?.logger?.info?.(`profile-write-lock: acquired (runId=${event.runId ?? "?"})`);
  }, { priority: 50, timeoutMs: ACQUIRE_TIMEOUT_MS + 5_000 });

  // after_tool_call is fire-and-forget in the runtime (errors here become a
  // warning log, nothing awaits or retries this handler) — every step below
  // is wrapped defensively so a failure in one doesn't skip the other.
  api.on("after_tool_call", async (event) => {
    if (event?.toolName !== EDIT_TOOL_NAME) return;
    if (event?.params?.path !== profilePath) return;

    const toolCallId = event.toolCallId;
    const lock = toolCallId ? heldLocks.get(toolCallId) : null;
    if (toolCallId) heldLocks.delete(toolCallId);
    try { await lock?.release(); } catch (err) { api?.logger?.warn?.(`profile-write-lock: release failed: ${err?.message ?? err}`); }
    api?.logger?.info?.(`profile-write-lock: released (runId=${event.runId ?? "?"}, hadLock=${Boolean(lock)}, success=${!event.error})`);

    if (!event.runId) return;
    try {
      await mkdir(resultsDir, { recursive: true });
      await writeFile(profileEditResultPathFor(event.runId, { resultsDir }), JSON.stringify({
        success: !event.error,
        error: event.error ? String(event.error?.message ?? event.error) : null,
        at: new Date().toISOString(),
      }));
    } catch (err) {
      api?.logger?.warn?.(`profile-write-lock: could not record edit result for runId=${event.runId}: ${err?.message ?? err}`);
    }
  });
}

export { DEFAULT_RESULTS_DIR as PROFILE_EDIT_RESULTS_DIR };
