// hooks/profile-read-gate.js — before_tool_call hook:
// makes any reader of PROFILE.md wait behind the same advisory lock
// gap-answers.js's settleGapAnswer() holds for its whole Thoth
// dispatch+write, so Cyrano's DRAFT-time read (write-workana-proposal/
// SKILL.md step 2) never sees a stale or torn file mid-edit. See
// gap-answers.js's header comment for the writer side of this contract.
//
// "Acquire then immediately release" is a barrier, not a real lock for the
// reader — it just refuses to let the read proceed until whatever writer
// currently holds the same lock name lets go. acquireLock/release reuse
// the exact same crash-safe, stale-PID-reaping primitive the writer uses
// (locks/lock.js), rather than a naive raw file-existence poll.
// lockNameForFile comes from that same module so the two sides can never
// construct different lock names for the same file.
//
// toolName/params.path below were inferred from openclaw's public plugin
// docs (api.on("before_tool_call", ...), event.toolName, event.params),
// not observed directly against this codebase's actual agent runtime —
// verify both against a real event log before relying on this.

import { acquireLock, lockNameForFile } from "../../../../locks/lock.js";
import path from "node:path";
import os from "node:os";

// Default reproduces the live deployment's PROFILE.md path byte-for-byte
// so production call sites stay zero-config; profilePath is a parameter
// so tests and non-default installs can point the gate at another file.
const DEFAULT_PROFILE_PATH = path.join(os.homedir(), ".openclaw", "workspace", ".shared", "freelance", "profile", "PROFILE.md");

const READ_TOOL_NAME = "read";
const WAIT_TIMEOUT_MS = 60_000;

export function registerProfileReadGate(api, { profilePath = DEFAULT_PROFILE_PATH } = {}) {
  api.on("before_tool_call", async (event) => {
    if (event?.toolName !== READ_TOOL_NAME) return;
    if (event?.params?.path !== profilePath) return;
    api?.logger?.info?.(`profile-read-gate: barrier hit for PROFILE.md read (runId=${event.runId ?? "?"})`);
    const lock = await acquireLock(lockNameForFile(profilePath), { timeoutMs: WAIT_TIMEOUT_MS, pollMs: 250 });
    await lock.release();
  }, { priority: 50, timeoutMs: WAIT_TIMEOUT_MS });
}
