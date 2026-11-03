// lock.js — cross-process mutex on a shared resource (e.g., Chrome profile).
//
// Bypasses the in-process manifold step-lock by living on disk: any
// consumer (pipeline nodes, ad-hoc CLI scripts, future automation) can
// acquire the same lock and serialize against each other.
//
// Usage:
//
//   import { withLock } from "./lock.js";
//   withLock("browser:freelance", async () => {
//     // ... do browser work ...
//   }, { timeoutMs: 60_000, staleMs: 10 * 60_000 })
//     .catch((err) => { console.error(err.message); process.exit(2); });
//
// Lock state:
//   ~/.openclaw/locks/<name>.lock
//     { pid, hostname, since, name, token, owner? }
//   `token` is unique per acquisition. `owner` is `{ kind, id, ... }` — the
//   unit of work the lock belongs to (see "Owners" below); absent for
//   callers that don't pass one and aren't running inside runWithLockOwner.
//
// Owners:
//   An owned lock stays held for as long as its owner's work is unresolved,
//   however long that is. Each owner kind registers a resolver
//   (registerOwnerResolver) answering "is this holder's work finished?";
//   the waiter asks it before reaping. A kind with no registered resolver,
//   kind "process", and owner-less locks fall back to the staleMs rule below.
//   Whoever registers a kind decides what "resolved" means for it — e.g.
//   manifold's invocation owner is resolved once the invocation wrote its
//   result or its flow is terminal.
//
// Stale recovery (in this order):
//   - PID liveness via `process.kill(pid, 0)` — false → holder died → reap,
//     whatever its owner.
//   - Owned by a kind with a resolver → reap only once the resolver says
//     resolved. staleMs is ignored: every flow runs inside the one gateway
//     process, so the holder PID is always alive and age is the only other
//     signal — and age says nothing about whether a long hold is abandoned.
//   - Otherwise, the staleMs floor (default 6h) as PID-reuse safety: if a
//     long-dead process's PID was recycled by the OS, `isPidAlive` returns
//     true for the unrelated new process; past staleMs, assume PID reuse
//     and reap.
//   Reaping is compare-and-delete: the file is only removed if it still
//   carries the token (or, for pre-token files, the pid+since) that was
//   judged stale, so a waiter never deletes a lock acquired in between.
//
// Release:
//   Only removes the file while it still carries this acquisition's token.
//   A holder whose lock was reaped (dead PID from the waiter's view, or its
//   owner judged resolved) must not delete the next holder's lock.
//
// Re-entry:
//   - Same async call chain (heldLockNames): a nested withLock() for a name
//     the chain already holds runs fn directly.
//   - Same owner, same process: an acquire whose owner equals the current
//     holder's owner gets a non-owning handle — release() is a no-op and
//     no tab lifecycle is layered on top. Covers work that re-enters from a
//     different async context (e.g. a second tool call of the same run).
//
// Wait semantics:
//   - Default `timeoutMs` is Infinity. The browser-profile queue has no
//     hard time limit: when the current holder releases, the next waiter
//     takes over, no matter how long they've been waiting. Callers can
//     override with a finite timeoutMs if they need a fail-fast.
//   - Optional `onWouldBlock` callback fires at most once per acquireLock()
//     call, right after the first failed acquire attempt — lets a caller
//     surface "this is about to wait" (e.g. persisting a queued status)
//     without polling itself. Never fires for a reentrant acquire.
//   - `now` and `sleep` are the clock the cooldown gap and the poll interval
//     are measured against, injectable so a caller can drive both without
//     real elapsed time. Both default to the real wall clock and real timers.
//     The cooldown sentinel itself is always stamped with the real
//     `Date.now()` (markReleasedSync has no per-call opts — it runs from the
//     process exit handler), so a non-default `now` only ever shifts this
//     call's own arithmetic, never what it leaves on disk.
//
// Crash safety:
//   - The lock file persists on process.exit; next acquirer reaps it via
//     the stale check above.
//   - We also register an `exit` listener that synchronously unlinks the
//     lock file (if still ours) so graceful exits release immediately,
//     regardless of whether main() did `process.exit()` directly.
//
// Shared state (owner resolvers, session owners, the current-owner context)
// lives on globalThis: jiti and native ESM can load this same file as two
// disconnected module instances within one process, and both must see the
// same registrations.

import { resolveStorePaths } from "../state/paths.js";
import { open, readFile, mkdir } from "node:fs/promises";
import { readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { AsyncLocalStorage } from "node:async_hooks";
import { wrapBrowserTabLifecycle } from "./browser-tabs.js";

function defaultLocksDir() {
  return resolveStorePaths().locksDir;
}
// Adapter-owned override: the openclaw adapter calls setLocksDir once with
// its resolved store paths. Falls back to the core-owned default so the
// CLI and tests acquire the same locks without any harness.
let _locksDirOverride = null;
export function setLocksDir(dir) {
  _locksDirOverride = dir ?? null;
}
function locksDir() {
  return _locksDirOverride ?? defaultLocksDir();
}

// Tracks lock names held by the CURRENT async call chain (not the whole
// process) — see withLock() below for why this exists.
const heldLockNames = new AsyncLocalStorage();

const SHARED_KEY = "__openclaw_lock_shared_state__";

function shared() {
  if (!globalThis[SHARED_KEY]) {
    globalThis[SHARED_KEY] = {
      resolvers: new Map(),
      sessionOwners: new Map(),
      currentOwner: new AsyncLocalStorage(),
    };
  }
  return globalThis[SHARED_KEY];
}

// resolver: { isResolved(holder) => boolean | Promise<boolean>,
//             describe?(owner) => string }
// `holder` is the parsed lock file ({ pid, since, token, owner, ... }).
export function registerOwnerResolver(kind, resolver) {
  if (!kind || typeof resolver?.isResolved !== "function") {
    throw new Error("registerOwnerResolver: kind and resolver.isResolved are required");
  }
  shared().resolvers.set(kind, resolver);
}

export function unregisterOwnerResolver(kind) {
  shared().resolvers.delete(kind);
}

// Maps an agent session to the owner its tool calls should lock as — e.g.
// manifold registers the dispatching invocation for the session it spawns.
export function registerSessionOwner(sessionKey, owner) {
  if (sessionKey && owner?.kind && owner?.id) shared().sessionOwners.set(sessionKey, owner);
}

export function clearSessionOwner(sessionKey) {
  if (sessionKey) shared().sessionOwners.delete(sessionKey);
}

export function sessionOwner(sessionKey) {
  return sessionKey ? shared().sessionOwners.get(sessionKey) ?? null : null;
}

// Runs fn with `owner` as the default owner for every acquireLock()/withLock()
// in its async call chain that doesn't pass one explicitly.
export function runWithLockOwner(owner, fn) {
  if (!owner?.kind || !owner?.id) return fn();
  return shared().currentOwner.run(owner, fn);
}

export function currentLockOwner() {
  return shared().currentOwner.getStore() ?? null;
}

export function sameOwner(a, b) {
  return Boolean(a?.kind && b?.kind && a.kind === b.kind && a.id != null && a.id === b.id);
}

export function describeOwner(owner) {
  if (!owner?.kind) return "an unowned holder";
  const custom = shared().resolvers.get(owner.kind)?.describe?.(owner);
  return custom || `${owner.kind} ${String(owner.id).slice(0, 8)}`;
}

function safeName(name) {
  return name.replace(/[^a-zA-Z0-9_-]/g, "_");
}

export function lockPathFor(name) {
  return path.join(locksDir(), `${safeName(name)}.lock`);
}

// Lock name for a file-keyed mutex. The `file:` prefix plus
// path.resolve() normalization must stay byte-identical to the derivation
// the workspace's file-lock helpers use (withFileLock) — every reader and
// writer of the same file (e.g. PROFILE.md: hooks/profile-read-gate.js
// vs gap-answers.js) derives its lock name here, and a divergent
// derivation would silently put them on two different locks that never
// exclude each other.
export function lockNameForFile(filePath) {
  return `file:${path.resolve(filePath)}`;
}

function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM = process exists but owned by a different user (still alive)
    return err?.code === "EPERM";
  }
}

async function tryAcquire(lockPath, name, token, owner) {
  let fh;
  try {
    fh = await open(lockPath, "wx");
    await fh.writeFile(JSON.stringify({
      pid: process.pid,
      hostname: os.hostname(),
      since: Date.now(),
      name,
      token,
      ...(owner ? { owner } : {}),
    }));
    return true;
  } catch {
    return false;
  } finally {
    if (fh) await fh.close().catch(() => null);
  }
}

async function readLockHolder(lockPath) {
  try {
    const raw = await readFile(lockPath, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function readLock(name) {
  return readLockHolder(lockPathFor(name));
}

function readLockHolderSync(lockPath) {
  try {
    return JSON.parse(readFileSync(lockPath, "utf-8"));
  } catch {
    return null;
  }
}

function sameAcquisition(a, b) {
  if (!a || !b) return false;
  if (a.token || b.token) return a.token === b.token;
  return a.pid === b.pid && a.since === b.since;
}

// Compare-and-delete: only removes the file if it is still the acquisition
// `expected` describes. A small read→unlink window remains (no atomic
// compare-and-unlink on a plain file); it's the same window the lock has
// always had, and every acquirer re-checks via exclusive create.
function unlinkIfSameSync(lockPath, expected) {
  if (!sameAcquisition(readLockHolderSync(lockPath), expected)) return false;
  try { unlinkSync(lockPath); return true; } catch { return false; }
}

// Cooldown sentinel: a separate, never-deleted file recording when a lock
// was last released. Unlike the lock file itself (removed on release), this
// persists across acquire/release cycles so the NEXT acquirer can tell how
// recently the resource was used, even though nothing was holding the lock
// in between. Opt-in via acquireLock's `cooldownMs` — callers that don't
// pass it see no behavior change (best-effort: a missing/unreadable
// sentinel just means "no known cooldown to honor").
function cooldownPath(name) {
  return path.join(locksDir(), `${safeName(name)}.cooldown.json`);
}

async function readLastReleasedAt(name) {
  try {
    const raw = await readFile(cooldownPath(name), "utf-8");
    return JSON.parse(raw)?.releasedAt ?? null;
  } catch {
    return null;
  }
}

// Synchronous by design — called from the same `process.on("exit")`
// handler that releases the lock file itself, where async writes can't be
// awaited.
function markReleasedSync(name) {
  try {
    writeFileSync(cooldownPath(name), JSON.stringify({ releasedAt: Date.now(), name }));
  } catch { /* best-effort — worst case, the next acquirer skips its cooldown wait */ }
}

// A resolver that throws is treated as "not resolved": waiting longer is
// recoverable, stealing a live holder's lock is not.
async function isOwnerResolved(holder) {
  const resolver = shared().resolvers.get(holder.owner.kind);
  try {
    return Boolean(await resolver.isResolved(holder));
  } catch {
    return false;
  }
}

async function reapStale(lockPath, staleMs) {
  const holder = await readLockHolder(lockPath);
  if (!holder) return false;
  // Dead process → reap (its exit listener didn't fire, e.g. SIGKILL).
  if (!isPidAlive(holder.pid)) return unlinkIfSameSync(lockPath, holder);
  // Owned by a kind that can say when its work is done → that is the only
  // signal; age alone never reaps it.
  const ownerKind = holder.owner?.kind;
  if (ownerKind && ownerKind !== "process" && shared().resolvers.has(ownerKind)) {
    if (!(await isOwnerResolved(holder))) return false;
    return unlinkIfSameSync(lockPath, holder);
  }
  // Alive AND held longer than staleMs → likely PID reuse (OS recycled
  // the original holder's PID for an unrelated process). Reap.
  const isFresh = (Date.now() - holder.since) < staleMs;
  if (!isFresh) return unlinkIfSameSync(lockPath, holder);
  // Alive and within staleMs → real holder doing real work (possibly an
  // indefinite retry waiting for Chrome to come back). Wait.
  return false;
}

function reentrantHandle(lockPath, holder) {
  return {
    path: lockPath,
    token: holder.token ?? null,
    owner: holder.owner ?? null,
    reentrant: true,
    isOwned: () => sameAcquisition(readLockHolderSync(lockPath), holder),
    async release() { /* the outer acquisition releases */ },
  };
}

export async function acquireLock(name, opts = {}) {
  // timeoutMs defaults to Infinity — see file header on "Wait semantics".
  // Callers can pass a finite number to fail-fast when wait time matters.
  // staleMs defaults to 6h and only applies to unowned/process-owned locks
  // — see file header on "Stale recovery".
  const { timeoutMs = Infinity, staleMs = 6 * 60 * 60_000, pollMs = 1000, onWouldBlock, cooldownMs = 0, cooldownJitterMs = 0, logger, now = () => Date.now(), sleep = (ms) => new Promise((r) => setTimeout(r, ms)) } = opts;
  const owner = opts.owner ?? currentLockOwner();
  await mkdir(locksDir(), { recursive: true });
  const lockPath = lockPathFor(name);
  const deadline = Number.isFinite(timeoutMs) ? now() + timeoutMs : Infinity;
  const token = randomUUID();
  const mine = { token };
  let released = false;
  // Fires at most once per call, on the first failed tryAcquire — not on
  // every subsequent poll tick, which would spam a "queued" status write
  // for the whole wait.
  let notifiedWouldBlock = false;
  while (true) {
    if (await tryAcquire(lockPath, name, token, owner)) {
      const releaseSync = () => {
        if (released) return;
        released = true;
        if (unlinkIfSameSync(lockPath, mine)) markReleasedSync(name);
      };
      // Synchronous release on process exit. Covers process.exit(),
      // unhandled exceptions, and SIGTERM/SIGINT (Node fires the exit
      // event after each).
      process.on("exit", releaseSync);
      // Cooldown: hold the lock we just won for a minimum gap since the
      // resource was last released, instead of letting the next holder
      // start the instant the previous one finishes. Exists because strict
      // serialization alone doesn't stop back-to-back Cloudflare
      // challenges in the same session from escalating each other's risk
      // score — spacing them out does. No-op for callers that don't pass
      // cooldownMs.
      if (cooldownMs > 0) {
        const lastReleasedAt = await readLastReleasedAt(name);
        if (lastReleasedAt != null) {
          const targetGap = cooldownMs + (cooldownJitterMs > 0 ? Math.random() * cooldownJitterMs : 0);
          const remaining = targetGap - (now() - lastReleasedAt);
          if (remaining > 0) await sleep(remaining);
        }
      }
      return {
        path: lockPath,
        token,
        owner: owner ?? null,
        reentrant: false,
        isOwned: () => sameAcquisition(readLockHolderSync(lockPath), mine),
        async release() {
          if (released) return;
          released = true;
          process.removeListener("exit", releaseSync);
          if (unlinkIfSameSync(lockPath, mine)) {
            markReleasedSync(name);
          } else {
            logger?.warn?.(`lock ${name}: no longer held by this acquisition at release — left the current holder's lock in place`);
          }
        },
      };
    }
    const holder = await readLockHolder(lockPath);
    if (holder && owner && sameOwner(holder.owner, owner) && holder.pid === process.pid) {
      return reentrantHandle(lockPath, holder);
    }
    if (!notifiedWouldBlock) {
      notifiedWouldBlock = true;
      await onWouldBlock?.();
    }
    // Failed — check if stale and retry
    if (await reapStale(lockPath, staleMs)) continue;
    if (Number.isFinite(deadline) && now() >= deadline) {
      const current = await readLockHolder(lockPath);
      const hint = current
        ? `held by ${current.owner ? `${describeOwner(current.owner)}, ` : ""}pid ${current.pid} on ${current.hostname} (acquired ${Math.round((now() - current.since) / 1000)}s ago)`
        : "lock file unreadable";
      const err = new Error(`acquireLock(${name}): timeout ${timeoutMs}ms — ${hint}`);
      err.code = "LOCK_TIMEOUT";
      err.holder = current;
      throw err;
    }
    await sleep(pollMs);
  }
}

export async function withLock(name, fn, opts = {}) {
  // Reentrancy guard: the on-disk lock is pid-scoped (tryAcquire fails for
  // anyone, including the SAME process, once the file exists), so a nested
  // withLock() call for the same name from within an already-locked async
  // call chain would otherwise poll forever waiting for itself to release
  // — a guaranteed self-deadlock. Concretely: a manifold fallback node
  // (e.g. SUBMIT) can declare crossProcessLock: "browser:freelance" and
  // hold it for its whole dispatch, while its own recover.before sequence
  // dispatches a node (e.g. ENSURE_INFRA_AGENT) that ALSO declares
  // crossProcessLock: "browser:freelance". heldLockNames tracks names held
  // by the current async chain (not the whole process — a genuinely
  // unrelated concurrent flow in the same process must still serialize
  // normally), so only a true nested re-entry skips re-acquiring.
  if (heldLockNames.getStore()?.has(name)) {
    return fn();
  }
  const lock = await acquireLock(name, opts);
  const nextHeld = new Set(heldLockNames.getStore() ?? []);
  nextHeld.add(name);
  const runHeld = () => heldLockNames.run(nextHeld, () => fn());
  // wrapBrowserTabLifecycle only wraps when `name` is "browser:<profile>"
  // for a profile in opts.browserProfiles (a no-op passthrough for every
  // other lock, and for every "browser:" name when the caller injects no
  // profiles) — applied on the real acquire/release path only, never for
  // a re-entry, so a nested acquirer never opens/closes its own tabs
  // mid-way through the outer holder's critical section. This is the
  // single point of enforcement for every "browser:*" lock consumer,
  // whether it's a pipeline node's declarative `crossProcessLock` field
  // (dispatched through executor.js, which threads the harness config's
  // browser.profiles in) or application code calling withLock directly
  // (e.g. freelance_workana-poll-inbox's scan-and-probe.js, which acquires
  // "browser:freelance" inline, multiple times, with no node-level
  // declaration at all) — see browser-tabs.js for what/why. isOwned keeps
  // a holder whose lock was taken over from closing the new holder's tabs.
  const guardedFn = lock.reentrant
    ? runHeld
    : wrapBrowserTabLifecycle(name, runHeld, { ...opts, isOwned: lock.isOwned });
  try {
    return await guardedFn();
  } finally {
    await lock.release();
  }
}
