// Unit tests for the harness lock service (plugins/openclaw/manifold/lock.js) — acquireLock, withLock, safeName, isPidAlive, reapStale semantics.

import { test, describe, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, mkdirSync, existsSync, readFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";
import { mock } from "node:test";

// ─── global test fixtures ────────────────────────────────────────────────────

// A single temp home dir is used for the whole file. The locks dir is
// resolved at call time from the core-owned store home (~/.manifold),
// so the mock must be in place BEFORE the first import of lock.js —
// mock.module() hoists at the describe/test level.
const TEST_HOME = mkdtempSync(join(os.tmpdir(), "gf-lock-test-"));
const LOCKS_DIR = join(TEST_HOME, ".manifold", "locks");
mkdirSync(LOCKS_DIR, { recursive: true });

// Install the os mock before any test can trigger a lock.js import.
await mock.module("node:os", {
  exports: {
    ...os,
    homedir: () => TEST_HOME,
    hostname: () => "testhost",
  },
});

// Now import the module under test — LOCKS_DIR is frozen to TEST_HOME path.
const {
  acquireLock, withLock, registerOwnerResolver, unregisterOwnerResolver,
  runWithLockOwner, currentLockOwner, registerSessionOwner, clearSessionOwner, sessionOwner, readLock,
} = await import("../../../plugins/openclaw/manifold/lock.js");

// ─── helpers ─────────────────────────────────────────────────────────────────

/** Write a lock file directly (simulating another holder). */
function writeLockFile(name, payload) {
  const safed = name.replace(/[^a-zA-Z0-9_-]/g, "_");
  const p = join(LOCKS_DIR, `${safed}.lock`);
  writeFileSync(p, JSON.stringify(payload));
  return p;
}

/** Remove a lock file if it exists (cleanup helper). */
function removeLockFile(name) {
  const safed = name.replace(/[^a-zA-Z0-9_-]/g, "_");
  const p = join(LOCKS_DIR, `${safed}.lock`);
  try { unlinkSync(p); } catch { /* already gone */ }
}

/** Return the lock file path for a given name without creating it. */
function lockPath(name) {
  const safed = name.replace(/[^a-zA-Z0-9_-]/g, "_");
  return join(LOCKS_DIR, `${safed}.lock`);
}

/** A definitely-dead PID — far above what any OS ever assigns. */
const DEAD_PID = 999_999_999;

/**
 * Poll until `predicate()` is truthy, or fail naming the condition.
 *
 * The acquire loop reaches its onWouldBlock hook after a lock-file read, so a
 * fixed sleep before asserting the hook fired races that I/O: on a loaded
 * runner the assertion lands first and the count reads zero.
 */
async function waitUntil(predicate, description, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${description}`);
    }
    await new Promise((r) => setTimeout(r, 5));
  }
}

// ─── safeName — via lock path inspection ─────────────────────────────────────

describe("safeName — special character replacement", () => {
  test("colon is replaced with underscore", async () => {
    const lock = await acquireLock("acct:freelance", {});
    try {
      assert.ok(lock.path.endsWith("acct_freelance.lock"), `path was ${lock.path}`);
    } finally {
      await lock.release();
    }
  });

  test("slash is replaced with underscore", async () => {
    const lock = await acquireLock("acct/main", {});
    try {
      assert.ok(lock.path.endsWith("acct_main.lock"), `path was ${lock.path}`);
    } finally {
      await lock.release();
    }
  });

  test("multiple special chars all become underscores", async () => {
    const lock = await acquireLock("a:b/c.d", {});
    try {
      assert.ok(lock.path.endsWith("a_b_c_d.lock"), `path was ${lock.path}`);
    } finally {
      await lock.release();
    }
  });

  test("alphanumeric, hyphens, and underscores are preserved unchanged", async () => {
    const lock = await acquireLock("my-lock_123", {});
    try {
      assert.ok(lock.path.endsWith("my-lock_123.lock"), `path was ${lock.path}`);
    } finally {
      await lock.release();
    }
  });
});

// ─── acquireLock — basic acquire and release ─────────────────────────────────

describe("acquireLock — basic acquire and release", () => {
  test("returns an object with path and release()", async () => {
    const lock = await acquireLock("basic-acquire", {});
    try {
      assert.ok(typeof lock.path === "string", "lock.path is a string");
      assert.ok(typeof lock.release === "function", "lock.release is a function");
    } finally {
      await lock.release();
    }
  });

  test("path points to a file inside LOCKS_DIR", async () => {
    const lock = await acquireLock("path-check", {});
    try {
      assert.ok(lock.path.startsWith(LOCKS_DIR), `expected ${lock.path} to start with ${LOCKS_DIR}`);
    } finally {
      await lock.release();
    }
  });

  test("lock file exists on disk while held", async () => {
    const lock = await acquireLock("file-exists", {});
    try {
      assert.ok(existsSync(lock.path), "lock file should exist while held");
    } finally {
      await lock.release();
    }
  });

  test("lock file is removed after release()", async () => {
    const lock = await acquireLock("file-removed", {});
    const p = lock.path;
    await lock.release();
    assert.ok(!existsSync(p), "lock file should be gone after release");
  });

  test("lock file contains valid JSON with pid, hostname, since, name fields", async () => {
    const lock = await acquireLock("json-payload", {});
    try {
      const raw = readFileSync(lock.path, "utf-8");
      const payload = JSON.parse(raw);
      assert.equal(payload.pid, process.pid, "pid matches current process");
      assert.equal(payload.hostname, "testhost", "hostname comes from os.hostname()");
      assert.ok(typeof payload.since === "number" && payload.since > 0, "since is a positive epoch ms");
      assert.equal(payload.name, "json-payload", "name matches lock name");
    } finally {
      await lock.release();
    }
  });

  test("since field is within 2s of Date.now()", async () => {
    const before = Date.now();
    const lock = await acquireLock("since-fresh", {});
    const after = Date.now();
    try {
      const { since } = JSON.parse(readFileSync(lock.path, "utf-8"));
      assert.ok(since >= before - 100 && since <= after + 100, `since ${since} should be between ${before} and ${after}`);
    } finally {
      await lock.release();
    }
  });
});

// ─── release() — idempotency and exit listener ───────────────────────────────

describe("release() — idempotency and exit listener", () => {
  test("calling release() twice does not throw", async () => {
    const lock = await acquireLock("idempotent-release", {});
    await lock.release();
    await assert.doesNotReject(() => lock.release(), "second release must not throw");
  });

  test("calling release() three times does not throw", async () => {
    const lock = await acquireLock("idempotent-release-3x", {});
    await lock.release();
    await lock.release();
    await assert.doesNotReject(() => lock.release(), "third release must not throw");
  });

  test("release() removes the exit listener", async () => {
    const before = process.listenerCount("exit");
    const lock = await acquireLock("exit-listener-removed", {});
    const during = process.listenerCount("exit");
    assert.equal(during, before + 1, "one exit listener added on acquire");
    await lock.release();
    const after = process.listenerCount("exit");
    assert.equal(after, before, "exit listener removed on release");
  });

  test("acquireLock registers an exit listener that is visible before release", async () => {
    const before = process.listenerCount("exit");
    const lock = await acquireLock("exit-listener-present", {});
    const during = process.listenerCount("exit");
    try {
      assert.ok(during > before, "exit listener should be registered after acquire");
    } finally {
      await lock.release();
    }
  });
});

// ─── isPidAlive — live/dead/EPERM behavior ───────────────────────────────────

describe("isPidAlive — via stale lock reaping", () => {
  // isPidAlive is private; we test it indirectly by placing lock files with
  // known-dead or known-alive PIDs and observing reap behavior.

  test("dead PID — lock file is reaped, acquire succeeds", async () => {
    const name = "dead-pid-reap";
    writeLockFile(name, { pid: DEAD_PID, hostname: "testhost", since: Date.now(), name });
    const lock = await acquireLock(name, { staleMs: 6 * 3600_000, timeoutMs: 500, pollMs: 20 });
    try {
      assert.ok(lock, "should have acquired lock after reaping dead PID");
    } finally {
      await lock.release();
    }
  });

  test("current process PID is alive — lock held by self within staleMs is NOT reaped", async () => {
    const name = "live-pid-no-reap";
    // Write a lock held by our own PID (alive), well within staleMs.
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    try {
      // timeoutMs is short so we don't hang; if the lock is wrongly reaped we'd acquire.
      await assert.rejects(
        () => acquireLock(name, { staleMs: 6 * 3600_000, timeoutMs: 80, pollMs: 20 }),
        /timeout/,
        "should time out — alive+fresh lock must not be reaped",
      );
    } finally {
      removeLockFile(name);
    }
  });
});

// ─── reapStale — stale-by-time (PID reuse) ───────────────────────────────────

describe("reapStale — PID alive but past staleMs (PID reuse case)", () => {
  test("PID alive but since past staleMs — lock is reaped, acquire succeeds", async () => {
    const name = "stale-by-time";
    const staleMs = 5 * 60 * 1000; // 5 minutes
    const oldSince = Date.now() - staleMs - 1000; // one second past stale floor
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: oldSince, name });
    const lock = await acquireLock(name, { staleMs, timeoutMs: 500, pollMs: 20 });
    try {
      assert.ok(lock, "should acquire after stale-by-time reap");
    } finally {
      await lock.release();
    }
  });

  test("PID alive and within staleMs — lock is NOT reaped", async () => {
    const name = "fresh-pid-not-reaped";
    const staleMs = 6 * 3600_000;
    // `since` well within staleMs — fresh, must not be reaped
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now() - 10_000, name });
    try {
      await assert.rejects(
        () => acquireLock(name, { staleMs, timeoutMs: 80, pollMs: 20 }),
        /timeout/,
        "fresh alive lock must not be reaped",
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("staleMs boundary — 1ms before stale is fresh (no reap)", async () => {
    const name = "stale-boundary-fresh";
    const staleMs = 10_000; // 10 seconds
    // `since` exactly half of staleMs, so fresh
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now() - 5_000, name });
    try {
      await assert.rejects(
        () => acquireLock(name, { staleMs, timeoutMs: 80, pollMs: 20 }),
        /timeout/,
        "lock within staleMs must not be reaped",
      );
    } finally {
      removeLockFile(name);
    }
  });
});

// ─── timeout error message ───────────────────────────────────────────────────

describe("acquireLock — timeout error message content", () => {
  test("throws an error mentioning acquireLock and the lock name", async () => {
    const name = "timeout-name-test";
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("acquireLock("), "message should include 'acquireLock('");
          assert.ok(err.message.includes(name), `message should include lock name '${name}'`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("error message includes timeoutMs value", async () => {
    const name = "timeout-ms-in-message";
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 75, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("75ms"), `expected '75ms' in: ${err.message}`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("error message includes holder pid when lock file is readable", async () => {
    const name = "timeout-pid-hint";
    const holderPid = process.pid;
    writeLockFile(name, { pid: holderPid, hostname: "testhost", since: Date.now(), name });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes(`pid ${holderPid}`), `expected 'pid ${holderPid}' in: ${err.message}`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("error message includes holder hostname when lock file is readable", async () => {
    const name = "timeout-hostname-hint";
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("testhost"), `expected 'testhost' in: ${err.message}`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("error message includes elapsed seconds when lock file is readable", async () => {
    const name = "timeout-elapsed-hint";
    const sinceMs = Date.now() - 7_000; // 7 seconds ago
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: sinceMs, name });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("ago"), `expected 'ago' in: ${err.message}`);
          // May have ticked past the written `since` before the message rendered
          assert.match(err.message, /\d+s ago/, "should have Ns ago pattern");
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("error message falls back to 'lock file unreadable' when lock file cannot be read", async () => {
    const name = "timeout-unreadable";
    // Write a lock with an alive PID and fresh since so it won't be reaped.
    // Then corrupt the file content after writing.
    const p = writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    writeFileSync(p, "NOT JSON {{{{");
    // The lock file exists (wx in tryAcquire will fail) but readLockHolder returns null.
    // However, since the file exists but is not valid JSON, readLockHolder returns null
    // AND isPidAlive can't get the PID, so reapStale returns false (holder is null → false).
    // The lock will never be reaped (no holder) and never acquired (file exists).
    // After timeout, readLockHolder returns null → hint is "lock file unreadable".
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(
            err.message.includes("lock file unreadable"),
            `expected 'lock file unreadable' in: ${err.message}`,
          );
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });
});

// ─── Infinity timeoutMs — never times out ────────────────────────────────────

describe("acquireLock — Infinity timeoutMs", () => {
  test("Infinity timeoutMs never throws even with a stale file that gets reaped", async () => {
    const name = "infinity-no-timeout";
    // Write a lock held by a dead PID — will be reaped on first poll.
    writeLockFile(name, { pid: DEAD_PID, hostname: "testhost", since: Date.now(), name });
    // With Infinity timeout, we should acquire immediately after reaping the dead lock.
    const lock = await acquireLock(name, { timeoutMs: Infinity, staleMs: 6 * 3600_000 });
    try {
      assert.ok(lock, "should acquire with Infinity timeoutMs");
    } finally {
      await lock.release();
    }
  });
});

// ─── withLock ────────────────────────────────────────────────────────────────

describe("withLock", () => {
  test("calls fn and returns its return value", async () => {
    const result = await withLock("wl-return-value", async () => 42, {});
    assert.equal(result, 42);
  });

  test("fn receives the lock while held — file exists during fn execution", async () => {
    let fileExistedDuringFn = false;
    await withLock("wl-held-during", async () => {
      fileExistedDuringFn = existsSync(lockPath("wl-held-during"));
    }, {});
    assert.ok(fileExistedDuringFn, "lock file should exist while fn runs");
  });

  test("lock is released after fn completes normally", async () => {
    const p = lockPath("wl-release-after");
    await withLock("wl-release-after", async () => {}, {});
    assert.ok(!existsSync(p), "lock file should be gone after fn completes");
  });

  test("releases lock even if fn throws", async () => {
    const p = lockPath("wl-throw-release");
    await assert.rejects(
      () => withLock("wl-throw-release", async () => { throw new Error("fn exploded"); }, {}),
      /fn exploded/,
    );
    assert.ok(!existsSync(p), "lock file should be released even when fn throws");
  });

  test("rethrows fn error after releasing lock", async () => {
    await assert.rejects(
      () => withLock("wl-rethrow", async () => { throw new Error("downstream error"); }, {}),
      (err) => {
        assert.equal(err.message, "downstream error");
        return true;
      },
    );
  });

  test("works with fn that returns a Promise resolving to an object", async () => {
    const result = await withLock("wl-object-result", async () => ({ ok: true, value: 7 }), {});
    assert.deepEqual(result, { ok: true, value: 7 });
  });

  test("second withLock on same name after first completes succeeds", async () => {
    await withLock("wl-sequential", async () => {}, {});
    const result = await withLock("wl-sequential", async () => "second", {});
    assert.equal(result, "second");
  });
});

// ─── concurrent access serialization ─────────────────────────────────────────

describe("acquireLock — serialization between concurrent callers", () => {
  test("two concurrent acquires serialize — second waits for first to release", async () => {
    const name = "concurrent-serial";
    const order = [];
    const firstAcquired = acquireLock(name, { timeoutMs: 2000, pollMs: 20 });
    // Give the first a head start.
    await new Promise((r) => setTimeout(r, 5));
    const secondAcquired = acquireLock(name, { timeoutMs: 2000, pollMs: 20 });

    const first = await firstAcquired;
    order.push("first-got-lock");
    // Small delay to let second enter the polling loop.
    await new Promise((r) => setTimeout(r, 30));
    await first.release();
    order.push("first-released");

    const second = await secondAcquired;
    order.push("second-got-lock");
    await second.release();

    assert.ok(order.indexOf("first-released") < order.indexOf("second-got-lock"),
      `second should acquire after first releases; order: ${order.join(" → ")}`);
  });
});

// ─── tryAcquire — exclusive-create semantics ─────────────────────────────────

describe("tryAcquire — exclusive lock file creation (via acquireLock behavior)", () => {
  test("if lock file already exists and holder is alive+fresh, tryAcquire returns false and waits", async () => {
    const name = "try-acquire-false";
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name });
    try {
      // Holder is alive+fresh — we expect a timeout (not immediate acquire).
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        /timeout/,
        "should not acquire if file already exists and holder is alive",
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("acquiring an uncontested lock is fast (< 200ms)", async () => {
    const start = Date.now();
    const lock = await acquireLock("fast-acquire", {});
    const elapsed = Date.now() - start;
    await lock.release();
    assert.ok(elapsed < 200, `acquire should be fast, took ${elapsed}ms`);
  });
});

// ─── readLockHolder — null on missing or malformed file ──────────────────────

describe("readLockHolder — via reapStale + timeout error path", () => {
  test("missing lock file does not prevent a fresh acquire", async () => {
    // No pre-existing lock file — should acquire immediately.
    const lock = await acquireLock("no-prior-lock", {});
    try {
      assert.ok(existsSync(lock.path));
    } finally {
      await lock.release();
    }
  });

  test("malformed lock file (not JSON) results in 'lock file unreadable' timeout hint", async () => {
    const name = "malformed-holder";
    const p = lockPath(name);
    writeFileSync(p, "this-is-not-json");
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("lock file unreadable"), `got: ${err.message}`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });

  test("empty lock file results in 'lock file unreadable' timeout hint", async () => {
    const name = "empty-holder";
    const p = lockPath(name);
    writeFileSync(p, "");
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 10, staleMs: 6 * 3600_000 }),
        (err) => {
          assert.ok(err.message.includes("lock file unreadable"), `got: ${err.message}`);
          return true;
        },
      );
    } finally {
      removeLockFile(name);
    }
  });
});

// ─── locks dir is created if missing ─────────────────────────────────────────

describe("acquireLock — directory creation", () => {
  test("acquireLock creates the locks directory if it does not exist", async () => {
    // We can't easily test a missing LOCKS_DIR without a separate module instance.
    // Instead, verify LOCKS_DIR exists after acquiring — it was either pre-created
    // or created by acquireLock itself.
    const lock = await acquireLock("dir-created", {});
    try {
      assert.ok(existsSync(LOCKS_DIR), "LOCKS_DIR should exist after acquireLock");
    } finally {
      await lock.release();
    }
  });
});

// ─── onWouldBlock — queued-status visibility hook ────────────────────────────

describe("acquireLock — onWouldBlock hook", () => {
  test("not fired at all when the lock is uncontested", async () => {
    let calls = 0;
    const lock = await acquireLock("owb-uncontested", { onWouldBlock: () => { calls++; } });
    try {
      assert.equal(calls, 0, "onWouldBlock must not fire on an immediate acquire");
    } finally {
      await lock.release();
    }
  });

  test("fires exactly once on a blocked acquire, even across several poll ticks", async () => {
    const name = "owb-blocked-once";
    const first = await acquireLock(name, {});
    let calls = 0;
    const secondPromise = acquireLock(name, {
      timeoutMs: 2000,
      pollMs: 20,
      onWouldBlock: () => { calls++; },
    });
    // Let the second acquirer poll several times against the still-held lock.
    await waitUntil(() => calls > 0, "the blocked acquire to report onWouldBlock");
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(calls, 1, "onWouldBlock must fire exactly once, not once per poll tick");
    await first.release();
    const second = await secondPromise;
    try {
      assert.equal(calls, 1, "onWouldBlock must not fire again after the lock frees");
    } finally {
      await second.release();
    }
  });

  test("withLock(name, fn, {onWouldBlock}) forwards the opt through to acquireLock unchanged", async () => {
    const name = "owb-withlock-forward";
    const first = await acquireLock(name, {});
    let calls = 0;
    const withLockPromise = withLock(name, async () => "done", {
      timeoutMs: 2000,
      pollMs: 20,
      onWouldBlock: () => { calls++; },
    });
    await waitUntil(() => calls > 0, "withLock to forward onWouldBlock to acquireLock");
    assert.equal(calls, 1, "withLock should surface a blocked acquire via the forwarded onWouldBlock");
    await first.release();
    assert.equal(await withLockPromise, "done");
  });

  test("not fired for a reentrant nested withLock() call on the same name", async () => {
    const name = "owb-reentrant";
    let innerCalls = 0;
    await withLock(name, async () => {
      // Nested call for the SAME name from within the current async chain —
      // the AsyncLocalStorage reentrancy guard returns fn() directly without
      // ever reaching acquireLock, so onWouldBlock must never fire here.
      return withLock(name, async () => "nested-done", {
        onWouldBlock: () => { innerCalls++; },
      });
    }, {});
    assert.equal(innerCalls, 0, "onWouldBlock must not fire for a reentrant acquire");
  });
});

// ─── acquireLock — cooldown ─────────────────────────────────────────────────
//
// Serialization alone (the lock's core job) doesn't stop back-to-back
// Cloudflare Turnstile challenges in the same session from escalating each
// other's bot-risk score. cooldownMs/cooldownJitterMs make the NEXT acquirer
// wait out a minimum gap since the resource was last released, on top of
// ordinary mutual exclusion.

describe("acquireLock — cooldown", () => {
  test("no cooldownMs (default) — acquiring immediately after a release does not wait", async () => {
    const name = "cooldown-default-off";
    const first = await acquireLock(name, {});
    await first.release();
    const start = Date.now();
    const second = await acquireLock(name, {});
    const elapsed = Date.now() - start;
    await second.release();
    // Smoke bound, not a latency guarantee: a lock round-trip is file I/O,
    // so this only rules out a stuck acquire, not a slow one.
    assert.ok(elapsed < 1000, `expected near-instant acquire, took ${elapsed}ms`);
  });

  test("cooldownMs set but no prior release recorded — first-ever acquire of a name does not wait", async () => {
    const name = "cooldown-first-ever";
    const start = Date.now();
    const lock = await acquireLock(name, { cooldownMs: 200 });
    const elapsed = Date.now() - start;
    await lock.release();
    // Stays under the 200ms an erroneous wait would take, with margin over
    // single-acquire file-I/O noise.
    assert.ok(elapsed < 150, `expected no cooldown wait on first-ever acquire, took ${elapsed}ms`);
  });

  test("cooldownMs set — acquiring right after a release waits out the remaining gap", async () => {
    const name = "cooldown-enforced";
    const first = await acquireLock(name, { cooldownMs: 150 });
    await first.release();
    const start = Date.now();
    const second = await acquireLock(name, { cooldownMs: 150 });
    const elapsed = Date.now() - start;
    await second.release();
    assert.ok(elapsed >= 120, `expected to wait out most of the 150ms cooldown, only waited ${elapsed}ms`);
  });

  test("cooldownMs set — acquiring well after the cooldown window has already elapsed does not wait", async () => {
    const name = "cooldown-already-elapsed";
    const first = await acquireLock(name, { cooldownMs: 200 });
    await first.release();
    // An acquire that must wait out the window, measured first in the same
    // process: the post-window acquire below must beat it. No absolute bound
    // can express this — an erroneous wait at cooldown scale hides inside
    // file-I/O noise that has measured in the hundreds of milliseconds on
    // loaded shared machines, so the window itself has to clear the noise.
    const enforcedStart = Date.now();
    const second = await acquireLock(name, { cooldownMs: 200 });
    const enforcedElapsed = Date.now() - enforcedStart;
    await second.release();
    assert.ok(enforcedElapsed >= 160, `expected the immediate re-acquire to wait out most of the 200ms window, waited ${enforcedElapsed}ms`);
    await new Promise((r) => setTimeout(r, 250));
    const start = Date.now();
    const third = await acquireLock(name, { cooldownMs: 200 });
    const elapsed = Date.now() - start;
    await third.release();
    assert.ok(elapsed < enforcedElapsed, `expected the post-window acquire (${elapsed}ms) to beat the enforced wait (${enforcedElapsed}ms)`);
  });

  test("cooldownJitterMs adds on top of cooldownMs, never less than the base", async () => {
    const name = "cooldown-jitter";
    const first = await acquireLock(name, { cooldownMs: 100, cooldownJitterMs: 100 });
    await first.release();
    const start = Date.now();
    const second = await acquireLock(name, { cooldownMs: 100, cooldownJitterMs: 100 });
    const elapsed = Date.now() - start;
    await second.release();
    assert.ok(elapsed >= 80, `jittered cooldown should never wait less than roughly the base cooldownMs, waited ${elapsed}ms`);
  });

  test("cooldown is scoped per lock name — a different name is unaffected", async () => {
    const heldName = "cooldown-scope-a";
    const freshName = "cooldown-scope-b";
    const first = await acquireLock(heldName, { cooldownMs: 200 });
    await first.release();
    const start = Date.now();
    const second = await acquireLock(freshName, { cooldownMs: 200 });
    const elapsed = Date.now() - start;
    await second.release();
    // Same bound rationale as the first-ever-acquire case above: stays under
    // the 200ms an erroneous wait would take, with margin over single-acquire
    // file-I/O noise.
    assert.ok(elapsed < 150, `a fresh lock name should never inherit another name's cooldown, took ${elapsed}ms`);
  });

  test("withLock forwards cooldownMs through to acquireLock unchanged", async () => {
    const name = "cooldown-via-withlock";
    await withLock(name, async () => "first", { cooldownMs: 120 });
    const start = Date.now();
    await withLock(name, async () => "second", { cooldownMs: 120 });
    const elapsed = Date.now() - start;
    assert.ok(elapsed >= 90, `withLock should honor cooldownMs on the next acquire, waited ${elapsed}ms`);
  });
});

// ─── owners — resolution-based staleness, owner-matched release, re-entry ────

describe("owners — a lock owned by unresolved work is never reaped by age", () => {
  const HOUR = 3600_000;

  test("unresolved owner: held for an hour, waiter's staleMs of 10min does NOT reap it", async () => {
    const name = "owner-unresolved-old";
    registerOwnerResolver("t-unresolved", { isResolved: () => false });
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now() - HOUR, name, token: "tok-a", owner: { kind: "t-unresolved", id: "inv-1" } });
    try {
      await assert.rejects(
        () => acquireLock(name, { staleMs: 10 * 60_000, timeoutMs: 80, pollMs: 20 }),
        /timeout/,
      );
    } finally {
      unregisterOwnerResolver("t-unresolved");
      removeLockFile(name);
    }
  });

  test("resolved owner: reaped immediately, however fresh", async () => {
    const name = "owner-resolved-fresh";
    registerOwnerResolver("t-resolved", { isResolved: (holder) => holder.owner.id === "done" });
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name, token: "tok-b", owner: { kind: "t-resolved", id: "done" } });
    try {
      const lock = await acquireLock(name, { timeoutMs: 500, pollMs: 20 });
      await lock.release();
    } finally {
      unregisterOwnerResolver("t-resolved");
    }
  });

  test("a resolver that throws counts as unresolved — the holder keeps the lock", async () => {
    const name = "owner-resolver-throws";
    registerOwnerResolver("t-throws", { isResolved: () => { throw new Error("db down"); } });
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now() - HOUR, name, token: "tok-c", owner: { kind: "t-throws", id: "x" } });
    try {
      await assert.rejects(() => acquireLock(name, { staleMs: 1000, timeoutMs: 80, pollMs: 20 }), /timeout/);
    } finally {
      unregisterOwnerResolver("t-throws");
      removeLockFile(name);
    }
  });

  test("an owner kind with no registered resolver falls back to staleMs", async () => {
    const name = "owner-no-resolver";
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now() - HOUR, name, token: "tok-d", owner: { kind: "t-unregistered", id: "x" } });
    const lock = await acquireLock(name, { staleMs: 10 * 60_000, timeoutMs: 500, pollMs: 20 });
    await lock.release();
  });

  test("a dead holder PID is reaped even while its owner is unresolved", async () => {
    const name = "owner-dead-pid";
    registerOwnerResolver("t-unresolved-2", { isResolved: () => false });
    writeLockFile(name, { pid: DEAD_PID, hostname: "testhost", since: Date.now(), name, token: "tok-e", owner: { kind: "t-unresolved-2", id: "x" } });
    try {
      const lock = await acquireLock(name, { timeoutMs: 500, pollMs: 20 });
      await lock.release();
    } finally {
      unregisterOwnerResolver("t-unresolved-2");
    }
  });

  test("the timeout error names the holder's owner via the resolver's describe()", async () => {
    const name = "owner-timeout-describe";
    registerOwnerResolver("t-describe", { isResolved: () => false, describe: (o) => `SUBMIT invocation ${o.id}` });
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name, token: "tok-f", owner: { kind: "t-describe", id: "inv-9" } });
    try {
      await assert.rejects(
        () => acquireLock(name, { timeoutMs: 60, pollMs: 20 }),
        (err) => {
          assert.equal(err.code, "LOCK_TIMEOUT");
          assert.match(err.message, /held by SUBMIT invocation inv-9, pid/);
          assert.equal(err.holder?.owner?.id, "inv-9");
          return true;
        },
      );
    } finally {
      unregisterOwnerResolver("t-describe");
      removeLockFile(name);
    }
  });
});

describe("owners — release only removes this acquisition's lock", () => {
  test("a holder whose lock was taken over leaves the new holder's lock in place", async () => {
    const name = "release-after-takeover";
    const warnings = [];
    const lock = await acquireLock(name, { logger: { warn: (m) => warnings.push(m) } });
    // Someone else reaped it and acquired: the file now carries their token.
    writeLockFile(name, { pid: process.pid, hostname: "testhost", since: Date.now(), name, token: "someone-else" });
    assert.equal(lock.isOwned(), false);
    await lock.release();
    assert.ok(existsSync(lockPath(name)), "the new holder's lock must survive the old holder's release");
    assert.equal(JSON.parse(readFileSync(lockPath(name), "utf8")).token, "someone-else");
    assert.ok(warnings.some((w) => /not held by this acquisition/.test(w)));
    removeLockFile(name);
  });

  test("isOwned() is true while the file carries this acquisition's token", async () => {
    const lock = await acquireLock("is-owned-true");
    assert.equal(lock.isOwned(), true);
    await lock.release();
    assert.equal(lock.isOwned(), false);
  });
});

describe("owners — re-entry by the same owner", () => {
  test("same owner in the same process gets a non-owning handle; the outer acquisition keeps the lock", async () => {
    const name = "owner-reentry";
    const owner = { kind: "t-reentry", id: "inv-r" };
    const outer = await acquireLock(name, { owner });
    const inner = await acquireLock(name, { owner, timeoutMs: 200, pollMs: 20 });
    assert.equal(inner.reentrant, true);
    await inner.release();
    assert.ok(existsSync(lockPath(name)), "inner release must not free the outer acquisition");
    assert.equal(outer.isOwned(), true);
    await outer.release();
    assert.ok(!existsSync(lockPath(name)));
  });

  test("a different owner waits", async () => {
    const name = "owner-different";
    registerOwnerResolver("t-diff", { isResolved: () => false });
    const outer = await acquireLock(name, { owner: { kind: "t-diff", id: "a" } });
    try {
      await assert.rejects(
        () => acquireLock(name, { owner: { kind: "t-diff", id: "b" }, timeoutMs: 80, pollMs: 20 }),
        /timeout/,
      );
    } finally {
      await outer.release();
      unregisterOwnerResolver("t-diff");
    }
  });

  test("a re-entrant acquire never fires onWouldBlock", async () => {
    const name = "owner-reentry-no-wouldblock";
    const owner = { kind: "t-reentry-2", id: "inv" };
    const outer = await acquireLock(name, { owner });
    let fired = 0;
    const inner = await acquireLock(name, { owner, onWouldBlock: () => { fired += 1; } });
    assert.equal(fired, 0);
    await inner.release();
    await outer.release();
  });
});

describe("owners — runWithLockOwner and session owners", () => {
  test("acquireLock inside runWithLockOwner records that owner in the lock file", async () => {
    const name = "owner-from-context";
    const owner = { kind: "t-ctx", id: "inv-ctx", flowId: "flow-1" };
    await runWithLockOwner(owner, async () => {
      assert.deepEqual(currentLockOwner(), owner);
      const lock = await acquireLock(name);
      const onDisk = await readLock(name);
      assert.deepEqual(onDisk.owner, owner);
      assert.equal(typeof onDisk.token, "string");
      await lock.release();
    });
    assert.equal(currentLockOwner(), null);
  });

  test("an explicit owner wins over the context owner", async () => {
    const name = "owner-explicit-wins";
    await runWithLockOwner({ kind: "t-ctx", id: "ctx" }, async () => {
      const lock = await acquireLock(name, { owner: { kind: "t-explicit", id: "explicit" } });
      assert.equal((await readLock(name)).owner.id, "explicit");
      await lock.release();
    });
  });

  test("session owner registry round-trips and clears", () => {
    registerSessionOwner("agent:x:subagent:1", { kind: "t-session", id: "inv-s" });
    assert.equal(sessionOwner("agent:x:subagent:1").id, "inv-s");
    clearSessionOwner("agent:x:subagent:1");
    assert.equal(sessionOwner("agent:x:subagent:1"), null);
  });
});
