// Unit tests for the before_tool_call hook that makes a PROFILE.md read
// wait behind gap-answers.js's settleGapAnswer() lock. Uses the real
// lock.js primitives against a temp HOME (same pattern as
// adapter/lock.test.js) rather than mocking acquireLock, so this actually
// exercises the writer/reader lock-name match instead of assuming it.
// The gated profile path is a fixture passed through the hook's
// profilePath option — no workspace imports, no real PROFILE.md.

import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";

const TEST_HOME = mkdtempSync(join(os.tmpdir(), "profile-read-gate-test-"));
mkdirSync(join(TEST_HOME, ".openclaw", "locks"), { recursive: true });

// LOCKS_DIR (the harness lock.js) is computed from os.homedir() at module-load
// time — mocking it before any import of lock.js keeps lock state inside
// the temp tree instead of the real ~/.openclaw/locks.
await mock.module("node:os", {
  exports: { ...os, homedir: () => TEST_HOME },
});

const PROFILE_PATH = join(TEST_HOME, "fixture", "profile", "PROFILE.md");

const { lockNameForFile, acquireLock } = await import("../../../plugins/openclaw/manifold/lock.js");
const { registerProfileReadGate } = await import("../../../plugins/openclaw/manifold/hooks/profile-read-gate.js");

function makeMockApi() {
  const handlers = [];
  return {
    on: (event, handler, opts) => { handlers.push({ event, handler, opts }); },
    _handlers: handlers,
  };
}

function register(api) {
  registerProfileReadGate(api, { profilePath: PROFILE_PATH });
}

describe("registerProfileReadGate", () => {
  it("registers exactly one before_tool_call hook", () => {
    const api = makeMockApi();
    register(api);
    assert.equal(api._handlers.length, 1);
    assert.equal(api._handlers[0].event, "before_tool_call");
  });

  it("no-ops for a non-read tool call, even against PROFILE_PATH", async () => {
    const api = makeMockApi();
    register(api);
    const { handler } = api._handlers[0];
    await assert.doesNotReject(() => handler({ toolName: "write", params: { path: PROFILE_PATH } }));
  });

  it("no-ops for a read of a different file", async () => {
    const api = makeMockApi();
    register(api);
    const { handler } = api._handlers[0];
    await assert.doesNotReject(() => handler({ toolName: "read", params: { path: "/some/other/file.md" } }));
  });

  it("proceeds immediately when no lock is currently held", async () => {
    const api = makeMockApi();
    register(api);
    const { handler } = api._handlers[0];
    const start = Date.now();
    await handler({ toolName: "read", params: { path: PROFILE_PATH } });
    assert.ok(Date.now() - start < 1000, "must not wait when nothing holds the lock");
  });

  it("waits for an in-progress writer lock on PROFILE_PATH, then proceeds once released", async () => {
    const api = makeMockApi();
    register(api);
    const { handler } = api._handlers[0];

    const holder = await acquireLock(lockNameForFile(PROFILE_PATH), { timeoutMs: 5000 });
    let resolved = false;
    const pending = handler({ toolName: "read", params: { path: PROFILE_PATH } }).then(() => { resolved = true; });

    await new Promise((r) => setTimeout(r, 100));
    assert.equal(resolved, false, "hook must not resolve while the writer's lock is held");

    await holder.release();
    await pending;
    assert.equal(resolved, true, "hook must resolve once the lock is released");
  });
});
