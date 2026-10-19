// Unit tests for the before_tool_call hook that makes a PROFILE.md read
// wait behind gap-answers.js's settleGapAnswer() lock. Uses the real
// lock.js/file-lock.js primitives against a temp HOME (same pattern as
// locks/lock.test.js) rather than mocking acquireLock, so this actually
// exercises the writer/reader lock-name match instead of assuming it.

import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";

const TEST_HOME = mkdtempSync(join(os.tmpdir(), "profile-read-gate-test-"));
mkdirSync(join(TEST_HOME, ".openclaw", "locks"), { recursive: true });

// PROFILE_PATH (lib/paths.js) and LOCKS_DIR (locks/lock.js) are both
// computed from os.homedir() at module-load time — mocking it before any
// import of either keeps writer and reader pointed at the same temp tree.
await mock.module("node:os", {
  exports: { ...os, homedir: () => TEST_HOME },
});

const { PROFILE_PATH } = await import(
  "file:///Users/herculano/.openclaw/workspace/.pipelines/freelance_workana-proposal/lib/paths.js"
);
const { lockNameForFile } = await import(
  "file:///Users/herculano/.openclaw/workspace/.shared/common/lib/file-lock.js"
);
const { acquireLock } = await import("../../../locks/lock.js");
const { registerProfileReadGate } = await import("../../../hooks/profile-read-gate.js");

function makeMockApi() {
  const handlers = [];
  return {
    on: (event, handler, opts) => { handlers.push({ event, handler, opts }); },
    _handlers: handlers,
  };
}

describe("registerProfileReadGate", () => {
  it("registers exactly one before_tool_call hook", () => {
    const api = makeMockApi();
    registerProfileReadGate(api);
    assert.equal(api._handlers.length, 1);
    assert.equal(api._handlers[0].event, "before_tool_call");
  });

  it("no-ops for a non-read tool call, even against PROFILE_PATH", async () => {
    const api = makeMockApi();
    registerProfileReadGate(api);
    const { handler } = api._handlers[0];
    await assert.doesNotReject(() => handler({ toolName: "write", params: { path: PROFILE_PATH } }));
  });

  it("no-ops for a read of a different file", async () => {
    const api = makeMockApi();
    registerProfileReadGate(api);
    const { handler } = api._handlers[0];
    await assert.doesNotReject(() => handler({ toolName: "read", params: { path: "/some/other/file.md" } }));
  });

  it("proceeds immediately when no lock is currently held", async () => {
    const api = makeMockApi();
    registerProfileReadGate(api);
    const { handler } = api._handlers[0];
    const start = Date.now();
    await handler({ toolName: "read", params: { path: PROFILE_PATH } });
    assert.ok(Date.now() - start < 1000, "must not wait when nothing holds the lock");
  });

  it("waits for an in-progress writer lock on PROFILE_PATH, then proceeds once released", async () => {
    const api = makeMockApi();
    registerProfileReadGate(api);
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
