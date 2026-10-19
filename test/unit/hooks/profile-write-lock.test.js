// Unit tests for the before_tool_call/after_tool_call pair that locks a
// real `edit` call against PROFILE.md and records its outcome. Uses the
// real lock.js/file-lock.js primitives against a temp HOME (same pattern
// as profile-read-gate.test.js) rather than mocking acquireLock, so this
// actually exercises the acquire/release/reader-barrier interaction
// instead of assuming it.

import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";

const TEST_HOME = mkdtempSync(join(os.tmpdir(), "profile-write-lock-test-"));
mkdirSync(join(TEST_HOME, ".openclaw", "locks"), { recursive: true });

// PROFILE_PATH (lib/paths.js), LOCKS_DIR (locks/lock.js), and
// PROFILE_EDIT_RESULTS_DIR (this hook) are all computed from
// os.homedir() at module-load time — mocking it before any import keeps
// every side pointed at the same temp tree.
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
const {
  registerProfileWriteLock,
  profileEditResultPathFor,
} = await import("../../../hooks/profile-write-lock.js");

function makeMockApi() {
  const handlers = [];
  return {
    on: (event, handler, opts) => { handlers.push({ event, handler, opts }); },
    logger: { info: () => {}, warn: () => {}, debug: () => {}, error: () => {} },
    _handlers: handlers,
    _find: (event) => handlers.find((h) => h.event === event)?.handler,
  };
}

describe("registerProfileWriteLock", () => {
  it("registers exactly one before_tool_call and one after_tool_call hook", () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    assert.equal(api._handlers.length, 2);
    assert.ok(api._handlers.some((h) => h.event === "before_tool_call"));
    assert.ok(api._handlers.some((h) => h.event === "after_tool_call"));
  });

  it("before_tool_call no-ops for a non-edit tool call, even against PROFILE_PATH", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    await assert.doesNotReject(() => api._find("before_tool_call")({ toolName: "read", params: { path: PROFILE_PATH } }));
  });

  it("before_tool_call no-ops for an edit of a different file", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    await assert.doesNotReject(() => api._find("before_tool_call")({ toolName: "edit", params: { path: "/some/other/file.md" } }));
  });

  it("acquires and releases the real lock across before/after for a matching edit call", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const before = api._find("before_tool_call");
    const after = api._find("after_tool_call");
    const event = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-1", runId: "run-1" };

    await before(event);
    // Lock is held now — a second acquire attempt (short timeout) must fail.
    await assert.rejects(() => acquireLock(lockNameForFile(PROFILE_PATH), { timeoutMs: 200, pollMs: 50 }));

    await after({ ...event, error: null });
    // Released — a fresh acquire should now succeed immediately.
    const holder = await acquireLock(lockNameForFile(PROFILE_PATH), { timeoutMs: 1000 });
    await holder.release();
  });

  it("a second writer's before_tool_call waits for the first's lock, then proceeds once after_tool_call releases it", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const before = api._find("before_tool_call");
    const after = api._find("after_tool_call");
    const eventA = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-a", runId: "run-a" };
    const eventB = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-b", runId: "run-b" };

    await before(eventA);
    let bResolved = false;
    const pendingB = before(eventB).then(() => { bResolved = true; });

    await new Promise((r) => setTimeout(r, 100));
    assert.equal(bResolved, false, "second writer must wait while the first holds the lock");

    await after({ ...eventA, error: null });
    await pendingB;
    assert.equal(bResolved, true, "second writer proceeds once the first releases");

    await after({ ...eventB, error: null }); // clean up B's own hold
  });

  it("after_tool_call with no error — writes {success:true, error:null} to the runId-keyed result file", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const before = api._find("before_tool_call");
    const after = api._find("after_tool_call");
    const event = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-ok", runId: "run-ok" };

    await before(event);
    await after({ ...event, error: null });

    const resultPath = profileEditResultPathFor("run-ok");
    assert.ok(existsSync(resultPath));
    const result = JSON.parse(await readFile(resultPath, "utf-8"));
    assert.equal(result.success, true);
    assert.equal(result.error, null);
  });

  it("after_tool_call with an error — writes {success:false, error:<message>} to the result file", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const before = api._find("before_tool_call");
    const after = api._find("after_tool_call");
    const event = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-err", runId: "run-err" };

    await before(event);
    await after({ ...event, error: new Error("old_text not found") });

    const result = JSON.parse(await readFile(profileEditResultPathFor("run-err"), "utf-8"));
    assert.equal(result.success, false);
    assert.equal(result.error, "old_text not found");
  });

  it("after_tool_call no-ops (no result file, no release attempt) for a non-edit tool call", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const after = api._find("after_tool_call");
    await assert.doesNotReject(() => after({ toolName: "read", params: { path: PROFILE_PATH }, runId: "run-noop", error: null }));
    assert.equal(existsSync(profileEditResultPathFor("run-noop")), false);
  });

  it("after_tool_call with no runId — releases the lock but does not throw trying to write a result file", async () => {
    const api = makeMockApi();
    registerProfileWriteLock(api);
    const before = api._find("before_tool_call");
    const after = api._find("after_tool_call");
    const event = { toolName: "edit", params: { path: PROFILE_PATH }, toolCallId: "tc-norun" };

    await before(event);
    await assert.doesNotReject(() => after({ ...event, error: null }));
    // Lock released despite the missing runId — a fresh acquire succeeds.
    const holder = await acquireLock(lockNameForFile(PROFILE_PATH), { timeoutMs: 1000 });
    await holder.release();
  });
});
