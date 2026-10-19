// Unit tests for gates/paused-flows.js — covers listPaused, isPaused, pause,
// resume, and write (via side-effects).

import { test, mock, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

// ─── mock node:fs and node:os before importing the module ────────────────────
// paused-flows.js reads STATE_FILE at module load time using path.join(os.homedir(), ...)
// We intercept fs and os so no real disk I/O happens.

const FAKE_HOME = "/fake-home";
const FAKE_STATE_FILE = `${FAKE_HOME}/.openclaw/state/pipeclaw-paused.json`;

// In-memory filesystem stub — keyed by absolute path.
const _files = new Map();
const _mkdirCalls = [];
const _writeFileCalls = [];

await mock.module("node:os", {
  exports: {
    homedir() {
      return FAKE_HOME;
    },
  },
});

await mock.module("node:fs", {
  exports: {
    readFileSync(filePath, _enc) {
      if (!_files.has(filePath)) {
        const err = new Error(`ENOENT: no such file: ${filePath}`);
        err.code = "ENOENT";
        throw err;
      }
      return _files.get(filePath);
    },
    writeFileSync(filePath, data, _enc) {
      _writeFileCalls.push({ filePath, data });
      _files.set(filePath, data);
    },
    mkdirSync(dirPath, opts) {
      _mkdirCalls.push({ dirPath, opts });
    },
  },
});

// Import the module under test AFTER mocks are registered.
const { listPaused, isPaused, pause, resume } =
  await import("../../../gates/paused-flows.js");

// ─── helper — reset in-memory state between tests ────────────────────────────

function resetFs() {
  _files.clear();
  _mkdirCalls.length = 0;
  _writeFileCalls.length = 0;
}

function seedFile(content) {
  _files.set(FAKE_STATE_FILE, content);
}

// ─── listPaused ───────────────────────────────────────────────────────────────

describe("listPaused", () => {
  beforeEach(resetFs);

  test("returns empty Set when STATE_FILE does not exist", () => {
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });

  test("returns empty Set when STATE_FILE contains invalid JSON", () => {
    seedFile("this is not json {{");
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });

  test("returns empty Set when parsed JSON is not an array (object)", () => {
    seedFile(JSON.stringify({ flowId: "abc" }));
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });

  test("returns empty Set when parsed JSON is not an array (null)", () => {
    seedFile(JSON.stringify(null));
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });

  test("returns empty Set when parsed JSON is not an array (string)", () => {
    seedFile(JSON.stringify("just-a-string"));
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });

  test("returns a Set of flowIds when STATE_FILE contains a valid JSON array", () => {
    const ids = ["flow-aaa", "flow-bbb", "flow-ccc"];
    seedFile(JSON.stringify(ids));
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 3);
    for (const id of ids) {
      assert.ok(result.has(id), `expected Set to contain ${id}`);
    }
  });

  test("returns empty Set when STATE_FILE contains an empty array", () => {
    seedFile(JSON.stringify([]));
    const result = listPaused();
    assert.ok(result instanceof Set);
    assert.equal(result.size, 0);
  });
});

// ─── isPaused ─────────────────────────────────────────────────────────────────

describe("isPaused", () => {
  beforeEach(resetFs);

  test("returns false for a flowId not in the paused set", () => {
    seedFile(JSON.stringify(["flow-x"]));
    assert.equal(isPaused("flow-y"), false);
  });

  test("returns false when STATE_FILE does not exist", () => {
    assert.equal(isPaused("any-flow"), false);
  });

  test("returns true for a flowId that is in the persisted set", () => {
    const flowId = "flow-to-check";
    seedFile(JSON.stringify([flowId, "other-flow"]));
    assert.equal(isPaused(flowId), true);
  });

  test("returns false after the flow has been removed from the set", () => {
    const flowId = "flow-ephemeral";
    seedFile(JSON.stringify([flowId]));
    // Simulate external removal by overwriting the file.
    seedFile(JSON.stringify([]));
    assert.equal(isPaused(flowId), false);
  });
});

// ─── pause ────────────────────────────────────────────────────────────────────

describe("pause", () => {
  beforeEach(resetFs);

  test("adds a flowId to the persisted set when starting empty", () => {
    const flowId = "flow-new";
    pause(flowId);
    const stored = listPaused();
    assert.ok(stored.has(flowId));
  });

  test("adds a flowId without removing pre-existing entries", () => {
    const existing = "flow-existing";
    seedFile(JSON.stringify([existing]));
    const newFlow = "flow-added";
    pause(newFlow);
    const stored = listPaused();
    assert.ok(stored.has(existing));
    assert.ok(stored.has(newFlow));
  });

  test("is idempotent — adding the same flowId twice yields one entry", () => {
    const flowId = "flow-dup";
    pause(flowId);
    pause(flowId);
    const stored = listPaused();
    assert.equal(stored.size, 1);
    assert.ok(stored.has(flowId));
  });

  test("calls writeFileSync with JSON array containing the flowId", () => {
    _writeFileCalls.length = 0;
    const flowId = "flow-written";
    pause(flowId);
    const lastWrite = _writeFileCalls[_writeFileCalls.length - 1];
    assert.ok(lastWrite, "writeFileSync should have been called");
    assert.equal(lastWrite.filePath, FAKE_STATE_FILE);
    const written = JSON.parse(lastWrite.data);
    assert.ok(Array.isArray(written));
    assert.ok(written.includes(flowId));
  });

  test("calls mkdirSync with recursive:true before writing", () => {
    _mkdirCalls.length = 0;
    pause("flow-mkdir");
    assert.ok(_mkdirCalls.length >= 1, "mkdirSync should be called");
    const call = _mkdirCalls[0];
    assert.ok(call.opts && call.opts.recursive === true);
  });
});

// ─── resume ───────────────────────────────────────────────────────────────────

describe("resume", () => {
  beforeEach(resetFs);

  test("removes a flowId from the persisted set", () => {
    const flowId = "flow-to-remove";
    seedFile(JSON.stringify([flowId, "flow-other"]));
    resume(flowId);
    const stored = listPaused();
    assert.equal(stored.has(flowId), false);
    assert.ok(stored.has("flow-other"));
  });

  test("is a no-op for a flowId that was never paused", () => {
    seedFile(JSON.stringify(["flow-keeper"]));
    // Should not throw and should leave existing entries intact.
    resume("flow-nonexistent");
    const stored = listPaused();
    assert.equal(stored.size, 1);
    assert.ok(stored.has("flow-keeper"));
  });

  test("is a no-op when the state file does not exist", () => {
    assert.doesNotThrow(() => resume("flow-missing"));
    // After resume the file should be written with an empty array.
    const stored = listPaused();
    assert.equal(stored.size, 0);
  });

  test("persists the removal — subsequent listPaused does not include it", () => {
    const flowId = "flow-gone";
    seedFile(JSON.stringify([flowId]));
    resume(flowId);
    // listPaused reads from the in-memory stub which was updated by writeFileSync.
    assert.equal(isPaused(flowId), false);
  });

  test("calling resume twice on the same flowId does not throw", () => {
    const flowId = "flow-double-resume";
    seedFile(JSON.stringify([flowId]));
    resume(flowId);
    assert.doesNotThrow(() => resume(flowId));
    assert.equal(isPaused(flowId), false);
  });
});

// ─── write (via side-effects) ─────────────────────────────────────────────────

describe("write side-effects (via pause/resume)", () => {
  beforeEach(resetFs);

  test("creates the STATE_FILE parent directory if it does not exist", () => {
    _mkdirCalls.length = 0;
    pause("flow-dir-create");
    const call = _mkdirCalls.find((c) => c.dirPath === `${FAKE_HOME}/.openclaw/state`);
    assert.ok(call, "mkdirSync should be called with the state directory path");
    assert.deepEqual(call.opts, { recursive: true });
  });

  test("serializes the Set as a JSON array (not an object)", () => {
    _writeFileCalls.length = 0;
    pause("flow-serialize");
    const lastWrite = _writeFileCalls[_writeFileCalls.length - 1];
    const parsed = JSON.parse(lastWrite.data);
    assert.ok(Array.isArray(parsed), "persisted value must be a JSON array");
  });

  test("round-trip: pause then resume leaves an empty array on disk", () => {
    const flowId = "flow-round-trip";
    pause(flowId);
    resume(flowId);
    const lastWrite = _writeFileCalls[_writeFileCalls.length - 1];
    const parsed = JSON.parse(lastWrite.data);
    assert.deepEqual(parsed, []);
  });

  test("multiple pauses accumulate in the persisted array", () => {
    const ids = ["flow-a", "flow-b", "flow-c"];
    for (const id of ids) pause(id);
    const stored = listPaused();
    assert.equal(stored.size, 3);
    for (const id of ids) {
      assert.ok(stored.has(id));
    }
  });
});
