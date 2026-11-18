// Unit tests for state/tool-handoff.js — sqlite-backed tool-result bridge.
//
// Every test runs against an isolated store home (mkdtemp MANIFOLD_DATA_DIR)
// so the real ~/.manifold is never touched. Resolution is per call, so
// setting the env once here (before any tool-handoff call) relocates the
// whole store for this file. node:test runs each file in its own process,
// so the override never leaks into other suites.

import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { mkdtempSync } from "node:fs";
import {
  writePendingResult,
  readPendingResult,
  clearPendingResult,
  waitForToolResult,
} from "../../../state/tool-handoff.js";
import { createRunStore } from "../../../state/run-store.js";
import { resolveStorePaths } from "../../../state/paths.js";

let tmpDir;
before(() => {
  tmpDir = mkdtempSync(path.join(os.tmpdir(), "manifold-tool-handoff-"));
  process.env.MANIFOLD_DATA_DIR = tmpDir;
});

function session() {
  return `agent:test:${randomUUID()}`;
}

async function seedRun(flowId) {
  const store = createRunStore({ dbPath: resolveStorePaths().runtimeDb });
  await store.insert({ flowId, status: "running" });
}

describe("writePendingResult / readPendingResult", () => {
  test("readPendingResult returns null when nothing was written", async () => {
    assert.equal(await readPendingResult(session()), null);
  });

  test("readPendingResult returns null for a falsy session key", async () => {
    assert.equal(await readPendingResult(null), null);
    assert.equal(await readPendingResult(""), null);
  });

  test("write then read round-trips kind, agent, runId and payload", async () => {
    const key = session();
    const payload = { score: 78, reasons: ["stack overlap"], nested: { ok: true } };
    await seedRun("run-1");
    await writePendingResult({ session: key, runId: "run-1", agentId: "scout", kind: "score", payload });
    const row = await readPendingResult(key);
    assert.equal(row.session, key);
    assert.equal(row.runId, "run-1");
    assert.equal(row.agentId, "scout");
    assert.equal(row.kind, "score");
    assert.deepEqual(row.payload, payload);
    await clearPendingResult(key);
  });

  test("a late result for an unknown run id keeps the payload and drops only the dangling runId", async () => {
    const key = session();
    await writePendingResult({ session: key, runId: "run-that-is-gone", agentId: "scout", kind: "score", payload: { late: true } });
    const row = await readPendingResult(key);
    assert.equal(row.runId, null);
    assert.equal(row.session, key);
    assert.deepEqual(row.payload, { late: true });
    await clearPendingResult(key);
  });

  test("write is latest-wins per session", async () => {
    const key = session();
    await writePendingResult({ session: key, payload: { v: 1 } });
    await writePendingResult({ session: key, payload: { v: 2 } });
    assert.deepEqual((await readPendingResult(key)).payload, { v: 2 });
    await clearPendingResult(key);
  });

  test("write is a no-op without a session key or with an undefined payload", async () => {
    await writePendingResult({ session: null, payload: { v: 1 } });
    await writePendingResult({ session: session(), payload: undefined });
    // Nothing observable to assert beyond "did not throw".
  });

  test("sessions are independent", async () => {
    const k1 = session();
    const k2 = session();
    await writePendingResult({ session: k1, payload: { v: "one" } });
    await writePendingResult({ session: k2, payload: { v: "two" } });
    assert.deepEqual((await readPendingResult(k1)).payload, { v: "one" });
    assert.deepEqual((await readPendingResult(k2)).payload, { v: "two" });
    await clearPendingResult(k1);
    await clearPendingResult(k2);
  });
});

describe("clearPendingResult", () => {
  test("removes the row so a retry never rereads it", async () => {
    const key = session();
    await writePendingResult({ session: key, payload: { v: 1 } });
    await clearPendingResult(key);
    assert.equal(await readPendingResult(key), null);
  });

  test("clearing an unknown key is a no-op", async () => {
    await clearPendingResult(`unknown-${randomUUID()}`);
  });
});

describe("waitForToolResult", () => {
  test("resolves immediately when the row already exists", async () => {
    const key = session();
    await writePendingResult({ session: key, kind: "score", payload: { v: 9 } });
    const { promise, cancel } = waitForToolResult(key, { pollMs: 5 });
    try {
      const row = await promise;
      assert.equal(row.session, key);
      assert.deepEqual(row.payload, { v: 9 });
    } finally {
      cancel();
      await clearPendingResult(key);
    }
  });

  test("resolves when the row lands after the waiter starts", async () => {
    const key = session();
    const { promise, cancel } = waitForToolResult(key, { pollMs: 5 });
    try {
      const pending = promise.then((row) => row);
      await new Promise((r) => setTimeout(r, 20));
      await writePendingResult({ session: key, payload: { late: true } });
      const row = await pending;
      assert.deepEqual(row.payload, { late: true });
    } finally {
      cancel();
      await clearPendingResult(key);
    }
  });

  test("cancel is safe to call twice and on an unknown session", async () => {
    const { cancel } = waitForToolResult(session(), { pollMs: 5 });
    cancel();
    cancel();
  });
});
