// Unit tests for state/tool-handoff.js — registerDispatch, resolveInvocationId,
// clearDispatch, writePendingResult, readPendingResult, clearPendingResult.
// Covers the single in-memory bridge a dispatched subagent's tool call uses
// to hand data back to the node that spawned it — see the module's own
// header comment for why this is in-memory, not persisted.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  registerDispatch,
  resolveInvocationId,
  clearDispatch,
  writePendingResult,
  readPendingResult,
  clearPendingResult,
  notifyToolFired,
  waitForToolSignal,
} from "../../../state/tool-handoff.js";

function basePayload(overrides = {}) {
  return {
    score: 78,
    reasons: ["stack overlap"],
    fitFlags: { language: true, budget: true, domain: true, time: true },
    ...overrides,
  };
}

describe("registerDispatch / resolveInvocationId / clearDispatch", () => {
  test("resolveInvocationId returns null for an unregistered sessionKey", () => {
    assert.strictEqual(resolveInvocationId(`unknown-${randomUUID()}`), null);
  });

  test("resolveInvocationId returns null for a falsy sessionKey", () => {
    assert.strictEqual(resolveInvocationId(null), null);
    assert.strictEqual(resolveInvocationId(""), null);
  });

  test("registerDispatch then resolveInvocationId returns the registered invocationId", () => {
    const sessionKey = `session-${randomUUID()}`;
    const invocationId = `inv-${randomUUID()}`;
    registerDispatch(sessionKey, invocationId);
    assert.strictEqual(resolveInvocationId(sessionKey), invocationId);
    clearDispatch(sessionKey);
  });

  test("registerDispatch does nothing when sessionKey or invocationId is falsy", () => {
    assert.doesNotThrow(() => registerDispatch(null, "inv-1"));
    assert.doesNotThrow(() => registerDispatch("session-1", null));
    assert.strictEqual(resolveInvocationId("session-1"), null);
  });

  test("clearDispatch removes the mapping", () => {
    const sessionKey = `session-${randomUUID()}`;
    registerDispatch(sessionKey, `inv-${randomUUID()}`);
    clearDispatch(sessionKey);
    assert.strictEqual(resolveInvocationId(sessionKey), null);
  });

  test("clearDispatch does not throw when the sessionKey was never registered", () => {
    assert.doesNotThrow(() => clearDispatch(`never-${randomUUID()}`));
  });

  test("two different sessionKeys resolve independently", () => {
    const s1 = `session-${randomUUID()}`;
    const s2 = `session-${randomUUID()}`;
    const i1 = `inv-${randomUUID()}`;
    const i2 = `inv-${randomUUID()}`;
    registerDispatch(s1, i1);
    registerDispatch(s2, i2);
    assert.strictEqual(resolveInvocationId(s1), i1);
    assert.strictEqual(resolveInvocationId(s2), i2);
    clearDispatch(s1);
    clearDispatch(s2);
  });
});

describe("writePendingResult / readPendingResult / clearPendingResult", () => {
  test("readPendingResult returns null when nothing was written", () => {
    assert.strictEqual(readPendingResult(`inv-${randomUUID()}`), null);
  });

  test("readPendingResult returns null for a falsy invocationId", () => {
    assert.strictEqual(readPendingResult(null), null);
    assert.strictEqual(readPendingResult(""), null);
  });

  test("writePendingResult then readPendingResult round-trips the exact payload", () => {
    const invocationId = `inv-${randomUUID()}`;
    const payload = basePayload();
    writePendingResult(invocationId, payload);
    assert.deepEqual(readPendingResult(invocationId), payload);
    clearPendingResult(invocationId);
  });

  test("writePendingResult does nothing when invocationId is falsy", () => {
    assert.doesNotThrow(() => writePendingResult(null, basePayload()));
    assert.doesNotThrow(() => writePendingResult("", basePayload()));
  });

  test("clearPendingResult removes the entry", () => {
    const invocationId = `inv-${randomUUID()}`;
    writePendingResult(invocationId, basePayload());
    clearPendingResult(invocationId);
    assert.strictEqual(readPendingResult(invocationId), null);
  });

  test("clearPendingResult does not throw when nothing was written", () => {
    assert.doesNotThrow(() => clearPendingResult(`never-${randomUUID()}`));
  });

  test("clearPendingResult does nothing when invocationId is falsy", () => {
    assert.doesNotThrow(() => clearPendingResult(null));
  });

  test("writePendingResult overwrites an existing entry for the same invocationId", () => {
    const invocationId = `inv-${randomUUID()}`;
    writePendingResult(invocationId, basePayload({ score: 10 }));
    writePendingResult(invocationId, basePayload({ score: 90 }));
    assert.strictEqual(readPendingResult(invocationId).score, 90);
    clearPendingResult(invocationId);
  });

  test("two different invocationIds are stored independently", () => {
    const i1 = `inv-${randomUUID()}`;
    const i2 = `inv-${randomUUID()}`;
    writePendingResult(i1, basePayload({ score: 11 }));
    writePendingResult(i2, basePayload({ score: 22 }));
    assert.strictEqual(readPendingResult(i1).score, 11);
    assert.strictEqual(readPendingResult(i2).score, 22);
    clearPendingResult(i1);
    clearPendingResult(i2);
  });

  test("clearing one invocationId does not affect another", () => {
    const i1 = `inv-${randomUUID()}`;
    const i2 = `inv-${randomUUID()}`;
    writePendingResult(i1, basePayload({ score: 1 }));
    writePendingResult(i2, basePayload({ score: 2 }));
    clearPendingResult(i2);
    assert.strictEqual(readPendingResult(i1).score, 1);
    assert.strictEqual(readPendingResult(i2), null);
    clearPendingResult(i1);
  });
});

describe("state survives being loaded as a distinct module instance (regression: jiti/CJS vs native-ESM split)", () => {
  // extensions/manifold/** loads through jiti's CJS transpilation while
  // pipeline files under workspace/.pipelines/** load via native ESM
  // `await import(url)`. Both import this file by the same file:// URL,
  // but jiti and native ESM do not share a module registry — each
  // produces its OWN separate instance, each with its own state. A plain
  // module-scope `const` Map silently split into two disconnected copies:
  // one loader's writePendingResult never reached the other's
  // readPendingResult — same process, same file, same invocationId,
  // `stored` was always null.
  //
  // This test can't reproduce jiti's CJS transpilation itself (that's a
  // build-tool concern, not something node:test exercises), but it proves
  // the actual fix — state keyed off `globalThis`, not module-closure
  // variables — by forcing a genuinely SEPARATE module instance the way
  // native ESM itself would for two different specifiers (a `?`-suffixed
  // cache-busting URL is a different module identity as far as Node's ESM
  // loader is concerned, same as jiti's CJS wrapper is a different identity
  // from the native ESM one). If state ever regresses to a plain module-
  // scope variable, this test starts failing immediately.
  test("writePendingResult in one module instance is visible via readPendingResult in a second, independently-imported instance", async () => {
    const secondInstanceUrl = `${new URL("../../../state/tool-handoff.js", import.meta.url).href}?instance=${randomUUID()}`;
    const second = await import(secondInstanceUrl);
    const invocationId = `inv-${randomUUID()}`;
    const payload = basePayload({ score: 55 });

    // Written via the module imported at the top of this file...
    writePendingResult(invocationId, payload);
    // ...read via a SEPARATE module instance (distinct specifier, own
    // closure) — only passes if the underlying state is a real
    // process-wide singleton (globalThis), not per-module-instance.
    assert.deepEqual(second.readPendingResult(invocationId), payload);

    second.clearPendingResult(invocationId);
    assert.strictEqual(readPendingResult(invocationId), null, "clear via the second instance must be visible via the first");
  });

  test("registerDispatch/resolveInvocationId also share state across independently-imported instances", async () => {
    const secondInstanceUrl = `${new URL("../../../state/tool-handoff.js", import.meta.url).href}?instance=${randomUUID()}`;
    const second = await import(secondInstanceUrl);
    const sessionKey = `session-${randomUUID()}`;
    const invocationId = `inv-${randomUUID()}`;

    second.registerDispatch(sessionKey, invocationId);
    assert.strictEqual(resolveInvocationId(sessionKey), invocationId);
    clearDispatch(sessionKey);
    assert.strictEqual(second.resolveInvocationId(sessionKey), null);
  });

  test("notifyToolFired/waitForToolSignal also share state across independently-imported instances", async () => {
    const secondInstanceUrl = `${new URL("../../../state/tool-handoff.js", import.meta.url).href}?instance=${randomUUID()}`;
    const second = await import(secondInstanceUrl);
    const runId = `run-${randomUUID()}`;

    const signal = waitForToolSignal(runId);
    second.notifyToolFired(runId);
    await signal.promise; // would hang (test timeout) if the two instances didn't share state
  });
});

describe("notifyToolFired / waitForToolSignal", () => {
  test("waitForToolSignal's promise resolves once notifyToolFired is called for the same runId", async () => {
    const runId = `run-${randomUUID()}`;
    const signal = waitForToolSignal(runId);
    let resolved = false;
    signal.promise.then(() => { resolved = true; });
    assert.equal(resolved, false, "must not resolve before notifyToolFired fires");
    notifyToolFired(runId);
    await signal.promise;
    assert.equal(resolved, true);
  });

  test("notifyToolFired for an unrelated runId does not resolve a different waiter", async () => {
    const runId = `run-${randomUUID()}`;
    const otherRunId = `run-${randomUUID()}`;
    const signal = waitForToolSignal(runId);
    let resolved = false;
    signal.promise.then(() => { resolved = true; });
    notifyToolFired(otherRunId);
    await Promise.resolve(); // let microtasks settle
    assert.equal(resolved, false);
    signal.cancel();
  });

  test("notifyToolFired for a runId with no waiters does not throw", () => {
    assert.doesNotThrow(() => notifyToolFired(`run-${randomUUID()}`));
  });

  test("notifyToolFired does nothing when runId is falsy", () => {
    assert.doesNotThrow(() => notifyToolFired(null));
    assert.doesNotThrow(() => notifyToolFired(""));
  });

  test("waitForToolSignal with a falsy runId returns a promise that never resolves and a no-op cancel", async () => {
    const signal = waitForToolSignal(null);
    let resolved = false;
    signal.promise.then(() => { resolved = true; });
    await Promise.resolve();
    assert.equal(resolved, false);
    assert.doesNotThrow(() => signal.cancel());
  });

  test("cancel() removes the waiter so a later notifyToolFired for that runId no longer resolves it", async () => {
    const runId = `run-${randomUUID()}`;
    const signal = waitForToolSignal(runId);
    let resolved = false;
    signal.promise.then(() => { resolved = true; });
    signal.cancel();
    notifyToolFired(runId);
    await Promise.resolve();
    assert.equal(resolved, false, "a cancelled waiter must not be woken by a later notify");
  });

  test("two independent waiters on the same runId both resolve on one notifyToolFired call", async () => {
    const runId = `run-${randomUUID()}`;
    const signalA = waitForToolSignal(runId);
    const signalB = waitForToolSignal(runId);
    notifyToolFired(runId);
    await Promise.all([signalA.promise, signalB.promise]);
  });

  test("cancelling one of two waiters on the same runId leaves the other one resolvable", async () => {
    const runId = `run-${randomUUID()}`;
    const signalA = waitForToolSignal(runId);
    const signalB = waitForToolSignal(runId);
    signalA.cancel();
    notifyToolFired(runId);
    await signalB.promise; // would hang (test timeout) if notify no longer reached signalB
  });

  test("notifyToolFired only fires once — a second call for the same runId is a no-op (no pending waiters left)", () => {
    const runId = `run-${randomUUID()}`;
    const signal = waitForToolSignal(runId);
    notifyToolFired(runId);
    signal.cancel(); // already resolved; cancel after the fact must not throw
    assert.doesNotThrow(() => notifyToolFired(runId));
  });
});
