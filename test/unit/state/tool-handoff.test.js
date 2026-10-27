// Unit tests for state/tool-handoff.js — registerTicket, resolveInvocationId,
// clearTicket, writePendingResult, readPendingResult, clearPendingResult.
// Covers the single in-memory bridge a claiming agent's tool call uses
// to hand data back to the parked invocation it claimed — see the module's
// own header comment for why this is in-memory, not persisted.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  registerTicket,
  resolveInvocationId,
  clearTicket,
  writePendingResult,
  readPendingResult,
  clearPendingResult,
} from "../../../state/tool-handoff.js";

function basePayload(overrides = {}) {
  return {
    score: 78,
    reasons: ["stack overlap"],
    fitFlags: { language: true, budget: true, domain: true, time: true },
    ...overrides,
  };
}

describe("registerTicket / resolveInvocationId / clearTicket", () => {
  test("resolveInvocationId returns null for an unregistered ticket", () => {
    assert.strictEqual(resolveInvocationId(`unknown-${randomUUID()}`), null);
  });

  test("resolveInvocationId returns null for a falsy ticket", () => {
    assert.strictEqual(resolveInvocationId(null), null);
    assert.strictEqual(resolveInvocationId(""), null);
  });

  test("registerTicket then resolveInvocationId returns the registered invocationId", () => {
    const ticket = `tick-${randomUUID()}`;
    const invocationId = `inv-${randomUUID()}`;
    registerTicket(ticket, invocationId);
    assert.strictEqual(resolveInvocationId(ticket), invocationId);
    clearTicket(ticket);
  });

  test("registerTicket does nothing when ticket or invocationId is falsy", () => {
    assert.doesNotThrow(() => registerTicket(null, "inv-1"));
    assert.doesNotThrow(() => registerTicket("tick-1", null));
    assert.strictEqual(resolveInvocationId("tick-1"), null);
  });

  test("clearTicket removes the mapping", () => {
    const ticket = `tick-${randomUUID()}`;
    registerTicket(ticket, `inv-${randomUUID()}`);
    clearTicket(ticket);
    assert.strictEqual(resolveInvocationId(ticket), null);
  });

  test("clearTicket does not throw when the ticket was never registered", () => {
    assert.doesNotThrow(() => clearTicket(`never-${randomUUID()}`));
  });

  test("two different tickets resolve independently", () => {
    const t1 = `tick-${randomUUID()}`;
    const t2 = `tick-${randomUUID()}`;
    const i1 = `inv-${randomUUID()}`;
    const i2 = `inv-${randomUUID()}`;
    registerTicket(t1, i1);
    registerTicket(t2, i2);
    assert.strictEqual(resolveInvocationId(t1), i1);
    assert.strictEqual(resolveInvocationId(t2), i2);
    clearTicket(t1);
    clearTicket(t2);
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

  test("registerTicket/resolveInvocationId also share state across independently-imported instances", async () => {
    const secondInstanceUrl = `${new URL("../../../state/tool-handoff.js", import.meta.url).href}?instance=${randomUUID()}`;
    const second = await import(secondInstanceUrl);
    const ticket = `tick-${randomUUID()}`;
    const invocationId = `inv-${randomUUID()}`;

    second.registerTicket(ticket, invocationId);
    assert.strictEqual(resolveInvocationId(ticket), invocationId);
    clearTicket(ticket);
    assert.strictEqual(second.resolveInvocationId(ticket), null);
  });
});
