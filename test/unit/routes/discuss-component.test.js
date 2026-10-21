/**
 * Unit tests for routes/discuss-component.js
 *
 * Verifies:
 *   - Persists the component before emitting anything.
 *   - Draft components emit proposed + pending with text mapping.
 *   - Meta components emit proposed + pending with fields mapping.
 *   - Unknown component kinds emit proposed only (no pending derivation).
 *   - Missing emitter still persists without throwing.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createComponentEmitter } from "../../../routes/discuss-component.js";

function makeStore() {
  const calls = [];
  return {
    calls,
    setComponent: async (flowId, itemId, component) => {
      calls.push({ flowId, itemId, component });
    },
  };
}

function makeEmit() {
  const calls = [];
  const emit = async (kind, payload) => {
    calls.push({ kind, payload });
    return { ok: true, kind, deliveries: {} };
  };
  emit.calls = calls;
  return emit;
}

// ── draft path ───────────────────────────────────────────────────────────────

describe("componentEmitter — proposed_draft", () => {
  test("persists first, then emits proposed + pending with text", async () => {
    const order = [];
    const discussStore = {
      setComponent: async (...a) => { order.push("persist"); return makeStore().setComponent(...a); },
    };
    const emit = async (kind, payload) => {
      order.push(`emit:${kind}`);
      return { ok: true, kind, deliveries: {} };
    };
    const componentEmitter = createComponentEmitter({ discussStore, emit });
    const component = { kind: "proposed_draft", text: "new body", draftId: null, updatedAt: 1234 };
    await componentEmitter({ flowId: "f", itemId: "i", component });

    assert.deepEqual(order, [
      "persist",
      "emit:discuss.component.proposed",
      "emit:discuss.pending.updated",
    ]);
  });

  test("pending carries text, null fields, component timestamp", async () => {
    const discussStore = makeStore();
    const emit = makeEmit();
    const componentEmitter = createComponentEmitter({ discussStore, emit });
    const component = { kind: "proposed_draft", text: "new body", draftId: null, updatedAt: 1234 };
    await componentEmitter({ flowId: "f", itemId: "i", component });

    assert.equal(emit.calls.length, 2);
    assert.equal(emit.calls[0].kind, "discuss.component.proposed");
    assert.equal(emit.calls[0].payload.component, component);
    assert.equal(emit.calls[1].kind, "discuss.pending.updated");
    assert.deepEqual(emit.calls[1].payload, {
      flowId: "f",
      itemId: "i",
      text: "new body",
      fields: null,
      updatedAt: 1234,
    });
  });
});

// ── meta path ────────────────────────────────────────────────────────────────

describe("componentEmitter — proposed_meta", () => {
  test("pending carries fields, null text", async () => {
    const discussStore = makeStore();
    const emit = makeEmit();
    const componentEmitter = createComponentEmitter({ discussStore, emit });
    const fields = { budget: "2400" };
    await componentEmitter({
      flowId: "f",
      itemId: "i",
      component: { kind: "proposed_meta", fields, updatedAt: 5678 },
    });

    assert.equal(emit.calls.length, 2);
    assert.equal(emit.calls[0].kind, "discuss.component.proposed");
    assert.deepEqual(emit.calls[1].payload, {
      flowId: "f",
      itemId: "i",
      text: null,
      fields,
      updatedAt: 5678,
    });
  });
});

// ── edges ────────────────────────────────────────────────────────────────────

describe("componentEmitter — edges", () => {
  test("unknown component kind emits proposed only", async () => {
    const discussStore = makeStore();
    const emit = makeEmit();
    const componentEmitter = createComponentEmitter({ discussStore, emit });
    await componentEmitter({ flowId: "f", itemId: "i", component: { kind: "other" } });
    assert.equal(emit.calls.length, 1);
    assert.equal(emit.calls[0].kind, "discuss.component.proposed");
  });

  test("missing emitter still persists without throwing", async () => {
    const discussStore = makeStore();
    const componentEmitter = createComponentEmitter({ discussStore, emit: null });
    await componentEmitter({ flowId: "f", itemId: "i", component: { kind: "proposed_draft", text: "t" } });
    assert.equal(discussStore.calls.length, 1);
  });

  test("persist failure does not block emission", async () => {
    const discussStore = { setComponent: async () => { throw new Error("db down"); } };
    const emit = makeEmit();
    const componentEmitter = createComponentEmitter({ discussStore, emit });
    await componentEmitter({ flowId: "f", itemId: "i", component: { kind: "proposed_draft", text: "t" } });
    assert.equal(emit.calls.length, 2);
  });
});
