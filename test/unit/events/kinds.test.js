/**
 * Unit tests for events/kinds.js
 *
 * Verifies:
 *   - Registry shape: 17 wire kinds (frozen, unique), 1 reserved.
 *   - Unknown/reserved kinds rejected everywhere.
 *   - Payload contracts enforced (required fields, session scope + ids).
 *   - Routing: dashboard-only kinds, SSE set.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  WIRE_KINDS,
  RESERVED_KINDS,
  isWireKind,
  isReservedKind,
  assertPayload,
  isDashboardOnly,
  isSseKind,
} from "../../../events/kinds.js";

// ── registry shape ───────────────────────────────────────────────────────────

describe("kinds — registry", () => {
  test("17 wire kinds, frozen, unique", () => {
    assert.equal(WIRE_KINDS.length, 17);
    assert.ok(Object.isFrozen(WIRE_KINDS));
    assert.equal(new Set(WIRE_KINDS).size, 17);
  });

  test("reserved holds gate.dissolved only", () => {
    assert.deepEqual([...RESERVED_KINDS], ["gate.dissolved"]);
  });

  test("every kind is dots-only, no plugin. prefix", () => {
    for (const kind of WIRE_KINDS) {
      assert.match(kind, /^[a-z]+(\.[a-z]+)+$/);
      assert.ok(!kind.startsWith("plugin."));
    }
  });
});

// ── predicates ───────────────────────────────────────────────────────────────

describe("kinds — predicates", () => {
  test("isWireKind true for registry, false otherwise", () => {
    assert.equal(isWireKind("gate.opened"), true);
    assert.equal(isWireKind("gate.dissolved"), false);
    assert.equal(isWireKind("nope"), false);
  });

  test("isReservedKind true only for reserved", () => {
    assert.equal(isReservedKind("gate.dissolved"), true);
    assert.equal(isReservedKind("gate.opened"), false);
  });
});

// ── payload contracts ────────────────────────────────────────────────────────

describe("kinds — assertPayload", () => {
  test("unknown kind throws", () => {
    assert.throws(() => assertPayload("nope", {}), /unknown event kind/);
  });

  test("reserved kind throws", () => {
    assert.throws(() => assertPayload("gate.dissolved", {}), /reserved/);
  });

  test("non-object payload throws", () => {
    assert.throws(() => assertPayload("gate.opened", null), /must be an object/);
  });

  test("missing required field throws", () => {
    assert.throws(() => assertPayload("gate.resolved", { flowId: "f" }), /itemId/);
  });

  test("valid payload returns untouched", () => {
    const payload = { flowId: "f", itemId: "i", decision: "approve", extra: 1 };
    assert.equal(assertPayload("gate.resolved", payload), payload);
  });

  test("session kinds require a valid scope", () => {
    assert.throws(
      () => assertPayload("session.transcript.completed", { scope: "x", text: "t" }),
      /scope/,
    );
  });

  test("scope=node requires nodeId, scope=discuss requires turnId", () => {
    assert.throws(
      () => assertPayload("session.transcript.completed", { scope: "node", text: "t" }),
      /nodeId/,
    );
    assert.throws(
      () => assertPayload("session.transcript.completed", { scope: "discuss", text: "t", nodeId: "n" }),
      /turnId/,
    );
    assert.doesNotThrow(() =>
      assertPayload("session.transcript.completed", { scope: "node", nodeId: "n", sessionKey: "s", text: "t" }),
    );
  });
});

// ── routing ────────────────────────────────────────────────────────────────────

describe("kinds — routing", () => {
  test("run.created is dashboard-only", () => {
    assert.equal(isDashboardOnly("run.created"), true);
    assert.equal(isDashboardOnly("run.updated"), false);
  });

  test("unknown kind throws in routing", () => {
    assert.throws(() => isDashboardOnly("nope"), /unknown/);
    assert.throws(() => isSseKind("nope"), /unknown/);
  });

  test("SSE set is session + discuss families", () => {
    assert.equal(isSseKind("session.transcript.completed"), true);
    assert.equal(isSseKind("discuss.pending.updated"), true);
    assert.equal(isSseKind("gate.opened"), false);
    assert.equal(isSseKind("run.updated"), false);
  });
});
