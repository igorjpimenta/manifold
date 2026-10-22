/**
 * Unit tests for events/catalog.js
 *
 * Verifies:
 *   - One reload fans out to gateway WS (canonical kind) and dashboard bus
 *     with identical content.
 *   - Missing dashboard sink degrades to WS-only without throwing.
 *   - Missing emitter degrades to dashboard-only without throwing.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createCatalogPublisher } from "../../../events/catalog.js";

function makeEmit() {
  const calls = [];
  const emit = async (kind, payload) => {
    calls.push({ kind, payload });
    return { ok: true, kind, deliveries: {} };
  };
  emit.calls = calls;
  return emit;
}

// ── fan-out ──────────────────────────────────────────────────────────────────

describe("publishCatalogChanged — fan-out", () => {
  test("gateway WS gets canonical kind, dashboard gets kind + content", async () => {
    const emit = makeEmit();
    const dashboardCalls = [];
    const publish = createCatalogPublisher({
      emit,
      getDashboardBroadcast: () => (e) => dashboardCalls.push(e),
    });

    await publish({ size: 3, ids: ["a", "b", "c"] });

    assert.equal(emit.calls.length, 1);
    assert.equal(emit.calls[0].kind, "pipeline.catalog.updated");
    assert.equal(emit.calls[0].payload.count, 3);
    assert.deepEqual(emit.calls[0].payload.ids, ["a", "b", "c"]);
    assert.equal(typeof emit.calls[0].payload.updatedAt, "number");

    assert.equal(dashboardCalls.length, 1);
    assert.equal(dashboardCalls[0].kind, "pipeline.catalog.updated");
    assert.equal(dashboardCalls[0].size, 3);
    assert.deepEqual(dashboardCalls[0].ids, ["a", "b", "c"]);
    assert.equal(dashboardCalls[0].at, emit.calls[0].payload.updatedAt, "both sinks share one timestamp");
  });

  test("missing dashboard sink degrades to WS-only", async () => {
    const emit = makeEmit();
    const publish = createCatalogPublisher({ emit, getDashboardBroadcast: () => null });
    await publish({ size: 1, ids: ["a"] });
    assert.equal(emit.calls.length, 1);
  });

  test("missing emitter degrades to dashboard-only", async () => {
    const dashboardCalls = [];
    const publish = createCatalogPublisher({
      emit: null,
      getDashboardBroadcast: () => (e) => dashboardCalls.push(e),
    });
    await publish({ size: 1, ids: ["a"] });
    assert.equal(dashboardCalls.length, 1);
    assert.equal(dashboardCalls[0].kind, "pipeline.catalog.updated");
  });
});
