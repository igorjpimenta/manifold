import { test } from "node:test";
import assert from "node:assert/strict";
import { buildLanesSnapshot, SUSPECT_STALE_MS } from "../../../dashboard/lanes.js";

const NOW = 1_800_000_000_000;

function run(overrides = {}) {
  return { status: "running", lastStep: "SCAN", pipelineId: "p1", updatedAt: NOW - 1_000, ...overrides };
}

function snapshot({ admitted, runs = [], limit = 4, queued = 0, now = NOW }) {
  return buildLanesSnapshot({
    limit,
    queued,
    admitted,
    runsByFlow: new Map(runs.map((r) => [r.flowId, r])),
    now,
  });
}

test("counts held, free and queued against the limit", () => {
  const s = snapshot({
    limit: 4,
    queued: 2,
    admitted: [
      { flowId: "a", kind: "flow-start", admittedAt: NOW - 5_000 },
      { flowId: "b", kind: "child-dispatch", admittedAt: NOW - 7_000 },
    ],
    runs: [{ ...run(), flowId: "a" }, { ...run(), flowId: "b" }],
  });
  assert.equal(s.held, 2);
  assert.equal(s.free, 2);
  assert.equal(s.queued, 2);
  assert.equal(s.ok, 2);
  assert.equal(s.suspect, 0);
  assert.equal(s.orphan, 0);
});

test("a running flow that moved recently holds an ok lane", () => {
  const s = snapshot({
    admitted: [{ flowId: "a", kind: "flow-start", admittedAt: NOW - 60_000 }],
    runs: [{ ...run(), flowId: "a", updatedAt: NOW - 1_000 }],
  });
  assert.equal(s.lanes[0].holder, "ok");
  assert.equal(s.lanes[0].ageMs, 60_000);
  assert.equal(s.lanes[0].idleMs, 1_000);
});

test("a running flow idle past the stale window is suspect", () => {
  const s = snapshot({
    admitted: [{ flowId: "a", kind: "flow-start", admittedAt: NOW - 3_600_000 }],
    runs: [{ ...run(), flowId: "a", updatedAt: NOW - (SUSPECT_STALE_MS + 1) }],
  });
  assert.equal(s.lanes[0].holder, "suspect");
  assert.equal(s.suspect, 1);
});

test("a terminal flow still holding a lane is an orphan", () => {
  for (const status of ["succeeded", "failed", "cancelled", "skipped"]) {
    const s = snapshot({
      admitted: [{ flowId: "a", kind: "flow-start", admittedAt: NOW - 5_000 }],
      runs: [{ ...run(), flowId: "a", status }],
    });
    assert.equal(s.lanes[0].holder, "orphan", `${status} must not read as a live lane`);
    assert.equal(s.orphan, 1);
  }
});

test("a blocked flow still holding a lane is an orphan — setWaiting should have released it", () => {
  const s = snapshot({
    admitted: [{ flowId: "a", kind: "child-dispatch", admittedAt: NOW - 5_000 }],
    runs: [{ ...run(), flowId: "a", status: "blocked" }],
  });
  assert.equal(s.lanes[0].holder, "orphan");
});

test("a lane whose flow row is gone is an orphan", () => {
  const s = snapshot({ admitted: [{ flowId: "ghost", kind: "flow-start", admittedAt: NOW - 5_000 }] });
  assert.equal(s.lanes[0].holder, "orphan");
  assert.equal(s.lanes[0].flowStatus, null);
  assert.equal(s.lanes[0].pipelineId, null);
});

test("never reports a negative free count when the pool is over-subscribed", () => {
  const s = snapshot({
    limit: 1,
    admitted: [
      { flowId: "a", kind: "flow-start", admittedAt: NOW },
      { flowId: "b", kind: "flow-start", admittedAt: NOW },
    ],
    runs: [{ ...run(), flowId: "a" }, { ...run(), flowId: "b" }],
  });
  assert.equal(s.free, 0);
  assert.equal(s.held, 2);
});

test("exposes the stale window it judged against", () => {
  const s = snapshot({ admitted: [] });
  assert.equal(s.staleMs, SUSPECT_STALE_MS);
  assert.deepEqual(s.lanes, []);
});
