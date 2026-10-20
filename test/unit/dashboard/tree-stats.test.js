/**
 * Unit tests for dashboard/tree-stats.js's stepsExecutedFromTree — the pure
 * computation dashboard/server.js's deriveSystemSummary uses for its
 * stepsExecuted stat, kept in its own dependency-free module so it's
 * testable without pulling in device.js's `openclaw` package import
 * (server.js itself can't be imported directly in a unit test).
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { stepsExecutedFromTree } from "../../../dashboard/tree-stats.js";

function row(nodeId, executionCount) {
  return { type: "node", nodeId, executionCount };
}

describe("stepsExecutedFromTree", () => {
  test("returns 0 for an empty tree", () => {
    assert.equal(stepsExecutedFromTree([]), 0);
  });

  test("returns 0 for a non-array input", () => {
    assert.equal(stepsExecutedFromTree(null), 0);
    assert.equal(stepsExecutedFromTree(undefined), 0);
  });

  test("sums executionCount across every row, not just row count", () => {
    // PRIME dispatched once, DRAFT dispatched 3 times (e.g. two retries) —
    // this must total 4: retries count as extra executions under the same
    // invocation row, not as separate rows.
    const tree = [row("PRIME", 1), row("DRAFT", 3)];
    assert.equal(stepsExecutedFromTree(tree), 4);
  });

  test("treats a missing executionCount as 0, not NaN", () => {
    const tree = [row("PRIME", 1), { type: "node", nodeId: "DRAFT" }];
    assert.equal(stepsExecutedFromTree(tree), 1);
  });
});
