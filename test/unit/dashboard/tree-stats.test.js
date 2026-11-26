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
    const tree = [row("PRIME", 1), row("DRAFT", 3)];
    assert.equal(stepsExecutedFromTree(tree), 4);
  });

  test("treats a missing executionCount as 0, not NaN", () => {
    const tree = [row("PRIME", 1), { type: "node", nodeId: "DRAFT" }];
    assert.equal(stepsExecutedFromTree(tree), 1);
  });
});
