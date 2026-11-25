/**
 * Unit tests for dashboard/search.js's error indexing: searchRuns indexes
 * errorEntries[] (sourced from flow_node_executions via an opts.errorsByFlow
 * Map). flow_node_executions unifies root and branch errors in one table,
 * so branch-level failures are searchable too.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { searchRuns } from "../../../dashboard/search.js";

function makeRun(overrides = {}) {
  return {
    flowId: "flow-1",
    status: "failed",
    lastStep: "SUBMIT",
    updatedAt: 1000,
    revision: 1,
    stateJson: { pipelineId: "test_pipe" },
    ...overrides,
  };
}

describe("searchRuns() — errorEntries indexing", () => {
  test("matches a flow-level error message via errorEntries (branchKey null)", () => {
    const run = makeRun({ flowId: "flow-root" });
    const errorsByFlow = new Map([
      ["flow-root", [{ nodeId: "SUBMIT", branchKey: null, message: "weekly-cap-reached", at: 1 }]],
    ]);
    const result = searchRuns([run], "weekly-cap-reached", { errorsByFlow });
    assert.equal(result.totalHits, 1);
    assert.equal(result.hits[0].flowId, "flow-root");
  });

  test("matches a branch-level error message via errorEntries (branchKey set)", () => {
    const run = makeRun({ flowId: "flow-branch" });
    const errorsByFlow = new Map([
      ["flow-branch", [{ nodeId: "SUBMIT_CODE", branchKey: "item-42", message: "draft file not found", at: 1 }]],
    ]);
    const result = searchRuns([run], "draft file not found", { errorsByFlow });
    assert.equal(result.totalHits, 1, "branch-level errors are searchable");
    assert.equal(result.hits[0].flowId, "flow-branch");
  });

  test("matches on the nodeId field, not just the message", () => {
    const run = makeRun({ flowId: "flow-node-match" });
    const errorsByFlow = new Map([
      ["flow-node-match", [{ nodeId: "SUBMIT_CODE", branchKey: "item-1", message: "some failure", at: 1 }]],
    ]);
    const result = searchRuns([run], "SUBMIT_CODE", { errorsByFlow });
    assert.equal(result.totalHits, 1);
  });

  test("does not match a run whose errorsByFlow entry doesn't contain the query", () => {
    const run = makeRun({ flowId: "flow-no-match" });
    const errorsByFlow = new Map([
      ["flow-no-match", [{ nodeId: "OTHER", branchKey: null, message: "unrelated text", at: 1 }]],
    ]);
    const result = searchRuns([run], "weekly-cap-reached", { errorsByFlow });
    assert.equal(result.totalHits, 0);
  });

  test("omitting errorsByFlow entirely does not throw — falls back to no error matches", () => {
    const run = makeRun({ flowId: "flow-no-map" });
    assert.doesNotThrow(() => {
      const result = searchRuns([run], "anything", {});
      assert.equal(result.totalHits, 0);
    });
  });

  test("structured fields (flowId, pipelineId, status) still match independent of errorEntries", () => {
    const run = makeRun({ flowId: "flow-structured", status: "succeeded" });
    const result = searchRuns([run], "flow-structured", {});
    assert.equal(result.totalHits, 1);
  });
});
