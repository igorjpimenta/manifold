/**
 * Unit tests for dashboard/stats.js's error-tracking redirect:
 * resolveDisplayStep/extractError/stepImpactHistogram now take an
 * already-fetched ErrorEntry[] (or Map<flowId, ErrorEntry[]>) instead of
 * reading run.stateJson.errors directly — stats.js stays DB-free.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { resolveDisplayStep, computeGlobalStats, computePipelineStats, computeNodeStats } from "../../../dashboard/stats.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeRun(overrides = {}) {
  return {
    flowId: "flow-1",
    status: "failed",
    lastStep: "PRIME",
    createdAt: 1_700_000_000_000,
    updatedAt: 1_700_000_010_000,
    endedAt: 1_700_000_010_000,
    revision: 1,
    stateJson: { pipelineId: "test_pipe" },
    ...overrides,
  };
}

function errEntry(nodeId, message, at = 1_700_000_005_000) {
  return { nodeId, branchKey: null, message, at };
}

// ── resolveDisplayStep ──────────────────────────────────────────────────────

describe("resolveDisplayStep()", () => {
  test("succeeded run returns lastStep regardless of runErrors", () => {
    const run = makeRun({ status: "succeeded", lastStep: "DONE" });
    assert.equal(resolveDisplayStep(run, [errEntry("IGNORED", "x")]), "DONE");
  });

  test("failed run with runErrors returns the LAST entry's nodeId", () => {
    const run = makeRun({ status: "failed" });
    const runErrors = [errEntry("EARLY", "first"), errEntry("SUBMIT_CODE", "second")];
    assert.equal(resolveDisplayStep(run, runErrors), "SUBMIT_CODE");
  });

  test("failed run with empty runErrors falls back to the registry lastStep", () => {
    const run = makeRun({ status: "failed", lastStep: "PRIME" });
    assert.equal(resolveDisplayStep(run, []), "PRIME");
  });

  test("non-failed status (blocked) ignores runErrors even when present, falls back to lastStep", () => {
    const run = makeRun({ status: "blocked", lastStep: "GATE" });
    assert.equal(resolveDisplayStep(run, [errEntry("SHOULD_NOT_BE_USED", "x")]), "GATE");
  });
});

// ── computeGlobalStats / computePipelineStats — failureStops via errorsByFlow ──

describe("computeGlobalStats() — failureStops threading", () => {
  test("a bare-string-originated error (already normalized to .message by the DB layer) clusters cleanly", () => {
    // A bare-string error is already normalized to .message by the DB layer,
    // so by the time it reaches stats.js it's a flat ErrorEntry with a plain
    // string .message — there's no object-vs-string branching left for
    // stats.js to get wrong.
    const run = makeRun({ flowId: "flow-bare-string", status: "failed", lastStep: "SUBMIT_CODE" });
    const errorsByFlow = new Map([
      ["flow-bare-string", [errEntry("SUBMIT_CODE", "weekly-cap-reached (18/0 sent)")]],
    ]);
    const result = computeGlobalStats([run], errorsByFlow);
    assert.equal(result.failureStops.length, 1);
    const cluster = result.failureStops[0];
    assert.equal(cluster.key, "SUBMIT_CODE");
    assert.equal(cluster.count, 1);
    assert.equal(cluster.errors.length, 1);
    assert.match(cluster.errors[0].exemplarMessage, /weekly-cap-reached/);
  });

  test("runs with no matching entry in errorsByFlow get an empty error history, not a crash", () => {
    const run = makeRun({ flowId: "flow-no-errors", status: "failed" });
    const result = computeGlobalStats([run], new Map());
    assert.equal(result.failureStops.length, 1);
    assert.equal(result.failureStops[0].errors[0].signature, "(no error message)");
  });

  test("errorsByFlow is optional — omitting it entirely does not throw", () => {
    const run = makeRun({ status: "failed" });
    assert.doesNotThrow(() => computeGlobalStats([run]));
  });

  test("two failed runs at the same step but different error messages become two sub-clusters", () => {
    const runA = makeRun({ flowId: "flow-a", status: "failed", lastStep: "SUBMIT_CODE" });
    const runB = makeRun({ flowId: "flow-b", status: "failed", lastStep: "SUBMIT_CODE" });
    const errorsByFlow = new Map([
      ["flow-a", [errEntry("SUBMIT_CODE", "rate limited")]],
      ["flow-b", [errEntry("SUBMIT_CODE", "auth expired")]],
    ]);
    const result = computeGlobalStats([runA, runB], errorsByFlow);
    assert.equal(result.failureStops.length, 1, "same (pipeline, step) is one top-level cluster");
    assert.equal(result.failureStops[0].count, 2);
    assert.equal(result.failureStops[0].errors.length, 2, "two distinct sub-clusters by message signature");
  });
});

describe("computePipelineStats() — failureStops threading", () => {
  test("threads errorsByFlow through to the per-pipeline failureStops", async () => {
    const run = makeRun({ flowId: "flow-p1", status: "failed", lastStep: "DRAFT" });
    const errorsByFlow = new Map([["flow-p1", [errEntry("DRAFT", "draft file not found")]]]);
    const result = await computePipelineStats([run], "test_pipe", null, errorsByFlow);
    assert.equal(result.failureStops.length, 1);
    assert.match(result.failureStops[0].errors[0].exemplarMessage, /draft file not found/);
  });
});

// The default auto-funnel's
// per-node visit check reads visitedByFlow (a Map<flowId, Set<nodeId>> the
// caller fetches in one batched round-trip).
describe("computePipelineStats() — auto-funnel visitedByFlow", () => {
  const FUNNEL_PIPELINE = {
    id: "test_pipe",
    nodes: { PRIME: { kind: "code" }, DRAFT: { kind: "code" } },
  };

  test("a node present in visitedByFlow's set for that flow scores 1", async () => {
    const run = makeRun({ flowId: "flow-p1", status: "succeeded" });
    const visitedByFlow = new Map([["flow-p1", new Set(["PRIME", "DRAFT"])]]);
    const result = await computePipelineStats([run], "test_pipe", FUNNEL_PIPELINE, null, null, null, visitedByFlow);
    const prime = result.funnel.find((s) => s.id === "PRIME");
    const draft = result.funnel.find((s) => s.id === "DRAFT");
    assert.equal(prime.total, 1);
    assert.equal(draft.total, 1);
  });

  test("a node absent from the run's visited set scores 0", async () => {
    const run = makeRun({ flowId: "flow-p1", status: "succeeded" });
    const visitedByFlow = new Map([["flow-p1", new Set(["PRIME"])]]);
    const result = await computePipelineStats([run], "test_pipe", FUNNEL_PIPELINE, null, null, null, visitedByFlow);
    assert.equal(result.funnel.find((s) => s.id === "PRIME").total, 1);
    assert.equal(result.funnel.find((s) => s.id === "DRAFT").total, 0);
  });

  test("a flowId missing from visitedByFlow entirely scores 0 for every stage, no throw", async () => {
    const run = makeRun({ flowId: "flow-no-entry", status: "succeeded" });
    const visitedByFlow = new Map([["some-other-flow", new Set(["PRIME"])]]);
    const result = await computePipelineStats([run], "test_pipe", FUNNEL_PIPELINE, null, null, null, visitedByFlow);
    assert.equal(result.funnel.find((s) => s.id === "PRIME").total, 0);
    assert.equal(result.funnel.find((s) => s.id === "DRAFT").total, 0);
  });

  test("omitting visitedByFlow entirely scores 0 for every stage, no throw", async () => {
    const run = makeRun({ flowId: "flow-p1", status: "succeeded" });
    const result = await computePipelineStats([run], "test_pipe", FUNNEL_PIPELINE);
    assert.equal(result.funnel.find((s) => s.id === "PRIME").total, 0);
    assert.equal(result.funnel.find((s) => s.id === "DRAFT").total, 0);
  });

  test("aggregates across multiple runs — total is the count of runs that visited the node", async () => {
    // PRIME (declared first): visited by both runs → total 2.
    // DRAFT (declared last, so computeFunnel's backward-propagation pass
    // never bumps it from a later stage): visited only by flow-a → total 1.
    const runA = makeRun({ flowId: "flow-a", status: "succeeded" });
    const runB = makeRun({ flowId: "flow-b", status: "succeeded" });
    const visitedByFlow = new Map([
      ["flow-a", new Set(["PRIME", "DRAFT"])],
      ["flow-b", new Set(["PRIME"])],
    ]);
    const result = await computePipelineStats([runA, runB], "test_pipe", FUNNEL_PIPELINE, null, null, null, visitedByFlow);
    assert.equal(result.funnel.find((s) => s.id === "PRIME").total, 2);
    assert.equal(result.funnel.find((s) => s.id === "DRAFT").total, 1);
  });
});

// computeNodeStats' visit
// count reads visitedByFlow (same batched Map the auto-funnel uses) —
// a separate function from the auto-funnel, so it needs its own fetch.
describe("computeNodeStats() — visitedByFlow", () => {
  test("a run whose visitedByFlow set contains the node counts as a visit", () => {
    const run = makeRun({ flowId: "flow-1", lastStep: "GATE" });
    const visitedByFlow = new Map([["flow-1", new Set(["PRIME", "DRAFT"])]]);
    const result = computeNodeStats([run], "test_pipe", "DRAFT", null, visitedByFlow);
    assert.equal(result.visits, 1);
  });

  test("a run whose visitedByFlow set does NOT contain the node, and isn't lastStep, doesn't count", () => {
    const run = makeRun({ flowId: "flow-1", lastStep: "GATE" });
    const visitedByFlow = new Map([["flow-1", new Set(["PRIME"])]]);
    const result = computeNodeStats([run], "test_pipe", "DRAFT", null, visitedByFlow);
    assert.equal(result.visits, 0);
  });

  test("lastStep === nodeId still counts as a visit even when absent from visitedByFlow — e.g. a still-running dispatch with no invocation row yet", () => {
    const run = makeRun({ flowId: "flow-1", status: "running", lastStep: "DRAFT" });
    const visitedByFlow = new Map([["flow-1", new Set()]]);
    const result = computeNodeStats([run], "test_pipe", "DRAFT", null, visitedByFlow);
    assert.equal(result.visits, 1);
  });

  test("omitting visitedByFlow entirely falls back to lastStep === nodeId only, no throw", () => {
    const visited = makeRun({ flowId: "flow-1", lastStep: "DRAFT" });
    const notVisited = makeRun({ flowId: "flow-2", lastStep: "GATE" });
    const result = computeNodeStats([visited, notVisited], "test_pipe", "DRAFT", null);
    assert.equal(result.visits, 1);
  });

  test("failedHere/blockedHere/terminatedHere still derive from lastStep+status, unaffected by visitedByFlow", () => {
    const failed = makeRun({ flowId: "flow-1", status: "failed", lastStep: "DRAFT" });
    const blocked = makeRun({ flowId: "flow-2", status: "blocked", lastStep: "DRAFT" });
    const visitedByFlow = new Map([["flow-1", new Set(["DRAFT"])], ["flow-2", new Set(["DRAFT"])]]);
    const result = computeNodeStats([failed, blocked], "test_pipe", "DRAFT", null, visitedByFlow);
    assert.equal(result.visits, 2);
    assert.equal(result.failedHere, 1);
    assert.equal(result.blockedHere, 1);
    assert.equal(result.terminatedHere, 1);
  });
});

// node identity (node_uuid) — a renamed node's older runs, recorded under
// the OLD node id in visitedByFlow, should still count toward the CURRENT
// stage once the caller resolves lineageByNodeId via pipeline-registry.js's
// getNodeLineageBatch and threads it through.
describe("computePipelineStats() — auto-funnel lineageByNodeId (rename continuity)", () => {
  const RENAMED_PIPELINE = {
    id: "test_pipe",
    nodes: { PRIME: { kind: "code" }, SUBMIT_SUMMARY: { kind: "code" } },
  };

  test("an old run recorded under the node's PRE-rename id still counts toward the current stage", async () => {
    const run = makeRun({ flowId: "flow-old", status: "succeeded" });
    // This run dispatched before the rename — its invocation was recorded
    // as VERIFY_SUBMIT, which isn't a stage id at all.
    const visitedByFlow = new Map([["flow-old", new Set(["PRIME", "VERIFY_SUBMIT"])]]);
    const lineageByNodeId = new Map([["SUBMIT_SUMMARY", new Set(["SUBMIT_SUMMARY", "VERIFY_SUBMIT"])]]);
    const result = await computePipelineStats([run], "test_pipe", RENAMED_PIPELINE, null, null, null, visitedByFlow, lineageByNodeId);
    assert.equal(result.funnel.find((s) => s.id === "SUBMIT_SUMMARY").total, 1);
  });

  test("omitting lineageByNodeId falls back to exact-match-only — the pre-rename run does NOT count", async () => {
    const run = makeRun({ flowId: "flow-old", status: "succeeded" });
    const visitedByFlow = new Map([["flow-old", new Set(["PRIME", "VERIFY_SUBMIT"])]]);
    const result = await computePipelineStats([run], "test_pipe", RENAMED_PIPELINE, null, null, null, visitedByFlow);
    assert.equal(result.funnel.find((s) => s.id === "SUBMIT_SUMMARY").total, 0, "backward-compatible default: no lineage means no cross-name matching");
  });

  test("a declared pipeline.funnel (not the auto-derived one) ignores lineageByNodeId entirely", async () => {
    const declaredFunnelPipeline = {
      id: "test_pipe",
      funnel: [{ id: "sent", label: "sent", key: "sent", size: "array" }],
    };
    const run = makeRun({ flowId: "flow-old", status: "succeeded", stateJson: { pipelineId: "test_pipe", sent: ["x"] } });
    const visitedByFlow = new Map([["flow-old", new Set(["VERIFY_SUBMIT"])]]);
    const lineageByNodeId = new Map([["sent", new Set(["sent", "VERIFY_SUBMIT"])]]);
    const result = await computePipelineStats([run], "test_pipe", declaredFunnelPipeline, null, null, null, visitedByFlow, lineageByNodeId);
    // Sized from state.sent (an array of length 1), not from visitedByFlow/
    // lineageByNodeId at all — proves the declared-funnel path never even
    // looks at lineage, matching the source read confirmed in stats.js.
    assert.equal(result.funnel.find((s) => s.id === "sent").total, 1);
  });
});

describe("computeNodeStats() — lineageByNodeId (rename continuity)", () => {
  test("a run visited under the node's OLD id counts once lineageByNodeId links them", () => {
    const run = makeRun({ flowId: "flow-1", lastStep: "GATE" });
    const visitedByFlow = new Map([["flow-1", new Set(["VERIFY_SUBMIT"])]]);
    const lineageByNodeId = new Map([["SUBMIT_SUMMARY", new Set(["SUBMIT_SUMMARY", "VERIFY_SUBMIT"])]]);
    const result = computeNodeStats([run], "test_pipe", "SUBMIT_SUMMARY", null, visitedByFlow, lineageByNodeId);
    assert.equal(result.visits, 1);
  });

  test("omitting lineageByNodeId falls back to exact-match-only", () => {
    const run = makeRun({ flowId: "flow-1", lastStep: "GATE" });
    const visitedByFlow = new Map([["flow-1", new Set(["VERIFY_SUBMIT"])]]);
    const result = computeNodeStats([run], "test_pipe", "SUBMIT_SUMMARY", null, visitedByFlow);
    assert.equal(result.visits, 0);
  });
});
