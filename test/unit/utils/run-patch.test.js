// Unit tests for utils/run-patch.js — pure RunSummary-shaped patch builders
// for the flow-state WS events. No DB access: every input is already fetched.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  prettifyPipelineId,
  pipelineMetaFor,
  buildRunPatch,
} from "../../../utils/run-patch.js";

describe("prettifyPipelineId", () => {
  test("splits the snake_case prefix into a display name", () => {
    assert.equal(prettifyPipelineId("review_pr"), "review: pr");
    assert.equal(prettifyPipelineId("discuss_draft"), "discuss: draft");
  });



  test("hyphens become spaces after the prefix split", () => {
    assert.equal(prettifyPipelineId("review_pr-flow"), "review: pr flow");
    assert.equal(prettifyPipelineId("review-pr-flow"), "review pr flow");
  });

  test("non-string or empty falls back to the generic label", () => {
    assert.equal(prettifyPipelineId(""), "Pipeline");
    assert.equal(prettifyPipelineId(null), "Pipeline");
    assert.equal(prettifyPipelineId(undefined), "Pipeline");
    assert.equal(prettifyPipelineId(42), "Pipeline");
  });
});

describe("pipelineMetaFor", () => {
  test("defaults come from the prettified id", () => {
    assert.deepEqual(pipelineMetaFor("review_pr"), {
      id: "review_pr",
      displayName: "review: pr",
      emoji: "📊",
      sfSymbol: "tray.fill",
    });
  });

  test("a lookup overrides any subset of the declared meta", () => {
    const lookup = (id) => (id === "review_pr" ? { displayName: "Review PR", emoji: "🔍" } : undefined);
    assert.deepEqual(pipelineMetaFor("review_pr", { pipelineLookup: lookup }), {
      id: "review_pr",
      displayName: "Review PR",
      emoji: "🔍",
      sfSymbol: "tray.fill",
    });
  });

  test("a lookup returning nothing keeps every default", () => {
    const meta = pipelineMetaFor("deploy_app", { pipelineLookup: () => undefined });
    assert.equal(meta.displayName, "deploy: app");
    assert.equal(meta.emoji, "📊");
  });
});

describe("buildRunPatch", () => {
  const profile = (id) => (id === "review_pr" ? { displayName: "Review PR", emoji: "🔍", sfSymbol: "doc.text" } : undefined);

  test("maps a complete flow row onto the RunSummary patch", () => {
    const flow = {
      flowId: "f1",
      status: "running",
      lastStep: "STEP_2",
      stateJson: {
        pipelineId: "review_pr",
        _display: { title: "PR 41", metrics: [{ label: "files", value: 3 }] },
      },
      waitJson: { summary: "waiting on human gate" },
      createdAt: 100,
      updatedAt: 200,
      endedAt: 300,
    };
    assert.deepEqual(buildRunPatch(flow, 999, { pipelineLookup: profile }), {
      flowId: "f1",
      pipelineId: "review_pr",
      status: "running",
      updatedAtMs: 200,
      endedAtMs: 300,
      createdAtMs: 100,
      statusReason: "waiting on human gate",
      displayTitle: "PR 41",
      displayMetrics: [{ label: "files", value: 3 }],
      currentNodeId: "STEP_2",
      pipelineDisplayName: "Review PR",
      pipelineEmoji: "🔍",
    });
  });

  test("missing timestamps fall back to now for updatedAt and null elsewhere", () => {
    const patch = buildRunPatch({ flowId: "f2", status: "queued" }, 4242);
    assert.equal(patch.updatedAtMs, 4242);
    assert.equal(patch.endedAtMs, null);
    assert.equal(patch.createdAtMs, null);
    assert.equal(patch.currentNodeId, null);
    assert.equal(patch.statusReason, null);
  });

  test("an explicit updatedAt of 0 is kept, not replaced by now", () => {
    const patch = buildRunPatch({ flowId: "f3", status: "running", updatedAt: 0 }, 4242);
    assert.equal(patch.updatedAtMs, 0);
  });

  test("no pipelineId: pipeline meta is null and nothing is looked up", () => {
    let looked = 0;
    const patch = buildRunPatch(
      { flowId: "f4", status: "running", stateJson: {} },
      1,
      { pipelineLookup: () => { looked += 1; return {}; } },
    );
    assert.equal(patch.pipelineId, null);
    assert.equal(patch.pipelineDisplayName, null);
    assert.equal(patch.pipelineEmoji, null);
    assert.equal(looked, 0);
  });

  test("display metrics that are not an array collapse to null, title to null", () => {
    const patch = buildRunPatch(
      { flowId: "f5", status: "running", stateJson: { pipelineId: "x_y", _display: { title: 7, metrics: "nope" } } },
      1,
    );
    assert.equal(patch.displayTitle, 7);
    assert.equal(patch.displayMetrics, null);
  });

  test("empty or absent state falls back cleanly", () => {
    const patch = buildRunPatch({ flowId: "f6", status: "succeeded", stateJson: null }, 10);
    assert.equal(patch.pipelineId, null);
    assert.equal(patch.displayTitle, null);
    assert.equal(patch.displayMetrics, null);
    assert.equal(patch.pipelineDisplayName, null);
  });
});
