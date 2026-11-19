// Unit tests for engine/result-replay.js — the pure merge shared by
// assembleState() and subflow-state.js's per-child `data` field. It replays a
// flow's own completed resultJson rows onto a base state object; nothing here
// touches a store.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { replayResults } from "../../../engine/result-replay.js";

const row = (nodeId, result, extra = {}) => ({
  nodeId,
  resultWrittenAt: 1000,
  resultJson: JSON.stringify(result),
  resultTooLargeBytes: null,
  ...extra,
});

describe("replayResults — merge order", () => {
  test("merges object results in row order, later rows winning", () => {
    const out = replayResults({ SEED: 1 }, [
      row("A", { A: { v: 1 }, shared: "first" }),
      row("B", { B: { v: 2 }, shared: "second" }),
    ]);
    assert.deepEqual(out, { SEED: 1, A: { v: 1 }, B: { v: 2 }, shared: "second" });
  });

  test("the base object is not mutated", () => {
    const base = { SEED: 1 };
    replayResults(base, [row("A", { A: 1 })]);
    assert.deepEqual(base, { SEED: 1 });
  });

  test("an empty or missing results list returns a copy of the base", () => {
    assert.deepEqual(replayResults({ a: 1 }, []), { a: 1 });
    const out = replayResults({ a: 1 }, undefined);
    assert.deepEqual(out, { a: 1 });
    assert.notEqual(out, undefined);
  });
});

describe("replayResults — row admission", () => {
  test("rows that never had their result written are skipped", () => {
    const out = replayResults({}, [row("A", { A: 1 }, { resultWrittenAt: null })]);
    assert.deepEqual(out, {});
  });

  test("unparseable JSON is skipped, leaving prior rows intact", () => {
    const out = replayResults({}, [
      row("A", { A: 1 }),
      { nodeId: "B", resultWrittenAt: 1, resultJson: "{not json", resultTooLargeBytes: null },
    ]);
    assert.deepEqual(out, { A: 1 });
  });

  test("null, array and scalar results are ignored", () => {
    const out = replayResults({}, [
      row("A", { A: 1 }),
      row("B", null),
      row("C", [1, 2]),
      row("D", "string"),
      row("E", 7),
    ]);
    assert.deepEqual(out, { A: 1 });
  });
});

describe("replayResults — truncated results", () => {
  test("too-large with no stored JSON replays an explicit marker", () => {
    const out = replayResults({}, [
      { nodeId: "BIG", resultWrittenAt: 1, resultJson: null, resultTooLargeBytes: 2048 },
    ]);
    assert.deepEqual(out, { BIG: { _resultTooLarge: true, bytes: 2048 } });
  });

  test("too-large WITH stored partial JSON merges the partial data instead of the marker", () => {
    const out = replayResults({}, [
      { nodeId: "PART", resultWrittenAt: 1, resultJson: '{"PART":{"kept":true}}', resultTooLargeBytes: 2048 },
    ]);
    assert.deepEqual(out, { PART: { kept: true } });
    assert.equal(out.PART._resultTooLarge, undefined);
  });

  test("a marker for one node does not disturb siblings", () => {
    const out = replayResults({}, [
      row("A", { A: { v: 1 } }),
      { nodeId: "BIG", resultWrittenAt: 1, resultJson: null, resultTooLargeBytes: 10 },
    ]);
    assert.deepEqual(out.A, { v: 1 });
    assert.deepEqual(out.BIG, { _resultTooLarge: true, bytes: 10 });
  });
});
