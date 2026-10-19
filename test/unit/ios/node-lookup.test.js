import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { resolveNodeDiscuss } from "../../../ios/node-lookup.js";

// ── resolveNodeDiscuss ────────────────────────────────────────────────────────

describe("resolveNodeDiscuss", () => {
  it("returns null when the pipeline meta has no matching top-level or nested node", () => {
    const meta = { nodes: { PROPOSE: { kind: "subflow", flow: { nodes: {} } } } };
    assert.equal(resolveNodeDiscuss(meta, "NOPE"), null);
  });

  it("returns the agent for a top-level human-gate node with discuss", () => {
    const meta = { nodes: { REVIEW: { kind: "human-gate", discuss: { agent: "main" } } } };
    assert.deepEqual(resolveNodeDiscuss(meta, "REVIEW"), { agent: "main" });
  });

  it("returns null for a top-level human-gate node without discuss", () => {
    const meta = { nodes: { REVIEW: { kind: "human-gate" } } };
    assert.equal(resolveNodeDiscuss(meta, "REVIEW"), null);
  });

  it("finds a loop's own discuss-enabled human-gate branch child when looked up by the LOOP's id", () => {
    const meta = {
      nodes: {
        PROPOSE: {
          kind: "subflow",
          flow: { nodes: { GATE: { kind: "human-gate", discuss: { agent: "main" } } } },
        },
      },
    };
    assert.deepEqual(resolveNodeDiscuss(meta, "PROPOSE"), { agent: "main" });
  });

  // Regression: gate-projector.js's projectGateItem looks discuss up by
  // the ACTUAL AWAITING SUB-NODE id (e.g. "GATE"), not its parent loop's id
  // ("PROPOSE") — this is what ios/index.js's nodeDiscussLookup was missing
  // before the branch-nested fallback was added, which meant
  // discussSupported was always false for every branch-mode loop gate
  // (workana-proposal, workana-reply, workana-touch-cadence) despite their
  // gate.js correctly declaring `discuss: { agent, promptBuilder }`.
  it("finds a branch-nested human-gate's discuss config when looked up by the SUB-NODE's own id", () => {
    const meta = {
      nodes: {
        PROPOSE: {
          kind: "subflow",
          flow: { nodes: { GATE: { kind: "human-gate", discuss: { agent: "main" } } } },
        },
      },
    };
    assert.deepEqual(resolveNodeDiscuss(meta, "GATE"), { agent: "main" });
  });

  it("returns null when the sub-node id matches but its discuss.agent isn't a string", () => {
    const meta = {
      nodes: {
        PROPOSE: {
          kind: "subflow",
          flow: { nodes: { GATE: { kind: "human-gate", discuss: { promptBuilder: () => "" } } } },
        },
      },
    };
    assert.equal(resolveNodeDiscuss(meta, "GATE"), null);
  });

  it("returns null when meta itself is missing", () => {
    assert.equal(resolveNodeDiscuss(undefined, "GATE"), null);
  });
});
