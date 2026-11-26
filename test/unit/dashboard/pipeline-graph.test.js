
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { serializePipeline } from "../../../dashboard/pipeline-graph.js";

function nodeIds(out) {
  return out.nodes.map((n) => n.id);
}

describe("serializePipeline — shape", () => {
  test("null in, null out", () => {
    assert.equal(serializePipeline(null), null);
    assert.equal(serializePipeline(undefined), null);
  });

  test("wraps the nodes between synthetic start and end markers", () => {
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" }, B: { kind: "manual" } },
      edges: { __start__: "A", A: "B", B: "__end__" },
    });
    assert.deepEqual(nodeIds(out), ["__start__", "A", "B", "__end__"]);
    assert.equal(out.nodes[0].kind, "start");
    assert.equal(out.nodes.at(-1).kind, "end");
    assert.equal(out.nodes[1].kind, "code");
  });

  test("carries id, goal, file, dir, groups; views stay null when undeclared", () => {
    const out = serializePipeline({
      id: "p", goal: "ship it", _filepath: "/x/index.js", _dir: "/x",
      nodes: {}, edges: {}, groups: { g1: { members: ["A"] } },
    });
    assert.equal(out.id, "p");
    assert.equal(out.goal, "ship it");
    assert.equal(out.file, "/x/index.js");
    assert.equal(out.dir, "/x");
    assert.deepEqual(out.groups, { g1: { members: ["A"] } });
    assert.equal(out.views, null);
  });

  test("declared views are shipped as declarations", () => {
    const views = [{ id: "outcomes", kind: "funnel" }];
    const out = serializePipeline({ id: "p", nodes: {}, edges: {}, views });
    assert.deepEqual(out.views, views);
  });

  test("node-level metadata travels with the node; functions do not", () => {
    const run = () => ({});
    const out = serializePipeline({
      id: "p",
      nodes: {
        AGENT: { kind: "agent", agent: "scout", groupId: "g1", concurrency: 2, waitKind: "human", run },
        PLAIN: { kind: "code", run },
      },
      edges: { __start__: "AGENT", AGENT: "PLAIN", PLAIN: "__end__" },
    });
    const agent = out.nodes.find((n) => n.id === "AGENT");
    assert.equal(agent.agent, "scout");
    assert.equal(agent.groupId, "g1");
    assert.equal(agent.concurrency, 2);
    assert.equal(agent.waitKind, "human");
    assert.equal(agent.run, undefined, "functions are stripped");
    assert.equal(JSON.stringify(out).includes("=>"), false, "no function source leaks into the payload");
  });
});

describe("serializePipeline — edges", () => {
  test("string edges are unconditional", () => {
    const out = serializePipeline({ id: "p", nodes: { A: { kind: "code" } }, edges: { __start__: "A", A: "__end__" } });
    assert.deepEqual(out.edges, [
      { from: "__start__", to: "A", conditional: false },
      { from: "A", to: "__end__", conditional: false },
    ]);
  });

  test("function edges use edgeHints when declared", () => {
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" }, B: { kind: "code" } },
      edges: { __start__: "A", A: () => {}, B: "__end__" },
      edgeHints: { A: ["B", "__end__"] },
    });
    const conditional = out.edges.filter((e) => e.from === "A");
    assert.deepEqual(conditional, [
      { from: "A", to: "B", conditional: true, via: null },
      { from: "A", to: "__end__", conditional: true, via: null },
    ]);
  });

  test("function edges without hints derive targets by scanning quoted known ids", () => {
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" }, B: { kind: "code" } },
      edges: {
        __start__: "A",
        A: () => "B",
        B: "__end__",
      },
    });
    const a = out.edges.filter((e) => e.from === "A");
    assert.equal(a.length, 1);
    assert.equal(a[0].to, "B");
    assert.equal(a[0].conditional, true);
    assert.match(a[0].via, /=> "B"/);
  });

  test("unknown quoted ids in a function edge are ignored", () => {
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" }, B: { kind: "code" } },
      edges: { __start__: "A", A: () => ({ NOPE: "missing", REAL: "B" }), B: "__end__" },
    });
    const a = out.edges.filter((e) => e.from === "A");
    assert.deepEqual(a.map((e) => e.to), ["B"]);
  });

  test("a function edge with no recognizable target becomes a '?' edge carrying the source", () => {
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" } },
      edges: { __start__: "A", A: () => 42, },
    });
    const a = out.edges.find((e) => e.from === "A");
    assert.equal(a.to, "?");
    assert.equal(a.conditional, true);
    assert.ok(a.via.length > 0);
  });

  test("long resolver sources are shortened to 200 characters", () => {
    const fn = new Function(`return "B" + "${"x".repeat(400)}";`);
    const out = serializePipeline({
      id: "p",
      nodes: { A: { kind: "code" }, B: { kind: "code" } },
      edges: { __start__: "A", A: fn, B: "__end__" },
    });
    const a = out.edges.find((e) => e.from === "A");
    assert.equal(a.to, "B");
    assert.equal(a.via.length, 200);
    assert.ok(a.via.endsWith("..."));
  });
});

describe("serializePipeline — subflow insets", () => {
  test("a subflow node ships its inner flow, namespaced under the parent node", () => {
    const out = serializePipeline({
      id: "p",
      nodes: {
        SUB: {
          kind: "subflow",
          groupId: "grp",
          flow: {
            nodes: { INNER: { kind: "code", skill: "review" } },
            edges: { __start__: "INNER", INNER: "__end__" },
          },
        },
      },
      edges: { __start__: "SUB", SUB: "__end__" },
    });
    const sub = out.nodes.find((n) => n.id === "SUB");
    assert.equal(sub.flow.parentNodeId, "SUB");
    assert.equal(sub.flow.groupId, "grp");
    assert.deepEqual(sub.flow.nodes.map((n) => n.id), ["__start__", "INNER", "__end__"]);
    assert.equal(sub.flow.nodes[1].skill, "review");
    assert.deepEqual(sub.flow.edges, [
      { from: "__start__", to: "INNER", conditional: false },
      { from: "INNER", to: "__end__", conditional: false },
    ]);
  });

  test("a subflow's conditional edge without hints scans its own inner ids only", () => {
    const out = serializePipeline({
      id: "p",
      nodes: {
        SUB: {
          kind: "subflow",
          flow: {
            nodes: { INNER: { kind: "code" }, OUTER_ONLY: { kind: "code" } },
            edges: { __start__: "INNER", INNER: () => "OUTER_ONLY" },
          },
        },
        OUTER_ONLY: { kind: "code" },
      },
      edges: { __start__: "SUB", SUB: "OUTER_ONLY", OUTER_ONLY: "__end__" },
    });
    const sub = out.nodes.find((n) => n.id === "SUB");
    const inner = sub.flow.edges.filter((e) => e.from === "INNER");
    assert.deepEqual(inner.map((e) => e.to), ["OUTER_ONLY"]);
    assert.equal(inner[0].conditional, true);
  });

  test("a non-subflow node carries no flow key", () => {
    const out = serializePipeline({
      id: "p", nodes: { A: { kind: "code", flow: { nodes: {} } } }, edges: {},
    });
    assert.equal(out.nodes[1].flow, undefined);
  });
});
