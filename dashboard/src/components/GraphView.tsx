// Cytoscape wrapper. Renders the pipeline topology with:
//   - explicit groups (pipeline.groups) as compound parents
//   - subflow nodes as compound parents containing their flow.nodes
//   - executed-path overlay (visited/unvisited nodes; taken/untaken edges)
//   - subflow outcome (green/amber/red border + count summary in label)
//
// Lifecycle is keyed on (pipeline, run?.flowId, run?.revision, tree) so the
// same instance survives unrelated re-renders but rebuilds when the data
// changes — tree arrives asynchronously after run, so it needs its own
// dependency slot rather than piggybacking on run?.revision.

import { useEffect, useRef } from "react";
import cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";
import type { FlowRunFull, FlowTreeItem, Pipeline, PipelineGroup } from "@/api/types";
import { buildTracedEdges } from "@/lib/step-history";
import { classifySubflows, subgraphOutcomeClass } from "@/lib/subflow";

// cytoscape-dagre auto-registers when loaded after cytoscape; calling .use is
// a no-op in 2.x but explicit registration is the documented path.
cytoscape.use(dagre as unknown as (cy: unknown) => void);

interface GraphViewProps {
  pipeline: Pipeline;
  run?: FlowRunFull | null;
  /** Executed-path source — GET /manifold/runs/:flowId/tree's items.
   *  null/undefined while loading or on a run with no history yet;
   *  tracedNodes/tracedEdges then stay null too. */
  tree?: FlowTreeItem[] | null;
  className?: string;
  onNodeClick?: (nodeId: string) => void;
  selectedNodeId?: string | null;
}

function groupParentClasses(group: PipelineGroup, tracedNodes: Set<string> | null): string {
  const classes = ["group-parent", `group-kind-${group.kind ?? "default"}`];
  if (!tracedNodes) return classes.join(" ");
  const members = group.members ?? [];
  const anyHit = members.some((m) => tracedNodes.has(m));
  classes.push(anyHit ? "group-visited" : "group-untouched");
  return classes.join(" ");
}

function nodeClasses(n: { id: string; kind: string }, run: FlowRunFull | null | undefined, tracedNodes: Set<string> | null, effectiveKind?: string): string {
  const classes: string[] = [];
  if (n.id === "__start__") classes.push("terminal", "start");
  else if (n.id === "__end__") classes.push("terminal", "end");
  else classes.push(`kind-${effectiveKind ?? n.kind}`);
  if (tracedNodes) classes.push(tracedNodes.has(n.id) ? "visited" : "unvisited");
  if (run && run.currentStep === n.id) {
    classes.push("current");
    const map: Record<string, string> = {
      waiting: "blocked", blocked: "blocked",
      running: "running", succeeded: "succeeded", failed: "failed", cancelled: "cancelled",
    };
    const extra = map[run.status];
    if (extra) classes.push(extra);
  }
  return classes.join(" ");
}

// Stylesheet entries use the JS form `{ selector, style: {...} }`. The
// @types/cytoscape public surface only models the CSS-string form, so we
// type the entries permissively here.
type StyleEntry = { selector: string; style: Record<string, unknown> };
function graphStyle(): StyleEntry[] {
  return [
    { selector: "node", style: {
        "background-color": "#1f262e",
        "border-width": 1.5,
        "border-color": "#2a3038",
        "label": "data(label)",
        "color": "#d8dde6",
        "font-family": "ui-monospace, Menlo, monospace",
        "font-size": 11,
        "text-valign": "center",
        "text-halign": "center",
        "width": "label",
        "height": 28,
        "padding": "10px",
        "shape": "round-rectangle",
    }},
    { selector: "node.terminal",    style: { "background-color": "#0e1116", "border-color": "#6e7681", "color": "#8b949e" }},
    { selector: "node.kind-code",          style: { "border-color": "#58a6ff" }},
    { selector: "node.kind-agent",         style: { "border-color": "#bc8cff" }},
    { selector: "node.kind-loop",           style: { "border-color": "#f0883e" }},
    { selector: "node.kind-human-gate",    style: { "border-color": "#d29922" }},
    { selector: "node.kind-gate", style: { "border-color": "#3fb950" }},
    { selector: "node.current",   style: { "border-width": 3, "background-color": "#1f6feb33" }},
    { selector: "node.running",   style: { "border-color": "#3fb950" }},
    { selector: "node.blocked",   style: { "border-color": "#d29922" }},
    { selector: "node.succeeded", style: { "border-color": "#79b8ff" }},
    { selector: "node.failed",    style: { "border-color": "#f85149" }},
    { selector: "node.cancelled", style: { "border-color": "#6e7681" }},
    { selector: "edge", style: {
        "width": 1.2,
        "line-color": "#2a3038",
        "target-arrow-color": "#2a3038",
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
    }},
    { selector: "edge.cond", style: { "line-style": "dashed", "line-color": "#6e7681", "target-arrow-color": "#6e7681" }},
    { selector: "node.unvisited", style: { "opacity": 0.32 }},
    { selector: "node.visited",   style: { "border-color": "#79b8ff" }},
    { selector: "edge.untaken",   style: { "opacity": 0.18 }},
    { selector: "edge.taken",     style: { "line-color": "#58a6ff", "target-arrow-color": "#58a6ff", "line-style": "solid", "width": 2 }},
    { selector: "node.group-parent", style: {
        "background-color": "transparent",
        "background-opacity": 0,
        "border-style": "dashed",
        "border-color": "#39424d",
        "border-width": 1.2,
        "label": "data(label)",
        "text-valign": "top",
        "text-halign": "center",
        "text-margin-y": -6,
        "font-size": 10,
        "color": "#8b949e",
        "padding": "16px",
        "shape": "round-rectangle",
    }},
    { selector: "node.group-primary-ok",   style: { "border-color": "#3fb950" }},
    { selector: "node.group-engaged",      style: { "border-color": "#d29922", "border-style": "solid", "border-width": 1.6 }},
    { selector: "node.group-untouched",    style: { "border-color": "#2a3038", "opacity": 0.5 }},
    { selector: "node.group-failed",       style: { "border-color": "#f85149", "border-style": "solid", "border-width": 1.6 }},
    { selector: "node.group-running",      style: { "border-color": "#58a6ff", "border-style": "solid", "border-width": 1.6 }},
    { selector: "node.selected", style: { "border-color": "#f0883e", "border-width": 3 }},
  ];
}

export function GraphView({ pipeline, run, tree, className, onNodeClick, selectedNodeId }: GraphViewProps) {
  const ref = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    if (cyRef.current) { try { cyRef.current.destroy(); } catch { /* swallow */ } cyRef.current = null; }

    const history = Array.isArray(tree) ? tree.map((i) => i.nodeId) : null;
    const tracedNodes = history ? new Set<string>(["__start__", ...history]) : null;
    const tracedEdges = history ? buildTracedEdges(history) : null;
    // `tree` (GET .../tree) carries each top-level node's kind/agent AS OF
    // this specific run's own invocation — point-in-time correct, unlike
    // `pipeline.nodes[].kind` below (the pipeline's CURRENT definition).
    // Only covers nodes this run actually dispatched (subflow_key IS NULL,
    // same scope as `history` above) — an unvisited node has no historical
    // record to fall back to, so it correctly keeps using the live kind.
    const treeKindByNodeId = new Map<string, string>();
    if (Array.isArray(tree)) {
      for (const item of tree) {
        if (item.kind) treeKindByNodeId.set(item.nodeId, item.kind);
      }
    }

    // Explicit groups → compound parents. Each group is either top-level
    // (its compound has no parent) or subflow-internal (its compound lives
    // inside a subflow node's compound). Loader guarantees a group's
    // members are all in the same scope.
    //
    // `groupOf` maps cytoscape node-id (which for subflow children is
    // `<parentNodeId>.<childId>`) → groupId. We render the group's
    // compound parent with the appropriate `parent:` set, then descendants
    // get `parent: __group:<groupId>` so cytoscape compounds them.
    const groupOf = new Map<string, string>();
    const groupScope = new Map<string, string | null>(); // groupId → null|parentForEachSubgraphId
    const groupParents: cytoscape.NodeDefinition[] = [];
    if (pipeline.groups && typeof pipeline.groups === "object") {
      for (const [groupId, group] of Object.entries(pipeline.groups)) {
        const members = group?.members ?? [];
        let scope: string | null = null;
        for (const m of members) {
          groupOf.set(m, groupId);
          const dot = m.indexOf(".");
          if (dot !== -1 && !scope) scope = m.slice(0, dot);
        }
        groupScope.set(groupId, scope);
        const data: cytoscape.NodeDataDefinition = {
          id: `__group:${groupId}`,
          label: group.label ?? groupId,
        };
        if (scope) data.parent = scope; // nest inside the loop node's compound
        groupParents.push({
          data,
          classes: groupParentClasses(group, tracedNodes),
        });
      }
    }

    // subflow → compound parents containing namespaced flow
    // children. A child's `parent:` is either the subflow node itself
    // OR a subflow-internal group's compound (which is in turn parented
    // by the subflow node). Cytoscape nests compounds via `parent:`
    // pointers — see groupParents construction above.
    interface CompoundEntry { children: cytoscape.NodeDefinition[]; childEdges: cytoscape.EdgeDefinition[]; concurrency?: number }
    const subgraphCompounds = new Map<string, CompoundEntry>();
    for (const n of pipeline.nodes) {
      if (n.kind !== "subflow" || !n.flow) continue;
      const parentId = n.id;
      const children: cytoscape.NodeDefinition[] = [];
      for (const bn of n.flow.nodes ?? []) {
        const fullId = `${parentId}.${bn.id}`;
        const innerGroup = groupOf.get(fullId);
        children.push({
          data: {
            id: fullId,
            label: bn.id,
            parent: innerGroup ? `__group:${innerGroup}` : parentId,
          },
          classes: nodeClasses({ id: bn.id, kind: bn.kind }, null, null),
        });
      }
      const childEdges: cytoscape.EdgeDefinition[] = (n.flow.edges ?? [])
        .map((e, i) => ({
          data: {
            id: `sge:${parentId}:${i}`,
            source: `${parentId}.${e.from}`,
            target: `${parentId}.${e.to}`,
          },
          classes: e.conditional ? "cond" : "",
        }))
        .filter((e) => !String(e.data.target).endsWith(".?"));
      subgraphCompounds.set(parentId, { children, childEdges, concurrency: n.concurrency });
    }

    const nodes: cytoscape.NodeDefinition[] = [];
    nodes.push(...groupParents);
    for (const n of pipeline.nodes) {
      const parent = groupOf.get(n.id);
      if (subgraphCompounds.has(n.id)) {
        const cmp = subgraphCompounds.get(n.id)!;
        const subflowCounts = classifySubflows(run?.stateJson?.subflows?.[n.id]);
        const base = `${n.id} (loop${cmp.concurrency ? ` ×${cmp.concurrency}` : ""})`;
        const label = subflowCounts.total > 0
          ? `${base} · ${subflowCounts.succeeded}/${subflowCounts.total} ok${subflowCounts.skipped ? ` · ${subflowCounts.skipped} ⊘` : ""}${subflowCounts.failed ? ` · ${subflowCounts.failed} ✗` : ""}${subflowCounts.awaiting ? ` · ${subflowCounts.awaiting} ⏳` : ""}${subflowCounts.running ? ` · ${subflowCounts.running} →` : ""}`
          : base;
        const cls = ["group-parent", "group-kind-subgraph"];
        if (subflowCounts.total > 0) cls.push(subgraphOutcomeClass(subflowCounts));
        else if (tracedNodes) cls.push(tracedNodes.has(n.id) ? "visited" : "unvisited");
        nodes.push({
          data: { id: n.id, label, ...(parent ? { parent: `__group:${parent}` } : {}) },
          classes: cls.join(" "),
        });
      } else {
        nodes.push({
          data: { id: n.id, label: n.id, ...(parent ? { parent: `__group:${parent}` } : {}) },
          classes: nodeClasses(n as { id: string; kind: string }, run, tracedNodes, treeKindByNodeId.get(n.id)),
        });
      }
    }
    for (const cmp of subgraphCompounds.values()) nodes.push(...cmp.children);

    const edges: cytoscape.EdgeDefinition[] = pipeline.edges
      .map((e, i) => {
        let source = e.from, target = e.to;
        if (subgraphCompounds.has(source)) source = `${source}.__end__`;
        if (subgraphCompounds.has(target)) target = `${target}.__start__`;
        const classes: string[] = [];
        if (e.conditional) classes.push("cond");
        if (tracedEdges) classes.push(tracedEdges.has(`${e.from}→${e.to}`) ? "taken" : "untaken");
        return {
          data: { id: `e${i}`, source, target, via: e.via ?? "" },
          classes: classes.join(" "),
        };
      })
      .filter((e) => e.data.target && e.data.target !== "?");
    for (const cmp of subgraphCompounds.values()) edges.push(...cmp.childEdges);

    const cy = cytoscape({
      container: ref.current,
      elements: { nodes, edges },
      style: graphStyle(),
      layout: { name: "dagre", rankDir: "LR", nodeSep: 24, rankSep: 70, edgeSep: 10 } as cytoscape.LayoutOptions,
      wheelSensitivity: 0.2,
    });
    cy.on("tap", "edge.cond", (evt) => {
      const via = evt.target.data("via");
      if (via) alert(via);
    });
    if (onNodeClick) {
      cy.on("tap", "node", (evt) => {
        const id = String(evt.target.id());
        // Skip group compounds, flow children (namespaced as parent.child),
        // and synthetic start/end pseudo-nodes — stats only apply to real
        // top-level pipeline nodes.
        if (id.startsWith("__group:")) return;
        if (id === "__start__" || id === "__end__") return;
        if (id.includes(".")) return;
        onNodeClick(id);
      });
    }
    if (selectedNodeId) {
      const el = cy.getElementById(selectedNodeId);
      if (el && el.length) el.addClass("selected");
    }
    cyRef.current = cy;
    return () => { try { cy.destroy(); } catch { /* swallow */ } };
  }, [pipeline, run?.flowId, run?.revision, tree, onNodeClick, selectedNodeId]);

  return <div ref={ref} className={`h-[320px] sm:h-[420px] md:h-[480px] bg-bg border border-border rounded ${className ?? ""}`} />;
}
