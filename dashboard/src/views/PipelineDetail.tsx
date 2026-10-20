import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { useClickOutside } from "@/lib/use-click-outside";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, THead, TBody, TR, TH, TD } from "@/components/ui/table";
import { GraphView } from "@/components/GraphView";
import { NodeContextCard } from "@/components/NodeContextCard";
import { CopyButton } from "@/components/CopyButton";
import { StatStrip } from "@/components/StatStrip";
import { Funnel } from "@/components/Funnel";
import { Histogram } from "@/components/Histogram";
import { DailyCostTrend } from "@/components/DailyCostTrend";
import { DailyTimeTrend } from "@/components/DailyTimeTrend";
import { Skeleton } from "@/components/ui/skeleton";
import { TodayStrip, estimateLast24hSpent, DAY_MS as TODAY_DAY_MS } from "@/components/TodayStrip";
import { computeAnomalies } from "@/lib/anomaly";
import { api } from "@/api/client";
import { useSse } from "@/api/sse";
import { cn } from "@/lib/utils";
import type { DeclaredView, Pipeline, PipelineCost, PipelineNode, PipelineStats, PipelineViews } from "@/api/types";

export function PipelineDetail({ pipelineId }: { pipelineId: string }) {
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [stats, setStats] = useState<PipelineStats | null>(null);
  const [cost, setCost] = useState<PipelineCost | null>(null);
  const [pipelineViews, setPipelineViews] = useState<PipelineViews | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const loadDef = () => {
    api.pipeline(pipelineId).then((p) => {
      setPipeline(p);
      setError(null);
    }).catch((e) => setError(e?.message ?? String(e)));
  };
  const loadStats = () => { api.pipelineStats(pipelineId).then(setStats).catch(() => {}); };
  // Cost walks the session JSONLs on disk — slow at >100 runs, so we DON'T
  // re-fire on every SSE event. Loads on mount + when the pipeline def
  // changes, which is good enough for an "is this worth running?" signal.
  const loadCost = () => { api.pipelineCost(pipelineId, 30).then(setCost).catch(() => setCost(null)); };
  // Pipeline-declared views walk small payloads (currently a sent/*.md
  // dir for the proposal pipeline's outcome funnel). Fast enough to
  // refire on flow_terminated when more records may have landed; also
  // loads on mount + pipeline switch.
  const loadViews = () => { api.pipelineViews(pipelineId, 30).then(setPipelineViews).catch(() => setPipelineViews(null)); };

  useEffect(() => { loadDef(); loadStats(); loadCost(); loadViews(); /* eslint-disable-line react-hooks/exhaustive-deps */ }, [pipelineId]);
  useSse((evt) => {
    if (evt.type === "pipelines_changed") { loadDef(); loadStats(); }
    else if (evt.type === "flow_created" || evt.type === "flow_changed" || evt.type === "flow_terminated") {
      loadStats();
    }
  });

  // Stable identity across re-renders — GraphView's effect depends on this
  // callback to know when to rebuild the cytoscape instance. An inline arrow
  // here would get a new reference on every render (including ones from
  // unrelated SSE-driven loadStats() calls fired by ANY pipeline's flow
  // activity), forcing a full graph teardown/rebuild each time.
  const handleNodeClick = useCallback((id: string) => {
    setSelectedNode((cur) => (cur === id ? null : id));
  }, []);

  if (error) {
    return (
      <>
        <a href="#/pipelines" className="text-fg-dim">← pipelines</a>
        <h1 className="font-mono mt-2 text-lg font-semibold">{pipelineId}</h1>
        <p className="text-status-red">{error} — the pipeline may have been removed.</p>
      </>
    );
  }
  if (!pipeline) {
    return (
      <>
        <a href="#/pipelines" className="text-fg-dim">← pipelines</a>
        <Skeleton className="h-6 w-72 mt-2 mb-1" />
        <Skeleton className="h-3 w-2/3 mb-4" />
        <div className="grid gap-3 mb-4 grid-cols-[repeat(auto-fit,minmax(150px,1fr))]">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="bg-bg-elev border border-border rounded-md px-[14px] py-3">
              <Skeleton className="h-3 w-16 mb-2" />
              <Skeleton className="h-7 w-20" />
              <Skeleton className="h-2 w-24 mt-2" />
            </div>
          ))}
        </div>
        <div className="flex flex-col lg:flex-row gap-3 lg:gap-5">
          <div className="flex-1 min-w-0">
            <Card><Skeleton className="h-4 w-32 mb-3" />{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-3 w-full mb-2" />)}</Card>
          </div>
          <div className="flex-1 min-w-0">
            <Card><Skeleton className="h-4 w-40 mb-3" />{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-3 w-full mb-2" />)}</Card>
          </div>
        </div>
      </>
    );
  }

  return (
    <>
      <a href="#/pipelines" className="text-fg-dim">← pipelines</a>
      <h1 className="font-mono mt-2 text-lg font-semibold">
        {pipelineId}
        <CopyButton value={pipelineId} label="copy pipelineId" className="ml-2" />
      </h1>
      {/* Goal sits under the h1 as a plain subtitle instead of its own
          metadata card — saves vertical real estate, and `file` was
          reference-only (available via /api/pipelines/:id when needed). */}
      {pipeline.goal && (
        <p className="text-sm text-fg-dim mt-1 mb-4">{pipeline.goal}</p>
      )}

      {/* Per-pipeline last-24h strip — same shape as the home overview's
          but scoped to this pipeline. recent24h is precomputed
          server-side; failures come from the same stats payload;
          spend is estimated from cost.perDay (UTC-day buckets). */}
      {(() => {
        const loading = stats === null || cost === null;
        if (loading) return <TodayStrip className="mb-4" loading runs={{ total: 0, ok: 0, failed: 0, active: 0 }} newFailures={0} spent={{ value: 0 }} />;
        const since = Date.now() - TODAY_DAY_MS;
        return (
          <TodayStrip
            className="mb-4"
            runs={stats!.recent24h}
            newFailures={(stats!.failureStops ?? []).filter((c) => (c.lastFailedAt ?? 0) >= since).length}
            spent={estimateLast24hSpent(cost!.perDay)}
          />
        );
      })()}

      {stats ? (
        <StatStrip
          stats={{
            total: stats.total,
            succeeded: stats.succeeded,
            failed: stats.failed,
            cancelled: stats.cancelled,
            terminated: stats.terminated,
            successRate: stats.successRate,
            duration: stats.duration,
            revision: stats.revision,
          }}
          // Scopes every clickable RUNS surface (the total value + each
          // status sub-count) to this pipeline. The total opens search
          // for `<pipelineId>`; each sub-count adds the status token so
          // the search backend's AND-merge filters to just that status.
          searchScope={pipelineId}
        />
      ) : (
        <div className="grid gap-3 mb-4 grid-cols-[repeat(auto-fit,minmax(150px,1fr))]">
          {[0, 1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-bg-elev border border-border rounded-md px-[14px] py-3">
              <Skeleton className="h-3 w-16 mb-2" />
              <Skeleton className="h-7 w-20" />
              <Skeleton className="h-2 w-24 mt-2" />
            </div>
          ))}
        </div>
      )}

      {cost === null && (
        <Card>
          <CardHeader><CardTitle>cost (last 30d)</CardTitle></CardHeader>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="border border-border rounded p-2">
                <Skeleton className="h-3 w-20 mb-1.5" />
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-2 w-32 mt-2" />
              </div>
            ))}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <div className="border border-border rounded p-2">
              {[0, 1, 2, 3, 4].map((i) => (
                <div key={i} className="flex items-center gap-2 mb-1.5">
                  <Skeleton className="h-3 w-32" />
                  <Skeleton className="h-2 flex-1" />
                  <Skeleton className="h-3 w-12" />
                </div>
              ))}
            </div>
            <div className="flex flex-col gap-3">
              {[0, 1].map((i) => (
                <div key={i} className="border border-border rounded p-2 flex-1">
                  <Skeleton className="h-3 w-24 mb-2" />
                  <div className="flex gap-[2px] h-12">
                    {[0, 1, 2, 3, 4, 5].map((j) => <Skeleton key={j} className="flex-1 rounded-[1px]" />)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}

      {cost && cost.runs > 0 && (
        <Card>
          <CardHeader><CardTitle>cost (last 30d)</CardTitle></CardHeader>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
            <CostTile label="total spend"        value={`$${cost.total.toFixed(2)}`} sub={`${cost.runs} run${cost.runs === 1 ? "" : "s"}`} />
            <CostTile
              label="mean / run"
              value={`$${cost.mean.toFixed(2)}`}
              sub={cost.p95 != null ? `p95 $${cost.p95.toFixed(2)}` : undefined}
            />
            <CostTile
              label="cost / success"
              value={cost.costPerSuccess !== null ? `$${cost.costPerSuccess.toFixed(2)}` : "—"}
              sub={cpsSubLine(cost.costPerSuccess, cost.costPerSuccessPrior, cost.succeeded)}
            />
            <CostTile label="models" value={Object.keys(cost.byModel).join(", ") || "—"} />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <NodeCostBreakdown byNode={cost.byNode} total={cost.total} stepOrder={stepOrder(pipeline)} nodeKind={nodeKindMap(pipeline)} nodeGroups={nodeGroupDefs(pipeline)} />
            {/* h-full lets the column stretch to match the NodeCostBreakdown
                sibling's height in the grid; each child gets flex-1 so
                the two trends share the available vertical space. */}
            <div className="flex flex-col gap-3 h-full">
              <DailyCostTrend perDay={cost.perDay} />
              <DailyTimeTrend perDay={cost.perDay} />
            </div>
          </div>
          <p className="text-xs text-fg-dim mt-3">
            <span className="font-mono">cost / success</span> is the ROI signal: total spend ÷ count of succeeded runs. If a successful run is worth more than this number on average, the pipeline is net-positive.
          </p>
        </Card>
      )}

      <div className="flex flex-col lg:flex-row gap-3 lg:gap-5">
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>funnel (succeeded + active runs)</CardTitle></CardHeader>
            <Funnel stages={stats?.funnel ?? null} />
          </Card>
        </div>
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>failure / cancel steps</CardTitle></CardHeader>
            <Histogram entries={stats?.failureStops ?? []} limit={10} />
          </Card>
        </div>
      </div>

      {/* Pipeline-declared views — generic dispatcher. Each entry
          renders via <DeclaredViewCard>, which switches on `kind`.
          Sits below the in-run funnel so the page reads top-to-bottom
          as "did the pipeline produce output → did the output land
          with a human" for pipelines that declare outcome views. */}
      {pipelineViews?.views.map((v) => (
        <DeclaredViewCard key={v.id} view={v} />
      ))}

      <Card>
        <CardHeader><CardTitle>graph</CardTitle></CardHeader>
        <GraphView
          pipeline={pipeline}
          onNodeClick={handleNodeClick}
          selectedNodeId={selectedNode}
        />
        <p className="text-xs text-fg-dim mt-2">tip: tap a node to inspect its visits and recent runs.</p>
      </Card>

      {selectedNode && (
        <NodeContextCard
          pipelineId={pipelineId}
          nodeId={selectedNode}
          onClose={() => setSelectedNode(null)}
        />
      )}

      <div className="flex flex-col lg:flex-row gap-3 lg:gap-5">
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>nodes</CardTitle></CardHeader>
            {/* Mobile card list (<sm): slot column was off-screen on
                phones. Cards put id + kind on row 1 and surface
                agent/slot only when present. */}
            <div className="sm:hidden flex flex-col gap-1.5">
              {pipeline.nodes.filter((n) => n.id !== "__start__" && n.id !== "__end__").map((n) => (
                <div key={n.id} className="rounded border border-border bg-bg-elev2/30 px-2.5 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm truncate flex-1">{n.id}</span>
                    <Badge variant="kind">{n.kind}</Badge>
                  </div>
                  {(n.agent || n.groupId) && (
                    <div className="mt-1 flex flex-wrap gap-x-3 text-[10px] font-mono text-fg-dim">
                      {n.agent && <span>agent: {n.agent}</span>}
                      {n.groupId && <span>group: {n.groupId}</span>}
                    </div>
                  )}
                </div>
              ))}
            </div>
            <div className="hidden sm:block">
              <Table>
                <THead>
                  <TR><TH>id</TH><TH>kind</TH><TH>agent</TH><TH>group</TH></TR>
                </THead>
                <TBody>
                  {pipeline.nodes.filter((n) => n.id !== "__start__" && n.id !== "__end__").map((n) => (
                    <TR key={n.id}>
                      <TD className="font-mono">{n.id}</TD>
                      <TD><Badge variant="kind">{n.kind}</Badge></TD>
                      <TD className="text-fg-dim font-mono">{n.agent ?? ""}</TD>
                      <TD className="text-fg-dim font-mono">{n.groupId ?? ""}</TD>
                    </TR>
                  ))}
                </TBody>
              </Table>
            </div>
          </Card>
        </div>
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>edges</CardTitle></CardHeader>
            {/* Mobile card list (<sm): `from → to` as a single line so
                long node ids don't fight a fixed column. Kind badge
                pinned to the right when conditional. */}
            <div className="sm:hidden flex flex-col gap-1.5">
              {pipeline.edges.map((e, i) => (
                <div key={i} className="rounded border border-border bg-bg-elev2/30 px-2.5 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs flex-1 min-w-0 break-all">
                      <span>{e.from}</span>
                      <span className="text-fg-dim mx-1">→</span>
                      <span>{e.to}</span>
                    </span>
                    {e.conditional && (
                      <span className="text-[10px] font-mono text-fg-dim shrink-0">conditional</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
            <div className="hidden sm:block">
              <Table>
                <THead>
                  <TR><TH>from</TH><TH>to</TH><TH>kind</TH></TR>
                </THead>
                <TBody>
                  {pipeline.edges.map((e, i) => (
                    <TR key={i}>
                      <TD className="font-mono">{e.from}</TD>
                      <TD className="font-mono">{e.to}</TD>
                      <TD className="text-fg-dim">{e.conditional ? "conditional" : "static"}</TD>
                    </TR>
                  ))}
                </TBody>
              </Table>
            </div>
          </Card>
        </div>
      </div>
    </>
  );
}

// Build a "node id → declaration order index" map from the pipeline. Used
// to sort the per-node breakdown by execution flow rather than spend.
// Keys are lowercased to match the dashboard's per-node rollup convention
// (which derives nodeIds from session keys — also lowercase). Sub-steps
// inside subflow nodes are slotted in at their parent's
// position, just after the parent, preserving the "flow reads top-down"
// intuition (PROPOSE → propose.draft → propose.gate → propose.submit
// reads as one block).
function stepOrder(pipeline: Pipeline | null): Map<string, number> {
  const map = new Map<string, number>();
  if (!pipeline) return map;
  let i = 0;
  for (const node of pipeline.nodes) {
    if (node.id === "__start__" || node.id === "__end__") continue;
    map.set(node.id.toLowerCase(), i++);
    if (node.flow?.nodes) {
      for (const sub of node.flow.nodes) {
        if (sub.id === "__start__" || sub.id === "__end__") continue;
        map.set(`${node.id}.${sub.id}`.toLowerCase(), i++);
      }
    }
  }
  return map;
}

// `nodeId.toLowerCase() → kind` so the per-node breakdown can annotate each
// row with code/agent/loop/gate/etc. Helps the reader
// interpret $0 rows (gate / code) at a glance without remembering the
// pipeline definition.
function nodeKindMap(pipeline: Pipeline | null): Map<string, string> {
  const map = new Map<string, string>();
  if (!pipeline) return map;
  for (const node of pipeline.nodes) {
    map.set(node.id.toLowerCase(), node.kind);
    if (node.flow?.nodes) {
      for (const sub of node.flow.nodes) {
        map.set(`${node.id}.${sub.id}`.toLowerCase(), sub.kind);
      }
    }
  }
  return map;
}

// Node group definitions for the cost breakdown's coalescing logic — one
// entry per `kind:"subflow"` node (the subflow's own direct/orchestration
// cost + its flow children). A subflow is a group because it's one logical
// step whose cost is scattered across several node_ids in cost_by_call, and
// showing them as disconnected sibling rows fragments the signal that
// belongs to the step.
//
// Derived structurally — NOT from `pipeline.groups`, which is an
// author-declared field almost nothing sets (pipeline-graph.js just passes
// it through verbatim; it's `null` for most pipelines). Member ids follow
// the same top-level-bare / subflow-internal-flattened convention as
// nodeKindMap/stepOrder above.
function nodeGroupDefs(pipeline: Pipeline | null): NodeGroupDef[] {
  if (!pipeline) return [];
  const out: NodeGroupDef[] = [];

  const addLoopGroup = (node: PipelineNode) => {
    if (node.kind !== "subflow" || !node.flow?.nodes) return;
    const members: NodeGroupLeaf[] = [{ id: node.id.toLowerCase(), role: "direct" }];
    for (const sub of node.flow.nodes) {
      if (sub.id === "__start__" || sub.id === "__end__") continue;
      members.push({ id: `${node.id}.${sub.id}`.toLowerCase(), role: "subflow" });
    }
    out.push({ groupId: `__group:${node.id.toLowerCase()}`, label: node.id, members });
  };

  for (const node of pipeline.nodes) addLoopGroup(node);
  return out;
}

// Combine multiple PipelineCostNodeEntry values into one. cost/calls add;
// meanMs is weighted by calls. p50/p95 (both latency and per-call cost)
// use a CDF approximation across members: when most calls hit a fast
// direct step and a few hit a slow flow-child, the combined p50 stays in
// the fast bucket (the median call lives there) while the p95 lands inside
// the slow bucket. The older MAX rule reported the worst member's
// percentile — useful for "how bad can it get" but misleading for "what
// does the typical call look like."
function combineEntries(entries: (import("@/api/types").PipelineCostNodeEntry | undefined)[]): import("@/api/types").PipelineCostNodeEntry {
  let cost = 0, calls = 0;
  let meanSum = 0, meanCalls = 0;
  for (const e of entries) {
    if (!e) continue;
    cost  += e.cost;
    calls += e.calls;
    if (e.meanMs != null && e.calls > 0) { meanSum += e.meanMs * e.calls; meanCalls += e.calls; }
  }

  // CDF percentile across members, parameterized by which member
  // percentile pair drives the bucket ordering and interpolation.
  // `axis` picks either ms (latency) or cost (per-call $) — same
  // walk, different fields.
  const cdfPctile = (
    p: number,
    axis: "ms" | "cost",
  ): number | null => {
    const get50 = (e: import("@/api/types").PipelineCostNodeEntry) => axis === "ms" ? e.p50Ms : e.p50Cost;
    const get95 = (e: import("@/api/types").PipelineCostNodeEntry) => axis === "ms" ? e.p95Ms : e.p95Cost;
    const sorted = entries
      .filter((e): e is import("@/api/types").PipelineCostNodeEntry =>
        !!e && e.calls > 0 && (get50(e) != null || get95(e) != null))
      .slice()
      .sort((a, b) => (get50(a) ?? 0) - (get50(b) ?? 0));
    const totalCalls = sorted.reduce((s, e) => s + e.calls, 0);
    if (totalCalls === 0) return null;
    const target = totalCalls * p / 100;
    let cum = 0;
    for (const e of sorted) {
      const next = cum + e.calls;
      if (target <= next) {
        const withinPct = ((target - cum) / e.calls) * 100;
        const m50 = get50(e) ?? 0;
        const m95 = get95(e) ?? m50;
        if (withinPct <= 50) return m50;
        if (withinPct >= 95) return m95;
        return m50 + ((withinPct - 50) / 45) * (m95 - m50);
      }
      cum = next;
    }
    return get95(sorted[sorted.length - 1]) ?? null;
  };

  // Group perDay = sum of each member's perDay by day. cost/calls/ms
  // are simple sums. p50Cost is a call-weighted approximation across
  // members on that day (we don't have the raw samples here — using
  // weighted mean of per-member medians; close enough for the chart).
  type DayAcc = {
    day: string; cost: number; calls: number; ms: number;
    p50CostNumer: number; p50CostDenom: number;
    p50MsNumer: number; p50MsDenom: number;
  };
  const dayMap = new Map<string, DayAcc>();
  for (const e of entries) {
    if (!e?.perDay) continue;
    for (const d of e.perDay) {
      const cur = dayMap.get(d.day) ?? {
        day: d.day, cost: 0, calls: 0, ms: 0,
        p50CostNumer: 0, p50CostDenom: 0,
        p50MsNumer: 0, p50MsDenom: 0,
      };
      cur.cost  += d.cost  ?? 0;
      cur.calls += d.calls ?? 0;
      cur.ms    += d.ms    ?? 0;
      if (d.p50Cost != null && d.calls > 0) {
        cur.p50CostNumer += d.p50Cost * d.calls;
        cur.p50CostDenom += d.calls;
      }
      if (d.p50Ms != null && d.calls > 0) {
        cur.p50MsNumer += d.p50Ms * d.calls;
        cur.p50MsDenom += d.calls;
      }
      dayMap.set(d.day, cur);
    }
  }
  const perDay = [...dayMap.values()]
    .map((d) => ({
      day: d.day,
      cost: d.cost,
      calls: d.calls,
      ms: d.ms,
      p50Cost: d.p50CostDenom > 0 ? d.p50CostNumer / d.p50CostDenom : null,
      p50Ms:   d.p50MsDenom   > 0 ? d.p50MsNumer   / d.p50MsDenom   : null,
    }))
    .sort((a, b) => a.day.localeCompare(b.day));

  return {
    cost,
    calls,
    p50Ms: cdfPctile(50, "ms"),
    p95Ms: cdfPctile(95, "ms"),
    meanMs: meanCalls > 0 ? meanSum / meanCalls : null,
    p50Cost: cdfPctile(50, "cost"),
    p95Cost: cdfPctile(95, "cost"),
    perDay,
  };
}

// Mini sparkline for the row-expansion trend. Mirrors DailyCostTrend's
// look (bars stacked left-to-right, height = value/max) but compact
// enough to fit inside an expansion grid. Metric switches with the
// row's mode — $ in cost mode (with a total/p50 sub-toggle for "total
// daily spend" vs "typical-call cost") or ms in time mode (total
// only). The sub-toggle is $-mode-only by design; per-day per-call
// median doesn't carry the same diagnostic value for latency where
// the typical call is already represented by the row's p50 stat.
type DayPoint = { day: string; cost: number; calls: number; ms: number; p50Cost: number | null; p50Ms: number | null };

function NodeTrendSparkline({ perDay, mode }: {
  perDay: DayPoint[];
  mode: "cost" | "time";
}) {
  // Local total/p50 toggle for $-mode. Lives at the deepest "forecast"
  // surface (a single node's daily trend) where p50 $/call is the
  // closest thing to a per-call projection — useful for "is this node
  // getting more expensive per call" diagnosis. Higher surfaces
  // (DailyCostTrend, by-pipeline header) stay total-only since the
  // typical-call lens doesn't carry meaningful context there.
  const [costMetric, setCostMetric] = useState<"total" | "p50">("total");
  // Tap-to-pin label — title= tooltips don't work on mobile.
  const [pinned, setPinned] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  useClickOutside(rootRef, () => setPinned(null));
  if (!perDay || perDay.length === 0) return <span className="col-span-2 sm:col-span-4 text-fg-dim italic">no per-day samples yet</span>;
  const metric = (d: DayPoint): number | null => {
    if (mode === "time") return d.p50Ms;
    return costMetric === "p50" ? d.p50Cost : d.cost;
  };
  const max = Math.max(0.0001, ...perDay.map((d) => metric(d) ?? 0));
  const pinnedDay = pinned ? perDay.find((d) => d.day === pinned) : null;
  const anomalies = computeAnomalies(perDay, (d) => metric(d) ?? 0);
  const pinnedRatio = pinnedDay ? anomalies.ratios.get(pinnedDay.day) : undefined;
  const fmt = (d: DayPoint) =>
    mode === "cost"
      ? (costMetric === "p50"
          ? `${d.day}: p50 \$${(d.p50Cost ?? 0).toFixed(4)} · ${d.calls} call${d.calls === 1 ? "" : "s"}`
          : `${d.day}: total \$${d.cost.toFixed(4)} · ${d.calls} call${d.calls === 1 ? "" : "s"}`)
      : `${d.day}: p50 ${formatDuration(d.p50Ms ?? 0)} · ${d.calls} call${d.calls === 1 ? "" : "s"}`;
  const subtitle = mode === "cost"
    ? `daily ${costMetric === "p50" ? "p50 $/call" : "$ total"}`
    : "daily p50 latency";
  return (
    <div ref={rootRef} className="col-span-2 sm:col-span-4 mt-1">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[9px] text-fg-dim font-mono">{subtitle}</span>
        {mode === "cost" && (
          <div
            className="inline-flex rounded border border-border overflow-hidden text-[9px] font-mono"
            onClick={(e) => e.stopPropagation()}
          >
            <button
              type="button"
              onClick={() => setCostMetric("total")}
              className={costMetric === "total" ? "px-1.5 py-px bg-status-blue/20 text-fg" : "px-1.5 py-px text-fg-dim hover:text-fg"}
            >total</button>
            <button
              type="button"
              onClick={() => setCostMetric("p50")}
              className={costMetric === "p50" ? "px-1.5 py-px border-l border-border bg-status-blue/20 text-fg" : "px-1.5 py-px border-l border-border text-fg-dim hover:text-fg"}
            >p50</button>
          </div>
        )}
      </div>
      <div className="text-right text-[9px] text-fg font-mono mb-0.5 min-h-[12px]">
        {pinnedDay && (
          <span>
            {fmt(pinnedDay)}
            {pinnedRatio != null && <span className="text-status-amber"> · {pinnedRatio.toFixed(1)}× mean</span>}
          </span>
        )}
      </div>
      <div className="flex gap-[2px] h-10">
        {perDay.map((d) => {
          const v = metric(d) ?? 0;
          const h = v > 0 ? Math.max(2, Math.round((v / max) * 100)) : 2;
          const isPinned = pinned === d.day;
          const muted = pinned !== null && !isPinned;
          const isAnomaly = anomalies.flagged.has(d.day);
          const ratio = anomalies.ratios.get(d.day);
          const fill = v > 0 ? (isAnomaly ? "bg-status-amber" : "bg-status-blue") : "bg-bg-elev2";
          const tip = isAnomaly && ratio != null ? `${fmt(d)} · ${ratio.toFixed(1)}× mean` : fmt(d);
          return (
            <button
              type="button"
              key={d.day}
              onClick={(ev) => { ev.stopPropagation(); setPinned(isPinned ? null : d.day); }}
              className="flex-1 min-w-[4px] relative h-full"
              title={tip}
            >
              <span
                className={`absolute bottom-0 left-0 right-0 rounded-[1px] ${fill} ${muted ? "opacity-65" : ""}`}
                style={{ height: `${h}%` }}
              />
            </button>
          );
        })}
      </div>
      <div className="flex justify-between mt-1 text-[9px] text-fg-dim font-mono">
        <span>{perDay[0].day}</span>
        <span>{perDay.length}d</span>
        <span>{perDay[perDay.length - 1].day}</span>
      </div>
    </div>
  );
}

// Bar color per node kind. Matches the graph-view's existing kind palette
// (see GraphView.tsx graphStyle entries) so a node has the same color in
// the cytoscape graph and in the cost breakdown — cross-surface recall.
// Unknown kinds (legacy nodes no longer in the pipeline def) fall back
// to a neutral gray.
const KIND_COLOR: Record<string, string> = {
  code: "#58a6ff",            // blue
  agent: "#bc8cff",           // purple
  subflow: "#f0883e",         // orange
  "human-gate": "#3fb950",    // green
};
const KIND_COLOR_FALLBACK = "#6e7681";

function CostTile({ label, value, sub, tone, title }: { label: string; value: string; sub?: React.ReactNode; tone?: "amber" | "red" | "green" | null; title?: string }) {
  const toneClass = tone === "red" ? "text-status-red"
    : tone === "amber" ? "text-status-amber"
    : tone === "green" ? "text-status-green"
    : "text-fg";
  return (
    <div className="border border-border rounded p-2" title={title}>
      <div className="text-xs text-fg-dim">{label}</div>
      <div className={`font-mono text-lg ${toneClass}`}>{value}</div>
      {sub && <div className="text-xs text-fg-dim mt-0.5">{sub}</div>}
    </div>
  );
}

// Sub-line for the cost/success tile: shows the current succeeded count
// AND a delta vs the same-length prior window (when available). Direction
// arrow is colored green when cost dropped, red when it rose — the
// economic signal flips green for "cheaper" no matter what the absolute
// number is.
function cpsSubLine(curr: number | null, prior: number | null, succeeded: number): React.ReactNode {
  if (curr === null) return succeeded > 0 ? `${succeeded} succeeded` : "no successes yet";
  const counts = `${succeeded} succeeded`;
  if (prior === null) return counts;
  const delta = curr - prior;
  if (Math.abs(delta) < 0.005) return <>{counts} · <span className="text-fg-dim">steady</span></>;
  const up = delta > 0;
  const cls = up ? "text-status-red" : "text-status-green";
  const sign = up ? "↑" : "↓";
  return (
    <>
      {counts} · <span className={cls}>{sign} ${Math.abs(delta).toFixed(2)}</span> vs prior
    </>
  );
}

// Top spend by node, with latency p50/p95 alongside. Surfaces "which step
// burns the most" AND "which step is slow even though it's cheap" — both
// useful for prompt/model changes targeting the right place.
//
// Ordered by pipeline declaration order (passed in as stepOrder) so the
// rows read top-to-bottom along the actual execution flow rather than
// purely by spend. Unknown nodes (historical names, deleted steps) fall
// to the bottom in cost-desc order.
/** A member of a node group — a real cost_by_call node id. */
interface NodeGroupLeaf {
  /** Lowercased node id. */
  id: string;
  /** "direct" for a subflow's own orchestration cost. "subflow" for an
   *  ordinary subflow child. */
  role: "direct" | "subflow";
}

interface NodeGroupDef {
  /** Collision-safe id — prefixed the same way GraphView.tsx prefixes
   *  compound node ids (`__group:${id}`). */
  groupId: string;
  label: string;
  /** The subflow's own direct-cost leaf followed by its flow children. */
  members: NodeGroupLeaf[];
}

function NodeCostBreakdown({
  byNode,
  total,
  stepOrder,
  nodeKind,
  nodeGroups,
}: {
  byNode: Record<string, import("@/api/types").PipelineCostNodeEntry>;
  total: number;
  stepOrder: Map<string, number>;
  nodeKind: Map<string, string>;
  /** Subflow (loop) groups for coalescing rows. Lowercased member ids. */
  nodeGroups: NodeGroupDef[];
}) {
  // Toggle between $ and time as the bar dimension. Row order (step order)
  // and trailing readouts stay constant so the cross-dimension comparison
  // is just a visual flip rather than a re-shuffle.
  const [mode, setMode] = useState<"cost" | "time">("cost");
  // Single-row expansion for tap-to-reveal full detail (mobile-friendly,
  // no hover required). One row at a time keeps the visual density low.
  const [expanded, setExpanded] = useState<string | null>(null);
  // Separate expansion state for a member row INSIDE an open group —
  // reusing `expanded` would collapse the parent group the moment you tap
  // a child (since `expanded` is keyed by row id and the child's id !=
  // the group's id).
  const [expandedSub, setExpandedSub] = useState<string | null>(null);
  // Sub-toggle for the trend sparkline in $-mode: total daily spend
  // vs per-call median ($/call median per day). Shared across all
  // expansions so the operator forms one consistent view.

  // Coalesce group members into a single row per group. A subflow's own
  // direct cost + its flow children implement the SAME logical step —
  // showing them as separate rows fragments the spend/time signal that
  // belongs to the step. The collapsed row uses the group's label (the
  // step name) and aggregates: cost = sum of member costs, p50/p95 = worst
  // of the members (the step's real-world latency on its bad path),
  // calls = sum.
  type GroupRow = {
    kind: "group";
    id: string;
    label: string;
    members: NodeGroupLeaf[];
    entry: import("@/api/types").PipelineCostNodeEntry;
    orderIdx: number;
  };
  type SingleRow = {
    kind: "single";
    id: string;
    entry: import("@/api/types").PipelineCostNodeEntry;
    orderIdx: number;
  };
  type Row = GroupRow | SingleRow;

  const groupedMembers = new Set<string>();
  const groupRows: GroupRow[] = [];
  for (const g of nodeGroups) {
    const leaves = g.members;
    const entries = leaves.map((leaf) => byNode[leaf.id]);
    if (entries.every((e) => !e)) continue;
    for (const leaf of leaves) groupedMembers.add(leaf.id);
    const combined = combineEntries(entries);
    if (combined.cost <= 0 && (combined.p50Ms ?? 0) <= 0) continue;
    groupRows.push({
      kind: "group",
      id: g.groupId,
      label: g.label,
      members: g.members,
      entry: combined,
      orderIdx: stepOrder.get(leaves[0]?.id ?? "") ?? Number.POSITIVE_INFINITY,
    });
  }

  const singleRows: SingleRow[] = Object.entries(byNode)
    .filter(([n, v]) => !groupedMembers.has(n) && (v.cost > 0 || (v.p50Ms ?? 0) > 0))
    .map(([n, v]) => ({
      kind: "single" as const,
      id: n,
      entry: v,
      orderIdx: stepOrder.get(n) ?? Number.POSITIVE_INFINITY,
    }));

  const rows: Row[] = [...groupRows, ...singleRows].sort((a, b) => {
    if (a.orderIdx !== b.orderIdx) return a.orderIdx - b.orderIdx;
    return b.entry.cost - a.entry.cost;
  });
  if (rows.length === 0) return null;

  // Time mode = typical-call latency view. The bar visualizes p50
  // (median per-call duration), display matches, and share is computed
  // off p50 × calls so all three read the same metric. The bimodal
  // tail story (mean × calls = total wall time, p95 = worst call) lives
  // in the expansion. combineEntries' CDF logic keeps the group p50
  // honest — the median lands in whichever member bucket most calls hit,
  // rather than the MAX-of-members trap.
  const max = mode === "cost"
    ? Math.max(...rows.map((r) => r.entry.cost))
    : Math.max(...rows.map((r) => r.entry.p50Ms ?? 0));
  const totalTime = rows.reduce((s, r) => s + (r.entry.p50Ms ?? 0) * r.entry.calls, 0);

  return (
    <div className="border border-border rounded p-2">
      <div className="flex items-center gap-2 mb-2">
        <div className="text-xs text-fg-dim">by node — {mode === "cost" ? "spend" : "wall time"}</div>
        <div className="ml-auto inline-flex rounded border border-border overflow-hidden text-[10px] font-mono">
          <button
            onClick={() => setMode("cost")}
            className={cn(
              "px-2 py-0.5",
              mode === "cost" ? "bg-status-blue/20 text-fg" : "text-fg-dim hover:text-fg",
            )}
          >$</button>
          <button
            onClick={() => setMode("time")}
            className={cn(
              "px-2 py-0.5 border-l border-border",
              mode === "time" ? "bg-status-blue/20 text-fg" : "text-fg-dim hover:text-fg",
            )}
          >time</button>
        </div>
      </div>
      {/*
        Single grid container so all rows share one column-template. Each
        per-row button uses `display: contents` (Tailwind `contents`)
        so its three cells participate directly in the parent grid —
        the `auto` first column then sizes to the WIDEST label across
        all rows, giving uniform bar-start AND bar-end positions
        across rows. The expanded detail block uses `col-span-3`.
      */}
      <div className="grid grid-cols-[auto_1fr_auto_auto] items-center gap-x-2 gap-y-1">
        {rows.map((row) => {
          const entry = row.entry;
          const p50 = formatDuration(entry.p50Ms);
          const p95 = formatDuration(entry.p95Ms);
          const barValue = mode === "cost" ? entry.cost : (entry.p50Ms ?? 0);
          const barPct = max > 0 ? Math.round((barValue / max) * 100) : 0;
          const share = mode === "cost"
            ? (total > 0 ? (entry.cost / total) * 100 : 0)
            : (totalTime > 0 ? ((entry.p50Ms ?? 0) * entry.calls / totalTime) * 100 : 0);
          const primaryCompact = mode === "cost" ? `$${entry.cost.toFixed(2)}` : p50;
          const isOpen = expanded === row.id;

          // Subflow groups use their real "subflow" kind color (orange) — a
          // subflow is a real NodeKind, so it already has a legend entry.
          // Single rows use their own node kind's color.
          const swatchColor = row.kind === "group"
            ? KIND_COLOR.subflow
            : (KIND_COLOR[nodeKind.get(row.id) ?? ""] ?? KIND_COLOR_FALLBACK);

          const labelText = row.kind === "group" ? row.label : row.id;
          // Every row gets the open/close caret (consistent
          // affordance across all tap-to-expand surfaces in the
          // dashboard). Group rows are distinguished by their swatch
          // color (orange = subflow) — no inline badge.
          const labelDecoration = isOpen ? "▾" : "▸";
          const titleText = row.kind === "group"
            ? `${row.label} · subflow group (${row.members.map((m) => m.id).join(" → ")}) · combined ${entry.calls} call${entry.calls === 1 ? "" : "s"} · $${entry.cost.toFixed(4)} · p50 ${p50} · p95 ${p95}`
            : `${row.id} · ${nodeKind.get(row.id) ?? "unknown kind"} · ${entry.calls} call${entry.calls === 1 ? "" : "s"} · $${entry.cost.toFixed(4)} · p50 ${p50} · p95 ${p95}`;

          return (
            <Fragment key={row.id}>
              <button
                type="button"
                onClick={() => setExpanded(isOpen ? null : row.id)}
                aria-expanded={isOpen}
                title={titleText}
                className="contents text-left group"
              >
                <span className="flex items-center gap-1.5 min-w-0 text-xs group-hover:bg-bg-elev2/40 rounded-l-[2px] pl-0.5">
                  <span
                    aria-hidden
                    className="inline-block w-2 h-2 rounded-[1px] shrink-0"
                    style={{ backgroundColor: swatchColor }}
                  />
                  <span className="font-mono truncate">{labelText}</span>
                  {labelDecoration && (
                    <span className="text-[10px] text-fg-dim leading-none shrink-0">{labelDecoration}</span>
                  )}
                </span>
                <span className="relative h-2 w-full bg-bg-elev2 rounded-[1px]">
                  <span className="absolute left-0 top-0 bottom-0 bg-status-blue rounded-[1px]" style={{ width: `${barPct}%` }} />
                </span>
                <span className="font-mono text-fg-dim tabular-nums text-right whitespace-nowrap text-xs group-hover:bg-bg-elev2/40">
                  {primaryCompact}
                </span>
                <span className="font-mono text-fg-dim tabular-nums text-right whitespace-nowrap text-[10px] group-hover:bg-bg-elev2/40 rounded-r-[2px] pr-0.5">
                  ({share.toFixed(0)}%)
                </span>
              </button>
              {/*
                Expansion behavior:
                 - Single rows: detail block with kind/calls/p50/p95/$.
                 - Group rows: same detail block (showing the group's
                   COMBINED stats), then the two member nodes as sub-rows.
                   Sub-row share is throughput (calls split) — answers
                   "did the cheap primary handle most attempts, or are
                   most falling through to the agent?". Bar scale on
                   sub-rows is group-local so a small primary cost
                   still shows visible width.
              */}
              {isOpen && row.kind === "group" && (
                <div className="col-span-4 pl-5 pr-1 pt-0.5 pb-1 text-[10px] font-mono text-fg-dim grid grid-cols-2 sm:grid-cols-4 gap-x-3 gap-y-0.5">
                  <span><span className="text-fg-dim">kind:</span> subflow group</span>
                  <span><span className="text-fg-dim">calls:</span> {entry.calls}</span>
                  <span><span className="text-fg-dim">$ total:</span> ${entry.cost.toFixed(4)}</span>
                  <span><span className="text-fg-dim">share:</span> {share.toFixed(1)}%</span>
                  <span><span className="text-fg-dim">p50 time:</span> {p50}</span>
                  <span><span className="text-fg-dim">p95 time:</span> {p95}</span>
                  <span><span className="text-fg-dim">p50 $/call:</span> {entry.p50Cost != null ? `$${entry.p50Cost.toFixed(4)}` : "—"}</span>
                  <span><span className="text-fg-dim">p95 $/call:</span> {entry.p95Cost != null ? `$${entry.p95Cost.toFixed(4)}` : "—"}</span>
                  {/* Skip the chart in $-mode for zero-cost nodes (the
                      bars would all be flat 0). Time mode always
                      renders since wall-time data exists for every
                      node — even code nodes have a measured ms. */}
                  {(mode === "time" || entry.cost > 0) && (
                    <NodeTrendSparkline perDay={entry.perDay} mode={mode} />
                  )}
                </div>
              )}
              {isOpen && row.kind === "group" && (() => {
                // Sub-row share % is THROUGHPUT (calls within the
                // IMMEDIATE parent group) — answers "did the direct step
                // dominate cost, or do the flow children?". The bar stays
                // scaled to the PIPELINE-WIDE max (same `max` as the
                // top-level rows) so a small member's actual proportional
                // spend doesn't get visually inflated by being normalized
                // against a tiny group.
                const zeroEntry = { cost: 0, calls: 0, p50Ms: null, p95Ms: null, meanMs: null, p50Cost: null, p95Cost: null, perDay: [] };

                // One member row: a leaf's own bar + role badge + tap-to-
                // expand detail.
                const renderLeaf = (leaf: NodeGroupLeaf, groupCalls: number) => {
                  const me = byNode[leaf.id] ?? zeroEntry;
                  const mp50 = formatDuration(me.p50Ms);
                  const mp95 = formatDuration(me.p95Ms);
                  const mkind = nodeKind.get(leaf.id) ?? "";
                  const mSwatch = KIND_COLOR[mkind] ?? KIND_COLOR_FALLBACK;
                  const mBarValue = mode === "cost" ? me.cost : (me.p50Ms ?? 0);
                  const mBarPct = max > 0 ? Math.round((mBarValue / max) * 100) : 0;
                  const mShare = groupCalls > 0 ? (me.calls / groupCalls) * 100 : 0;
                  const mPrimary = mode === "cost" ? `$${me.cost.toFixed(2)}` : mp50;
                  const mIsOpen = expandedSub === leaf.id;
                  const rowIndent = "pl-4";
                  const detailIndent = "pl-8";
                  return (
                    <Fragment key={`${row.id}:${leaf.id}`}>
                      <button
                        type="button"
                        onClick={() => setExpandedSub(mIsOpen ? null : leaf.id)}
                        aria-expanded={mIsOpen}
                        title={`${leaf.id} · ${mkind} · ${leaf.role} · ${me.calls} call${me.calls === 1 ? "" : "s"} · $${me.cost.toFixed(4)} · p50 ${mp50} · p95 ${mp95}`}
                        className="contents text-left group"
                      >
                        <span className={`flex items-center gap-1.5 min-w-0 text-xs ${rowIndent} text-fg-dim group-hover:bg-bg-elev2/40`}>
                          <span aria-hidden className="inline-block w-2 h-2 rounded-[1px] shrink-0" style={{ backgroundColor: mSwatch }} />
                          <span className="font-mono truncate">{leaf.id}</span>
                          <span className="text-[9px] font-mono text-fg-dim border border-border rounded px-1 py-px leading-none shrink-0">
                            {leaf.role}
                          </span>
                          <span className="text-[10px] text-fg-dim leading-none shrink-0">{mIsOpen ? "▾" : "▸"}</span>
                        </span>
                        <span className="relative h-1.5 w-full bg-bg-elev2 rounded-[1px]">
                          <span className="absolute left-0 top-0 bottom-0 bg-status-blue/60 rounded-[1px]" style={{ width: `${mBarPct}%` }} />
                        </span>
                        <span className="font-mono text-fg-dim tabular-nums text-right whitespace-nowrap text-xs group-hover:bg-bg-elev2/40">
                          {mPrimary}
                        </span>
                        <span className="font-mono text-fg-dim tabular-nums text-right whitespace-nowrap text-[10px] pr-0.5 group-hover:bg-bg-elev2/40" title={`throughput share within this group: ${me.calls} of ${groupCalls} calls`}>
                          ({mShare.toFixed(0)}%)
                        </span>
                      </button>
                      {mIsOpen && (
                        <div className={`col-span-4 ${detailIndent} pr-1 pt-0.5 pb-1 text-[10px] font-mono text-fg-dim grid grid-cols-2 sm:grid-cols-4 gap-x-3 gap-y-0.5`}>
                          <span><span className="text-fg-dim">kind:</span> {mkind || "—"}</span>
                          <span><span className="text-fg-dim">calls:</span> {me.calls}</span>
                          <span><span className="text-fg-dim">$ total:</span> ${me.cost.toFixed(4)}</span>
                          <span><span className="text-fg-dim">group share:</span> {mShare.toFixed(1)}%</span>
                          <span><span className="text-fg-dim">p50 time:</span> {mp50}</span>
                          <span><span className="text-fg-dim">p95 time:</span> {mp95}</span>
                          <span><span className="text-fg-dim">p50 $/call:</span> {me.p50Cost != null ? `$${me.p50Cost.toFixed(4)}` : "—"}</span>
                          <span><span className="text-fg-dim">p95 $/call:</span> {me.p95Cost != null ? `$${me.p95Cost.toFixed(4)}` : "—"}</span>
                          {(mode === "time" || me.cost > 0) && (
                            <NodeTrendSparkline perDay={me.perDay} mode={mode} />
                          )}
                        </div>
                      )}
                    </Fragment>
                  );
                };

                const groupCalls = row.members.reduce((s, leaf) => s + (byNode[leaf.id]?.calls ?? 0), 0);
                // Always render every member of the group — a subflow
                // group is inherently its own direct cost + every flow
                // child, and a child with no calls yet (0, $0) is itself
                // the signal that it hasn't run. Skipping members with no
                // byNode entry made the group look like it only had one node.
                return row.members.map((leaf) => renderLeaf(leaf, groupCalls));
              })()}
              {isOpen && row.kind === "single" && (
                <div className="col-span-4 pl-5 pr-1 pt-0.5 pb-1 text-[10px] font-mono text-fg-dim grid grid-cols-2 sm:grid-cols-4 gap-x-3 gap-y-0.5">
                  <span><span className="text-fg-dim">kind:</span> {nodeKind.get(row.id) || "—"}</span>
                  <span><span className="text-fg-dim">calls:</span> {entry.calls}</span>
                  <span><span className="text-fg-dim">$ total:</span> ${entry.cost.toFixed(4)}</span>
                  <span><span className="text-fg-dim">share:</span> {share.toFixed(1)}%</span>
                  <span><span className="text-fg-dim">p50 time:</span> {p50}</span>
                  <span><span className="text-fg-dim">p95 time:</span> {p95}</span>
                  <span><span className="text-fg-dim">p50 $/call:</span> {entry.p50Cost != null ? `$${entry.p50Cost.toFixed(4)}` : "—"}</span>
                  <span><span className="text-fg-dim">p95 $/call:</span> {entry.p95Cost != null ? `$${entry.p95Cost.toFixed(4)}` : "—"}</span>
                  {/* Skip the chart in $-mode for zero-cost nodes (the
                      bars would all be flat 0). Time mode always
                      renders since wall-time data exists for every
                      node — even code nodes have a measured ms. */}
                  {(mode === "time" || entry.cost > 0) && (
                    <NodeTrendSparkline perDay={entry.perDay} mode={mode} />
                  )}
                </div>
              )}
            </Fragment>
          );
        })}
      </div>
      <KindLegend kinds={new Set([
        // Real node kinds present in the breakdown — group rows
        // contribute their member nodes' kinds so a row that
        // coalesces a `code` + `agent` pair surfaces both.
        ...rows.flatMap((r) => r.kind === "group" ? r.members.map((m) => nodeKind.get(m.id) ?? "") : [nodeKind.get(r.id) ?? ""]),
      ])} />
      <p className="text-[10px] text-fg-dim mt-1">columns: <span className="font-mono">node · bar · {mode === "cost" ? "$ (share of spend)" : "p50 (share of typical wall time)"}</span> — color swatch = node kind. tap a row for full detail.</p>
    </div>
  );
}

// Tiny color-key showing only the kinds present in the current breakdown.
// Matches the graph-view kind palette so the user sees the same color
// for `agent` (purple) here and in the cytoscape graph.
function KindLegend({ kinds }: { kinds: Set<string> }) {
  const present = Array.from(kinds).filter(Boolean).sort();
  if (present.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 mt-3 pt-2 border-t border-border text-[10px] text-fg-dim">
      {present.map((k) => (
        <span key={k} className="inline-flex items-center gap-1">
          <span className="inline-block w-2.5 h-2.5 rounded-[1px]" style={{ backgroundColor: KIND_COLOR[k] ?? KIND_COLOR_FALLBACK }} />
          <span className="font-mono">{k}</span>
        </span>
      ))}
    </div>
  );
}

function formatDuration(ms: number | null): string {
  if (ms === null || !Number.isFinite(ms)) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m${Math.round((ms % 60_000) / 1000)}s`;
  return `${(ms / 3_600_000).toFixed(1)}h`;
}


// Generic dispatcher for pipeline-declared views. Switches on `kind`
// and delegates to a per-kind renderer. New kinds plug in here with a
// matching server-side computer in views.js.
function DeclaredViewCard({ view }: { view: DeclaredView }) {
  if (view.error) {
    return (
      <Card>
        <CardHeader><CardTitle>{view.label}</CardTitle></CardHeader>
        <p className="text-status-red text-xs font-mono">{view.error}</p>
      </Card>
    );
  }
  if (view.kind === "funnel") return <FunnelViewCard view={view as Extract<DeclaredView, { kind: "funnel" }>} />;
  return (
    <Card>
      <CardHeader><CardTitle>{view.label}</CardTitle></CardHeader>
      <p className="text-fg-dim italic text-xs font-mono">unknown view kind: {view.kind}</p>
    </Card>
  );
}

// kind: "funnel" renderer. Stages + conversions come from the server;
// the client only handles formatting (sub-1% display) and the optional
// byOutcome breakdown.
function FunnelViewCard({ view }: { view: Extract<DeclaredView, { kind: "funnel" }> }) {
  if (view.total === 0) {
    return (
      <Card>
        <CardHeader><CardTitle>{view.label}</CardTitle></CardHeader>
        <p className="text-fg-dim italic text-xs">no records in window</p>
      </Card>
    );
  }
  const max = Math.max(1, ...view.funnel.map((s) => s.count));
  const overall = view.funnel[view.funnel.length - 1]?.conversionFromStart;
  const firstLabel = view.funnel[0]?.label ?? "";
  const lastLabel = view.funnel[view.funnel.length - 1]?.label ?? "";
  // Order outcomes for the breakdown: progression states first, then
  // terminal lost/won at the end. Unknown keys (added later) trail.
  const breakdownOrder = ["pending", "viewed", "replied", "qualified", "meeting-scheduled", "won", "lost"];
  const breakdown = view.byOutcome
    ? Object.entries(view.byOutcome).sort(([a], [b]) => {
        const ia = breakdownOrder.indexOf(a);
        const ib = breakdownOrder.indexOf(b);
        return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
      })
    : [];

  return (
    <Card>
      <CardHeader>
        <CardTitle>{view.label}</CardTitle>
        <span className="text-fg-dim font-mono text-xs">{view.total} record{view.total === 1 ? "" : "s"}</span>
      </CardHeader>
      <div className="grid grid-cols-[auto_1fr_auto_auto] items-center gap-x-3 gap-y-2 font-mono text-xs">
        {view.funnel.map((s, i) => {
          const conv = s.conversionFromPrev;
          const convTxt = i === 0
            ? ""
            : conv == null
              ? "—"
              : conv === 0
                ? "0%"
                : conv < 0.01
                  ? "<1%"
                  : conv < 0.1
                    ? `${(conv * 100).toFixed(1)}%`
                    : `${(conv * 100).toFixed(0)}%`;
          const drop = conv != null && conv < 0.5;
          return (
            <Fragment key={s.id}>
              <span className="text-fg-dim">{s.label}</span>
              <span className="relative h-[18px] bg-bg-elev2 rounded-[3px] overflow-hidden">
                <span
                  className="absolute top-0 left-0 bottom-0"
                  style={{
                    width: `${(s.count / max * 100).toFixed(1)}%`,
                    background: "linear-gradient(90deg, #1f6feb, #58a6ff)",
                  }}
                />
              </span>
              <span className="text-right whitespace-nowrap tabular-nums">{s.count}</span>
              <span className={`text-right whitespace-nowrap tabular-nums ${drop ? "text-status-amber" : "text-fg-dim"}`}>{convTxt}</span>
            </Fragment>
          );
        })}
      </div>
      {breakdown.length > 0 && (
        <div className="mt-3 text-[11px] text-fg-dim font-mono">
          breakdown: {breakdown.map(([k, v], i) => (
            <span key={k}>
              {i > 0 && <span className="mx-1">·</span>}
              <span className="text-fg">{v}</span> {k}
            </span>
          ))}
        </div>
      )}
      {overall != null && (
        <div className="mt-1 text-[11px] text-fg-dim font-mono">
          overall: {(overall * 100).toFixed(1)}% ({firstLabel} → {lastLabel})
          {overall === 0 && <span className="text-status-amber"> · no progression yet</span>}
        </div>
      )}
    </Card>
  );
}
