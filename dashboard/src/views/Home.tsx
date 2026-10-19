import { useEffect, useMemo, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, THead, TBody, TR, TH, TD } from "@/components/ui/table";
import { StatusBadge } from "@/components/StatusBadge";
import { StatStrip } from "@/components/StatStrip";
import { Sparkline } from "@/components/Sparkline";
import { Histogram } from "@/components/Histogram";
import { PipelineCostBreakdown } from "@/components/PipelineCostBreakdown";
import { DailyCostTrend } from "@/components/DailyCostTrend";
import { DailyTimeTrend } from "@/components/DailyTimeTrend";
import { Skeleton } from "@/components/ui/skeleton";
import { TodayStrip, estimateLast24hSpent, DAY_MS as TODAY_DAY_MS } from "@/components/TodayStrip";
import { formatDuration } from "@/lib/format";
import { SubflowSummary } from "@/components/SubflowSummary";
import { NeedsAttention } from "@/components/NeedsAttention";
import { ClickToCopy } from "@/components/ClickToCopy";
import { api } from "@/api/client";
import { useSse } from "@/api/sse";
import { formatTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { AllPipelinesCost, FlowRunSummary, GlobalStats, Pipeline } from "@/api/types";

// "waiting" is never actually written anywhere (confirmed repo-wide) — gate
// pauses are always persisted as "blocked" — so it's not offered as a filter.
const STATUS_FILTERS = ["", "running", "blocked", "succeeded", "failed", "cancelled"] as const;
const RANGES = { all: null, "1h": 3_600_000, "24h": 86_400_000, "7d": 604_800_000, "30d": 2_592_000_000 } as const;
type RangeKey = keyof typeof RANGES;

// Filter state lives in the URL hash query so a filtered view is shareable
// and survives a refresh. The route prefix is `#/` (Home); anything after
// `?` is parsed as query params.
function readHashFilters() {
  const hash = window.location.hash || "#/";
  const qIdx = hash.indexOf("?");
  const qs = qIdx >= 0 ? hash.slice(qIdx + 1) : "";
  const params = new URLSearchParams(qs);
  const range = (params.get("range") ?? "all") as RangeKey;
  return {
    status:   params.get("status")   ?? "",
    pipeline: params.get("pipeline") ?? "",
    range:    (range in RANGES) ? range : "all" as RangeKey,
    query:    params.get("q")        ?? "",
  };
}

function writeHashFilters(f: { status: string; pipeline: string; range: RangeKey; query: string }) {
  const params = new URLSearchParams();
  if (f.status)   params.set("status",   f.status);
  if (f.pipeline) params.set("pipeline", f.pipeline);
  if (f.range !== "all") params.set("range", f.range);
  if (f.query)    params.set("q",        f.query);
  const qs = params.toString();
  const next = qs ? `#/?${qs}` : "#/";
  if (next !== window.location.hash) window.history.replaceState(null, "", next);
}

export function Home() {
  const [stats, setStats] = useState<GlobalStats | null>(null);
  // Nullable until first fetch lands — lets the skeleton distinguish
  // "still loading" from "loaded, empty" (which renders the existing
  // empty-state copy instead of a placeholder).
  const [pipelines, setPipelines] = useState<Pipeline[] | null>(null);
  const [pipelinesCost, setPipelinesCost] = useState<AllPipelinesCost | null>(null);
  const [runs, setRuns] = useState<FlowRunSummary[] | null>(null);
  // Unfiltered recent window feeds NeedsAttention so the strip reflects
  // system-level signals (stuck / failed) regardless of the table filters.
  const [allRecent, setAllRecent] = useState<FlowRunSummary[] | null>(null);
  // Day pinned on the runs/day Sparkline → filters the runs table to
  // that UTC calendar day. Drop-in extension of the existing filter
  // chain; range/status/pipeline/query all still apply on top.
  const [pinnedDay, setPinnedDay] = useState<string | null>(null);
  const initial = readHashFilters();
  const [status, setStatus]     = useState<string>(initial.status);
  const [pipeline, setPipeline] = useState<string>(initial.pipeline);
  const [range, setRange]       = useState<RangeKey>(initial.range);
  const [query, setQuery]       = useState<string>(initial.query);

  const loadStats     = () => { api.stats().then(setStats); };
  const loadPipelines = () => { api.pipelines().then(setPipelines); };
  // When a day is pinned, scope the fetch to that UTC day so older
  // days (outside the default top-200-by-updatedAt window) populate.
  // Without this, pinning a 7-day-old day on the chart shows N>0 on
  // the bar but an empty table — the runs simply weren't fetched.
  const loadRuns = () => {
    const params: { status?: string; pipelineId?: string; limit?: number; since?: number; until?: number } = {
      status, pipelineId: pipeline || undefined, limit: 200,
    };
    if (pinnedDay) {
      const lo = Date.UTC(
        Number(pinnedDay.slice(0, 4)),
        Number(pinnedDay.slice(5, 7)) - 1,
        Number(pinnedDay.slice(8, 10)),
      );
      params.since = lo;
      params.until = lo + 86_400_000;
      params.limit = 1000;
    }
    api.runs(params).then(setRuns);
  };
  const loadRecent    = () => { api.runs({ limit: 200 }).then(setAllRecent); };
  // Cost roll-up walks session JSONLs per run — slow at scale, so we
  // refresh only on mount + on pipelines_changed (rare). Visiting the
  // detail page hydrates the same data fresh.
  const loadPipelinesCost = () => { api.allPipelinesCost(30).then(setPipelinesCost).catch(() => setPipelinesCost(null)); };

  useEffect(() => { loadStats(); loadPipelines(); loadRecent(); loadPipelinesCost(); }, []);
  useEffect(() => {
    writeHashFilters({ status, pipeline, range, query });
    loadRuns();
    /* eslint-disable-line react-hooks/exhaustive-deps */
  }, [status, pipeline, range, query, pinnedDay]);

  useSse((evt) => {
    if (evt.type === "flow_created" || evt.type === "flow_changed" || evt.type === "flow_terminated") {
      loadRuns();
      loadRecent();
      loadStats();
    } else if (evt.type === "pipelines_changed") {
      loadPipelines();
      loadPipelinesCost();
    }
  });

  // Date range + free-text are applied client-side over the already-fetched
  // 200-row window (status + pipeline are pushed to the server). With 200
  // rows, client-side filtering stays well under a frame.
  const visibleRuns = useMemo(() => {
    const cutoff = RANGES[range];
    const sinceMs = cutoff != null ? Date.now() - cutoff : null;
    const needle = query.trim().toLowerCase();
    // pinned-day filter is a UTC calendar bucket (matches the
    // Sparkline's day keys which are UTC YYYY-MM-DD).
    const dayLo = pinnedDay ? Date.UTC(
      Number(pinnedDay.slice(0, 4)),
      Number(pinnedDay.slice(5, 7)) - 1,
      Number(pinnedDay.slice(8, 10)),
    ) : null;
    const dayHi = dayLo != null ? dayLo + 86_400_000 : null;
    return (runs ?? []).filter((r) => {
      if (sinceMs != null && (r.updatedAt ?? 0) < sinceMs) return false;
      if (dayLo != null && dayHi != null) {
        const t = r.createdAt ?? 0;
        if (t < dayLo || t >= dayHi) return false;
      }
      if (needle) {
        const hay = [r.pipelineId, r.currentStep, r.flowId, r.statusReason, r.waitKind]
          .filter(Boolean).join(" ").toLowerCase();
        if (!hay.includes(needle)) return false;
      }
      return true;
    });
  }, [runs, range, query, pinnedDay]);

  const filtersActive = !!(status || pipeline || range !== "all" || query || pinnedDay);
  const clearAll = () => { setStatus(""); setPipeline(""); setRange("all"); setQuery(""); setPinnedDay(null); };

  return (
    <>
      <NeedsAttention runs={allRecent ?? []} />

      {(() => {
        const loading = allRecent === null || stats === null || pipelinesCost === null;
        if (loading) return <TodayStrip loading runs={{ total: 0, ok: 0, failed: 0, active: 0 }} newFailures={0} spent={{ value: 0 }} />;
        const since = Date.now() - TODAY_DAY_MS;
        const rs = allRecent!.filter((r) => (r.updatedAt ?? 0) >= since);
        return (
          <TodayStrip
            runs={{
              total: rs.length,
              ok: rs.filter((r) => r.status === "succeeded").length,
              failed: rs.filter((r) => r.status === "failed").length,
              active: rs.filter((r) => r.status === "running" || r.status === "blocked").length,
            }}
            newFailures={(stats!.failureStops ?? []).filter((c) => (c.lastFailedAt ?? 0) >= since).length}
            spent={estimateLast24hSpent(pipelinesCost!.total.perDay)}
          />
        );
      })()}

      <h2 className="text-[15px] font-semibold uppercase tracking-[0.5px] text-fg-dim mt-6 mb-2.5">overview</h2>
      {stats
        ? <StatStrip stats={stats} />
        : (
          <div className="grid gap-3 mb-4 grid-cols-[repeat(auto-fit,minmax(150px,1fr))]">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="bg-bg-elev border border-border rounded-md px-[14px] py-3">
                <Skeleton className="h-3 w-16 mb-2" />
                <Skeleton className="h-7 w-20" />
                <Skeleton className="h-2 w-24 mt-2" />
              </div>
            ))}
          </div>
        )}

      {/* Portfolio-wide cost section — mirrors PipelineDetail's cost
          card layout (tiles + by-X breakdown + daily charts), but at
          the cross-pipeline level.

          The cost API walks every session JSONL on disk, so it lands
          seconds after the other endpoints. Render a skeleton in the
          meantime so the home doesn't reflow when data arrives. */}
      {pipelinesCost === null && (
        <Card>
          <CardHeader><CardTitle>cost (last 30d)</CardTitle></CardHeader>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
            {[0, 1, 2].map((i) => (
              <div key={i} className="border border-border rounded p-2">
                <Skeleton className="h-3 w-20 mb-1.5" />
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-2 w-32 mt-2" />
              </div>
            ))}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <div className="border border-border rounded p-2">
              <Skeleton className="h-3 w-28 mb-2" />
              {[0, 1, 2, 3].map((i) => (
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
                  <Skeleton className="h-3 w-20 mb-2" />
                  <Skeleton className="h-3 w-full mb-1" />
                  <div className="flex gap-[2px] h-12">
                    {[0, 1, 2, 3, 4, 5].map((j) => (
                      <Skeleton key={j} className="flex-1 rounded-[1px]" />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}
      {pipelinesCost && pipelinesCost.total.spend > 0 && (
        <Card>
          <CardHeader><CardTitle>cost (last 30d)</CardTitle></CardHeader>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 text-sm">
            <HomeCostTile
              label="total spend"
              value={`$${pipelinesCost.total.spend.toFixed(2)}`}
              sub={`${pipelinesCost.total.runs} run${pipelinesCost.total.runs === 1 ? "" : "s"} across ${pipelinesCost.pipelines.length} pipeline${pipelinesCost.pipelines.length === 1 ? "" : "s"}`}
            />
            <HomeCostTile
              label="mean / run"
              value={pipelinesCost.total.mean != null ? `$${pipelinesCost.total.mean.toFixed(2)}` : "—"}
              sub={pipelinesCost.total.p95Cost != null ? `p95 $${pipelinesCost.total.p95Cost.toFixed(2)}` : undefined}
            />
            <HomeCostTile
              label="cost / success"
              value={pipelinesCost.total.costPerSuccess != null ? `$${pipelinesCost.total.costPerSuccess.toFixed(2)}` : "—"}
              sub={pipelinesCost.total.succeeded > 0 ? `${pipelinesCost.total.succeeded} succeeded` : "no successes yet"}
            />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <PipelineCostBreakdown entries={pipelinesCost.pipelines} pipelines={pipelines ?? []} />
            <div className="flex flex-col gap-3">
              <DailyCostTrend perDay={pipelinesCost.total.perDay} />
              <DailyTimeTrend perDay={pipelinesCost.total.perDay} />
            </div>
          </div>
        </Card>
      )}

      <div className="flex flex-col md:flex-row gap-3 md:gap-5">
        <div className="flex-1 min-w-0">
          <Card className="mb-0">
            <CardHeader><CardTitle>runs / day (last 30d)</CardTitle></CardHeader>
            {stats
              ? <Sparkline buckets={stats.perDay} pinned={pinnedDay} onPinChange={setPinnedDay} />
              : (
                <>
                  <Skeleton className="h-3 w-20 self-end mb-1 ml-auto" />
                  <div className="flex gap-[2px] h-9 mt-1.5">
                    {Array.from({ length: 14 }, (_, i) => (
                      <Skeleton key={i} className="flex-1 min-w-[4px] rounded-[1px]" />
                    ))}
                  </div>
                  <div className="flex justify-between mt-1.5">
                    <Skeleton className="h-2.5 w-16" />
                    <Skeleton className="h-2.5 w-12" />
                    <Skeleton className="h-2.5 w-16" />
                  </div>
                </>
              )}
          </Card>
        </div>
        <div className="flex-1 min-w-0">
          <Card className="mb-0">
            <CardHeader><CardTitle>top failure / cancel steps</CardTitle></CardHeader>
            {stats
              ? <Histogram entries={stats.failureStops} showPipeline />
              : (
                <div className="space-y-2">
                  {[0, 1].map((g) => (
                    <div key={g} className="border border-border rounded">
                      <div className="px-2 py-1 border-b border-border/60">
                        <Skeleton className="h-3 w-40" />
                      </div>
                      <div className="px-2 py-1.5 space-y-1.5">
                        {[0, 1].map((r) => (
                          <div key={r} className="flex items-center gap-2">
                            <Skeleton className="h-3 w-32" />
                            <Skeleton className="h-3 flex-1" />
                            <Skeleton className="h-3 w-10" />
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
          </Card>
        </div>
      </div>

      {/* Pipelines sidebar dropped — the cost-by-pipeline breakdown
          card above is a strict superset (name + cost + tone + tap-
          to-expand detail). Historical pipelines (in DB but no source
          on disk) surface there too with a dim row + badge. */}
      <div>
        <div>
          <div className="flex items-baseline justify-between mb-2.5 gap-3 flex-wrap">
            <h2 className="text-[15px] font-semibold uppercase tracking-[0.5px] text-fg-dim">runs</h2>
            <span className="font-mono text-xs text-fg-dim">
              {runs === null
                ? "…"
                : visibleRuns.length === runs.length ? `${runs.length}` : `${visibleRuns.length} / ${runs.length}`}
              {filtersActive && (
                <button onClick={clearAll} className="ml-2 underline hover:text-fg">clear filters</button>
              )}
            </span>
          </div>

          {/* Row 1: pipeline + range + search */}
          <div className="flex gap-2 mb-2 flex-wrap">
            <select
              value={pipeline}
              onChange={(e) => setPipeline(e.target.value)}
              className="bg-bg-elev border border-border rounded px-2 py-1 text-sm font-mono text-fg min-w-0"
            >
              <option value="">all pipelines</option>
              {(pipelines ?? []).map((p) => (
                <option key={p.id} value={p.id}>{p.id}{p.historical ? " (historical)" : ""}</option>
              ))}
            </select>
            <select
              value={range}
              onChange={(e) => setRange(e.target.value as RangeKey)}
              className="bg-bg-elev border border-border rounded px-2 py-1 text-sm font-mono text-fg"
            >
              {(Object.keys(RANGES) as RangeKey[]).map((r) => (
                <option key={r} value={r}>{r === "all" ? "all time" : `last ${r}`}</option>
              ))}
            </select>
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="search step / pipeline / flowId…"
              className="bg-bg-elev border border-border rounded px-2 py-1 text-sm text-fg flex-1 min-w-[160px]"
            />
          </div>

          {/* Row 2: status chips */}
          <div className="flex gap-2 flex-wrap mb-3 items-center">
            {STATUS_FILTERS.map((s) => (
              <Button
                key={s || "all"}
                variant="chip"
                onClick={() => setStatus(s)}
                className={cn(status === s && "bg-bg-elev2 text-fg border-fg-dim")}
              >
                {s || "all"}
              </Button>
            ))}
          </div>
          <Card>
            {runs === null
              ? (
                <div className="space-y-2">
                  {[0, 1, 2, 3, 4, 5].map((i) => (
                    <div key={i} className="flex items-center gap-3 py-1">
                      <Skeleton className="h-3 w-16" />
                      <Skeleton className="h-3 w-40" />
                      <Skeleton className="h-4 w-16" />
                      <Skeleton className="h-3 w-24" />
                      <Skeleton className="h-3 w-12 ml-auto" />
                    </div>
                  ))}
                </div>
              )
              : visibleRuns.length === 0
              ? <div className="text-fg-dim italic">
                  no runs{filtersActive && " matching the current filters"}
                </div>
              : (
                <>
                  {/*
                    Mobile-first card list (<sm). Drops wait + subflows
                    columns — too low-signal for the vertical space they
                    eat at 390px. Status pill leads each card so the
                    operator can scan failures/blocks at a glance.
                  */}
                  <div className="sm:hidden flex flex-col gap-2">
                    {visibleRuns.map((r) => (
                      <a
                        key={r.flowId}
                        href={`#/runs/${r.flowId}`}
                        className="block rounded border border-border bg-bg-elev2/30 hover:bg-bg-elev2 px-3 py-2"
                      >
                        <div className="flex items-center gap-2">
                          <StatusBadge status={r.status} />
                          <span className="font-mono text-sm truncate flex-1">{r.pipelineId ?? "?"}</span>
                          <span className="text-[10px] text-fg-dim shrink-0">{formatTime(r.updatedAt)}</span>
                        </div>
                        <div className="flex items-baseline gap-2 mt-0.5 text-xs text-fg-dim font-mono">
                          <span>{r.flowId.slice(0, 8)}</span>
                          {r.currentStep && <span>· {r.currentStep}</span>}
                        </div>
                      </a>
                    ))}
                  </div>
                  {/* Desktop / tablet table — unchanged. */}
                  <div className="hidden sm:block">
                    <Table>
                      <THead>
                        <TR>
                          <TH>flow</TH><TH>pipeline</TH><TH>status</TH><TH>step</TH><TH>wait</TH><TH>subflows</TH><TH>updated</TH>
                        </TR>
                      </THead>
                      <TBody>
                        {visibleRuns.map((r) => (
                          <TR key={r.flowId} className="cursor-pointer hover:bg-bg-elev2" onClick={() => { window.location.hash = `#/runs/${r.flowId}`; }}>
                            <TD className="font-mono text-xs">
                              <ClickToCopy value={r.flowId} label="copy flowId">{r.flowId.slice(0, 8)}</ClickToCopy>
                            </TD>
                            <TD className="font-mono">{r.pipelineId ?? "?"}</TD>
                            <TD><StatusBadge status={r.status} /></TD>
                            <TD className="font-mono text-fg-dim">{r.currentStep ?? "?"}</TD>
                            <TD className="font-mono text-fg-dim">{r.waitKind ?? ""}</TD>
                            <TD><SubflowSummary groups={r.subflows} /></TD>
                            <TD className="text-fg-dim">{formatTime(r.updatedAt)}</TD>
                          </TR>
                        ))}
                      </TBody>
                    </Table>
                  </div>
                </>
              )
            }
          </Card>
        </div>
      </div>
    </>
  );
}

// Cost tile for the overview's cost (last 30d) section. Same shape as
// PipelineDetail's CostTile but local to Home so the import surface
// stays small. Sub line accepts ReactNode so callers can colorize.
function HomeCostTile({ label, value, sub }: { label: string; value: string; sub?: React.ReactNode }) {
  return (
    <div className="border border-border rounded p-2">
      <div className="text-xs text-fg-dim">{label}</div>
      <div className="font-mono text-lg">{value}</div>
      {sub && <div className="text-xs text-fg-dim mt-0.5">{sub}</div>}
    </div>
  );
}

