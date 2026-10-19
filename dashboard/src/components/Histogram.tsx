import { Fragment, useState } from "react";
import type { HistogramEntry } from "@/api/types";
import { formatDuration } from "@/lib/format";
import { useIgnoredClusters, clusterKey } from "@/lib/use-ignored-clusters";

type SortKey = "impact" | "count" | "recency";

// Recency thresholds. <24h = active fire, <7d = recent simmering, older
// = historical. Drives row tone so the operator can spot fresh clusters
// without reading dates.
const DAY_MS = 86_400_000;
function recencyTone(at: number | null | undefined): { text: string; label: string } {
  if (!at) return { text: "text-fg-dim/60", label: "no data" };
  const age = Date.now() - at;
  if (age < DAY_MS) return { text: "text-status-red", label: "today" };
  if (age < 7 * DAY_MS) return { text: "text-status-amber", label: `${Math.floor(age / DAY_MS)}d ago` };
  return { text: "text-fg-dim/60", label: `${Math.floor(age / DAY_MS)}d ago` };
}

// Tiny inline sparkline reused per row. Bars proportional to the row's
// per-day failure count; flat rows render as low-contrast stubs so an
// "all on one day" cluster stands out visually.
function MiniBars({ perDay }: { perDay: { day: string; count: number }[] }) {
  if (!perDay?.length) return null;
  const max = Math.max(1, ...perDay.map((d) => d.count));
  return (
    <span className="inline-flex items-end gap-[1px] h-3 align-middle" aria-hidden>
      {perDay.map((d) => {
        const h = d.count > 0 ? Math.max(20, Math.round((d.count / max) * 100)) : 8;
        return (
          <span
            key={d.day}
            className={`w-[3px] rounded-[1px] ${d.count > 0 ? "bg-status-amber" : "bg-bg-elev2"}`}
            style={{ height: `${h}%` }}
            title={`${d.day}: ${d.count}`}
          />
        );
      })}
    </span>
  );
}

// Failure/cancel histogram — Sprint 1 of the strategic redesign.
// Each row is a CLUSTER (step + normalized error signature) rather
// than just a step name, so two failures with the same root cause
// collapse into one triage row. Row tone signals recency (today red /
// 7d amber / older dim) so the operator can spot fresh fires without
// reading dates. Sparkline shows per-day occurrence to surface
// "clustered today" vs "scattered across the month".
//
// Sort dimensions:
//   - impact (default): count × p50 block time
//   - count: pure frequency
//   - recency: most recent first
export function Histogram({ entries, limit = 8, showPipeline = false }: { entries: HistogramEntry[]; limit?: number; showPipeline?: boolean }) {
  const [sort, setSort] = useState<SortKey>("impact");
  const [expanded, setExpanded] = useState<string | null>(null);
  // Suppression: once the operator's triaged a cluster, they can hide
  // it from the active queue so fresh fires actually stand out. State
  // is localStorage-backed via useIgnoredClusters.
  const { ignored, ignore, unignore } = useIgnoredClusters();
  const [showIgnored, setShowIgnored] = useState(false);
  if (!entries?.length) return <div className="italic text-fg-dim py-4">none</div>;
  const hasImpactSignal = entries.some((e) => (e.impactMs ?? 0) > 0);
  const hasRecency = entries.some((e) => (e.lastFailedAt ?? 0) > 0);

  const sorted = (() => {
    if (sort === "count" || !hasImpactSignal) return [...entries].sort((a, b) => b.count - a.count);
    if (sort === "recency") return [...entries].sort((a, b) => (b.lastFailedAt ?? 0) - (a.lastFailedAt ?? 0));
    return entries; // backend already sorted by impact desc
  })();
  const isIgnored = (e: HistogramEntry) => clusterKey(e.pipelineId, e.key) in ignored;
  const ignoredCount = sorted.filter(isIgnored).length;
  // Default view filters out ignored clusters. The "show ignored"
  // toggle inverts: when on, only ignored rows appear so the operator
  // can unignore them. Two modes are easier to reason about than a
  // merged-but-flagged list.
  const visible = showIgnored
    ? sorted.filter(isIgnored)
    : sorted.filter((e) => !isIgnored(e));
  const top = visible.slice(0, limit);
  const metricValue = (e: HistogramEntry) => sort === "count" ? e.count : (e.impactMs ?? e.count);
  const max = Math.max(...top.map(metricValue));

  // When grouping by pipeline, collect each pipeline's rows together so
  // the umbrella truly contains its children — even when sort order
  // would otherwise interleave a third pipeline's row between two of
  // this pipeline's rows. Pipelines themselves are ordered by their
  // top entry's metric, so the highest-impact umbrella appears first.
  const grouped: { pipelineId: string | null; rows: HistogramEntry[] }[] = (() => {
    if (!showPipeline) return [{ pipelineId: null, rows: top }];
    const byPipe = new Map<string, HistogramEntry[]>();
    const order: string[] = [];
    for (const e of top) {
      const pid = e.pipelineId ?? "?";
      if (!byPipe.has(pid)) { byPipe.set(pid, []); order.push(pid); }
      byPipe.get(pid)!.push(e);
    }
    return order.map((pid) => ({ pipelineId: pid, rows: byPipe.get(pid)! }));
  })();

  return (
    <div>
      {hasImpactSignal && (
        <div className="flex items-center gap-2 mb-2">
          <div className="text-xs text-fg-dim">failure clusters — {sort === "impact" ? "by impact" : sort === "count" ? "by count" : "most recent"}</div>
          <div className="ml-auto inline-flex rounded border border-border overflow-hidden text-[10px] font-mono">
            <button
              type="button"
              onClick={() => setSort("impact")}
              className={sort === "impact" ? "px-2 py-0.5 bg-status-blue/20 text-fg" : "px-2 py-0.5 text-fg-dim hover:text-fg"}
            >impact</button>
            <button
              type="button"
              onClick={() => setSort("count")}
              className={sort === "count" ? "px-2 py-0.5 border-l border-border bg-status-blue/20 text-fg" : "px-2 py-0.5 border-l border-border text-fg-dim hover:text-fg"}
            >count</button>
            {hasRecency && (
              <button
                type="button"
                onClick={() => setSort("recency")}
                className={sort === "recency" ? "px-2 py-0.5 border-l border-border bg-status-blue/20 text-fg" : "px-2 py-0.5 border-l border-border text-fg-dim hover:text-fg"}
              >recent</button>
            )}
          </div>
        </div>
      )}
      {/*
        5 columns: step + signature | bar | sparkline | last-seen | count.
        Tap-to-expand pattern matches NodeCostBreakdown / Funnel — the
        clicked row reveals the full untruncated error message + recent
        flow ids underneath via `col-span-5`. One row open at a time.
      */}
      {grouped.map((group, gi) => (
        <div
          key={group.pipelineId ?? `g${gi}`}
          className={showPipeline ? "border border-border rounded mb-2 last:mb-0" : ""}
        >
          {/* Umbrella header: pipeline-scoped roll-up across its rows.
              Counts collapsed errors and total occurrences so the
              operator sees at the umbrella level whether this pipeline
              has one broken step or many. */}
          {showPipeline && group.pipelineId && (() => {
            const totalCount = group.rows.reduce((s, r) => s + r.count, 0);
            const distinctErrors = group.rows.reduce((s, r) => s + (r.errors?.length ?? 0), 0);
            const newestAt = Math.max(0, ...group.rows.map((r) => r.lastFailedAt ?? 0));
            const headTone = recencyTone(newestAt || null);
            return (
              <div className="flex items-center gap-2 px-2 py-1 border-b border-border/60 bg-bg-elev2/30">
                <span className="font-mono text-[11px] text-fg truncate">{group.pipelineId}</span>
                <span className={`text-[10px] ${headTone.text}`}>{headTone.label}</span>
                <span className="ml-auto text-[10px] text-fg-dim tabular-nums">
                  {group.rows.length} step{group.rows.length === 1 ? "" : "s"} ·{" "}
                  {totalCount} occurrence{totalCount === 1 ? "" : "s"}
                  {distinctErrors > 0 && <> · {distinctErrors} distinct error{distinctErrors === 1 ? "" : "s"}</>}
                </span>
              </div>
            );
          })()}
          <div className={`grid grid-cols-[auto_1fr_auto_auto_auto] items-center gap-x-2.5 gap-y-1.5 font-mono text-xs ${showPipeline ? "px-2 py-1.5" : ""}`}>
        {group.rows.map((i, idx) => {
          const tone = recencyTone(i.lastFailedAt);
          const rowKey = `${i.pipelineId ?? ""}::${i.key}::${gi}::${idx}`;
          const isOpen = expanded === rowKey;
          return (
            <Fragment key={rowKey}>
              <button
                type="button"
                onClick={() => setExpanded(isOpen ? null : rowKey)}
                aria-expanded={isOpen}
                className="contents text-left group"
              >
                {/* Collapsed view: step only (pipeline lives in the
                    header above when showPipeline=true). Per-error
                    breakdown is in the expansion. */}
                <span className="min-w-0 max-w-[260px] group-hover:bg-bg-elev2/40 rounded-l-[2px] pl-0.5" title={`${showPipeline && i.pipelineId ? `${i.pipelineId} · ` : ""}${i.key}`}>
                  <span className="flex items-center gap-1 whitespace-nowrap">
                    <span className={`truncate ${tone.text}`}>{i.key}</span>
                    <span className="text-[10px] text-fg-dim shrink-0">{isOpen ? "▾" : "▸"}</span>
                  </span>
                </span>
                <span className="relative h-3 bg-bg-elev2 rounded-[2px] overflow-hidden group-hover:bg-bg-elev2/60">
                  <span
                    className="absolute top-0 left-0 bottom-0 bg-status-amber opacity-75"
                    style={{ width: `${(metricValue(i) / Math.max(1, max) * 100).toFixed(1)}%` }}
                  />
                </span>
                <span className="px-1 group-hover:bg-bg-elev2/40">
                  {i.perDay && i.perDay.length > 0 ? <MiniBars perDay={i.perDay} /> : null}
                </span>
                <span className={`text-right whitespace-nowrap text-[10px] ${tone.text} group-hover:bg-bg-elev2/40`} title={i.lastFailedAt ? new Date(i.lastFailedAt).toISOString() : ""}>
                  {tone.label}
                </span>
                <span
                  className="text-right text-fg-dim whitespace-nowrap tabular-nums pr-0.5 group-hover:bg-bg-elev2/40 rounded-r-[2px]"
                  title={i.p50BlockMs != null ? `${i.count} × ${formatDuration(i.p50BlockMs)} median block` : `${i.count} occurrences`}
                >
                  {sort === "count"
                    ? i.count
                    : (i.p50BlockMs != null ? `${i.count}×${formatDuration(i.p50BlockMs)}` : `${i.count}`)}
                </span>
              </button>
              {isOpen && (
                <div className="col-span-5 pl-1 pr-1 pt-0.5 pb-1.5 text-[10px] text-fg-dim space-y-2">
                  {/* Per-error breakdown. Each distinct error signature
                      at this step renders as a sub-block with its own
                      count + recency + full message + recent flow ids.
                      Sorted by recency desc so the freshest bug is on
                      top. */}
                  {i.errors && i.errors.length > 0 ? i.errors.map((err, ei) => {
                    const eTone = recencyTone(err.lastFailedAt);
                    return (
                      <div key={ei} className="border-l border-border pl-2">
                        <div className="flex items-baseline gap-2 mb-0.5">
                          <span className="tabular-nums text-fg">{err.count}×</span>
                          <span className={`text-[10px] ${eTone.text}`}>{eTone.label}</span>
                          <span className="text-fg-dim truncate">{err.signature}</span>
                        </div>
                        {err.exemplarMessage && err.exemplarMessage !== err.signature && (
                          <pre className="font-mono text-[10px] whitespace-pre-wrap break-words bg-bg-elev2 border border-border rounded px-1.5 py-1 text-fg-dim max-h-40 overflow-auto">
{err.exemplarMessage}
                          </pre>
                        )}
                        {err.recentRuns && err.recentRuns.length > 0 && (
                          <div className="flex flex-wrap gap-1.5 mt-1">
                            <span className="text-fg-dim">recent:</span>
                            {err.recentRuns.map((r) => (
                              <a
                                key={r.flowId}
                                href={`#/runs/${r.flowId}`}
                                onClick={(e) => e.stopPropagation()}
                                className="font-mono text-fg-dim hover:text-fg underline-offset-2 hover:underline"
                                title={r.at ? new Date(r.at).toISOString() : ""}
                              >
                                {r.flowId.slice(0, 8)}
                              </a>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  }) : <p className="italic">no error messages recorded</p>}
                  <div className="flex items-center gap-2 pt-1 font-mono text-fg-dim/80">
                    <span>
                      {i.count} total occurrence{i.count === 1 ? "" : "s"} across {i.errors?.length ?? 0} distinct error{(i.errors?.length ?? 0) === 1 ? "" : "s"}
                      {i.p50BlockMs != null && <> · median block {formatDuration(i.p50BlockMs)}</>}
                      {i.impactMs != null && i.impactMs > 0 && <> · total impact {formatDuration(i.impactMs)}</>}
                    </span>
                    {/* Ignore / unignore action — hides this cluster
                        from the active triage queue. State is
                        persisted to localStorage, so the choice
                        survives reload. Cluster key is (pipeline,
                        step); ignoring covers all signatures within. */}
                    {(() => {
                      const ck = clusterKey(i.pipelineId, i.key);
                      const isIgn = ck in ignored;
                      return (
                        <button
                          type="button"
                          onClick={(ev) => {
                            ev.stopPropagation();
                            if (isIgn) unignore(ck); else ignore(ck);
                          }}
                          className="ml-auto text-[10px] text-fg-dim hover:text-fg underline-offset-2 hover:underline"
                          title={isIgn ? "stop ignoring — bring this cluster back into the active queue" : "ignore this cluster — hide from the active queue until unignored"}
                        >
                          {isIgn ? "unignore" : "ignore cluster"}
                        </button>
                      );
                    })()}
                  </div>
                </div>
              )}
            </Fragment>
          );
        })}
          </div>
        </div>
      ))}
      {/* Footer summary + show/hide ignored toggle. Only renders when
          there's at least one ignored cluster in the current entries. */}
      {ignoredCount > 0 && (
        <div className="mt-2 text-[10px] font-mono text-fg-dim">
          {showIgnored
            ? <>showing {ignoredCount} ignored cluster{ignoredCount === 1 ? "" : "s"} · <button type="button" onClick={() => setShowIgnored(false)} className="underline-offset-2 hover:underline hover:text-fg">back to active</button></>
            : <>{ignoredCount} cluster{ignoredCount === 1 ? "" : "s"} ignored · <button type="button" onClick={() => setShowIgnored(true)} className="underline-offset-2 hover:underline hover:text-fg">show</button></>
          }
        </div>
      )}
    </div>
  );
}
