// Overview-level cost breakdown: one row per pipeline, mirroring
// PipelineDetail's NodeCostBreakdown structure (single grid +
// display:contents + tap-to-expand). Answers "which pipeline is
// eating my budget" at the home view, the same way NodeCostBreakdown
// answers "which step is eating my pipeline's budget."
//
// Mode toggle: $ (total spend) / time (typical-run latency p50).
// Expansion: per-pipeline details + trend sparkline reusing the
// daily-cost lens with a total / p50 sub-toggle in $ mode (same UX
// as the per-node row expansions).

import { Fragment, useEffect, useRef, useState } from "react";
import type { AllPipelinesCostEntry, Pipeline } from "@/api/types";
import { formatDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useClickOutside } from "@/lib/use-click-outside";
import { computeAnomalies } from "@/lib/anomaly";

const HISTORICAL_PREF_KEY = "gf:cost-by-pipeline:show-historical";

export function PipelineCostBreakdown({
  entries,
  pipelines,
}: {
  entries: AllPipelinesCostEntry[];
  /** Pipeline defs from /api/pipelines — used to mark historical pipelines and look up display metadata. */
  pipelines: Pipeline[];
}) {
  const [mode, setMode] = useState<"cost" | "time">("cost");
  const [expanded, setExpanded] = useState<string | null>(null);
  // Historical pipelines (source file removed but runs still in the
  // DB) are noise in the everyday view — they hold a row but never
  // contribute current-window spend. Default to hidden; persist the
  // operator's choice so audit sessions don't re-toggle every reload.
  const [showHistorical, setShowHistorical] = useState<boolean>(() => {
    if (typeof localStorage === "undefined") return false;
    return localStorage.getItem(HISTORICAL_PREF_KEY) === "1";
  });
  useEffect(() => {
    if (typeof localStorage === "undefined") return;
    localStorage.setItem(HISTORICAL_PREF_KEY, showHistorical ? "1" : "0");
  }, [showHistorical]);

  const pipelineById = new Map(pipelines.map((p) => [p.id, p]));
  const everything = entries.filter((e) => {
    if (pipelineById.get(e.pipelineId)?.historical) return true;
    return e.total > 0 || (e.p50Ms ?? 0) > 0;
  });
  const historicalCount = everything.filter((e) => pipelineById.get(e.pipelineId)?.historical).length;
  const visible = showHistorical
    ? everything
    : everything.filter((e) => !pipelineById.get(e.pipelineId)?.historical);
  if (visible.length === 0 && historicalCount === 0) {
    return <div className="italic text-fg-dim py-2 text-xs">no pipelines in window</div>;
  }

  // $-mode = total spend, time-mode = typical-run p50 latency. No
  // total/p50 sub-toggle: typical-call cost lives in the per-pipeline
  // detail page; here the operator wants the cleaner spend ranking.
  const totalSpend = visible.reduce((s, e) => s + e.total, 0);
  const max = mode === "cost"
    ? Math.max(...visible.map((e) => e.total))
    : Math.max(...visible.map((e) => e.p50Ms ?? 0));
  const totalTime = visible.reduce((s, e) => s + (e.p50Ms ?? 0) * e.runs, 0);

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <div className="text-xs text-fg-dim">by pipeline — {mode === "cost" ? "spend" : "typical-run latency"}</div>
        <div className="ml-auto inline-flex items-center gap-2">
          {historicalCount > 0 && (
            <button
              type="button"
              onClick={() => setShowHistorical((v) => !v)}
              className="text-[10px] font-mono text-fg-dim hover:text-fg underline-offset-2 hover:underline"
              title={showHistorical
                ? `hide historical pipelines (source removed but runs in DB) — ${historicalCount} hidden`
                : `show ${historicalCount} historical pipeline${historicalCount === 1 ? "" : "s"} — source file removed, kept for run-history context`
              }
            >
              {showHistorical ? `hide historical (${historicalCount})` : `+${historicalCount} historical`}
            </button>
          )}
          <div className="inline-flex rounded border border-border overflow-hidden text-[10px] font-mono">
            <button
              onClick={() => setMode("cost")}
              className={cn("px-2 py-0.5", mode === "cost" ? "bg-status-blue/20 text-fg" : "text-fg-dim hover:text-fg")}
            >$</button>
            <button
              onClick={() => setMode("time")}
              className={cn("px-2 py-0.5 border-l border-border", mode === "time" ? "bg-status-blue/20 text-fg" : "text-fg-dim hover:text-fg")}
            >time</button>
          </div>
        </div>
      </div>
      {/*
        Same display:contents grid pattern as NodeCostBreakdown. 4
        columns: name | bar | primary | share. Expansion uses col-span-4.
      */}
      <div className="grid grid-cols-[auto_1fr_auto_auto] items-center gap-x-2 gap-y-1">
        {visible.map((e) => {
          const isOpen = expanded === e.pipelineId;
          const p50 = formatDuration(e.p50Ms);
          const p95 = formatDuration(e.p95Ms);
          const barValue = mode === "cost" ? e.total : (e.p50Ms ?? 0);
          const barPct = max > 0 ? Math.round((barValue / max) * 100) : 0;
          const share = mode === "cost"
            ? (totalSpend > 0 ? (e.total / totalSpend) * 100 : 0)
            : (totalTime > 0 ? ((e.p50Ms ?? 0) * e.runs / totalTime) * 100 : 0);
          const primaryCompact = mode === "cost" ? `$${e.total.toFixed(2)}` : p50;
          const isHistorical = pipelineById.get(e.pipelineId)?.historical === true;
          return (
            <Fragment key={e.pipelineId}>
              <button
                type="button"
                onClick={() => setExpanded(isOpen ? null : e.pipelineId)}
                aria-expanded={isOpen}
                className="contents text-left group"
                title={`${e.pipelineId} · ${e.runs} run${e.runs === 1 ? "" : "s"} · $${e.total.toFixed(4)} · p50 ${p50} · p95 ${p95}`}
              >
                <span className={cn("flex items-center gap-1.5 min-w-0 text-xs group-hover:bg-bg-elev2/40 rounded-l-[2px] pl-0.5", isHistorical && "opacity-65 italic")}>
                  <a
                    href={`#/pipelines/${encodeURIComponent(e.pipelineId)}`}
                    onClick={(ev) => ev.stopPropagation()}
                    className="font-mono truncate hover:underline underline-offset-2"
                  >
                    {e.pipelineId}
                  </a>
                  <span className="text-[10px] text-fg-dim shrink-0">{isOpen ? "▾" : "▸"}</span>
                  {isHistorical && (
                    <span
                      className="bg-bg-elev2 text-fg-dim border border-border rounded-[10px] px-1.5 py-px font-mono text-[9px] shrink-0"
                      title="pipeline source no longer on disk — kept for historical run context"
                    >
                      historical
                    </span>
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
              {isOpen && (
                <div className="col-span-4 pl-5 pr-1 pt-0.5 pb-1 text-[10px] font-mono text-fg-dim grid grid-cols-2 sm:grid-cols-4 gap-x-3 gap-y-0.5">
                  <span><span className="text-fg-dim">runs:</span> {e.runs}</span>
                  <span><span className="text-fg-dim">succeeded:</span> {e.succeeded}</span>
                  <span>
                    <span className="text-fg-dim">$ / ok:</span>{" "}
                    {e.costPerSuccess != null ? `$${e.costPerSuccess.toFixed(2)}` : "—"}
                  </span>
                  <span><span className="text-fg-dim">$ total:</span> ${e.total.toFixed(4)}</span>
                  <span><span className="text-fg-dim">p50 time:</span> {p50}</span>
                  <span><span className="text-fg-dim">p95 time:</span> {p95}</span>
                  <span><span className="text-fg-dim">p50 $/run:</span> {e.p50Cost != null ? `$${e.p50Cost.toFixed(4)}` : "—"}</span>
                  <span><span className="text-fg-dim">p95 $/run:</span> {e.p95Cost != null ? `$${e.p95Cost.toFixed(4)}` : "—"}</span>
                  {/* Trend sparkline — $-mode is total daily spend,
                      time-mode is daily p50 latency. No total/p50
                      sub-toggle at this surface. */}
                  {(mode === "time" || e.total > 0) && (
                    <PipelineTrendSparkline perDay={e.perDay} mode={mode} />
                  )}
                </div>
              )}
            </Fragment>
          );
        })}
      </div>
      <p className="text-[10px] text-fg-dim mt-1">
        columns: <span className="font-mono">pipeline · bar · {mode === "cost" ? "$ (share of spend)" : "p50 (share of typical wall time)"}</span> — tap a row for full detail.
      </p>
    </div>
  );
}

type DayPoint = { day: string; cost: number; runs: number; ms: number; p50Cost: number | null; p50Ms: number | null };

function PipelineTrendSparkline({ perDay, mode }: {
  perDay: DayPoint[];
  mode: "cost" | "time";
}) {
  // Local total/p50 toggle for $-mode — matches the per-node row
  // expansion's deepest-surface toggle. p50 here = median per-run cost
  // that day, so the operator can see "is the typical run getting more
  // expensive" even when daily totals look flat.
  const [costMetric, setCostMetric] = useState<"total" | "p50">("total");
  // Tap-to-pin day label (no native hover on mobile).
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
          ? `${d.day}: p50 $${(d.p50Cost ?? 0).toFixed(4)} · ${d.runs} run${d.runs === 1 ? "" : "s"}`
          : `${d.day}: total $${d.cost.toFixed(4)} · ${d.runs} run${d.runs === 1 ? "" : "s"}`)
      : `${d.day}: p50 ${formatDuration(d.p50Ms ?? 0)} · ${d.runs} run${d.runs === 1 ? "" : "s"}`;
  const subtitle = mode === "cost"
    ? `daily ${costMetric === "p50" ? "p50 $/run" : "$ total"}`
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
