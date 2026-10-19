// Daily wall-time trend, sibling of DailyCostTrend. Toggle picks p50
// (typical-run duration, default) vs p95 (tail-run duration). Bars
// represent the chosen percentile of run durations completing that
// day, so the operator can see at a glance whether runs are getting
// slower over time and whether the tail is drifting away from the
// median. Used by both the home overview (portfolio-wide) and the
// per-pipeline detail.

import { useMemo, useRef, useState } from "react";
import { formatDuration } from "@/lib/format";
import { useClickOutside } from "@/lib/use-click-outside";
import { computeAnomalies } from "@/lib/anomaly";

export function DailyTimeTrend({ perDay }: {
  perDay: { day: string; cost: number; runs: number; p50Ms: number | null; p95Ms: number | null }[];
}) {
  const [metric, setMetric] = useState<"p50" | "p95">("p50");
  const [pinned, setPinned] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  useClickOutside(rootRef, () => setPinned(null));
  if (!perDay?.length) return null;
  const value = (d: { p50Ms: number | null; p95Ms: number | null }): number =>
    metric === "p95" ? (d.p95Ms ?? 0) : (d.p50Ms ?? 0);
  const max = Math.max(1, ...perDay.map(value));
  const pinnedDay = pinned ? perDay.find((d) => d.day === pinned) : null;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const anomalies = useMemo(() => computeAnomalies(perDay, value), [perDay, metric]);
  const pinnedRatio = pinnedDay ? anomalies.ratios.get(pinnedDay.day) : undefined;
  return (
    <div ref={rootRef} className="border border-border rounded p-2 flex-1 flex flex-col">
      <div className="flex items-center gap-2 mb-1">
        <div className="text-xs text-fg-dim">daily {metric} run duration</div>
        {anomalies.flagged.size > 0 && (
          <span className="text-[9px] text-status-amber font-mono" title="amber bars exceed the other days' mean by 1.5×+ — outliers in the window">
            ▲ {anomalies.flagged.size}
          </span>
        )}
        <div className="ml-auto inline-flex rounded border border-border overflow-hidden text-[10px] font-mono">
          <button
            type="button"
            onClick={() => setMetric("p50")}
            className={metric === "p50" ? "px-2 py-0.5 bg-status-blue/20 text-fg" : "px-2 py-0.5 text-fg-dim hover:text-fg"}
          >p50</button>
          <button
            type="button"
            onClick={() => setMetric("p95")}
            className={metric === "p95" ? "px-2 py-0.5 border-l border-border bg-status-blue/20 text-fg" : "px-2 py-0.5 border-l border-border text-fg-dim hover:text-fg"}
          >p95</button>
        </div>
      </div>
      <div className="text-right text-[10px] text-fg font-mono mb-1 min-h-[14px]">
        {pinnedDay && (
          <span>
            {pinnedDay.day}: {metric} {formatDuration(value(pinnedDay))} · {pinnedDay.runs} run{pinnedDay.runs === 1 ? "" : "s"}
            {pinnedRatio != null && <span className="text-status-amber"> · {pinnedRatio.toFixed(1)}× mean</span>}
          </span>
        )}
      </div>
      <div className="flex gap-[2px] flex-1 min-h-12">
        {perDay.map((d) => {
          const v = value(d);
          const h = v > 0 ? Math.max(2, Math.round((v / max) * 100)) : 2;
          const isPinned = pinned === d.day;
          const muted = pinned !== null && !isPinned;
          const isAnomaly = anomalies.flagged.has(d.day);
          const ratio = anomalies.ratios.get(d.day);
          const fill = v > 0 ? (isAnomaly ? "bg-status-amber" : "bg-status-blue") : "bg-bg-elev2";
          const tip = isAnomaly && ratio != null
            ? `${d.day}: ${metric} ${formatDuration(v)} · ${d.runs} run${d.runs === 1 ? "" : "s"} · ${ratio.toFixed(1)}× mean`
            : `${d.day}: ${metric} ${formatDuration(v)} · ${d.runs} run${d.runs === 1 ? "" : "s"}`;
          return (
            <button
              type="button"
              key={d.day}
              onClick={() => setPinned(isPinned ? null : d.day)}
              className="flex-1 min-w-[6px] relative h-full"
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
      <div className="flex justify-between mt-1.5 text-fg-dim font-mono text-[11px]">
        <span>{perDay[0].day}</span>
        <span>{perDay.length} day{perDay.length === 1 ? "" : "s"}</span>
        <span>{perDay[perDay.length - 1].day}</span>
      </div>
    </div>
  );
}
