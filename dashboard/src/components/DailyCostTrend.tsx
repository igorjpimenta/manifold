// Daily cost trend bar chart: total spend per day, normalized to the
// busiest day in the window. Renders pipeline cadence ("am I spending
// more this week?") with no external chart library. p50/run isn't
// surfaced here — typical-call cost lives in the row-expansion
// sparklines where it has per-node / per-pipeline context.
//
// Tap any bar to pin its day + value in a label strip below; tap again
// (or tap a different bar) to swap. Title-attr tooltips don't work on
// mobile, so this is the touch-friendly equivalent.

import { useMemo, useRef, useState } from "react";
import { useClickOutside } from "@/lib/use-click-outside";
import { computeAnomalies } from "@/lib/anomaly";

export function DailyCostTrend({ perDay }: {
  perDay: { day: string; cost: number; runs: number }[];
}) {
  const [pinned, setPinned] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  useClickOutside(rootRef, () => setPinned(null));
  const anomalies = useMemo(() => computeAnomalies(perDay ?? [], (d) => d.cost), [perDay]);
  if (!perDay?.length) return null;
  const max = Math.max(0.0001, ...perDay.map((d) => d.cost));
  const pinnedDay = pinned ? perDay.find((d) => d.day === pinned) : null;
  const pinnedRatio = pinnedDay ? anomalies.ratios.get(pinnedDay.day) : undefined;
  return (
    <div ref={rootRef} className="border border-border rounded p-2 flex-1 flex flex-col">
      <div className="flex items-center gap-2 mb-1">
        <div className="text-xs text-fg-dim">daily cost</div>
        {anomalies.flagged.size > 0 && (
          <span className="text-[9px] text-status-amber font-mono" title="amber bars exceed the other days' mean by 1.5×+ — outliers in the window">
            ▲ {anomalies.flagged.size} outlier{anomalies.flagged.size === 1 ? "" : "s"}
          </span>
        )}
      </div>
      <div className="text-right text-[10px] text-fg font-mono mb-1 min-h-[14px]">
        {pinnedDay && (
          <span>
            {pinnedDay.day}: ${pinnedDay.cost.toFixed(4)} · {pinnedDay.runs} run{pinnedDay.runs === 1 ? "" : "s"}
            {pinnedRatio != null && <span className="text-status-amber"> · {pinnedRatio.toFixed(1)}× mean</span>}
          </span>
        )}
      </div>
      {/*
        Each slot is a full-height button so the click target spans
        the entire column rather than just the visible bar height —
        tapping a thin bar is otherwise nearly impossible. The
        colored fill renders as an absolute child anchored to the
        bottom of the slot.
      */}
      <div className="flex gap-[2px] flex-1 min-h-12">
        {perDay.map((d) => {
          const h = d.cost > 0 ? Math.max(2, Math.round((d.cost / max) * 100)) : 2;
          const isPinned = pinned === d.day;
          const muted = pinned !== null && !isPinned;
          const isAnomaly = anomalies.flagged.has(d.day);
          const ratio = anomalies.ratios.get(d.day);
          const fill = d.cost > 0 ? (isAnomaly ? "bg-status-amber" : "bg-status-blue") : "bg-bg-elev2";
          const tip = isAnomaly && ratio != null
            ? `${d.day}: $${d.cost.toFixed(4)} · ${d.runs} run${d.runs === 1 ? "" : "s"} · ${ratio.toFixed(1)}× mean`
            : `${d.day}: $${d.cost.toFixed(4)} · ${d.runs} run${d.runs === 1 ? "" : "s"}`;
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
