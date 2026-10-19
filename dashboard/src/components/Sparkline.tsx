import { useMemo, useRef, useState } from "react";
import type { PerDayBucket } from "@/api/types";
import { useClickOutside } from "@/lib/use-click-outside";
import { computeAnomalies } from "@/lib/anomaly";

export function Sparkline({
  buckets,
  pinned: controlledPinned,
  onPinChange,
}: {
  buckets: PerDayBucket[];
  /** When provided, makes the component controlled — Home can react to the pinned day (e.g. filter the runs table). */
  pinned?: string | null;
  onPinChange?: (day: string | null) => void;
}) {
  // Hybrid: uncontrolled by default (own state), controlled when the
  // parent passes `pinned` + `onPinChange`. Same component works on
  // the home (filters runs table) and anywhere else standalone.
  const [internalPinned, setInternalPinned] = useState<string | null>(null);
  const pinned = controlledPinned !== undefined ? controlledPinned : internalPinned;
  const setPinned = (day: string | null) => {
    if (controlledPinned === undefined) setInternalPinned(day);
    onPinChange?.(day);
  };
  const rootRef = useRef<HTMLDivElement>(null);
  useClickOutside(rootRef, () => setPinned(null));
  const anomalies = useMemo(() => computeAnomalies(buckets ?? [], (d) => d.total), [buckets]);
  if (!buckets?.length) return <div className="italic text-fg-dim py-4">no daily data</div>;
  const max = Math.max(1, ...buckets.map((d) => d.total));
  const pinnedBucket = pinned ? buckets.find((d) => d.day === pinned) : null;
  const pinnedRatio = pinnedBucket ? anomalies.ratios.get(pinnedBucket.day) : undefined;
  return (
    <div ref={rootRef}>
      {/* Outlier chip + pinned label slot mirror DailyCostTrend's
          structure so the three daily charts read as one family.
          The CardHeader above already carries the section title
          ("runs / day"), so we don't repeat it here. */}
      {anomalies.flagged.size > 0 && (
        <div className="text-[9px] text-status-amber font-mono mb-1" title="amber bars (inner success fill) exceed the other days' mean by 1.5×+ — outliers in the window">
          ▲ {anomalies.flagged.size} outlier{anomalies.flagged.size === 1 ? "" : "s"}
        </div>
      )}
      <div className="text-right text-[10px] text-fg font-mono mb-1 min-h-[14px]">
        {pinnedBucket && (
          <span>
            {pinnedBucket.day}: {pinnedBucket.total} runs ({pinnedBucket.succeeded} ok · {pinnedBucket.failed} fail · {pinnedBucket.cancelled} canc)
            {pinnedRatio != null && <span className="text-status-amber"> · {pinnedRatio.toFixed(1)}× mean</span>}
          </span>
        )}
      </div>
      <div className="flex gap-[2px] h-12">
        {buckets.map((d) => {
          const h = Math.round((d.total / max) * 100);
          const okShare = d.total > 0 ? d.succeeded / d.total : 0;
          const isAnomaly = anomalies.flagged.has(d.day);
          const ratio = anomalies.ratios.get(d.day);
          const tip = isAnomaly && ratio != null
            ? `${d.day}: ${d.total} runs (${d.succeeded} ok) · ${ratio.toFixed(1)}× mean`
            : `${d.day}: ${d.total} runs (${d.succeeded} ok)`;
          const isPinned = pinned === d.day;
          const muted = pinned !== null && !isPinned;
          // Anomaly = amber inner fill; the outer slot stays
          // bg-elev2 since it represents total volume regardless of
          // outcome — only the success-rate fill carries the color.
          const innerColor = isAnomaly ? "bg-status-amber" : "bg-status-blue";
          return (
            <button
              type="button"
              key={d.day}
              onClick={() => setPinned(isPinned ? null : d.day)}
              className="flex-1 min-w-[6px] relative h-full"
              title={tip}
            >
              <span
                className={`absolute left-0 right-0 bottom-0 bg-bg-elev2 rounded-[1px] ${muted ? "opacity-65" : ""}`}
                style={{ height: `${h}%` }}
              >
                <span
                  className={`absolute left-0 right-0 bottom-0 rounded-[1px] ${innerColor}`}
                  style={{ height: `${Math.round(okShare * 100)}%` }}
                />
              </span>
            </button>
          );
        })}
      </div>
      <div className="flex justify-between mt-1.5 text-fg-dim font-mono text-[11px]">
        <span>{buckets[0]?.day}</span>
        <span>{buckets.length} day{buckets.length === 1 ? "" : "s"}</span>
        <span>{buckets[buckets.length - 1]?.day}</span>
      </div>
    </div>
  );
}
