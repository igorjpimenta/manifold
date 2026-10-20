// Last-24h activity strip — compact "is anything on fire right now?"
// glance. Used at the home overview (cross-pipeline) and at each
// pipeline-detail page (pipeline-scoped). The parent computes the
// scope-specific values; this component is pure presentation, so the
// same UI serves both surfaces.

import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

export const DAY_MS = 86_400_000;

export type Last24hRuns = { total: number; ok: number; failed: number; active: number };

export function TodayStrip({
  runs,
  newFailures,
  spent,
  loading,
  className,
}: {
  runs: Last24hRuns;
  /** Count of failure clusters whose most-recent failure landed in the window. */
  newFailures: number;
  /** { value, title? }: estimated spend in the window + an optional
   *  tooltip explaining how the estimate was derived. */
  spent: { value: number; title?: string };
  loading?: boolean;
  /** Extra classes merged onto the outer container — lets parents
   *  tweak spacing per-view (the pipeline-detail page needs a bottom
   *  margin before StatStrip; the home doesn't since its sibling h2
   *  already provides mt-6). */
  className?: string;
}) {
  if (loading) {
    return (
      <div className={cn("bg-bg-elev border border-border rounded-md px-3 py-2 mt-3 flex items-center gap-4", className)}>
        <Skeleton className="h-3 w-12" />
        <Skeleton className="h-3 w-20" />
        <Skeleton className="h-3 w-32" />
        <Skeleton className="h-3 w-24" />
      </div>
    );
  }
  return (
    <div className={cn("bg-bg-elev border border-border rounded-md px-3 py-2 mt-3 flex items-center gap-4 flex-wrap font-mono text-xs", className)}>
      <span className="text-fg-dim uppercase tracking-[0.5px] text-[11px]">last 24h</span>
      <span
        className={runs.total === 0 ? "text-fg-dim" : "text-fg"}
        title={`${runs.ok} ok · ${runs.failed} fail · ${runs.active} active`}
      >
        <span className="tabular-nums">{runs.total}</span> run{runs.total === 1 ? "" : "s"}
      </span>
      <span className={newFailures === 0 ? "text-fg-dim" : "text-status-red"}>
        <span className="tabular-nums">{newFailures}</span> new failure cluster{newFailures === 1 ? "" : "s"}
      </span>
      <span
        className={spent.value === 0 ? "text-fg-dim" : "text-fg"}
        title={spent.title}
      >
        <span className="tabular-nums">~${spent.value.toFixed(2)}</span> spent
      </span>
    </div>
  );
}

// Estimated last-24h spend from a perDay (UTC-calendar-day buckets)
// array. Today's bucket falls fully inside the window; yesterday's
// overlaps by (24 − hoursIntoUtcDay)/24, so add that proportional
// slice. Returns { value, title } shaped for the strip's spent prop.
export function estimateLast24hSpent(perDay: { day: string; cost: number }[]): { value: number; title: string } {
  const now = Date.now();
  const nowDate = new Date(now);
  const utcMidnight = Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate());
  const hoursIntoUtcDay = (now - utcMidnight) / 3_600_000;
  const yesterdayShare = Math.max(0, Math.min(1, (24 - hoursIntoUtcDay) / 24));
  const utcDayKey = (d: Date) =>
    `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
  const todayKey = utcDayKey(nowDate);
  const yesterdayKey = utcDayKey(new Date(now - DAY_MS));
  const todayCost = perDay.find((d) => d.day === todayKey)?.cost ?? 0;
  const yesterdayCost = perDay.find((d) => d.day === yesterdayKey)?.cost ?? 0;
  return {
    value: todayCost + yesterdayCost * yesterdayShare,
    title: `≈ today's UTC bucket ($${todayCost.toFixed(2)}) + ${Math.round(yesterdayShare * 100)}% of yesterday's ($${yesterdayCost.toFixed(2)})`,
  };
}
