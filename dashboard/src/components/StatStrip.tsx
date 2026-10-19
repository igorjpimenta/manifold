// Overview stat strip used on the home view + pipeline-detail. Each stat card
// is a label + big mono value + sub-line; the runs card adds a stacked bar
// breaking down by status. Color helper picks green/amber/red for success-rate
// based on banding from the v0 design.

import { formatDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { DurationStats, RevisionStats } from "@/api/types";

export interface StatStripInput {
  total: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  active?: number;
  terminated: number;
  successRate: number | null;
  duration: DurationStats;
  revision?: RevisionStats;
}

function classForRate(r: number | null): string {
  if (r == null) return "";
  if (r >= 0.7) return "text-status-green";
  if (r >= 0.4) return "text-status-amber";
  return "text-status-red";
}

function widthPct(n: number, total: number): string {
  return total > 0 ? `${(n / total * 100).toFixed(1)}%` : "0%";
}

export function StatStrip({ stats, searchScope }: { stats: StatStripInput; searchScope?: string }) {
  const { total, succeeded, failed, cancelled, active = 0, terminated, successRate, duration } = stats;
  // When the caller passes a `searchScope` (PipelineDetail does — the
  // pipeline id), the runs number + each status sub-count become
  // individual links to the search page. Total → free-text q only;
  // each sub-count → q + structured status filter (exact equality
  // server-side, doesn't false-positive on the substring "failed"
  // showing up in error messages). Home view passes no scope so the
  // tile renders as plain text.
  const hrefFor = (status?: string) => {
    if (!searchScope) return null;
    const qp = new URLSearchParams({ q: searchScope });
    if (status) qp.set("status", status);
    return `#/search?${qp.toString()}`;
  };
  return (
    <div className="grid gap-3 mb-4 grid-cols-[repeat(auto-fit,minmax(150px,1fr))]">
      <StatCard
        label="runs"
        value={String(total)}
        valueHref={hrefFor()}
        valueHrefTitle="see all runs in search"
      >
        <div
          className="flex h-1.5 rounded mt-2 bg-bg-elev2 overflow-hidden"
          title={`${succeeded} succeeded · ${failed} failed · ${cancelled} cancelled · ${active} active`}
        >
          <span className="bg-status-blue" style={{ width: widthPct(succeeded, total) }} />
          <span className="bg-status-red"  style={{ width: widthPct(failed, total) }} />
          <span className="bg-status-grey" style={{ width: widthPct(cancelled, total) }} />
          <span className="bg-status-green" style={{ width: widthPct(active, total) }} />
        </div>
        <div className="text-fg-dim text-xs mt-1 font-mono">
          <StatusLink count={succeeded} label="ok"     href={hrefFor("succeeded")} />
          {" · "}
          <StatusLink count={failed}    label="fail"   href={hrefFor("failed")} />
          {" · "}
          <StatusLink count={cancelled} label="canc"   href={hrefFor("cancelled")} />
          {active > 0 && <>
            {" · "}
            <StatusLink count={active} label="active" href={hrefFor("running")} />
          </>}
        </div>
      </StatCard>
      <StatCard
        label="success rate"
        value={successRate != null ? `${(successRate * 100).toFixed(1)}%` : "—"}
        valueClassName={classForRate(successRate)}
        sub={`${succeeded} / ${terminated} terminated`}
      />
      <StatCard
        label="median duration"
        value={formatDuration(duration.p50Ms)}
        sub={`p95 ${formatDuration(duration.p95Ms)} · n=${duration.n}`}
      />
      <StatCard
        label="mean duration"
        value={formatDuration(duration.meanMs)}
        sub={`across ${duration.n} succeeded`}
      />
      {stats.revision && (
        <StatCard
          label="median revisions"
          value={stats.revision.p50 != null ? String(stats.revision.p50) : "—"}
          sub={`p95 ${stats.revision.p95 ?? "—"} · n=${stats.revision.n}`}
        />
      )}
    </div>
  );
}

function StatCard({
  label, value, sub, children, valueClassName, valueHref, valueHrefTitle,
}: {
  label: string;
  value: string;
  sub?: string;
  children?: React.ReactNode;
  valueClassName?: string;
  /** When set, the big value is wrapped in an anchor — used for drill-into-search affordance. Sub-counts handle their own linking. */
  valueHref?: string | null;
  valueHrefTitle?: string;
}) {
  const valueEl = (
    <span className={cn("font-mono text-2xl font-semibold leading-tight mt-0.5 inline-block", valueClassName)}>{value}</span>
  );
  return (
    <div className="bg-bg-elev border border-border rounded-md px-[14px] py-3">
      <div className="text-fg-dim text-[11px] uppercase tracking-[0.5px]">{label}</div>
      <div>
        {valueHref
          ? <a href={valueHref} title={valueHrefTitle} className="hover:bg-bg-elev2 rounded px-1 -mx-1 transition-colors">{valueEl}</a>
          : valueEl}
      </div>
      {children}
      {sub && <div className="text-fg-dim text-xs font-mono mt-0.5">{sub}</div>}
    </div>
  );
}

// Inline status link in the runs sub-line. Renders as plain text when
// no href (Home view, no search scope) so the layout stays identical.
function StatusLink({ count, label, href }: { count: number; label: string; href: string | null }) {
  const body = <><span className="text-fg">{count}</span> {label}</>;
  if (!href) return <span>{body}</span>;
  return (
    <a
      href={href}
      title={`see ${label === "ok" ? "succeeded" : label === "canc" ? "cancelled" : label === "active" ? "running" : label} runs in search`}
      className="hover:bg-bg-elev2 rounded px-0.5 -mx-0.5 transition-colors"
    >
      {body}
    </a>
  );
}
