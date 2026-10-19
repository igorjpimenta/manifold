// Compact stacked-bar + total for the home runs table "subflows" column.

import type { ParallelGroupSummary } from "@/api/types";

export function SubflowSummary({ groups }: { groups: ParallelGroupSummary[] | null }) {
  if (!groups || groups.length === 0) return <span className="text-fg-dim">—</span>;
  let total = 0, done = 0, awaiting = 0, failed = 0, running = 0;
  for (const g of groups) { total += g.total; done += g.done; awaiting += g.awaiting; failed += g.failed; running += g.running; }
  const pct = (n: number) => total > 0 ? `${(n / total * 100).toFixed(1)}%` : "0%";
  const tooltip = groups.map((g) =>
    `${g.groupId}: ${g.total} · ${g.done} done · ${g.awaiting} awaiting · ${g.failed} failed · ${g.running} running`
  ).join(" | ");
  return (
    <div className="flex items-center min-w-[100px]" title={tooltip}>
      <div className="flex h-1.5 flex-1 rounded-[3px] overflow-hidden bg-bg-elev2">
        <span className="bg-status-blue"  style={{ width: pct(done) }} />
        <span className="bg-status-green" style={{ width: pct(running) }} />
        <span className="bg-status-red"   style={{ width: pct(failed) }} />
        <span className="bg-status-grey"  style={{ width: pct(awaiting) }} />
      </div>
      <span className="ml-1.5 font-mono text-[11px] text-fg-dim">{done}/{total}</span>
    </div>
  );
}
