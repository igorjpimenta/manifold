// Aggregated paths summary at the top of each loop group on the
// run-detail subflows card. Groups subflows by pathSignature and shows each
// distinct path once with its count. Compact single-line form when every
// subflow took the same route.

import { SubflowPathPills } from "./SubflowPathPills";
import { pathSignature } from "@/lib/subflow";
import type { SubflowData } from "@/api/types";

export function GroupPathsSummary({
  entries,
  subSteps,
}: {
  entries: Array<[string, SubflowData]>;
  subSteps: string[];
}) {
  if (!entries.length) return null;
  const groups = new Map<string, { count: number; sample: SubflowData }>();
  for (const [, b] of entries) {
    const sig = pathSignature(b, subSteps);
    const slot = groups.get(sig);
    if (slot) slot.count++;
    else groups.set(sig, { count: 1, sample: b });
  }
  const sorted = [...groups.values()].sort((a, b) => b.count - a.count);
  if (sorted.length === 1) {
    return (
      <div className="flex items-center gap-2 mb-3 text-[11px] font-mono">
        <span className="text-fg-dim">all {sorted[0].count} took:</span>
        <SubflowPathPills subflow={sorted[0].sample} subSteps={subSteps} />
      </div>
    );
  }
  return (
    <div className="mb-3">
      <div className="text-fg-dim font-mono text-[11px] mb-1.5">{groups.size} distinct paths:</div>
      <div className="grid gap-1">
        {sorted.map((g, i) => (
          <div key={i} className="flex items-center gap-2 py-0.5">
            <span className="min-w-[30px] text-right text-fg-dim text-[11px] font-mono">{g.count}×</span>
            <SubflowPathPills subflow={g.sample} subSteps={subSteps} />
          </div>
        ))}
      </div>
    </div>
  );
}
