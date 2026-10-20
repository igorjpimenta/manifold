// "This run vs N prior succeeded" card on run-detail.

import { Card, CardHeader, CardTitle } from "./ui/card";
import { formatDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { FlowRunFull, RunCompare } from "@/api/types";

export function RunComparison({ compare, run }: { compare: RunCompare | null; run: FlowRunFull }) {
  if (!compare || compare.peerCount === 0) return null;
  const dur = formatDuration(compare.durationMs);
  const med = formatDuration(compare.durationMedianMs);
  const ratio = compare.durationRatio;
  let deltaCls = "bg-bg-elev2 text-fg-dim";
  let deltaTxt = "≈";
  if (ratio != null) {
    if (ratio < 0.85) {
      deltaCls = "bg-status-green/15 text-status-green";
      deltaTxt = `${Math.round((1 - ratio) * 100)}% faster`;
    } else if (ratio > 1.15) {
      deltaCls = "bg-status-red/15 text-status-red";
      deltaTxt = `${Math.round((ratio - 1) * 100)}% slower`;
    } else {
      deltaTxt = "near median";
    }
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle>this run vs {compare.peerCount} prior succeeded runs of {run.pipelineId}</CardTitle>
      </CardHeader>
      <div className="grid gap-3 grid-cols-[repeat(auto-fit,minmax(150px,1fr))]">
        <Stat label="duration" value={dur}>
          <div className="text-xs text-fg-dim font-mono">
            median {med} · <span className={cn("inline-block px-2 py-px rounded-[10px] font-mono text-[11px]", deltaCls)}>{deltaTxt}</span>
          </div>
        </Stat>
        <Stat label="revisions" value={String(compare.revision)}>
          <div className="text-xs text-fg-dim font-mono">median {compare.revisionMedian ?? "?"}</div>
        </Stat>
      </div>
    </Card>
  );
}

function Stat({ label, value, children }: { label: string; value: string; children?: React.ReactNode }) {
  return (
    <div className="bg-bg-elev border border-border rounded-md px-[14px] py-3">
      <div className="text-fg-dim text-[11px] uppercase tracking-[0.5px]">{label}</div>
      <div className="font-mono text-2xl font-semibold leading-tight mt-0.5">{value}</div>
      {children}
    </div>
  );
}
