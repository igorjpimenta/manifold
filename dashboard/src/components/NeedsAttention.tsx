// "Needs attention" strip — a glanceable summary of runs that warrant
// operator action. Renders nothing when there's nothing to flag, so the
// strip's presence is itself the signal.
//
// Categories surfaced (cheap derivations over the home-page runs window):
//   - stuck:  status=waiting|blocked, updated > 1h ago
//   - failed: status=failed, ended/updated < 24h ago
//
// Lower-priority signals (low-success-rate pipelines, stalled runs
// without recent state change) require multi-pipeline aggregation that
// the home stats endpoint doesn't expose yet — parked for a follow-up.

import { useState } from "react";
import { Card } from "@/components/ui/card";
import { formatTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { FlowRunSummary } from "@/api/types";

const HOUR = 3_600_000;
const DAY  = 86_400_000;

interface Bucket {
  label: string;
  className: string;
  runs: FlowRunSummary[];
}

function classify(runs: FlowRunSummary[]): Bucket[] {
  const now = Date.now();
  // "waiting" is never actually written anywhere (confirmed repo-wide) —
  // gate pauses are always persisted as "blocked".
  const stuck = runs.filter((r) =>
    r.status === "blocked" &&
    r.updatedAt != null &&
    now - r.updatedAt > HOUR
  );
  const failed = runs.filter((r) =>
    r.status === "failed" &&
    r.updatedAt != null &&
    now - r.updatedAt < DAY
  );
  const buckets: Bucket[] = [];
  if (stuck.length)  buckets.push({ label: "stuck > 1h",         className: "text-status-amber", runs: stuck });
  if (failed.length) buckets.push({ label: "failed in last 24h", className: "text-status-red",   runs: failed });
  return buckets;
}

export function NeedsAttention({ runs }: { runs: FlowRunSummary[] }) {
  const [open, setOpen] = useState<string | null>(null);
  const buckets = classify(runs);
  if (buckets.length === 0) return null;

  return (
    <Card className="border-status-amber/30 bg-status-amber/[0.04]">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-status-amber font-semibold uppercase tracking-[0.5px] text-xs">needs attention</span>
        <span className="text-fg-dim text-xs">— glanceable signals; expand to see the runs</span>
      </div>
      <div className="flex flex-wrap gap-2">
        {buckets.map((b) => (
          <button
            key={b.label}
            onClick={() => setOpen(open === b.label ? null : b.label)}
            className={cn(
              "font-mono text-xs px-2.5 py-1 rounded-full border border-border bg-bg-elev hover:bg-bg-elev2 transition-colors",
              open === b.label && "bg-bg-elev2"
            )}
          >
            <span className={b.className}>{b.runs.length}</span> {b.label}
          </button>
        ))}
      </div>
      {open && (() => {
        const bucket = buckets.find((b) => b.label === open);
        if (!bucket) return null;
        return (
          <div className="mt-3 border-t border-border pt-2 grid grid-cols-1 gap-1 text-sm">
            {bucket.runs.slice(0, 8).map((r) => (
              <a
                key={r.flowId}
                href={`#/runs/${r.flowId}`}
                className="flex items-center gap-3 py-1 hover:bg-bg-elev2 rounded px-2 -mx-2 min-w-0"
              >
                <span className="font-mono text-xs text-fg-dim w-[72px] shrink-0">{r.flowId.slice(0, 8)}</span>
                <span className="font-mono flex-1 min-w-0 truncate">{r.pipelineId ?? "?"}</span>
                <span className="font-mono text-fg-dim text-xs truncate hidden sm:inline">{r.currentStep ?? "?"}</span>
                <span className="text-fg-dim text-xs whitespace-nowrap shrink-0">{formatTime(r.updatedAt)}</span>
              </a>
            ))}
            {bucket.runs.length > 8 && (
              <div className="text-fg-dim text-xs italic px-2 mt-1">…and {bucket.runs.length - 8} more — refine filters to see them all</div>
            )}
          </div>
        );
      })()}
    </Card>
  );
}
