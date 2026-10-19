// Executed-path trail under the run-detail main graph: the flow's visited
// top-level nodes (from GET /pipeclaw/runs/:flowId/tree — see RunDetail.tsx)
// rendered as pills with ❯ separators. Consecutive duplicates (a genuine
// loop-back reentry, e.g. PROPOSE → PROPOSE) get a `repeat` style so the
// operator can distinguish a single step from the same step happening twice.

import { cn } from "@/lib/utils";

export function PathTrail({ history }: { history: string[] | null | undefined }) {
  if (!history || history.length === 0) return null;
  const full = ["__start__", ...history];
  return (
    <div className="flex flex-wrap gap-1 items-center mt-2.5 pt-2.5 border-t border-dashed border-border font-mono text-[11px]">
      <span className="text-fg-dim mr-1.5">path · {history.length} step{history.length === 1 ? "" : "s"}:</span>
      {full.map((step, i) => {
        const repeat = i > 0 && full[i - 1] === step;
        return (
          <span key={i} className="flex items-center gap-1">
            <span
              className={cn(
                "px-1.5 py-0.5 rounded border text-[11px] font-mono",
                repeat
                  ? "bg-status-green/10 border-status-green/35 text-status-green"
                  : "bg-bg-elev2 text-fg border-border"
              )}
            >
              {step}
            </span>
            {i < full.length - 1 && <span className="text-fg-dim text-[10px] mx-0.5">❯</span>}
          </span>
        );
      })}
    </div>
  );
}
