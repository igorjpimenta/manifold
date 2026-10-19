// Per-subflow trajectory rendered as pills with ❯ separators. Same visual
// language as the top-level PathTrail. Distinguishes outcomes per sub-step.

import { Badge } from "./ui/badge";
import type { SubflowData } from "@/api/types";

export function SubflowPathPills({ subflow, subSteps }: { subflow: SubflowData | null | undefined; subSteps: string[] }) {
  if (!subflow) return <span className="text-fg-dim">—</span>;
  const pills: { label: string; variant: "succeeded" | "failed" | "cancelled" | "waiting"; suffix?: string; title?: string }[] = [];
  for (const step of subSteps) {
    const d = subflow.data?.[step];
    if (d) {
      let variant: "succeeded" | "failed" | "cancelled" = "succeeded";
      let suffix: string | undefined;
      if (d._error) variant = "failed";
      else if (step === "GATE" && d.decision && d.decision !== "approve") {
        variant = "cancelled";
        suffix = " ✗";
      } else if (step === "SUBMIT" && d.ok === false) variant = "failed";
      let title = "";
      try { title = JSON.stringify(d).slice(0, 200); } catch { /* swallow */ }
      pills.push({ label: step, variant, suffix, title });
    } else if (subflow.awaiting?.subNodeId === step) {
      pills.push({ label: step, variant: "waiting", suffix: " ⏳", title: `subflow awaiting decision at ${step}` });
    }
  }
  if (pills.length === 0) return <span className="text-fg-dim">—</span>;
  return (
    <div className="flex flex-wrap items-center gap-0.5">
      {pills.map((p, i) => (
        <span key={i} className="flex items-center gap-0.5">
          <Badge variant={p.variant} size="sm" title={p.title}>{p.label}{p.suffix ?? ""}</Badge>
          {i < pills.length - 1 && <span className="text-fg-dim text-[10px] mx-0.5">❯</span>}
        </span>
      ))}
    </div>
  );
}
