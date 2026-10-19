import { Badge } from "./ui/badge";
import type { RunStatus } from "@/api/types";

// "waiting" is never actually written anywhere (confirmed repo-wide) — gate
// pauses are always persisted as "blocked".
const VARIANTS: Record<string, "running" | "blocked" | "succeeded" | "failed" | "cancelled" | "default"> = {
  running:   "running",
  blocked:   "blocked",
  succeeded: "succeeded",
  failed:    "failed",
  cancelled: "cancelled",
  skipped:   "cancelled",
};

export function StatusBadge({ status }: { status: RunStatus | null | undefined }) {
  const v = (status && VARIANTS[status]) || "default";
  return <Badge variant={v}>{status ?? "?"}</Badge>;
}
