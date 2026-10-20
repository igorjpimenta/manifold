// Pulsing placeholder block. Shape-mimicking skeletons across the
// dashboard compose these so the layout doesn't reflow when async
// data arrives.

import { cn } from "@/lib/utils";

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("bg-bg-elev2 rounded animate-pulse", className)} />;
}
