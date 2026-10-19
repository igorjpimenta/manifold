// CancelButton — terminal-transition control for a non-terminal flow.
// Two-click affordance (click → "click again to confirm" within 4s)
// so a misclick doesn't cancel a long-running flow. The button is
// silently absent for terminal flows; the parent decides whether to
// render based on `run.status`.

import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { api } from "@/api/client";
import { cn } from "@/lib/utils";

const CONFIRM_WINDOW_MS = 4_000;

export function CancelButton({ flowId }: { flowId: string }) {
  // 'idle' → first click arms 'armed' for CONFIRM_WINDOW_MS, second click commits
  const [phase, setPhase] = useState<"idle" | "armed" | "busy">("idle");
  const [error, setError] = useState<string | null>(null);
  const disarmTimer = useRef<number | null>(null);

  useEffect(() => () => {
    if (disarmTimer.current != null) window.clearTimeout(disarmTimer.current);
  }, []);

  function arm() {
    setPhase("armed");
    setError(null);
    if (disarmTimer.current != null) window.clearTimeout(disarmTimer.current);
    disarmTimer.current = window.setTimeout(() => setPhase("idle"), CONFIRM_WINDOW_MS);
  }

  async function commit() {
    if (disarmTimer.current != null) window.clearTimeout(disarmTimer.current);
    setPhase("busy");
    try {
      await api.cancel(flowId);
      // SSE (flow_terminated) refreshes the parent; the button disappears
      // once `run.status` flips terminal.
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase("idle");
    }
  }

  return (
    <div className="flex items-center gap-2">
      {error && <span className="text-status-red text-xs">{error}</span>}
      <Button
        variant="chip"
        onClick={phase === "armed" ? commit : arm}
        disabled={phase === "busy"}
        className={cn(
          "text-status-red border-status-red/40 hover:bg-status-red/10",
          phase === "armed" && "bg-status-red/15 border-status-red/60",
          phase === "busy" && "opacity-60",
        )}
      >
        {phase === "busy" ? "cancelling…"
         : phase === "armed" ? "click again to confirm"
         : "cancel run"}
      </Button>
    </div>
  );
}
