// RerunButton — starts a fresh run of the same pipeline with the same
// args (overridable in a future iteration). Only renders for terminal
// flows: the singleton-per-pipeline guard rejects starting while the
// source is still running, and there's no operator value in re-running
// a flow that's still in flight.
//
// On success, navigates to the new run's detail page.

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { api } from "@/api/client";
import { cn } from "@/lib/utils";

export function RerunButton({ flowId }: { flowId: string }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function click() {
    setBusy(true);
    setError(null);
    try {
      const result = await api.rerun(flowId);
      window.location.hash = `#/runs/${result.flowId}`;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  }

  return (
    <div className="flex items-center gap-2">
      {error && <span className="text-status-red text-xs">{error}</span>}
      <Button
        variant="chip"
        onClick={click}
        disabled={busy}
        className={cn(
          "text-accent border-accent/40 hover:bg-accent/10",
          busy && "opacity-60",
        )}
      >
        {busy ? "starting…" : "re-run"}
      </Button>
    </div>
  );
}
