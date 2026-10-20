// One-tap clipboard button for IDs the operator routinely hands off to
// CLI commands (`openclaw pipeclaw cancel <flowId>`, etc.). Falls back
// to a non-secure copy when navigator.clipboard isn't available (rare on
// the tailnet but possible if served from a non-https origin).

import { useState } from "react";
import { cn } from "@/lib/utils";

interface CopyButtonProps {
  value: string;
  /** Optional descriptor for hover/aria — defaults to "copy". */
  label?: string;
  className?: string;
}

export function CopyButton({ value, label = "copy", className }: CopyButtonProps) {
  const [state, setState] = useState<"idle" | "ok" | "err">("idle");

  async function onClick(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
      } else {
        // Fallback: hidden textarea + document.execCommand. Required when
        // the dashboard is served from http (no Clipboard API) on a non-
        // secure origin.
        const ta = document.createElement("textarea");
        ta.value = value;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
      }
      setState("ok");
    } catch {
      setState("err");
    }
    setTimeout(() => setState("idle"), 1200);
  }

  return (
    <button
      type="button"
      onClick={onClick}
      title={`${label}: ${value}`}
      aria-label={label}
      className={cn(
        "inline-flex items-center align-middle text-fg-dim hover:text-fg leading-none px-1 py-0.5 rounded border border-transparent hover:border-border text-xs",
        state === "ok" && "text-status-green",
        state === "err" && "text-status-red",
        className,
      )}
    >
      {state === "ok" ? "✓" : state === "err" ? "✕" : "⧉"}
    </button>
  );
}
