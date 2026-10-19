// Wraps a label/text node so a click on it copies `value` to the clipboard
// instead of triggering parent handlers (e.g. a row's onClick navigation).
// Used in dense tables where an inline ⧉ icon would add clutter; detail
// pages keep the explicit CopyButton because there's no competing click.

import { useState } from "react";
import { cn } from "@/lib/utils";

interface ClickToCopyProps {
  value: string;
  /** Optional descriptor for the title attribute — defaults to "copy". */
  label?: string;
  className?: string;
  children: React.ReactNode;
}

export function ClickToCopy({ value, label = "copy", className, children }: ClickToCopyProps) {
  const [state, setState] = useState<"idle" | "ok" | "err">("idle");

  async function onClick(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
      } else {
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
    <span
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          // Cast — KeyboardEvent reuses the same handler intent (copy on
          // activate), not a real mouse-event sequence.
          onClick(e as unknown as React.MouseEvent);
        }
      }}
      title={`${label}: ${value}`}
      aria-label={`${label} ${value}`}
      className={cn(
        "cursor-copy hover:text-fg transition-colors",
        state === "ok" && "text-status-green",
        state === "err" && "text-status-red",
        className,
      )}
    >
      {children}
      {state === "ok" && <span aria-hidden className="ml-1 text-status-green">✓</span>}
    </span>
  );
}
