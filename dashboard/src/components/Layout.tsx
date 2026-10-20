import type { ReactNode } from "react";
import { useSseState } from "@/api/sse";
import { cn } from "@/lib/utils";

export function Layout({ children }: { children: ReactNode }) {
  const conn = useSseState();
  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-10 flex items-center gap-3 sm:gap-4 bg-bg-elev px-3 sm:px-5 py-[10px] border-b border-border">
        <a href="#/" className="font-mono font-semibold tracking-wide text-fg">pipeclaw</a>
        <nav className="flex gap-3 sm:gap-4 flex-1">
          <a href="#/" className="text-fg-dim py-1 hover:no-underline hover:text-fg">runs</a>
          <a href="#/pipelines" className="text-fg-dim py-1 hover:no-underline hover:text-fg">pipelines</a>
          <a href="#/search" className="text-fg-dim py-1 hover:no-underline hover:text-fg">search</a>
        </nav>
        <div
          className={cn(
            "font-mono text-xs",
            conn === "live" ? "text-status-green" : conn === "dead" ? "text-status-red" : "text-fg-dim"
          )}
        >
          {conn === "live" ? "live" : conn === "dead" ? "disconnected" : "connecting…"}
        </div>
      </header>
      <main className="max-w-[1400px] mx-auto p-3 sm:p-5">{children}</main>
    </div>
  );
}
