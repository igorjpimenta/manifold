// Shared transcript-row + turn renderers, used by TranscriptPane (full
// list view) and SubflowsCard (per-subflow drill-down). Lifted out of
// TranscriptPane so both surfaces use identical styling and behavior.

import { formatTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { Transcript, TranscriptTurn } from "@/api/types";

interface TranscriptRowProps {
  transcript: Transcript;
  isOpen: boolean;
  onToggle: () => void;
  /** Hide the itemKey column when the parent already groups by it (e.g. subflow drill-down). */
  hideItemKey?: boolean;
}

export function TranscriptRow({ transcript, isOpen, onToggle, hideItemKey }: TranscriptRowProps) {
  const { nodeId, itemKey, turns, startedAt } = transcript;
  return (
    <div className="border border-border rounded">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-3 px-2 py-1.5 text-left hover:bg-bg-elev2 min-w-0"
      >
        <span className="text-fg-dim font-mono text-xs w-4 shrink-0">{isOpen ? "▾" : "▸"}</span>
        <span className="font-mono text-sm flex-1 min-w-0 truncate">{nodeId}</span>
        {!hideItemKey && itemKey && (
          <span className="font-mono text-fg-dim text-xs truncate hidden sm:inline">· {itemKey}</span>
        )}
        <span className="font-mono text-fg-dim text-xs whitespace-nowrap shrink-0">{turns.length} turn{turns.length === 1 ? "" : "s"}</span>
        <span className="text-fg-dim text-xs whitespace-nowrap shrink-0 hidden md:inline">{formatTime(startedAt)}</span>
      </button>
      {isOpen && (
        <div className="border-t border-border p-3 grid gap-3">
          {turns.length === 0
            ? <div className="text-fg-dim italic text-xs">(empty session)</div>
            : turns.map((turn, i) => <Turn key={i} turn={turn} />)
          }
        </div>
      )}
    </div>
  );
}

export function Turn({ turn }: { turn: TranscriptTurn }) {
  const isUser = turn.role === "user";
  return (
    <div className="grid grid-cols-[68px_1fr] gap-2 min-w-0">
      <span className={cn(
        "font-mono text-[11px] uppercase tracking-[0.5px] pt-0.5",
        isUser ? "text-accent" : "text-status-green"
      )}>
        {turn.role}
      </span>
      <div className="min-w-0">
        {turn.thinking && (
          <details className="mb-1">
            <summary className="text-fg-dim text-xs cursor-pointer hover:text-fg">thinking ({turn.thinking.length} chars)</summary>
            <pre className="text-xs text-fg-dim whitespace-pre-wrap break-words bg-bg-elev2/40 rounded p-2 mt-1 max-h-64 overflow-auto">{turn.thinking}</pre>
          </details>
        )}
        <pre className="text-sm whitespace-pre-wrap break-words font-sans">{turn.text}</pre>
        {turn.truncated && (
          <div className="text-fg-dim italic text-xs mt-1">…truncated at 8KB on the server</div>
        )}
      </div>
    </div>
  );
}
