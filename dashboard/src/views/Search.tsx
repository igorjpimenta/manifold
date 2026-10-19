// Deep search across runs — covers structured fields, state.errors[],
// and top-level state JSON keys. See dashboard/search.js for the
// indexed surface. Query lives in the URL hash (`#/search?q=...`) so
// results are sharable and survive refresh.

import { useEffect, useMemo, useRef, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { StatusBadge } from "@/components/StatusBadge";
import { ClickToCopy } from "@/components/ClickToCopy";
import { api } from "@/api/client";
import type { SearchResponse } from "@/api/types";

function fmtRel(ts: number | null): string {
  if (!ts) return "";
  const d = Date.now() - ts;
  if (d < 60_000) return `${Math.floor(d / 1000)}s ago`;
  if (d < 3_600_000) return `${Math.floor(d / 60_000)}m ago`;
  if (d < 86_400_000) return `${Math.floor(d / 3_600_000)}h ago`;
  return `${Math.floor(d / 86_400_000)}d ago`;
}

function readHashState(): { q: string; status: string } {
  const hash = window.location.hash || "";
  const i = hash.indexOf("?");
  if (i < 0) return { q: "", status: "" };
  const params = new URLSearchParams(hash.slice(i + 1));
  return { q: params.get("q") ?? "", status: params.get("status") ?? "" };
}

function writeHashState({ q, status }: { q: string; status: string }) {
  const qp = new URLSearchParams();
  if (q) qp.set("q", q);
  if (status) qp.set("status", status);
  const next = qp.toString() ? `#/search?${qp.toString()}` : "#/search";
  if (window.location.hash !== next) window.history.replaceState(null, "", next);
}

export function Search() {
  const initial = readHashState();
  const [draft, setDraft] = useState<string>(initial.q);
  const [committed, setCommitted] = useState<string>(initial.q);
  // Structured status filter — applied server-side, exact equality.
  // Reads from `?status=…` so deep links from elsewhere (PipelineDetail's
  // RUNS sub-counts) hydrate the filter on first load.
  const [status, setStatus] = useState<string>(initial.status);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Debounce keystrokes → committed query → fetch. 200ms feels live but
  // doesn't fire on every character; matches the home-filter cadence.
  useEffect(() => {
    const t = setTimeout(() => setCommitted(draft.trim()), 200);
    return () => clearTimeout(t);
  }, [draft]);

  useEffect(() => {
    writeHashState({ q: committed, status });
    if (!committed && !status) { setResult(null); setError(null); return; }
    setLoading(true);
    setError(null);
    let cancelled = false;
    api.search(committed, { limit: 100, status })
      .then((r) => { if (!cancelled) setResult(r); })
      .catch((e) => { if (!cancelled) setError(e?.message ?? String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [committed, status]);

  // Focus on mount — primary verb for this page is typing a query.
  useEffect(() => { inputRef.current?.focus(); }, []);

  return (
    <>
      <h1 className="font-mono mt-2 text-lg font-semibold">search</h1>
      <Card>
        <input
          ref={inputRef}
          type="search"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="search errors, state, IDs… (e.g. 'shortlist budget' — all tokens must match)"
          className="w-full bg-bg border border-border rounded px-3 py-2 text-sm font-mono outline-none focus:border-accent"
          autoComplete="off"
        />
        {status && (
          <div className="mt-2 flex items-center gap-2 text-xs font-mono">
            <span className="text-fg-dim">status:</span>
            <span className="inline-flex items-center gap-1.5 bg-bg-elev2 text-fg border border-border rounded-[10px] px-2 py-0.5">
              {status}
              <button
                type="button"
                onClick={() => setStatus("")}
                className="text-fg-dim hover:text-fg"
                aria-label="clear status filter"
                title="clear status filter"
              >
                ×
              </button>
            </span>
          </div>
        )}
        <SearchHelp />
      </Card>

      {error && <p className="text-status-red text-sm">{error}</p>}

      {!committed && !status && !error && (
        <p className="italic text-fg-dim text-sm">enter a query to begin.</p>
      )}

      {(committed || status) && loading && !result && (
        <div className="space-y-2 mt-2">
          {[0, 1, 2, 3].map((i) => (
            <Card key={i} className="mb-0">
              <Skeleton className="h-3 w-40 mb-2" />
              <Skeleton className="h-3 w-full mb-1" />
              <Skeleton className="h-3 w-2/3" />
            </Card>
          ))}
        </div>
      )}

      {result && <Results result={result} />}
    </>
  );
}

function SearchHelp() {
  return (
    <p className="text-xs text-fg-dim mt-2">
      Tokens must all appear (AND). Searched: flowId, pipelineId, status, currentStep, statusReason, goal,
      waitJson hint/reason, state.errors[*], and top-level state.* values. Not searched (yet): agent transcripts.
    </p>
  );
}

function Results({ result }: { result: SearchResponse }) {
  const summary = useMemo(() => {
    if (result.totalHits === 0) return "no matches.";
    const shown = result.hits.length;
    const total = result.totalHits;
    if (result.truncated) return `${shown} of ${total} matches (showing most-recent ${shown}). refine to narrow.`;
    return `${total} match${total === 1 ? "" : "es"}.`;
  }, [result]);

  return (
    <Card>
      <CardHeader><CardTitle>results — {summary}</CardTitle></CardHeader>
      {result.hits.length === 0 ? (
        <p className="italic text-fg-dim text-sm">try a shorter query, drop a token, or check for typos.</p>
      ) : (
        <ul className="flex flex-col gap-3">
          {result.hits.map((h) => (
            <li
              key={h.flowId}
              className="border border-border rounded p-2 cursor-pointer hover:bg-bg-elev2"
              onClick={() => { window.location.hash = `#/runs/${h.flowId}`; }}
            >
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm">
                <ClickToCopy value={h.flowId} label="copy flowId" className="font-mono text-fg">
                  {h.flowId.slice(0, 12)}…
                </ClickToCopy>
                <StatusBadge status={h.status} />
                {h.pipelineId && (
                  <a
                    href={`#/pipelines/${encodeURIComponent(h.pipelineId)}`}
                    onClick={(e) => e.stopPropagation()}
                    className="font-mono text-xs text-fg-dim hover:text-fg hover:underline"
                  >
                    {h.pipelineId}
                  </a>
                )}
                {h.currentStep && <span className="font-mono text-xs text-fg-dim">@ {h.currentStep}</span>}
                <span className="text-xs text-fg-dim ml-auto">{fmtRel(h.updatedAt)}</span>
              </div>
              <ul className="mt-2 flex flex-col gap-1">
                {h.matches.slice(0, 5).map((m, i) => (
                  <li key={i} className="font-mono text-xs leading-snug">
                    <span className="text-fg-dim">{m.field}:</span>{" "}
                    <span className="text-fg">{m.snippet}</span>
                  </li>
                ))}
                {h.matches.length > 5 && (
                  <li className="text-xs text-fg-dim italic">… {h.matches.length - 5} more match{h.matches.length - 5 === 1 ? "" : "es"} in this run.</li>
                )}
              </ul>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
