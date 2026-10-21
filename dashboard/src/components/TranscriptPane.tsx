// TranscriptPane — chat-style timeline of the subagent sessions a flow
// spawned. Reads parsed messages from `/api/runs/:flowId/transcripts`.
//
// Each transcript is one (agent, node, item) trio — corresponds to one
// session JSONL file under `~/.openclaw/agents/<agent>/sessions/`. The
// pane groups them by agent, lists them collapsed, expands to a
// user→assistant chat view on click. Thinking content (when present)
// is rendered subdued in a separate block from the assistant's reply.

import { useEffect, useMemo, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TranscriptRow } from "@/components/TranscriptRow";
import { api } from "@/api/client";
import type { Transcript } from "@/api/types";

interface TranscriptPaneProps {
  flowId: string;
  /** If parent passes transcripts (lifted-state pattern), skip internal fetch. */
  transcripts?: Transcript[] | null;
}

export function TranscriptPane({ flowId, transcripts: externalTranscripts }: TranscriptPaneProps) {
  const [internalTranscripts, setInternalTranscripts] = useState<Transcript[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openKey, setOpenKey] = useState<string | null>(null);
  const useExternal = externalTranscripts !== undefined;
  const transcripts = useExternal ? externalTranscripts : internalTranscripts;

  useEffect(() => {
    if (useExternal) return; // parent provides data; no fetch
    let cancelled = false;
    setInternalTranscripts(null);
    setError(null);
    api.runTranscripts(flowId)
      .then((ts) => { if (!cancelled) setInternalTranscripts(ts); })
      .catch((e) => { if (!cancelled) setError(e?.message ?? String(e)); });
    return () => { cancelled = true; };
  }, [flowId, useExternal]);

  const byAgent = useMemo(() => {
    if (!transcripts) return new Map<string, Transcript[]>();
    const m = new Map<string, Transcript[]>();
    for (const t of transcripts) {
      const arr = m.get(t.agent) ?? [];
      arr.push(t);
      m.set(t.agent, arr);
    }
    return m;
  }, [transcripts]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>transcripts</CardTitle>
        {transcripts && (
          <span className="font-mono text-xs text-fg-dim">
            {transcripts.length === 0
              ? "no subagent sessions for this flow"
              : `${transcripts.length} session${transcripts.length === 1 ? "" : "s"} across ${byAgent.size} agent${byAgent.size === 1 ? "" : "s"}`}
          </span>
        )}
      </CardHeader>

      {error && <div className="text-status-red text-sm">{error}</div>}
      {!transcripts && !error && <div className="text-fg-dim italic text-sm">loading…</div>}
      {transcripts && transcripts.length === 0 && (
        <div className="text-fg-dim italic text-sm">
          This flow didn't spawn any subagent sessions, or the session keys don't
          match the manifold default pattern.
        </div>
      )}

      {transcripts && transcripts.length > 0 && (
        <div className="grid gap-3">
          {[...byAgent.entries()].map(([agent, list]) => (
            <div key={agent} className="border border-border rounded p-2 bg-bg-elev2/30">
              <div className="flex items-center gap-2 mb-2 flex-wrap">
                <Badge variant="kind">{agent}</Badge>
                <span className="text-fg-dim text-xs font-mono">{list.length} session{list.length === 1 ? "" : "s"}</span>
              </div>
              <div className="grid grid-cols-1 gap-1">
                {list.map((t) => (
                  <TranscriptRow
                    key={t.sessionKey}
                    transcript={t}
                    isOpen={openKey === t.sessionKey}
                    onToggle={() => setOpenKey(openKey === t.sessionKey ? null : t.sessionKey)}
                  />
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

