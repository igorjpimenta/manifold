// Right-rail context panel shown when a graph node is tapped. Surfaces
// per-node visit/failure/block counts and the most recent runs that touched
// the node — clicking a row deep-links into the run detail view.

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, THead, TBody, TR, TH, TD } from "@/components/ui/table";
import { StatusBadge } from "@/components/StatusBadge";
import { api } from "@/api/client";
import type { NodeStats } from "@/api/types";

function fmtRel(ts: number | null): string {
  if (!ts) return "";
  const d = Date.now() - ts;
  if (d < 60_000) return `${Math.floor(d / 1000)}s ago`;
  if (d < 3_600_000) return `${Math.floor(d / 60_000)}m ago`;
  if (d < 86_400_000) return `${Math.floor(d / 3_600_000)}h ago`;
  return `${Math.floor(d / 86_400_000)}d ago`;
}

export function NodeContextCard({
  pipelineId,
  nodeId,
  onClose,
}: {
  pipelineId: string;
  nodeId: string;
  onClose: () => void;
}) {
  const [stats, setStats] = useState<NodeStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setStats(null);
    setError(null);
    api.pipelineNodeStats(pipelineId, nodeId)
      .then((s) => { if (!cancelled) setStats(s); })
      .catch((e) => { if (!cancelled) setError(e?.message ?? String(e)); });
    return () => { cancelled = true; };
  }, [pipelineId, nodeId]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <span className="font-mono">{nodeId}</span>
          <button
            className="ml-3 text-xs text-fg-dim hover:text-fg"
            onClick={onClose}
            aria-label="close node context"
          >
            ✕ close
          </button>
        </CardTitle>
      </CardHeader>
      {error && <p className="text-status-red text-sm">{error}</p>}
      {!error && !stats && <p className="italic text-fg-dim text-sm">loading…</p>}
      {stats && (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-3">
            <Stat label="visits"      value={stats.visits} />
            <Stat label="failed-here" value={stats.failedHere} tone={stats.failedHere ? "red" : null} />
            <Stat label="blocked-here" value={stats.blockedHere} tone={stats.blockedHere ? "amber" : null} />
            <Stat label="terminated-here" value={stats.terminatedHere} tone={stats.terminatedHere ? "amber" : null} />
          </div>
          <div className="text-xs text-fg-dim mb-1">
            visited by {stats.visits} of {stats.totalRuns} run{stats.totalRuns === 1 ? "" : "s"}
          </div>
          {stats.recentRuns.length === 0 ? (
            <p className="italic text-fg-dim text-sm">no runs have touched this node yet.</p>
          ) : (
            <>
              {/* Mobile card list (<sm). currentStep clipped off the
                  table on phones; the card surfaces the status pill +
                  relative time on the same row, with step + flow id
                  on the second row. */}
              <div className="sm:hidden flex flex-col gap-1.5">
                {stats.recentRuns.map((r) => (
                  <a
                    key={r.flowId}
                    href={`#/runs/${r.flowId}`}
                    className="block rounded border border-border bg-bg-elev2/30 hover:bg-bg-elev2 px-2.5 py-1.5"
                  >
                    <div className="flex items-center gap-2">
                      <StatusBadge status={r.status} />
                      <span className="font-mono text-xs truncate flex-1">{r.flowId.slice(0, 12)}…</span>
                      <span className="text-[10px] text-fg-dim shrink-0">{fmtRel(r.updatedAt)}</span>
                    </div>
                    {r.currentStep && (
                      <div className="mt-0.5 text-[11px] font-mono text-fg-dim">{r.currentStep}</div>
                    )}
                  </a>
                ))}
              </div>
              <div className="hidden sm:block">
                <Table>
                  <THead>
                    <TR><TH>flow</TH><TH>status</TH><TH>currentStep</TH><TH>updated</TH></TR>
                  </THead>
                  <TBody>
                    {stats.recentRuns.map((r) => (
                      <TR key={r.flowId}>
                        <TD className="font-mono text-xs">
                          <a href={`#/runs/${r.flowId}`} className="hover:underline">{r.flowId.slice(0, 12)}…</a>
                        </TD>
                        <TD><StatusBadge status={r.status} /></TD>
                        <TD className="font-mono text-xs text-fg-dim">{r.currentStep ?? ""}</TD>
                        <TD className="text-xs text-fg-dim">{fmtRel(r.updatedAt)}</TD>
                      </TR>
                    ))}
                  </TBody>
                </Table>
              </div>
            </>
          )}
        </>
      )}
    </Card>
  );
}

function Stat({ label, value, tone }: { label: string; value: number; tone?: "red" | "amber" | null }) {
  const toneClass = tone === "red" ? "text-status-red" : tone === "amber" ? "text-status-amber" : "text-fg";
  return (
    <div className="border border-border rounded p-2">
      <div className="text-xs text-fg-dim">{label}</div>
      <div className={`font-mono text-lg ${toneClass}`}>{value}</div>
    </div>
  );
}
