// Per-group view on the run-detail page, one group per kind:"subflow" node
// that produced subflows. Shows the group header (counts), aggregated
// paths summary, sub-step funnel bars, and a sorted table of subflows
// with their actual trajectories as pills. Each subflow row is clickable →
// expands inline to show per-sub-step outputs, errors, and the
// transcripts belonging to that subflow.
//
// Subflow data comes from GET /api/runs/:flowId/subflows (flow_branches,
// not stateJson — subflow data hasn't lived on stateJson since the
// branch-store migration), fetched by RunDetail and passed down.

import { Fragment, useState } from "react";
import { Card, CardHeader } from "./ui/card";
import { Badge } from "./ui/badge";
import { Table, THead, TBody, TR, TH, TD } from "./ui/table";
import { SubflowPathPills } from "./SubflowPathPills";
import { GroupPathsSummary } from "./GroupPathsSummary";
import { TranscriptRow } from "./TranscriptRow";
import { StateTree } from "./StateTree";
import { badgeVariantForSubflow, subflowStatus, inferSubStepOrder, statusRank } from "@/lib/subflow";
import { formatTime } from "@/lib/format";
import type { SubflowData, Pipeline, Transcript } from "@/api/types";

interface SubflowsCardProps {
  subflows: Record<string, Record<string, SubflowData>> | null;
  pipeline: Pipeline | null;
  /** Lifted from RunDetail; null while loading, [] when fetched. */
  transcripts: Transcript[] | null;
}

export function SubflowsCard({ subflows, pipeline, transcripts }: SubflowsCardProps) {
  const parallel = subflows;
  if (!parallel || typeof parallel !== "object" || Object.keys(parallel).length === 0) return null;

  let totalSubflows = 0;
  const groups = Object.entries(parallel);
  for (const [, b] of groups) totalSubflows += b ? Object.keys(b).length : 0;

  return (
    <Card>
      <CardHeader>
        <h3 className="text-sm font-semibold m-0">subflows</h3>
        <span className="text-fg-dim font-mono text-xs">
          {totalSubflows} total across {groups.length} group(s)
        </span>
      </CardHeader>
      {groups.map(([groupId, subflows]) => (
        <GroupSection
          key={groupId}
          groupId={groupId}
          subflows={subflows ?? {}}
          pipeline={pipeline}
          transcripts={transcripts}
        />
      ))}
    </Card>
  );
}

function GroupSection({
  groupId,
  subflows,
  pipeline,
  transcripts,
}: {
  groupId: string;
  subflows: Record<string, SubflowData>;
  pipeline: Pipeline | null;
  transcripts: Transcript[] | null;
}) {
  const [openSubflowId, setOpenSubflowId] = useState<string | null>(null);
  const [openTranscriptKey, setOpenTranscriptKey] = useState<string | null>(null);
  const entries = Object.entries(subflows);
  if (entries.length === 0) return null;
  const subflowArr = entries.map(([_, b]) => b);
  const pipelineNode = pipeline?.nodes.find((n) => n.id === groupId);
  const subSteps = inferSubStepOrder(pipelineNode, subflowArr);

  const doneCount    = entries.filter(([_, b]) => b?.step === "__end__" && !b?.failedAt).length;
  const failedCount  = entries.filter(([_, b]) => b?.failedAt).length;
  const awaitingCount = entries.filter(([_, b]) => b?.awaiting).length;
  const runningCount = entries.length - doneCount - failedCount - awaitingCount;

  type FunnelRow = { key: string; label: string; count: number };
  const reachedCounts: FunnelRow[] = subSteps.map((step) => ({
    key: step,
    label: step,
    count: entries.filter(([_, b]) => stepReached(b, step, subSteps)).length,
  }));

  const sorted = [...entries].sort(([_, a], [__, b]) => statusRank(a) - statusRank(b));

  return (
    <div className="mb-[18px]">
      <CardHeader className="mb-2">
        <h3 className="text-[13px] font-semibold font-mono m-0">{groupId}</h3>
        <span className="text-fg-dim font-mono text-xs">
          {entries.length} subflow{entries.length === 1 ? "" : "s"} · {doneCount} done · {awaitingCount} awaiting · {failedCount} failed · {runningCount} running
        </span>
      </CardHeader>

      <GroupPathsSummary entries={entries} subSteps={subSteps} />

      {/* Single grid container: per-row layout is label | bar | count. */}
      <div className="grid grid-cols-[200px_1fr_40px] gap-x-2.5 gap-y-1 font-mono text-xs mb-3">
        {reachedCounts.map((s) => {
          const barWidth = `${(s.count / Math.max(1, entries.length) * 100).toFixed(1)}%`;
          return (
            <Fragment key={s.key}>
              <span className="text-fg-dim">{s.label}</span>
              <div className="relative h-3 bg-bg-elev2 rounded-[2px] overflow-hidden">
                <div
                  className="absolute top-0 left-0 bottom-0"
                  style={{
                    width: barWidth,
                    background: "linear-gradient(90deg, #1f6feb, #58a6ff)",
                  }}
                />
              </div>
              <span className="text-right text-fg-dim">{s.count}</span>
            </Fragment>
          );
        })}
      </div>

      {/*
        Mobile-first card list (<sm). The desktop table's 6 columns
        clipped error + updated off-screen at 390px and the path pill
        column was unreadable. Card layout puts status + truncated id
        on row 1, path pills wrap to row 2, updated + error on row 3.
        The whole card is the tap-target — drops the arrow indicator.
      */}
      <div className="sm:hidden flex flex-col gap-1.5 text-xs">
        {sorted.map(([bid, b]) => {
          const status = subflowStatus(b);
          const data = b?.data ?? {};
          const errorBit = b?.failedAt
            ? (b.errorEntries?.[b.errorEntries.length - 1]?.message ?? "").slice(0, 80)
            : (data.DRAFT?._error ? `DRAFT: ${String(data.DRAFT._error).slice(0, 80)}` : "");
          const display = bid.length > 32 ? bid.slice(0, 29) + "…" : bid;
          const isOpen = openSubflowId === bid;
          return (
            <div
              key={bid}
              className="rounded border border-border bg-bg-elev2/30"
            >
              <button
                type="button"
                onClick={() => { setOpenSubflowId(isOpen ? null : bid); setOpenTranscriptKey(null); }}
                className="block w-full text-left px-2.5 py-2 hover:bg-bg-elev2"
                aria-expanded={isOpen}
              >
                <div className="flex items-center gap-2">
                  <Badge variant={badgeVariantForSubflow(status)}>{status}</Badge>
                  <span className="font-mono truncate flex-1" title={bid}>{display}</span>
                  <span className="text-[10px] text-fg-dim shrink-0">{formatTime(b?.updatedAt ?? null)}</span>
                  <span className="text-[10px] text-fg-dim">{isOpen ? "▾" : "▸"}</span>
                </div>
                <div className="mt-1.5">
                  <SubflowPathPills subflow={b} subSteps={subSteps} />
                </div>
                {errorBit && (
                  <div className="mt-1 text-fg-dim text-[11px] break-words">{errorBit}</div>
                )}
              </button>
              {isOpen && (
                <SubflowDetail
                  subflowId={bid}
                  subflow={b}
                  groupId={groupId}
                  transcripts={transcripts}
                  openTranscriptKey={openTranscriptKey}
                  onToggleTranscript={(k) => setOpenTranscriptKey(openTranscriptKey === k ? null : k)}
                />
              )}
            </div>
          );
        })}
      </div>
      <div className="hidden sm:block">
        <Table className="text-xs">
          <THead>
            <TR>
              <TH className="w-4"></TH>
              <TH>subflow</TH>
              <TH>path</TH>
              <TH>status</TH>
              <TH>updated</TH>
              <TH>error</TH>
            </TR>
          </THead>
          <TBody>
            {sorted.map(([bid, b]) => {
              const status = subflowStatus(b);
              const data = b?.data ?? {};
              const errorBit = b?.failedAt
                ? (b.errorEntries?.[b.errorEntries.length - 1]?.message ?? "").slice(0, 60)
                : (data.DRAFT?._error ? `DRAFT: ${String(data.DRAFT._error).slice(0, 60)}` : "");
              const display = bid.length > 28 ? bid.slice(0, 25) + "…" : bid;
              const isOpen = openSubflowId === bid;
              return (
                <Fragment key={bid}>
                  <TR
                    className="cursor-pointer hover:bg-bg-elev2"
                    onClick={() => { setOpenSubflowId(isOpen ? null : bid); setOpenTranscriptKey(null); }}
                  >
                    <TD className="text-fg-dim font-mono">{isOpen ? "▾" : "▸"}</TD>
                    <TD className="font-mono text-xs" title={bid}>{display}</TD>
                    <TD><SubflowPathPills subflow={b} subSteps={subSteps} /></TD>
                    <TD><Badge variant={badgeVariantForSubflow(status)}>{status}</Badge></TD>
                    <TD className="text-fg-dim font-mono">{formatTime(b?.updatedAt ?? null)}</TD>
                    <TD className="text-fg-dim text-xs">{errorBit}</TD>
                  </TR>
                  {isOpen && (
                    <TR>
                      <TD colSpan={6} className="!p-0">
                        <SubflowDetail
                          subflowId={bid}
                          subflow={b}
                          groupId={groupId}
                          transcripts={transcripts}
                          openTranscriptKey={openTranscriptKey}
                          onToggleTranscript={(k) => setOpenTranscriptKey(openTranscriptKey === k ? null : k)}
                        />
                      </TD>
                    </TR>
                  )}
                </Fragment>
              );
            })}
          </TBody>
        </Table>
      </div>
    </div>
  );
}

function SubflowDetail({
  subflowId,
  subflow,
  groupId,
  transcripts,
  openTranscriptKey,
  onToggleTranscript,
}: {
  subflowId: string;
  subflow: SubflowData | undefined;
  groupId: string;
  transcripts: Transcript[] | null;
  openTranscriptKey: string | null;
  onToggleTranscript: (sessionKey: string) => void;
}) {
  const data = subflow?.data ?? {};
  const dataEntries = Object.entries(data);
  const errors = subflow?.errorEntries ?? [];
  // Match transcripts whose itemKey is this subflow's id. The session-key
  // tail is `<nodeId>-<itemKey>`, parsed by the server; nodes inside a
  // subflow use `<parentId>.<childId>` so we also accept transcripts
  // whose nodeId starts with `${groupId.toLowerCase()}.` — belt and
  // suspenders for casing skew between source and runtime.
  const subflowTranscripts = (transcripts ?? []).filter(
    (t) => t.itemKey === subflowId,
  );

  return (
    <div className="border-t border-border bg-bg-elev2/20 px-3 py-3 grid gap-3">
      <div>
        <div className="text-fg-dim text-[11px] uppercase tracking-[0.5px] mb-1.5">sub-step outputs</div>
        {dataEntries.length === 0
          ? <div className="text-fg-dim italic text-xs">(no sub-step data yet)</div>
          : (
            <div className="grid gap-0.5">
              {dataEntries.map(([step, result]) => (
                <StateTree key={step} value={result} label={step} />
              ))}
            </div>
          )}
      </div>

      {errors.length > 0 && (
        <div>
          <div className="text-fg-dim text-[11px] uppercase tracking-[0.5px] mb-1.5">errors</div>
          <div className="grid gap-1">
            {errors.map((e, i) => (
              <pre key={i} className="text-xs text-status-red whitespace-pre-wrap break-words bg-bg-elev2/40 rounded p-2">
                {e.nodeId}: {e.message}
              </pre>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="text-fg-dim text-[11px] uppercase tracking-[0.5px] mb-1.5">
          transcripts {subflowTranscripts.length > 0 && <span className="text-fg-dim normal-case tracking-normal">· {subflowTranscripts.length} session{subflowTranscripts.length === 1 ? "" : "s"}</span>}
        </div>
        {transcripts === null
          ? <div className="text-fg-dim italic text-xs">loading…</div>
          : subflowTranscripts.length === 0
            ? <div className="text-fg-dim italic text-xs">no subagent sessions matched (itemKey={subflowId}, group={groupId})</div>
            : (
              <div className="grid gap-1.5">
                {subflowTranscripts.map((t) => (
                  <TranscriptRow
                    key={t.sessionKey}
                    transcript={t}
                    isOpen={openTranscriptKey === t.sessionKey}
                    onToggle={() => onToggleTranscript(t.sessionKey)}
                    hideItemKey
                  />
                ))}
              </div>
            )}
      </div>
    </div>
  );
}


function stepReached(b: SubflowData | undefined, target: string, allSteps: string[]): boolean {
  if (!b) return false;
  if (b.data && target in b.data) return true;
  if (b.step === "__end__") return b.data ? target in b.data : false;
  const curIdx = allSteps.indexOf(b.step ?? "");
  const tgtIdx = allSteps.indexOf(target);
  return curIdx >= 0 && tgtIdx >= 0 && curIdx >= tgtIdx;
}
