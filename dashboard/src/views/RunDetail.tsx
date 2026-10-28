import { Fragment, useEffect, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { GraphView } from "@/components/GraphView";
import { PathTrail } from "@/components/PathTrail";
import { SubflowsCard } from "@/components/SubflowsCard";
import { RunComparison } from "@/components/RunComparison";
import { TranscriptPane } from "@/components/TranscriptPane";
import { GateForm } from "@/components/GateForm";
import { CancelButton } from "@/components/CancelButton";
import { RerunButton } from "@/components/RerunButton";
import { StatusBadge } from "@/components/StatusBadge";
import { StateTree } from "@/components/StateTree";
import { CopyButton } from "@/components/CopyButton";
import { Skeleton } from "@/components/ui/skeleton";
import { api, artifactUrl } from "@/api/client";
import { useWs } from "@/api/ws";
import { formatBytes, formatTime } from "@/lib/format";
import type { ArtifactEntry, SubflowData, FlowRunFull, FlowTreeItem, Pipeline, RunCompare, RunCost, LiveEvent, Transcript } from "@/api/types";

export function RunDetail({ flowId }: { flowId: string }) {
  const [run, setRun] = useState<FlowRunFull | null>(null);
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactEntry[]>([]);
  const [transcripts, setTranscripts] = useState<Transcript[] | null>(null);
  const [compare, setCompare] = useState<RunCompare | null>(null);
  const [cost, setCost] = useState<RunCost | null>(null);
  const [subflows, setSubflows] = useState<Record<string, Record<string, SubflowData>> | null>(null);
  const [tree, setTree] = useState<FlowTreeItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadAll = async () => {
    try {
      const r = await api.run(flowId);
      setRun(r);
      setError(null);
      if (r.pipelineId && (!pipeline || pipeline.id !== r.pipelineId)) {
        api.pipeline(r.pipelineId).then(setPipeline).catch(() => setPipeline(null));
      }
      api.runEvents(flowId).then((evs) => setEvents(evs.slice().reverse())).catch(() => setEvents([]));
      api.runArtifacts(flowId).then(setArtifacts).catch(() => setArtifacts([]));
      api.runTranscripts(flowId).then(setTranscripts).catch(() => setTranscripts([]));
      api.runCompare(flowId).then(setCompare).catch(() => setCompare(null));
      api.runCost(flowId).then(setCost).catch(() => setCost(null));
      api.runSubflows(flowId).then(setSubflows).catch(() => setSubflows(null));
      api.runTree(flowId).then((r) => setTree(r.items)).catch(() => setTree(null));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => { loadAll(); /* eslint-disable-line react-hooks/exhaustive-deps */ }, [flowId]);

  useWs((evt) => {
    if (!("flowId" in evt) || evt.flowId !== flowId) return;
    setEvents((prev) => [evt, ...prev]);
    if (evt.kind !== "run.created") loadAll();
  });

  if (error) {
    return (
      <>
        <a href="#/" className="text-fg-dim">← runs</a>
        <p className="text-status-red mt-2">{error}</p>
      </>
    );
  }
  if (!run) {
    return (
      <>
        <a href="#/" className="text-fg-dim">← runs</a>
        <Skeleton className="h-6 w-80 mt-2 mb-4" />
        <Card>
          <div className="grid grid-cols-[100px_1fr] sm:grid-cols-[140px_1fr] gap-y-2 gap-x-3">
            {[0, 1, 2, 3, 4].map((i) => (
              <Fragment key={i}>
                <Skeleton className="h-3 w-20" />
                <Skeleton className="h-3 w-3/4" />
              </Fragment>
            ))}
          </div>
        </Card>
        <Card><Skeleton className="h-4 w-32 mb-3" />{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-3 w-full mb-2" />)}</Card>
        <Card><Skeleton className="h-4 w-40 mb-3" /><Skeleton className="h-40 w-full" /></Card>
      </>
    );
  }

  return (
    <>
      <a href="#/" className="text-fg-dim">← runs</a>
      <div className="flex items-center justify-between gap-3 mt-2 mb-4 flex-wrap">
        <h1 className="font-mono text-lg font-semibold break-all min-w-0">
          {flowId}
          <CopyButton value={flowId} label="copy flowId" className="ml-2" />
        </h1>
        {(run.status === "running" || run.status === "blocked") && (
          <CancelButton flowId={flowId} />
        )}
        {(run.status === "succeeded" || run.status === "failed" || run.status === "cancelled") && (
          <RerunButton flowId={flowId} />
        )}
      </div>

      <Card>
        <dl className="grid grid-cols-[100px_1fr] sm:grid-cols-[140px_1fr] gap-y-1 gap-x-3 text-sm">
          <dt className="text-fg-dim">pipeline</dt>
          <dd>
            <a href={`#/pipelines/${encodeURIComponent(run.pipelineId ?? "")}`} className="font-mono">{run.pipelineId ?? "?"}</a>
            {run.pipelineId && <CopyButton value={run.pipelineId} label="copy pipelineId" className="ml-2" />}
          </dd>
          <dt className="text-fg-dim">status</dt><dd><StatusBadge status={run.status} /></dd>
          <dt className="text-fg-dim">current step</dt><dd className="font-mono">{run.currentStep ?? "?"}</dd>
          <dt className="text-fg-dim">revision</dt><dd className="font-mono text-fg-dim">{run.revision}</dd>
          <dt className="text-fg-dim">runId</dt>
          <dd className="font-mono text-fg-dim text-xs break-all">
            {run.runId ?? "?"}
            {run.runId && <CopyButton value={run.runId} label="copy runId" className="ml-2" />}
          </dd>
          {cost && cost.total > 0 && (
            <>
              <dt className="text-fg-dim">cost</dt>
              <dd className="font-mono" title={`models: ${Object.keys(cost.byModel).join(", ")} · sessions: ${cost.bySession.length}`}>
                ${cost.total.toFixed(4)}
                <span className="text-fg-dim text-xs ml-2">
                  {cost.bySession.length} session{cost.bySession.length === 1 ? "" : "s"} · {(cost.tokens.input + cost.tokens.output + cost.tokens.cacheRead).toLocaleString()} tokens
                </span>
              </dd>
            </>
          )}
          <dt className="text-fg-dim">created</dt><dd className="text-fg-dim">{formatTime(run.createdAt)}</dd>
          <dt className="text-fg-dim">updated</dt><dd className="text-fg-dim">{formatTime(run.updatedAt)}</dd>
          {run.endedAt && <><dt className="text-fg-dim">ended</dt><dd className="text-fg-dim">{formatTime(run.endedAt)}</dd></>}
        </dl>
      </Card>

      <RunComparison compare={compare} run={run} />

      {run.waitJson && (
        <Card>
          <CardHeader><CardTitle>waiting on {run.waitJson.kind ?? "?"}</CardTitle></CardHeader>
          <dl className="grid grid-cols-[100px_1fr] sm:grid-cols-[140px_1fr] gap-y-1 gap-x-3 text-sm">
            <dt className="text-fg-dim">node</dt><dd className="font-mono">{run.waitJson.nodeId ?? "?"}</dd>
            {run.waitJson.reason && <><dt className="text-fg-dim">reason</dt><dd>{run.waitJson.reason}</dd></>}
            {Array.isArray(run.waitJson.needed) && run.waitJson.needed.length > 0 && <>
              <dt className="text-fg-dim">needed</dt>
              <dd>{run.waitJson.needed.map((n) => <kbd key={n} className="font-mono bg-bg-elev2 border border-border px-1.5 rounded text-xs mr-1">{n}</kbd>)}</dd>
            </>}
            {run.waitJson.hint && <><dt className="text-fg-dim">hint</dt><dd>{run.waitJson.hint}</dd></>}
            {Array.isArray(run.waitJson.pending) && run.waitJson.pending.length > 0 && <>
              <dt className="text-fg-dim">pending</dt>
              <dd>{run.waitJson.pending.map((p, i) => {
                const id = typeof p === "string" ? p : (p as { id?: unknown })?.id;
                const label = typeof id === "string" ? id : `(unnamed-${i})`;
                return <kbd key={`${label}-${i}`} className="font-mono bg-bg-elev2 border border-border px-1.5 rounded text-xs mr-1">{label}</kbd>;
              })}</dd>
            </>}
          </dl>
          {Array.isArray(run.waitJson.pending) && run.waitJson.pending.length > 0 && (
            <GateForm flowId={flowId} pending={run.waitJson.pending as unknown[]} />
          )}
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>graph</CardTitle>
          <span className="text-fg-dim font-mono text-xs">current: {run.currentStep ?? "?"}</span>
        </CardHeader>
        {pipeline && <GraphView pipeline={pipeline} run={run} tree={tree} />}
        <PathTrail history={tree?.map((i) => i.nodeId)} />
      </Card>

      <SubflowsCard subflows={subflows} pipeline={pipeline} transcripts={transcripts} />

      <TranscriptPane flowId={flowId} transcripts={transcripts} />

      <div className="flex flex-col lg:flex-row gap-3 lg:gap-5">
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>state</CardTitle></CardHeader>
            <StateTree value={run.stateJson} label="state" defaultOpen />
          </Card>
          <Card>
            <CardHeader><CardTitle>errors</CardTitle></CardHeader>
            {(run.errorEntries ?? []).length === 0
              ? <div className="text-fg-dim italic">none</div>
              : (
                <div>
                  {(run.errorEntries ?? []).slice(-30).map((e, i) => (
                    <div key={i} className="py-1 border-b border-dashed border-border last:border-b-0">
                      <span className="font-mono text-fg-dim">{e.nodeId}</span> {e.message}
                    </div>
                  ))}
                </div>
              )}
          </Card>
        </div>
        <div className="flex-1 min-w-0">
          <Card>
            <CardHeader><CardTitle>events</CardTitle></CardHeader>
            {events.length === 0
              ? <div className="text-fg-dim italic">none yet — events accrue while the dashboard is running</div>
              : (
                <div className="font-mono text-xs max-h-96 overflow-auto">
                  {events.map((evt, i) => (
                    <div key={i} className="grid grid-cols-[56px_90px_1fr] sm:grid-cols-[80px_110px_1fr] gap-2 py-1 border-b border-dashed border-border last:border-b-0">
                      <span className="text-fg-dim">{formatTime((evt as any).at).split(" ")[1] ?? ""}</span>
                      <span className={evt.kind === "run.ended" ? "text-status-green" : evt.kind === "run.created" ? "text-status-amber" : "text-accent"}>{evt.kind}</span>
                      <span>{formatEventBody(evt)}</span>
                    </div>
                  ))}
                </div>
              )}
          </Card>
          <Card>
            <CardHeader><CardTitle>artifacts</CardTitle></CardHeader>
            {artifacts.length === 0
              ? <div className="text-fg-dim italic">none</div>
              : artifacts.map((a) => (
                <div key={a.name} className="flex justify-between py-1">
                  <a href={artifactUrl(flowId, a.name)} target="_blank" rel="noreferrer" className="font-mono">{a.name}</a>
                  <span className="text-fg-dim">{formatBytes(a.size)}</span>
                </div>
              ))}
          </Card>
        </div>
      </div>
    </>
  );
}

function formatEventBody(evt: LiveEvent): string {
  switch (evt.kind) {
    case "run.created":   return `created at ${evt.currentStep ?? "?"} (${evt.status})`;
    case "run.ended": return `terminated (${evt.status})`;
    case "run.updated": {
      const step = evt.prevStep !== evt.currStep ? `step ${evt.prevStep ?? "?"} → ${evt.currStep ?? "?"}` : `rev ${evt.revision}`;
      const status = evt.prevStatus !== evt.currStatus ? ` · ${evt.prevStatus} → ${evt.currStatus}` : "";
      const keys = (evt.stateKeysAdded?.length || evt.stateKeysRemoved?.length)
        ? ` · keys ${(evt.stateKeysAdded ?? []).map((k) => `+${k}`).join(" ")} ${(evt.stateKeysRemoved ?? []).map((k) => `-${k}`).join(" ")}`
        : "";
      const wait = evt.waitKind ? ` · ${evt.waitKind}` : "";
      return `${step}${status}${keys}${wait}`;
    }
    case "pipeline.catalog.updated": return `pipelines: ${evt.ids.join(", ")}`;
  }
}
