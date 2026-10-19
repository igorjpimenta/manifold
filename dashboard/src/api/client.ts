// Typed fetch wrappers for the dashboard's read-only HTTP API. The server
// is dashboard/server.js (mounted by pipeclaw). All endpoints return JSON.

import type {
  AllPipelinesCost,
  ArtifactEntry,
  FlowRunFull,
  FlowRunSummary,
  FlowTreeItem,
  GlobalStats,
  NodeStats,
  Pipeline,
  PipelineCost,
  PipelineStats,
  PipelineViews,
  RunCompare,
  RunCost,
  SearchResponse,
  SseEvent,
  SubflowData,
  Transcript,
} from "./types";

// vite sets BASE_URL to the configured base (with trailing slash). All API
// paths are prefixed so the dashboard works both at `/` locally and at
// `/graph/` behind the tailscale proxy without rebuilding per env.
const API = `${import.meta.env.BASE_URL}api`;

async function json<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: HTTP ${r.status}`);
  return r.json() as Promise<T>;
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await r.json().catch(() => ({} as Record<string, unknown>));
  if (!r.ok) {
    const errMsg = typeof (payload as { error?: unknown })?.error === "string"
      ? (payload as { error: string }).error
      : `HTTP ${r.status}`;
    throw new Error(`${url}: ${errMsg}`);
  }
  return payload as T;
}

export const api = {
  pipelines: () => json<Pipeline[]>(`${API}/pipelines`),
  pipeline:  (id: string) => json<Pipeline>(`${API}/pipelines/${encodeURIComponent(id)}`),
  runs:      (filters: { status?: string; pipelineId?: string; limit?: number; since?: number; until?: number } = {}) => {
    const q = new URLSearchParams();
    if (filters.status) q.set("status", filters.status);
    if (filters.pipelineId) q.set("pipelineId", filters.pipelineId);
    if (filters.limit) q.set("limit", String(filters.limit));
    if (filters.since != null) q.set("since", String(filters.since));
    if (filters.until != null) q.set("until", String(filters.until));
    return json<FlowRunSummary[]>(`${API}/runs?${q.toString()}`);
  },
  run:           (flowId: string) => json<FlowRunFull>(`${API}/runs/${flowId}`),
  runEvents:     (flowId: string) => json<SseEvent[]>(`${API}/runs/${flowId}/events`),
  runArtifacts:  (flowId: string) => json<ArtifactEntry[]>(`${API}/runs/${flowId}/artifacts`),
  runTranscripts:(flowId: string) => json<Transcript[]>(`${API}/runs/${flowId}/transcripts`),
  runCompare:    (flowId: string) => json<RunCompare | null>(`${API}/runs/${flowId}/compare`),
  runSubflows:   (flowId: string) => json<Record<string, Record<string, SubflowData>>>(`${API}/runs/${flowId}/subflows`),
  runTree:       (flowId: string) => json<{ items: FlowTreeItem[] }>(`${API}/runs/${flowId}/tree`),
  stats:         () => json<GlobalStats>(`${API}/stats`),
  pipelineStats: (id: string) => json<PipelineStats>(`${API}/stats/pipelines/${encodeURIComponent(id)}`),
  pipelineNodeStats: (pipelineId: string, nodeId: string) =>
    json<NodeStats>(`${API}/pipelines/${encodeURIComponent(pipelineId)}/nodes/${encodeURIComponent(nodeId)}/stats`),
  // `status` is a structured equality filter applied before the
  // free-text match; q can be empty when status alone is enough.
  search: (q: string, opts: { limit?: number; status?: string } = {}) => {
    const limit = opts.limit ?? 50;
    const qp = new URLSearchParams();
    if (q) qp.set("q", q);
    if (opts.status) qp.set("status", opts.status);
    qp.set("limit", String(limit));
    return json<SearchResponse>(`${API}/search?${qp.toString()}`);
  },
  runCost: (flowId: string) => json<RunCost>(`${API}/runs/${flowId}/cost`),
  pipelineCost: (pipelineId: string, sinceDays = 30) =>
    json<PipelineCost>(`${API}/stats/pipelines/${encodeURIComponent(pipelineId)}/cost?sinceDays=${sinceDays}`),
  allPipelinesCost: (sinceDays = 30) =>
    json<AllPipelinesCost>(`${API}/stats/cost?sinceDays=${sinceDays}`),
  // Pipeline-declared dashboard views — each entry is the computed
  // payload for one `views[]` declaration. Empty `views` array when
  // the pipeline declares none; caller skips rendering.
  pipelineViews: (pipelineId: string, sinceDays = 30) =>
    json<PipelineViews>(`${API}/stats/pipelines/${encodeURIComponent(pipelineId)}/views?sinceDays=${sinceDays}`),

  approve: (flowId: string, body: { itemId: string; decision: "approve" | "reject" | "edit"; text?: string }) =>
    postJson<{ decision: string; itemId: string; stillPending: number; status: "blocked" | "resumed" }>(
      `${API}/runs/${flowId}/approve`,
      body,
    ),

  cancel: (flowId: string) =>
    postJson<{ cancelled: true; wasStatus: string }>(`${API}/runs/${flowId}/cancel`, {}),

  rerun: (sourceFlowId: string, body: { args?: Record<string, unknown> } = {}) =>
    postJson<{ flowId: string; pipelineId: string | null; rerunOf: string }>(
      `${API}/runs/${sourceFlowId}/rerun`,
      body,
    ),

  respond: (flowId: string, text: string) =>
    postJson<{ responded: true; nodeId: string; status: "resumed" }>(
      `${API}/runs/${flowId}/respond`,
      { text },
    ),
};

export const artifactUrl = (flowId: string, name: string) =>
  `${API}/runs/${flowId}/artifacts/${encodeURIComponent(name)}`;
