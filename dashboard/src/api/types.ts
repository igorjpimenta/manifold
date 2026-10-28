// Type definitions for the API surface exposed by dashboard/server.js.
// These mirror what server.js + pipeline-graph.js + stats.js produce.
// Kept loose where the backend is loose (optional fields, unknown JSON state).

// ─── Pipeline definitions (from /api/pipelines/:id) ──────────────────────────

export type NodeKind =
  | "start"
  | "end"
  | "code"
  | "agent"
  | "subflow"
  | "human-gate";

export interface PipelineNode {
  id: string;
  kind: NodeKind | string;
  agent?: string;
  groupId?: string;
  concurrency?: number;
  waitKind?: string;
  /** For subflow nodes, the inner flow pipeline. */
  flow?: FlowTopology;
}

export interface FlowTopology {
  parentNodeId: string;
  groupId: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

export interface PipelineEdge {
  from: string;
  to: string;
  conditional: boolean;
  /** Source text of the conditional edge function (truncated by the serializer). */
  via?: string;
}

export interface PipelineGroup {
  label?: string;
  kind?: string;
  members: string[];
}

export interface Pipeline {
  id: string;
  goal: string | null;
  file: string | null;
  dir: string | null;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  groups: Record<string, PipelineGroup> | null;
  /** Marker for /api/pipelines list — populated only when the pipeline def is gone but runs remain. */
  historical?: boolean;
  runs?: number;
}

// ─── Flow runs (from /api/runs and /api/runs/:flowId) ───────────────────────

// "waiting" is never actually written anywhere (confirmed repo-wide) — gate
// pauses are always persisted as "blocked".
export type RunStatus = "running" | "blocked" | "succeeded" | "failed" | "cancelled" | "skipped" | string;

export interface ParallelGroupSummary {
  groupId: string;
  total: number;
  done: number;
  failed: number;
  awaiting: number;
  running: number;
}

export interface FlowRunSummary {
  flowId: string;
  pipelineId: string | null;
  runId: string | null;
  status: RunStatus;
  lastStep: string | null;
  currentStep: string | null;
  revision: number;
  statusReason: string | null;
  waitKind: string | null;
  createdAt: number | null;
  updatedAt: number | null;
  endedAt: number | null;
  errorsCount: number;
  subflows: ParallelGroupSummary[] | null;
}

export interface WaitJson {
  kind?: string;
  nodeId?: string;
  reason?: string;
  needed?: string[];
  hint?: string;
  /**
   * Pending entries are either raw string ids ("listing-123") or objects
   * with at least `.id` (e.g. `{ id: "listing-123", reason: "approve to ship" }`).
   * The GateForm handles both shapes; the type stays loose.
   */
  pending?: (string | { id: string; reason?: string; hint?: string; [k: string]: unknown })[];
  items?: unknown[];
  /**
   * Optional opt-in, generic across wait kinds: a node's `blockedInfo.choices`
   * is passed through verbatim onto `waitJson.choices` by the executor
   * (see engine/executor.js). Each entry is a label a pipeline author can
   * offer as a fixed response option. No dashboard component currently
   * renders this — it's plumbed through as API surface for future use.
   */
  choices?: string[];
}

/**
 * Canonical error-record shape shared across every consumer — SQL row
 * (flow_node_executions), JSON wire response, this TS type, and the iOS
 * Swift struct all use the same field set. Replaces the old per-field
 * inconsistency (SubflowData.errors was typed string[] but the real wire
 * payload could contain objects — that mistyping is what caused
 * SubflowsCard.tsx's .slice() crash; RunState.errors was typed
 * {node?,message?}[], a third, different shape).
 */
export interface ErrorEntry {
  nodeId: string;
  /** null = root-level (flow) error; set = subflow sub-node error — both live in flow_node_executions now. */
  subflowKey: string | null;
  message: string;
  at: number;
}

export interface SubflowData {
  /** Subflow's most recently consumed sub-step. `__end__` when terminal. */
  step?: string;
  /** Per-sub-step outputs. Keys are sub-node ids (e.g. DRAFT, GATE, STAMP, SUBMIT). */
  data?: Record<string, SubflowStepResult | undefined>;
  /** Normalized error history sourced from flow_node_executions. */
  errorEntries?: ErrorEntry[];
  failedAt?: number;
  doneAt?: number;
  awaiting?: { subNodeId: string; requestPayload?: unknown; requestedAt?: number };
  updatedAt?: number;
}

export interface SubflowStepResult {
  _error?: string;
  ok?: boolean;
  decision?: string;
  [key: string]: unknown;
}

/** One row from GET /manifold/runs/:flowId/tree (routes/runs.js's buildTreeNodeRows)
 *  — one per top-level (subflow_key IS NULL) invocation, ordered by invokedAt.
 *  Backs PathTrail/GraphView's executed-path rendering (SUBFLOW_PARENT_STATE_
 *  ACCESS.design.md's Phase 3b), replacing stateJson._stepHistory as the source. */
export interface FlowTreeItem {
  type: "node";
  invocationId: string;
  nodeId: string;
  kind: string | null;
  agent: string | null;
  status: RunStatus;
  enteredAt: number | null;
  exitedAt: number | null;
  execKind: string | null;
  executionCount: number;
  everRetriedOrRevived: boolean;
  hasChildren: boolean;
  rollup?: { succeeded: number; failed: number; pending: number; skipped: number; cancelled: number };
}

export interface RunState {
  pipelineId?: string;
  runId?: string;
  startedAt?: number;
  endedAt?: number;
  args?: Record<string, unknown>;
  subflows?: Record<string, Record<string, SubflowData>>;
  [key: string]: unknown;
}

export interface FlowRunFull {
  flowId: string;
  pipelineId: string | null;
  runId: string | null;
  status: RunStatus;
  lastStep: string | null;
  currentStep: string | null;
  revision: number;
  statusReason: string | null;
  stateJson: RunState;
  waitJson: WaitJson | null;
  /** Phase 1.5: normalized error history sourced from flow_node_executions, sibling to stateJson (not nested — keeps flow-level and subflow-level shapes symmetric). */
  errorEntries?: ErrorEntry[];
  createdAt: number | null;
  updatedAt: number | null;
  endedAt: number | null;
}

// ─── Stats (from /api/stats and /api/stats/pipelines/:id) ────────────────────

export interface DurationStats {
  n: number;
  p50Ms: number | null;
  p95Ms: number | null;
  meanMs: number | null;
}

export interface RevisionStats {
  n: number;
  p50: number | null;
  p95: number | null;
}

export interface PerDayBucket {
  day: string;
  total: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface HistogramEntry {
  key: string;
  /** Pipeline owning this cluster — only useful at the global overview, where the same step name can collide across pipelines. */
  pipelineId?: string;
  count: number;
  /** Median block-time (run creation → terminal failure) for runs ending at this step. Null when block-time signal is missing. */
  p50BlockMs?: number | null;
  /** count × p50BlockMs — total operator-impact ranked unit. 0 when no block-time. */
  impactMs?: number;
  /** Timestamp of most recent failure in this cluster — drives the recency tone (today red / 7d amber / older dim). */
  lastFailedAt?: number | null;
  /** Failure count per day across a fixed window (default 14d). Aligned across all clusters so the sparkline canvases match. */
  perDay?: { day: string; count: number }[];
  /** Per-error breakdown inside this (pipeline, step) cluster. Different signatures stack as sub-rows in the tap-to-expand detail instead of duplicating top-level rows. Sorted by recency desc. */
  errors?: {
    signature: string;
    count: number;
    lastFailedAt: number | null;
    exemplarMessage: string | null;
    recentRuns: { flowId: string; status: string; at: number }[];
  }[];
}

export interface PipelineBreakdownEntry {
  pipelineId: string;
  total: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  active: number;
}

export interface GlobalStats {
  total: number;
  statusCounts: Record<string, number>;
  succeeded: number;
  failed: number;
  cancelled: number;
  active: number;
  terminated: number;
  successRate: number | null;
  duration: DurationStats;
  perDay: PerDayBucket[];
  failureStops: HistogramEntry[];
  pipelineBreakdown: PipelineBreakdownEntry[];
}

// ─── Declared dashboard views (from /api/stats/pipelines/:id/views) ─────────
// Each pipeline can declare an array of opt-in surfaces; the dashboard
// dispatches on `kind`. Unit + meaning are per-view-kind.

export interface DeclaredViewFunnelStage {
  id: string;
  label: string;
  count: number;
  conversionFromPrev: number | null;
  conversionFromStart?: number;
}

export interface DeclaredViewFunnel {
  id: string;
  kind: "funnel";
  label: string;
  sinceMs: number | null;
  total: number;
  funnel: DeclaredViewFunnelStage[];
  /** Per-`outcome` distribution when the underlying source carried an `outcome` field (frontmatter-dir source). Omitted otherwise. */
  byOutcome?: Record<string, number>;
  error?: string;
}

export interface DeclaredViewUnknown {
  id: string;
  kind: string;
  label: string;
  sinceMs: number | null;
  error?: string;
}

export type DeclaredView = DeclaredViewFunnel | DeclaredViewUnknown;

export interface PipelineViews {
  sinceMs: number | null;
  views: DeclaredView[];
}

export interface FunnelStage {
  id: string;
  label: string;
  key?: string;
  n: number;
  total: number;
  p50: number | null;
  /** 95th percentile per-run yield at this stage — pairs with `min` to surface variance the p50 hides. */
  p95: number | null;
  /** Minimum per-run yield — non-zero days are noise; a 0 here is "the funnel ran dry on at least one run." */
  min: number | null;
  mean: number | null;
  conversionFromPrev?: number | null;
  conversionFromStart?: number | null;
  /** Per-subflow attrition funnel — present when the stage's underlying node is a subflow parent. */
  subStages?: FunnelStage[];
}

export interface PipelineStats {
  pipelineId: string;
  /** Status breakdown for runs whose updatedAt landed in the last 24h. Feeds the per-pipeline TodayStrip. */
  recent24h: { total: number; ok: number; failed: number; active: number };
  total: number;
  statusCounts: Record<string, number>;
  succeeded: number;
  failed: number;
  cancelled: number;
  terminated: number;
  successRate: number | null;
  duration: DurationStats;
  revision: RevisionStats;
  failureStops: HistogramEntry[];
  funnel: FunnelStage[] | null;
  recentRuns: unknown[];
}

export interface SearchMatch {
  /** Where the hit lives: "flowId", "statusReason", "state.errors[2].message", "state.shortlist", … */
  field: string;
  /** ±~40 chars around the first token match, with `…` ellipses + whitespace collapsed. */
  snippet: string;
  /** Offset of the first token's first occurrence in the matched field's full text. */
  matchedAt: number;
}

export interface SearchHit {
  flowId: string;
  pipelineId: string | null;
  status: RunStatus;
  lastStep: string | null;
  currentStep: string | null;
  updatedAt: number | null;
  revision: number;
  matches: SearchMatch[];
}

export interface SearchResponse {
  query: string;
  totalHits: number;
  hits: SearchHit[];
  truncated: boolean;
}

export interface RunCostNodeEntry {
  cost: number;
  durationMs: number;
  calls: number;
}

export interface RunCost {
  flowId: string;
  total: number;
  tokens: { input: number; output: number; cacheRead: number; cacheWrite: number };
  byModel: Record<string, number>;
  byNode: Record<string, RunCostNodeEntry>;
  bySession: { sessionKey: string; agent: string; nodeId: string; itemKey: string | null; sessionId: string; models: string[]; total: number; turns: number; durationMs: number }[];
}

export interface PipelineCostNodeEntry {
  cost: number;
  calls: number;
  p50Ms: number | null;
  p95Ms: number | null;
  meanMs: number | null;
  /** Median per-call cost. Null when no cost-bearing calls. */
  p50Cost: number | null;
  /** 95th percentile per-call cost — surfaces the runaway turn the mean hides. */
  p95Cost: number | null;
  /** Per-day rollup for the row-expansion sparkline. cost / ms are sums; p50Cost / p50Ms are per-call medians for that day; calls is the call count. */
  perDay: { day: string; cost: number; calls: number; ms: number; p50Cost: number | null; p50Ms: number | null }[];
}

export interface PipelineCost {
  pipelineId: string;
  runs: number;
  succeeded: number;
  total: number;
  mean: number;
  /** 95th percentile of per-run cost — surfaces runaway runs the mean hides. */
  p95: number | null;
  /** Total spend ÷ count of succeeded runs. Null until at least one run has succeeded — the load-bearing ROI signal. */
  costPerSuccess: number | null;
  /** costPerSuccess over the same-length window immediately prior — for the trend delta. Null when no prior-window data. */
  costPerSuccessPrior: number | null;
  byModel: Record<string, number>;
  byNode: Record<string, PipelineCostNodeEntry>;
  perDay: { day: string; cost: number; runs: number; p50Cost: number | null; p50Ms: number | null; p95Ms: number | null }[];
  since: number | null;
  until: number | null;
  perRun: { flowId: string; status: string; updatedAt: number | null; cost: number }[];
}

export interface AllPipelinesCostEntry {
  pipelineId: string;
  runs: number;
  succeeded: number;
  total: number;
  costPerSuccess: number | null;
  lastUpdatedAt: number;
  /** Mean per-run spend. Null when no cost-bearing runs in the window. */
  meanCost: number | null;
  /** p50 / p95 of per-run cost — surfaces the runaway-run tail that the mean hides. */
  p50Cost: number | null;
  p95Cost: number | null;
  /** Mean / p50 / p95 of per-run wall time. */
  meanMs: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
  /** Per-day rollup — cost / runs / ms sums plus per-day p50 of the samples for the chart's p50 toggle. */
  perDay: { day: string; cost: number; runs: number; ms: number; p50Cost: number | null; p50Ms: number | null }[];
}

export interface AllPipelinesCost {
  sinceMs: number | null;
  pipelines: AllPipelinesCostEntry[];
  /** Cross-pipeline rollup for the overview tiles + portfolio-wide daily trend charts. Percentiles are over the union of run-level samples (NOT per-pipeline percentiles averaged). */
  total: {
    spend: number;
    runs: number;
    succeeded: number;
    mean: number | null;
    p50Cost: number | null;
    p95Cost: number | null;
    meanMs: number | null;
    p50Ms: number | null;
    p95Ms: number | null;
    costPerSuccess: number | null;
    perDay: { day: string; cost: number; runs: number; ms: number; p50Cost: number | null; p50Ms: number | null; p95Ms: number | null }[];
  };
}

export interface NodeStats {
  pipelineId: string;
  nodeId: string;
  visits: number;
  failedHere: number;
  blockedHere: number;
  terminatedHere: number;
  recentRuns: { flowId: string; status: string; lastStep: string | null; currentStep: string | null; updatedAt: number | null; revision: number }[];
  totalRuns: number;
}

export interface RunCompare {
  peerCount: number;
  durationMs: number | null;
  durationMedianMs: number | null;
  durationRatio: number | null;
  revision: number;
  revisionMedian: number | null;
}

// ─── Live events (WS /api/events/ws + REST run history) ──────────────────────

export type LiveEvent =
  | { kind: "run.created"; flowId: string; at: number; status: RunStatus; lastStep: string | null; currentStep: string | null; pipelineId: string | null }
  | { kind: "run.updated"; flowId: string; at: number; prevStatus: RunStatus; currStatus: RunStatus; prevStep: string | null; currStep: string | null; revision: number; stateKeysAdded: string[]; stateKeysRemoved: string[]; waitKind: string | null }
  | { kind: "run.ended"; flowId: string; at: number; status: RunStatus }
  | { kind: "pipeline.catalog.updated"; at: number; size: number; ids: string[] };

export interface ArtifactEntry {
  name: string;
  size: number;
  mtime: number;
}

export interface TranscriptTurn {
  role: "user" | "assistant";
  text: string;
  thinking: string | null;
  truncated: boolean;
  timestamp: string | null;
}

export interface Transcript {
  sessionKey: string;
  agent: string;
  nodeId: string;
  itemKey: string | null;
  sessionId: string;
  startedAt: number | null;
  updatedAt: number | null;
  turns: TranscriptTurn[];
}
