// server.js — read-only HTTP dashboard for manifold.
//
// Bound to 127.0.0.1 only (no LAN exposure, no auth surface). All routes are
// read-only — actuation stays in the CLI and Telegram surfaces. Live updates
// are pushed over WebSocket from the event-bus (event-log.js), which fires
// on every store write — no polling lag, no per-client catch-up stream:
// the run-detail page reloads full state from REST after any reconnect.
//
// File-serving for the bundled SPA uses node:http only; no express/fastify.

import http from "node:http";
import { attachWsEndpoint } from "../events/ws-frame.js";
import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { serializePipeline } from "./pipeline-graph.js";
import { computeGlobalStats, computePipelineStats, compareRunToMedian, computeNodeStats, resolveDisplayStep } from "./stats.js";
import { searchRuns } from "./search.js";
import { costForRun, costForPipeline, costForAllPipelines } from "./cost.js";
import { computeViews } from "./views.js";
import { stepsExecutedFromTree } from "./tree-stats.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";
import { assembleState } from "../engine/executor-helpers.js";
import { getAllSubflowGroups } from "../state/subflow-state.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
// Static assets come from vite's build output under `dist/`. Run `npm run
// build` from this directory after editing the React sources to refresh.
const DIST_DIR = path.join(HERE, "dist");

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js":   "application/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".md":   "text/markdown; charset=utf-8",
  ".txt":  "text/plain; charset=utf-8",
  ".svg":  "image/svg+xml",
  ".png":  "image/png",
  ".jpg":  "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif":  "image/gif",
  ".ico":  "image/x-icon",
};

export function createDashboardServer({ writeStore, readStore, failureStore, discussStore, nodeExecutionsStore, pipelinesRef, pipelineRegistryStore, ensurePipelinesLoaded, executor, eventBus, port, host, logger, recordItemDecision }) {
  let server = null;
  // Live WS door (events/ws-frame.js) — opened in start(), drained on stop().
  let door = null;
  let unsubscribe = null;

  // ── Direct store reads ────────────────────────────────────────────────────
  // Every read below hits manifold's own stores with the same query logic
  // the gateway's /manifold/* routes use — no gateway hop. Payload shapes
  // match the old gateway responses field-for-field so the React app is untouched.

  // Adapter-injected in production; process singleton otherwise (the same
  // binding the gateway routes read through).
  function nx() {
    return nodeExecutionsStore ?? getNodeExecutionsStore();
  }

  // Normalize a listRunsPage row: *Ms suffix timestamps + synthesized
  // stateJson so stats functions that access stateJson.pipelineId work.
  function adaptRun(r) {
    return {
      ...r,
      createdAt: r.createdAt ?? r.createdAtMs,
      updatedAt: r.updatedAt ?? r.updatedAtMs,
      endedAt:   r.endedAt   ?? r.endedAtMs,
      stateJson: r.stateJson ?? { pipelineId: r.pipelineId },
    };
  }

  // All top-level runs, trimmed to the same 500-row window the gateway's
  // list endpoint served (limit=min(500, 9999), limit+1 fetch, pop).
  async function fetchAllRuns({ pipelineId, status } = {}) {
    const page = await readStore.listRunsPage({
      pipelineId: pipelineId || null,
      status: status || null,
      limit: 501,
    });
    const rows = page.slice(0, 500);
    const childSummaryMap = await nx().summarizeChildrenByFlowIds(rows.map((r) => r.flowId));
    const EMPTY_SUBFLOWS = { total: 0, pending: 0, failed: 0, skipped: 0, succeeded: 0 };
    const discussSummaries = discussStore ? await discussStore.getBulkSummaries(rows.map((r) => r.flowId)) : {};
    const runs = rows.map((r) => {
      const discuss = discussSummaries[r.flowId] ?? null;
      const subflows = childSummaryMap.get(r.flowId) ?? EMPTY_SUBFLOWS;
      return {
        ...r,
        subflowTotal: subflows.total,
        subflowPending: subflows.pending,
        subflowFailed: subflows.failed,
        subflowSkipped: subflows.skipped,
        subflowSucceeded: subflows.succeeded,
        pendingDraftItemIds: discuss?.pendingItemIds ?? [],
      };
    });
    return runs.map(adaptRun);
  }

  // Full run payload — same shape as the gateway's sendFullRun
  // (routes/runs.js): assembleState-enriched stateJson, gate_records gates,
  // failure_events errorEntries, *Ms timestamps, parent linkage. Returns
  // null when the flow does not exist.
  async function fetchFlow(flowId) {
    const flow = await readStore.get(flowId);
    if (!flow) return null;
    return readFullRun(flow);
  }

  async function readFullRun(flow) {
    const store = nx();
    const enrichedState = await assembleState(flow.flowId, {
      runStore: readStore, nodeExecutionsStore: store, baseState: flow.stateJson ?? {},
    });
    const gates = await store.listGateRecordsForFlow(flow.flowId);
    const { loop: _loop, errors: _errors, gates: _staleGates, ...stateWithoutLegacy } = enrichedState;
    let parentFlowId = null, parentNodeId = null, parentSubflowKey = null;
    let resolvedPipelineId = flow.stateJson?.pipelineId ?? null;
    if (flow.parentInvocationId) {
      const invocation = await store.getInvocation(flow.parentInvocationId);
      if (invocation) {
        parentFlowId = invocation.flowId;
        parentNodeId = invocation.nodeId;
        parentSubflowKey = invocation.subflowKey;
        const parentFlow = await readStore.get(parentFlowId);
        if (parentFlow) resolvedPipelineId = parentFlow.pipelineId ?? resolvedPipelineId;
      }
    }
    return {
      flowId: flow.flowId,
      pipelineId: resolvedPipelineId,
      runId: flow.stateJson?.runId ?? null,
      status: flow.status,
      currentStep: flow.currentStep ?? null,
      lastStep: flow.lastStep,
      revision: flow.revision,
      statusReason: flow.statusReason ?? null,
      stateJson: { ...stateWithoutLegacy, gates },
      waitJson: flow.waitJson ?? null,
      errorEntries: failureStore ? await failureStore.queryErrorsByFlow(flow.flowId) : [],
      createdAtMs: flow.createdAt ?? null,
      updatedAtMs: flow.updatedAt ?? null,
      endedAtMs: flow.endedAt ?? null,
      parentFlowId,
      nodeId: parentNodeId,
      subflowKey: parentSubflowKey,
    };
  }

  // Errors for a set of flowIds → Map<flowId, ErrorEntry[]> (same source as
  // the gateway's /manifold/errors: failureStore.queryErrorsByFlowIds).
  async function fetchErrors(flowIds) {
    if (!flowIds.length) return new Map();
    if (!failureStore) return new Map();
    return failureStore.queryErrorsByFlowIds(flowIds);
  }

  // Batched "which top-level nodes did each flow visit" → Map<flowId,
  // Set<nodeId>>, backing the auto-funnel visit check without one round-trip
  // per run. Empty Map on error — the funnel then reports 0 for every
  // stage, same as "no runs visited yet", rather than throwing.
  async function fetchTreeSummary(flowIds) {
    if (!flowIds.length) return new Map();
    try {
      const visitedMap = await nx().listVisitedNodeIdsByFlow(flowIds);
      return new Map([...visitedMap.entries()].map(([flowId, nodeIds]) => [flowId, new Set(nodeIds ?? [])]));
    } catch { return new Map(); }
  }

  // Subflow rows for a flow — same { subflows: [...] } envelope the
  // gateway's sendSubflowsList serves (legacy errors column stripped).
  async function fetchSubflows(flowId) {
    try {
      const grouped = await getAllSubflowGroups({
        flowId, runStore: readStore, nodeExecutionsStore: nx(),
      });
      const subflows = Object.values(grouped).flatMap((group) =>
        Object.values(group).map(({ errors: _errors, ...rest }) => rest)
      );
      return { subflows };
    } catch { return {}; }
  }

  // This flow's own top-level node list — the same row shape the gateway's
  // /tree root serves (one row per invocation, ordered by invokedAt).
  // Empty array on error rather than throwing — callers treat "no tree
  // data" the same as "flow has no history yet".
  async function fetchTree(flowId) {
    try {
      const flow = await readStore.get(flowId);
      if (!flow) return [];
      return await buildTreeNodeRows(flow);
    } catch { return []; }
  }

  // Ports routes/runs.js's buildTreeNodeRows (same flow-status fallback and
  // point-in-time kind/agent reads, same shallow immediate-children rollup).
  async function buildTreeNodeRows(flow) {
    const store = nx();
    const pipelineId = flow.stateJson?.pipelineId;
    const invocations = await store.listInvocationsForFlow(flow.flowId);
    const failedExecutionIds = new Set(
      ((failureStore ? await failureStore.queryErrorsByFlow(flow.flowId) : []) ?? [])
        .map((e) => e.executionId).filter(Boolean)
    );
    const versionRequests = invocations.map((inv) => ({ pipelineId, nodeName: inv.nodeId, atMs: inv.invokedAt }));
    const versions = await pipelineRegistryStore.getNodeVersionsBatch(versionRequests);
    const versionByInvocationId = new Map(invocations.map((inv, i) => [inv.invocationId, versions[i]]));
    const subflowNodeIds = [
      ...new Set(invocations.filter((inv) => versionByInvocationId.get(inv.invocationId)?.kind === "subflow").map((inv) => inv.nodeId)),
    ];
    const childrenByNodeId = new Map();
    if (subflowNodeIds.length > 0) {
      const invIdsByNodeId = await store.listInvocationIdsForNodes(flow.flowId, subflowNodeIds);
      const allInvIds = [...invIdsByNodeId.values()].flat();
      const allChildren = await readStore.listForParentInvocations(allInvIds);
      const nodeIdByInvId = new Map();
      for (const [nodeId, ids] of invIdsByNodeId) {
        for (const id of ids) nodeIdByInvId.set(id, nodeId);
      }
      for (const child of allChildren) {
        const nodeId = nodeIdByInvId.get(child.parentInvocationId);
        if (!nodeId) continue;
        if (!childrenByNodeId.has(nodeId)) childrenByNodeId.set(nodeId, []);
        childrenByNodeId.get(nodeId).push(child);
      }
    }
    const lastIdx = invocations.length - 1;
    return invocations.map((inv, idx) => {
      const version = versionByInvocationId.get(inv.invocationId);
      const kind = version?.kind ?? null;
      const isSubflowNode = kind === "subflow";
      const kids = isSubflowNode ? (childrenByNodeId.get(inv.nodeId) ?? []) : [];
      let status;
      if (inv.latestExecutionId && failedExecutionIds.has(inv.latestExecutionId)) {
        status = "failed";
      } else if (idx === lastIdx) {
        status = flow.status;
      } else {
        status = "succeeded";
      }
      const row = {
        type: "node",
        invocationId: inv.invocationId,
        nodeId: inv.nodeId,
        kind,
        agent: version?.agent ?? null,
        status,
        enteredAt: inv.enteredAt,
        exitedAt: inv.exitedAt,
        execKind: inv.kind,
        executionCount: inv.executionCount,
        everRetriedOrRevived: inv.everRetriedOrRevived,
        hasChildren: isSubflowNode && kids.length > 0,
      };
      if (isSubflowNode) row.rollup = bucketChildFlowsByStatus(kids);
      return row;
    });
  }

  // Shallow rollup (immediate children only) — same status-bucket shape
  // summarizeChildrenByFlowIds uses.
  function bucketChildFlowsByStatus(childFlowRows) {
    const rollup = { succeeded: 0, failed: 0, pending: 0, skipped: 0, cancelled: 0 };
    for (const c of childFlowRows) {
      if (c.status === "succeeded") rollup.succeeded++;
      else if (c.status === "failed") rollup.failed++;
      else if (c.status === "skipped") rollup.skipped++;
      else if (c.status === "cancelled") rollup.cancelled++;
      else rollup.pending++;
    }
    return rollup;
  }

  // TTL cache for stats endpoints — these are polled by the SPA on a timer
  // and can tolerate a few seconds of staleness. Keyed on fetch parameters;
  // runs + errors are always fetched together and cached as a pair.
  const _statsCache = new Map();
  const STATS_TTL_MS = 5_000;

  async function cachedRunsAndErrors(key, runsOpts = {}) {
    const now = Date.now();
    const hit = _statsCache.get(key);
    if (hit && hit.expiresAt > now) return hit.data;
    const runs = await fetchAllRuns(runsOpts);
    const errorsByFlow = await fetchErrors(runs.map((r) => r.flowId));
    const data = { runs, errorsByFlow };
    _statsCache.set(key, { data, expiresAt: now + STATS_TTL_MS });
    return data;
  }

  // Same /graph prefix strip as handle() below: the bundle is built with
  // vite base `/graph/`, so the WS URL embeds the prefix when served
  // under it and arrives bare on direct loopback.
  function wsPathMatch(pathname) {
    return pathname === "/api/events/ws" || pathname === "/graph/api/events/ws";
  }

  function dispatchWs(event) {
    try {
      door?.broadcast(event);
    } catch { /* client gone; cleanup on close */ }
  }

  async function handle(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    // The bundle is built with vite base `/graph/`, so all HTML references
    // (assets, API client URLs) embed that prefix. We strip it here so the
    // route table below stays prefix-agnostic. This works two ways:
    //   - Direct loopback: `127.0.0.1:7430/graph/...` arrives with the prefix.
    //   - Behind `tailscale serve --set-path=/graph`: tailscale already strips
    //     the mount prefix before forwarding, so backend sees the unprefixed
    //     path and the `if` is a no-op.
    let pathname = url.pathname;
    if (pathname.startsWith("/graph/")) {
      pathname = pathname.slice("/graph".length);
    }

    try {
      // ── API ────────────────────────────────────────────────────────────────
      if (req.method === "GET" && pathname === "/api/health") {
        return sendJson(res, 200, {
          ok: true,
          pipelines: pipelinesRef.value.size,
          bus: eventBus.snapshot(),
        });
      }

      if (req.method === "GET" && pathname === "/api/pipelines") {
        await ensurePipelinesLoaded?.();
        const implemented = [...pipelinesRef.value.values()].map((p) => ({
          ...serializePipeline(p),
          historical: false,
        }));
        // Augment with pipeline ids that exist only in flow_runs history — the
        // operator may have run them in the past and then removed the
        // definition file. We show them dimmed so the dashboard surfaces all
        // runs in context, not just the ones whose source still exists.
        const implementedIds = new Set(implemented.map((p) => p.id));
        const runCounts = new Map();
        for (const r of await fetchAllRuns()) {
          const pid = r.stateJson?.pipelineId;
          if (!pid || implementedIds.has(pid)) continue;
          runCounts.set(pid, (runCounts.get(pid) ?? 0) + 1);
        }
        const historical = [...runCounts.entries()].map(([id, runs]) => ({
          id, goal: null, file: null, dir: null,
          nodes: [], edges: [],
          historical: true, runs,
        }));
        return sendJson(res, 200, [...implemented, ...historical]);
      }

      const pipelineMatch = pathname.match(/^\/api\/pipelines\/([^/]+)$/);
      if (req.method === "GET" && pipelineMatch) {
        await ensurePipelinesLoaded?.();
        const p = pipelinesRef.value.get(decodeURIComponent(pipelineMatch[1]));
        if (!p) return sendJson(res, 404, { error: "pipeline not found" });
        return sendJson(res, 200, serializePipeline(p));
      }

      if (req.method === "GET" && pathname === "/api/stats") {
        const { runs, errorsByFlow } = await cachedRunsAndErrors("all");
        return sendJson(res, 200, computeGlobalStats(runs, errorsByFlow));
      }

      // /api/stats/cost?sinceDays=N — cross-pipeline cost roll-up for the
      // home-page sidebar. One disk walk per run (via costForRun); fine
      // at <1k runs, revisit when it bites.
      if (req.method === "GET" && pathname === "/api/stats/cost") {
        const sinceDays = Number(url.searchParams.get("sinceDays") ?? "");
        const sinceMs = Number.isFinite(sinceDays) && sinceDays > 0
          ? Date.now() - sinceDays * 86_400_000
          : null;
        const cost = await costForAllPipelines(await fetchAllRuns(), { sinceMs });
        return sendJson(res, 200, cost);
      }

      // Cost subroute must precede the broader pipelineStats match below —
      // otherwise the `.+$` regex captures "<id>/cost" as the pipelineId
      // and the more specific endpoint is never reached.
      const pipeCostMatch = pathname.match(/^\/api\/stats\/pipelines\/(.+)\/cost$/);
      if (req.method === "GET" && pipeCostMatch) {
        const pipelineId = decodeURIComponent(pipeCostMatch[1]);
        const sinceDays = Number(url.searchParams.get("sinceDays") ?? "");
        const sinceMs = Number.isFinite(sinceDays) && sinceDays > 0
          ? Date.now() - sinceDays * 86_400_000
          : null;
        const priorWindowMs = sinceMs ? Date.now() - sinceMs : null;
        const cost = await costForPipeline(await fetchAllRuns({ pipelineId }), pipelineId, { sinceMs, priorWindowMs });
        return sendJson(res, 200, cost);
      }

      // /api/stats/pipelines/:id/views?sinceDays=N — pipeline-declared
      // view payloads. Each entry is the computed result for one
      // `views[]` declaration (see views.js header for the
      // declaration shape). Empty array when the pipeline declares
      // none — caller skips rendering.
      const pipeViewsMatch = pathname.match(/^\/api\/stats\/pipelines\/(.+)\/views$/);
      if (req.method === "GET" && pipeViewsMatch) {
        const pipelineId = decodeURIComponent(pipeViewsMatch[1]);
        const pipeline = pipelinesRef.value.get(pipelineId);
        if (!pipeline) return sendJson(res, 404, { error: "pipeline not loaded", pipelineId });
        const sinceDays = Number(url.searchParams.get("sinceDays") ?? "");
        const sinceMs = Number.isFinite(sinceDays) && sinceDays > 0
          ? Date.now() - sinceDays * 86_400_000
          : null;
        const views = await computeViews(pipeline, { sinceMs });
        return sendJson(res, 200, { sinceMs, views });
      }

      const pipelineStatsMatch = pathname.match(/^\/api\/stats\/pipelines\/(.+)$/);
      if (req.method === "GET" && pipelineStatsMatch) {
        const pipelineId = decodeURIComponent(pipelineStatsMatch[1]);
        // Pass the pipeline def so the funnel + any future
        // pipeline-declared metrics are read transparently rather than
        // looked up in a hardcoded dashboard-side map.
        const pipeline = pipelinesRef.value.get(pipelineId) ?? null;
        const { runs, errorsByFlow } = await cachedRunsAndErrors("all");
        const mine = runs.filter((r) => (r.stateJson?.pipelineId ?? "") === pipelineId);
        const visitedByFlow = await fetchTreeSummary(mine.map((r) => r.flowId));
        // Lets an older run's invocation under a since-renamed node id
        // still count toward the auto-derived funnel's current stage —
        // see pipeline-registry.js's getNodeLineageBatch.
        const lineageByNodeId = await pipelineRegistryStore.getNodeLineageBatch(pipelineId, Object.keys(pipeline?.nodes ?? {}));
        return sendJson(res, 200, await computePipelineStats(runs, pipelineId, pipeline, errorsByFlow, null, null, visitedByFlow, lineageByNodeId));
      }

      // /api/pipelines/:id/nodes/:nodeId/stats — per-node aggregation for the
      // graph-view click panel. Scans the pipeline's runs and counts how many
      // visited / failed at / blocked at / terminated at this node, plus the
      // most-recent 10 runs that touched it.
      const nodeStatsMatch = pathname.match(/^\/api\/pipelines\/([^/]+)\/nodes\/([^/]+)\/stats$/);
      if (req.method === "GET" && nodeStatsMatch) {
        const pipelineId = decodeURIComponent(nodeStatsMatch[1]);
        const nodeId = decodeURIComponent(nodeStatsMatch[2]);
        const { runs, errorsByFlow } = await cachedRunsAndErrors(`pipeline:${pipelineId}`, { pipelineId });
        const nodeStatsMine = runs.filter((r) => (r.stateJson?.pipelineId ?? "") === pipelineId);
        const nodeStatsVisited = await fetchTreeSummary(nodeStatsMine.map((r) => r.flowId));
        const nodeStatsLineage = await pipelineRegistryStore.getNodeLineageBatch(pipelineId, [nodeId]);
        return sendJson(res, 200, computeNodeStats(runs, pipelineId, nodeId, errorsByFlow, nodeStatsVisited, nodeStatsLineage));
      }

      // /api/search?q=<query>&status=<status>&limit=N — substring
      // search over structured fields + errorEntries[] (flow_node_executions,
      // covers both root and subflow errors) + top-level state keys.
      // `status` is a structured filter applied before the free-text match
      // (exact equality vs r.status). Both q and status are optional;
      // status-only returns every run with that status, snippet-less. See
      // search.js header for design call.
      if (req.method === "GET" && pathname === "/api/search") {
        const q = url.searchParams.get("q") ?? "";
        const status = url.searchParams.get("status") ?? "";
        const limitRaw = url.searchParams.get("limit");
        const limit = limitRaw ? Math.max(1, Math.min(500, Number(limitRaw))) : 50;
        const searchRunsList = await fetchAllRuns();
        const errorsByFlow = await fetchErrors(searchRunsList.map((r) => r.flowId));
        return sendJson(res, 200, searchRuns(searchRunsList, q, { limit, status, errorsByFlow }));
      }

      // /api/runs/:flowId/cost — sum usage.cost.total across every agent
      // session the flow spawned. Disk-walks the agent session JSONLs
      // (same data source as the transcript endpoint). See cost.js for
      // the aggregation shape.
      const runCostMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/cost$/i);
      if (req.method === "GET" && runCostMatch) {
        const cost = await costForRun(runCostMatch[1]);
        return sendJson(res, 200, cost);
      }

      // /api/runs/:flowId/subflows — subflow rows grouped by groupId,
      // shaped like the old stateJson.subflows mirror ({[groupId]:
      // {[subflowKey]: SubflowData}}) so SubflowsCard can consume it
      // without a shape change on the frontend.
      const runSubflowsMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/subflows$/i);
      if (req.method === "GET" && runSubflowsMatch) {
        const flowId = runSubflowsMatch[1];
        const grouped = await fetchSubflows(flowId);
        return sendJson(res, 200, grouped);
      }

      // /api/runs/:flowId/tree — node-invocation tree, the same
      // shape/endpoint companion clients already consume at the gateway
      // level. Backs PathTrail/GraphView's executed-path rendering
      // instead of stateJson._stepHistory.
      const runTreeMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/tree$/i);
      if (req.method === "GET" && runTreeMatch) {
        const items = await fetchTree(runTreeMatch[1]);
        return sendJson(res, 200, { items });
      }

      const runCompareMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/compare$/i);
      if (req.method === "GET" && runCompareMatch) {
        const flow = await fetchFlow(runCompareMatch[1]);
        if (!flow) return sendJson(res, 404, { error: "run not found" });
        const peers = (await fetchAllRuns({ pipelineId: flow.pipelineId })).map(adaptRun);
        return sendJson(res, 200, compareRunToMedian(flow, peers));
      }

      if (req.method === "GET" && pathname === "/api/runs") {
        const statusFilter = url.searchParams.get("status");
        const pipelineFilter = url.searchParams.get("pipelineId");
        // since/until filter by createdAt — used when the home pins a
        // day on the runs/day chart. Lets the table show runs from
        // older days that the default top-200-by-updatedAt window
        // wouldn't otherwise include.
        const sinceParam = parseInt(url.searchParams.get("since") ?? "", 10);
        const untilParam = parseInt(url.searchParams.get("until") ?? "", 10);
        const sinceMs = Number.isFinite(sinceParam) ? sinceParam : null;
        const untilMs = Number.isFinite(untilParam) ? untilParam : null;
        const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "200", 10) || 200, 1000);
        let rows = await fetchAllRuns({ pipelineId: pipelineFilter || undefined, status: statusFilter || undefined });
        if (sinceMs != null) rows = rows.filter((r) => (r.createdAt ?? 0) >= sinceMs);
        if (untilMs != null) rows = rows.filter((r) => (r.createdAt ?? 0) < untilMs);
        rows.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
        rows = rows.slice(0, limit);
        const errorsByFlow = await fetchErrors(rows.map((r) => r.flowId));
        rows = await Promise.all(rows.map((r) => summariseRun(r, errorsByFlow.get(r.flowId) ?? [])));
        return sendJson(res, 200, rows);
      }

      const runMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)$/i);
      if (req.method === "GET" && runMatch) {
        const flow = await fetchFlow(runMatch[1]);
        if (!flow) return sendJson(res, 404, { error: "run not found" });
        return sendJson(res, 200, fullRun(flow, flow.errorEntries ?? []));
      }

      const runEventsMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/events$/i);
      if (req.method === "GET" && runEventsMatch) {
        return sendJson(res, 200, await eventBus.getEventsFor(runEventsMatch[1]));
      }

      const runSummaryMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/summary$/i);
      if (req.method === "GET" && runSummaryMatch) {
        return handleSummary(res, runSummaryMatch[1]);
      }

      const artifactsListMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/artifacts$/i);
      if (req.method === "GET" && artifactsListMatch) {
        return sendArtifactsList(res, artifactsListMatch[1]);
      }

      const transcriptsMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/transcripts$/i);
      if (req.method === "GET" && transcriptsMatch) {
        return sendTranscripts(res, transcriptsMatch[1]);
      }

      const artifactGetMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/artifacts\/(.+)$/i);
      if (req.method === "GET" && artifactGetMatch) {
        return sendArtifact(res, artifactGetMatch[1], decodeURIComponent(artifactGetMatch[2]));
      }

      // ── Actuation: respond to a gate item ───────────────────────────────
      // Delegates to the shared recordItemDecision helper (same write site
      // as TG callback, CLI, and the bridge's /manifold/gate/resolve).
      // POST body: { itemId, decision: "approve"|"reject"|"edit", text? }.
      const approveMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/approve$/i);
      if (req.method === "POST" && approveMatch) {
        return handleApprove(req, res, approveMatch[1]);
      }

      const cancelMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/cancel$/i);
      if (req.method === "POST" && cancelMatch) {
        return handleCancel(req, res, cancelMatch[1]);
      }

      const rerunMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/rerun$/i);
      if (req.method === "POST" && rerunMatch) {
        return handleRerun(req, res, rerunMatch[1]);
      }

      const pauseMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/pause$/i);
      if (req.method === "POST" && pauseMatch) {
        return handlePause(req, res, pauseMatch[1]);
      }

      const resumeMatch = pathname.match(/^\/api\/runs\/([0-9a-f-]+)\/resume$/i);
      if (req.method === "POST" && resumeMatch) {
        return handleResume(req, res, resumeMatch[1]);
      }

      if (req.method === "POST" && pathname === "/api/runs/bulk-cancel") {
        return handleBulkCancel(req, res);
      }

      // ── Static SPA fallback ─────────────────────────────────────────────
      // Live events moved to the WS door (/api/events/ws, attached in
      // start()); there is no HTTP fallback — a client that cannot
      // Upgrade must poll the REST history endpoints instead.
      // Any unmatched GET maps to a file inside `dist/`. `/` and `/index.html`
      // serve the SPA shell; vite-emitted `/assets/<hash>.{js,css}` URLs land
      // here too. Path-traversal guarded by ensuring the resolved absolute
      // path stays inside DIST_DIR.
      if (req.method === "GET") {
        const rel = pathname === "/" ? "index.html" : pathname.slice(1);
        const resolved = path.resolve(DIST_DIR, rel);
        if (!resolved.startsWith(DIST_DIR + path.sep) && resolved !== DIST_DIR) {
          return sendJson(res, 403, { error: "forbidden" });
        }
        return serveFile(res, resolved);
      }

      return sendJson(res, 404, { error: "not found", path: pathname });
    } catch (err) {
      logger?.warn?.(`manifold dashboard handler error on ${pathname}: ${err?.message ?? err}`);
      return sendJson(res, 500, { error: "internal", message: err?.message ?? String(err) });
    }
  }

  async function sendArtifactsList(res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "run not found" });
    const pipelineId = flow.stateJson?.pipelineId;
    const runId = flow.stateJson?.runId;
    if (!pipelineId || !runId) return sendJson(res, 200, []);
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline?._dir) return sendJson(res, 200, []);
    const runDir = path.join(pipeline._dir, "runs", runId);
    try {
      const entries = await readdir(runDir, { withFileTypes: true });
      const out = [];
      for (const ent of entries) {
        if (!ent.isFile()) continue;
        const full = path.join(runDir, ent.name);
        const s = await stat(full);
        out.push({ name: ent.name, size: s.size, mtime: s.mtimeMs });
      }
      return sendJson(res, 200, out);
    } catch (err) {
      if (err.code === "ENOENT") return sendJson(res, 200, []);
      throw err;
    }
  }

  // Sessions are opaque: transcript attribution comes from the session
  // ledger, never from the session's shape.
  async function sendTranscripts(res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "run not found" });
    return sendJson(res, 200, []);
  }

  async function readJsonBody(req, maxBytes = 64 * 1024) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      let total = 0;
      req.on("data", (c) => {
        total += c.length;
        if (total > maxBytes) {
          req.destroy();
          reject(new Error("body too large"));
          return;
        }
        chunks.push(c);
      });
      req.on("end", () => {
        if (total === 0) return resolve({});
        try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf-8"))); }
        catch (err) { reject(err); }
      });
      req.on("error", reject);
    });
  }

  async function handleApprove(req, res, flowId) {
    let body;
    try { body = await readJsonBody(req); }
    catch (err) { return sendJson(res, 400, { error: "invalid JSON body", message: err?.message ?? String(err) }); }
    const { itemId, decision, text } = body ?? {};
    if (typeof itemId !== "string" || !itemId) return sendJson(res, 400, { error: "itemId (string) required" });
    if (!["approve", "reject", "edit"].includes(decision)) return sendJson(res, 400, { error: "decision must be approve|reject|edit" });
    if (decision === "edit" && typeof text !== "string") return sendJson(res, 400, { error: "decision=edit requires text" });
    if (!recordItemDecision) {
      return sendJson(res, 500, { error: "gate resolver unavailable", message: "createDashboardServer was not given recordItemDecision" });
    }

    // Delegates to the shared recordItemDecision helper. Same write site as
    // the CLI's `manifold approve` and the /manifold/gate/resolve route that
    // companion harnesses POST to.
    // Resolution -> state.approvalPending[itemId] = {decision, source: "dashboard"};
    // the existing consolidator advances the flow when all items decide.
    const result = await recordItemDecision({
      flowId,
      itemId,
      decision,
      source: "dashboard",
      editedText: text,
    });
    if (!result.ok) {
      const status = result.code === "flow_not_found" ? 404 : 409;
      return sendJson(res, status, { error: result.code, code: result.code });
    }
    return sendJson(res, 200, { decision, itemId, status: result.status });
  }

  async function handleRerun(req, res, sourceFlowId) {
    if (!executor?.rerun) {
      return sendJson(res, 501, { error: "executor.rerun not available on this dashboard instance" });
    }
    let body;
    try { body = await readJsonBody(req); }
    catch (err) { return sendJson(res, 400, { error: "invalid JSON body", message: err?.message ?? String(err) }); }
    const argsOverride = body && typeof body === "object" && "args" in body ? body.args : undefined;
    if (argsOverride !== undefined && (argsOverride === null || typeof argsOverride !== "object" || Array.isArray(argsOverride))) {
      return sendJson(res, 400, { error: "args must be a JSON object" });
    }
    let record;
    try { record = executor.rerun(sourceFlowId, argsOverride); }
    catch (err) {
      const msg = err?.message ?? String(err);
      // Singleton-guard rejection surfaces as "pipeline already has N active flow(s)";
      // surface as 409 so the client can offer "cancel the active one first".
      if (msg.includes("active flow")) return sendJson(res, 409, { error: msg });
      if (msg.includes("not found")) return sendJson(res, 404, { error: msg });
      logger?.warn?.(`manifold dashboard rerun: ${sourceFlowId.slice(0, 8)} failed: ${msg}`);
      return sendJson(res, 500, { error: "rerun failed", message: msg });
    }
    // Fire the executor loop in the background; the HTTP response returns
    // the new flowId immediately and the WS door drives the dashboard.
    setImmediate(() => {
      Promise.resolve()
        .then(() => ensurePipelinesLoaded?.())
        .then(() => executor.runFlow(record.flowId, "fresh"))
        .catch((err) => logger?.warn?.(`manifold dashboard rerun: runFlow(${record.flowId.slice(0, 8)}) failed: ${err?.message ?? err}`));
    });
    return sendJson(res, 200, {
      flowId: record.flowId,
      pipelineId: record.stateJson?.pipelineId ?? null,
      rerunOf: sourceFlowId,
    });
  }

  async function handleCancel(req, res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "flow not found" });
    if (TERMINAL_STATUS_SET.has(flow.status)) {
      return sendJson(res, 409, { error: `flow is already terminal (status=${flow.status}); nothing to cancel` });
    }
    if (!executor?.cancel) {
      return sendJson(res, 501, { error: "executor.cancel not available on this dashboard instance" });
    }
    let body;
    try { body = await readJsonBody(req); } catch { body = {}; }
    const reason = typeof body?.reason === "string" && body.reason.trim() ? body.reason.trim() : null;
    let result;
    try { result = await executor.cancel(flowId, reason); }
    catch (err) {
      logger?.warn?.(`manifold dashboard cancel: executor.cancel(${flowId.slice(0, 8)}) failed: ${err?.message ?? err}`);
      return sendJson(res, 500, { error: "cancel failed", message: err?.message ?? String(err) });
    }
    if (!result?.cancelled) {
      return sendJson(res, 409, { error: "cancel did not apply", reason: result?.reason ?? "unknown" });
    }
    return sendJson(res, 200, { cancelled: true, wasStatus: flow.status });
  }

  async function handlePause(_req, res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "flow not found" });
    if (flow.status !== "running") {
      return sendJson(res, 409, { error: `flow is ${flow.status}; only running flows can be paused` });
    }
    if (flow.stateJson?._paused) {
      return sendJson(res, 200, { paused: true, already: true });
    }
    if (!writeStore) return sendJson(res, 501, { error: "writeStore not available" });
    const nextState = { ...flow.stateJson, _paused: true };
    const result = await writeStore.setRunning({
      flowId, expectedRevision: flow.revision,
      lastStep: flow.lastStep, stateJson: nextState,
    });
    if (!result?.applied) {
      return sendJson(res, 409, { error: "pause did not apply", reason: result?.reason ?? "revision conflict" });
    }
    return sendJson(res, 200, { paused: true });
  }

  async function handleResume(_req, res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "flow not found" });
    if (flow.status !== "running" || !flow.stateJson?._paused) {
      return sendJson(res, 409, { error: `flow is not paused (status=${flow.status})` });
    }
    if (!writeStore) return sendJson(res, 501, { error: "writeStore not available" });
    const { _paused: _, ...nextState } = flow.stateJson;
    const result = await writeStore.setRunning({
      flowId, expectedRevision: flow.revision,
      lastStep: flow.lastStep, stateJson: nextState,
    });
    if (!result?.applied) {
      return sendJson(res, 409, { error: "resume did not apply", reason: result?.reason ?? "revision conflict" });
    }
    setImmediate(() => {
      executor?.resume?.(flowId)?.catch?.((err) =>
        logger?.warn?.(`manifold dashboard resume: runFlow(${flowId.slice(0, 8)}) failed: ${err?.message ?? err}`)
      );
    });
    return sendJson(res, 200, { resumed: true });
  }

  async function handleBulkCancel(req, res) {
    let body;
    try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { error: "invalid body" }); }
    const { flowIds } = body ?? {};
    const reason = typeof body?.reason === "string" && body.reason.trim() ? body.reason.trim() : null;
    if (!Array.isArray(flowIds) || flowIds.length === 0) {
      return sendJson(res, 400, { error: "flowIds must be a non-empty array" });
    }
    if (!executor?.cancel) return sendJson(res, 501, { error: "executor.cancel not available" });
    const results = await Promise.all(
      flowIds.map(async (flowId) => {
        try {
          const result = await executor.cancel(flowId, reason);
          return { flowId, cancelled: !!result?.cancelled, reason: result?.reason ?? null };
        } catch (err) {
          return { flowId, cancelled: false, reason: err?.message ?? String(err) };
        }
      })
    );
    const cancelled = results.filter((r) => r.cancelled).length;
    return sendJson(res, 200, { cancelled, total: flowIds.length, results });
  }

  async function handleSummary(res, flowId) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "flow not found" });
    const state = flow.stateJson ?? {};
    const pipelineId = state.pipelineId;
    const pipeline = pipelineId ? pipelinesRef.value.get(pipelineId) : null;

    // Try pipeline-declared runAudit.summarize first.
    const audit = pipeline?.runAudit;
    if (audit?.runsDir && typeof audit.summarize === "function" && state.runId) {
      try {
        const raw = await readFile(path.join(audit.runsDir, state.runId, "final-state.json"), "utf-8");
        const finalState = JSON.parse(raw);
        const summary = await Promise.resolve(audit.summarize(finalState));
        return sendJson(res, 200, { flowId, status: flow.status, summary, source: "pipeline" });
      } catch { /* fall through to system summary */ }
    }

    return sendJson(res, 200, { flowId, status: flow.status, summary: await deriveSystemSummary(state, flow, flowId), source: "system" });
  }

  async function deriveSystemSummary(state, flow, flowId) {
    const out = {};

    if (state.startedAt) {
      const endedAt = flow.endedAt ?? flow.updatedAt ?? Date.now();
      out.durationMs = endedAt - state.startedAt;
    }

    const stepsExecuted = stepsExecutedFromTree(await fetchTree(flowId));
    if (stepsExecuted > 0) out.stepsExecuted = stepsExecuted;

    const loop = await fetchSubflows(flowId);
    if (loop && Object.keys(loop).length > 0) {
      let items = 0, successes = 0, failures = 0;
      for (const group of Object.values(loop)) {
        if (!group || typeof group !== "object") continue;
        for (const branch of Object.values(group)) {
          items++;
          if (branch?.doneAt) successes++;
          else if (branch?.failedAt) failures++;
        }
      }
      if (items > 0) {
        out.loopItems = items;
        if (successes > 0) out.loopSuccesses = successes;
        if (failures > 0) out.loopFailures = failures;
      }
    }

    const flowErrors = flow.errorEntries ?? [];
    if (flowErrors.length > 0) {
      out.errorCount = flowErrors.length;
      out.recentErrors = flowErrors.slice(-3).map((e) => ({
        node: e.nodeId,
        message: e.message.slice(0, 120),
      }));
    }

    if (typeof state.cost === "number") out.cost = state.cost;

    return out;
  }

  async function sendArtifact(res, flowId, name) {
    const flow = await fetchFlow(flowId);
    if (!flow) return sendJson(res, 404, { error: "run not found" });
    const pipelineId = flow.stateJson?.pipelineId;
    const runId = flow.stateJson?.runId;
    if (!pipelineId || !runId) return sendJson(res, 404, { error: "no run artifacts" });
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline?._dir) return sendJson(res, 404, { error: "pipeline dir unknown" });
    const runDir = path.join(pipeline._dir, "runs", runId);
    const resolved = path.resolve(runDir, name);
    // Path-traversal guard: resolved must live inside runDir.
    if (!resolved.startsWith(runDir + path.sep)) {
      return sendJson(res, 403, { error: "forbidden" });
    }
    return serveFile(res, resolved);
  }

  return {
    async start() {
      if (server) return;

      server = http.createServer((req, res) => { handle(req, res); });
      door = attachWsEndpoint(server, { match: wsPathMatch, logger });
      await new Promise((resolve, reject) => {
        const onErr = (err) => { server = null; reject(err); };
        server.once("error", onErr);
        server.listen(port, host, () => {
          server.off("error", onErr);
          resolve();
        });
      });
      unsubscribe = eventBus.subscribe(dispatchWs);
      logger?.info?.(`manifold dashboard: listening on http://${host}:${port}`);
    },
    async stop() {
      if (unsubscribe) { unsubscribe(); unsubscribe = null; }
      if (door) { door.close(); door = null; }
      if (server) {
        await new Promise((resolve) => server.close(() => resolve()));
        server = null;
      }
    },
    url() { return `http://${host}:${port}`; },
  };
}

// ─── helpers ─────────────────────────────────────────────────────────────────

async function summariseRun(r, runErrors = []) {
  return {
    flowId: r.flowId,
    pipelineId: r.pipelineId ?? r.stateJson?.pipelineId ?? null,
    runId: r.runId ?? r.stateJson?.runId ?? null,
    status: r.status,
    lastStep: resolveDisplayStep(r, runErrors),
    revision: r.revision,
    statusReason: r.statusReason ?? null,
    waitKind: r.waitJson?.kind ?? null,
    createdAt: r.createdAt ?? null,
    updatedAt: r.updatedAt ?? null,
    endedAt: r.endedAt ?? null,
    errorsCount: runErrors.length,
    // Derive compact subflow summary from the pre-aggregated child counts.
    subflows: summariseSubflowRollup(r),
  };
}

// Compact subflow summary from the pre-aggregated child counts merged onto
// each list row (summarizeChildrenByFlowIds) — avoids N+1 subflow queries.
function summariseSubflowRollup(r) {
  const total = r.subflowTotal ?? 0;
  if (total === 0) return null;
  const failed   = r.subflowFailed   ?? 0;
  const awaiting = r.subflowPending  ?? 0;
  const done     = (r.subflowSucceeded ?? 0) + (r.subflowSkipped ?? 0);
  const running  = Math.max(0, total - done - failed - awaiting);
  return [{ groupId: "summary", total, done, awaiting, failed, running }];
}

function fullRun(r, runErrors = []) {
  // Strip `errors` — failure_events is now the sole store for error history
  // ("Eradicate soft errors" Phase 1.5); errorEntries below (runErrors,
  // read from failureStore) is the normalized replacement. Only historical rows persisted before that
  // redirect still carry a baked-in errors[] in their stored stateJson.
  const { errors: _errors, ...stateWithoutLegacy } = r.stateJson ?? {};
  return {
    flowId: r.flowId,
    pipelineId: r.stateJson?.pipelineId ?? null,
    runId: r.stateJson?.runId ?? null,
    status: r.status,
    lastStep: resolveDisplayStep(r, runErrors),
    revision: r.revision,
    statusReason: r.statusReason ?? null,
    stateJson: stateWithoutLegacy,
    waitJson: r.waitJson ?? null,
    // Normalized error history sourced from flow_node_executions — sibling
    // to stateJson (not nested inside it) so flow-level and subflow-level
    // shapes stay symmetric.
    errorEntries: runErrors,
    createdAt: r.createdAt ?? null,
    updatedAt: r.updatedAt ?? null,
    endedAt: r.endedAt ?? null,
  };
}

async function serveFile(res, absPath) {
  try {
    const body = await readFile(absPath);
    const ext = path.extname(absPath).toLowerCase();
    const type = MIME[ext] ?? "application/octet-stream";
    res.writeHead(200, { "Content-Type": type, "Cache-Control": "no-cache" });
    res.end(body);
  } catch (err) {
    if (err.code === "ENOENT") return sendJson(res, 404, { error: "not found" });
    return sendJson(res, 500, { error: "internal", message: err?.message ?? String(err) });
  }
}

function sendJson(res, code, body) {
  const buf = Buffer.from(JSON.stringify(body));
  res.writeHead(code, { "Content-Type": MIME[".json"], "Cache-Control": "no-cache" });
  res.end(buf);
}
