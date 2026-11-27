import { getNodeExecutionsStore } from "../state/node-executions.js";
import { getAllSubflowGroups } from "../state/subflow-state.js";
import { assembleState } from "../engine/executor-helpers.js";
import { authorizeBearerRequest, sendUnauthorized } from "./bearer-auth.js";
import { missingIdentityFields, refuseMissingIdentity } from "./operator.js";

export function registerRunRoutes({
  api,
  readStore,
  costForAllPipelines,
  runStore,
  pipelinesRef,
  pipelineRegistryStore,
  rerunElevated,
  discussStore,
  failureStore,
  sessionIndex,
  handleDiscuss,
  handleDiscussHistory,
  handleDiscussApplyDraft,
  handleDiscussAudio,
  handleDiscussAudioGet,
  handleDiscussAudioRetry,
}) {

  api.registerHttpRoute({
    path: "/manifold/cost",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const sinceMsRaw = url.searchParams.get("sinceMs");
      const sinceMs = sinceMsRaw ? Number(sinceMsRaw) : null;
      if (sinceMsRaw && (!Number.isFinite(sinceMs) || sinceMs < 0)) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "invalid sinceMs" }));
        return true;
      }
      try {
        const runs = await readStore.listCostSnapshot();
        const cost = await costForAllPipelines(runs, { sinceMs });
        const total = cost.pipelines.reduce((s, p) => s + (p.total || 0), 0);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({
          sinceMs: cost.sinceMs,
          untilMs: Date.now(),
          total,
          pipelines: cost.pipelines,
        }));
      } catch (err) {
        api.logger.warn(`manifold cost handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/runs",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const limitRaw = url.searchParams.get("limit");
      const limit = Math.max(1, Math.min(500, Number(limitRaw) || 50));
      const pipelineIdFilter = url.searchParams.get("pipelineId");
      const beforeMs = url.searchParams.has("before_ms")
        ? parseInt(url.searchParams.get("before_ms"), 10) : null;
      const beforeId = url.searchParams.get("before_id") ?? null;
      try {
        const statusParam = url.searchParams.get("status");
        const statusCounts = await readStore.listStatusCounts({ pipelineId: pipelineIdFilter });
        const totalKnown = statusParam
          ? (statusCounts[statusParam] ?? 0)
          : Object.values(statusCounts).reduce((s, n) => s + n, 0);
        // limit+1 so hasMore detection needs no extra COUNT query.
        const page = await readStore.listRunsPage({
          pipelineId: pipelineIdFilter,
          status: statusParam,
          limit: limit + 1,
          beforeMs,
          beforeId,
        });
        const hasMore = page.length > limit;
        if (hasMore) page.pop();
        const last = page[page.length - 1];
        const discussSummaries = await discussStore.getBulkSummaries(page.map((r) => r.flowId));
        const childSummaryMap = await getNodeExecutionsStore().summarizeChildrenByFlowIds(page.map((r) => r.flowId));
        const EMPTY_SUBFLOWS = { total: 0, pending: 0, failed: 0, skipped: 0, succeeded: 0 };
        const runs = page.map((r) => {
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
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({
          runs,
          hasMore,
          totalKnown,
          statusCounts,
          nextBeforeMs: last?.createdAtMs ?? null,
          nextBeforeId: last?.flowId ?? null,
        }));
      } catch (err) {
        api.logger.warn(`manifold runs handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/errors",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const raw = url.searchParams.get("flowIds") ?? "";
      const flowIds = raw.split(",").map((s) => s.trim()).filter(Boolean).slice(0, 500);
      if (!flowIds.length) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flowIds required" }));
        return true;
      }
      try {
        const errorsMap = await failureStore.queryErrorsByFlowIds(flowIds);
        const errors = {};
        for (const [flowId, entries] of errorsMap) errors[flowId] = entries;
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ errors }));
      } catch (err) {
        api.logger.warn(`manifold errors handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/queue",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      try {
        // Deliberately lean — no bulk discuss/subflow-summary fetches (those
        // mean nothing for a queued item's glance view).
        const items = (await readStore.listQueued()).map((r) => ({
          flowId: r.flowId,
          pipelineId: r.pipelineId,
          parentFlowId: r.parentFlowId,
          nodeId: r.nodeId,
          subflowKey: r.subflowKey,
          status: "queued",
          currentStep: r.currentStep,
          lastStep: r.lastStep,
          statusReason: r.statusReason,
          createdAtMs: r.createdAt,
          updatedAtMs: r.updatedAt,
        }));
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ items }));
      } catch (err) {
        api.logger.warn(`manifold queue handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // Batched "which top-level nodes did each flow visit" for the dashboard
  // auto-funnel, without one round-trip per run. Same shape as /manifold/errors
  // (comma-separated flowIds, capped 500); top-level rather than nested under
  // /manifold/runs/ because that prefix route below would swallow it.
  api.registerHttpRoute({
    path: "/manifold/tree-summary",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const raw = url.searchParams.get("flowIds") ?? "";
      const flowIds = raw.split(",").map((s) => s.trim()).filter(Boolean).slice(0, 500);
      if (!flowIds.length) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flowIds required" }));
        return true;
      }
      try {
        const visitedMap = await getNodeExecutionsStore().listVisitedNodeIdsByFlow(flowIds);
        const summaries = {};
        for (const [flowId, nodeIds] of visitedMap) summaries[flowId] = { nodeIds: [...nodeIds] };
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ summaries }));
      } catch (err) {
        api.logger.warn(`manifold tree-summary handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/runs/",
    auth: "plugin",
    match: "prefix",
    handler: async (req, res) => {
      if (req.method !== "GET" && req.method !== "POST") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET, POST");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const tail = url.pathname.slice("/manifold/runs/".length);
      const segments = tail.split("/").filter(Boolean);
      const flowId = segments[0];
      if (!flowId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flowId required" }));
        return true;
      }
      const flow = await readStore.get(flowId);
      if (!flow) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flow not found", flowId }));
        return true;
      }
      try {
        if (segments.length === 1 && req.method === "GET") {
          return await sendFullRun(res, flow);
        }
        if (segments[1] === "artifacts" && segments.length === 2 && req.method === "GET") {
          return await sendArtifactsList(res, flow);
        }
        if (segments[1] === "artifacts" && segments.length >= 3 && req.method === "GET") {
          const name = decodeURIComponent(segments.slice(2).join("/"));
          return await sendArtifactFile(res, flow, name);
        }
        if (segments[1] === "subflows" && segments.length === 2 && req.method === "GET") {
          return await sendSubflowsList(res, flow);
        }
        if (segments[1] === "tree" && segments.length === 2 && req.method === "GET") {
          return await sendTreeRoot(res, flow);
        }
        if (segments[1] === "tree" && segments[2] === "expand" && segments.length === 3 && req.method === "GET") {
          return await sendTreeExpand(res, flow, url);
        }
        if (segments[1] === "tree" && segments[2] === "invocation" && segments.length === 4 && req.method === "GET") {
          return await sendInvocationDetail(res, flow, decodeURIComponent(segments[3]));
        }
        if (segments[1] === "transcripts" && segments.length === 2 && req.method === "GET") {
          return await sendTranscriptsList(res, flow, url);
        }
        if (segments[1] === "compare" && segments.length === 2 && req.method === "GET") {
          return await sendCompareToPeers(res, flow);
        }
        if (segments[1] === "rerun" && segments.length === 2 && req.method === "POST") {
          return await handleRerun(req, res, flow);
        }
        if (segments[1] === "discuss" && segments.length === 2 && req.method === "POST") {
          return await handleDiscuss(req, res, flow);
        }
        if (segments[1] === "discuss" && segments.length === 2 && req.method === "GET") {
          return await handleDiscussHistory(req, res, flow);
        }
        if (segments[1] === "discuss" && segments[2] === "apply-draft"
            && segments.length === 3 && req.method === "POST") {
          return await handleDiscussApplyDraft(req, res, flow);
        }
        if (segments[1] === "discuss" && segments[2] === "audio"
            && segments.length === 3 && req.method === "POST") {
          return await handleDiscussAudio(req, res, flow);
        }
        if (segments[1] === "discuss" && segments[2] === "audio"
            && segments.length === 4 && req.method === "GET") {
          return await handleDiscussAudioGet(req, res, flow, decodeURIComponent(segments[3]));
        }
        if (segments[1] === "discuss" && segments[2] === "audio" && segments[3] === "retry"
            && segments.length === 4 && req.method === "POST") {
          return await handleDiscussAudioRetry(req, res, flow);
        }
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "unknown subpath or method" }));
        return true;
      } catch (err) {
        api.logger.warn(`manifold run-detail handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
        return true;
      }
    },
  });

  async function sendFullRun(res, flow) {
    const nodeExecutionsStore = getNodeExecutionsStore();
    // assembleState (resultJson replayed over live stateJson), matching how
    // the engine's dispatch loop and routes/discuss.js read state — the raw
    // stateJson blob alone is not enriched.
    const enrichedState = await assembleState(flow.flowId, {
      runStore, nodeExecutionsStore, baseState: flow.stateJson ?? {},
    });
    // .gates must NEVER come from assembleState()'s per-key merge: every
    // human-gate node writes a FULL SNAPSHOT of state.gates into its own
    // resultJson, so replaying rows in invokedAt order can resolve to a stale
    // snapshot if a later gate opens out of order. gate_records (one row per
    // gate, no merge) is the staleness-safe source.
    const gates = await nodeExecutionsStore.listGateRecordsForFlow(flow.flowId);
    // Strip `loop` (served via GET .../subflows) and `errors`
    // (failure_events/errorEntries is the sole error-history store — errors[]
    // never rides the wire).
    const { loop: _loop, errors: _errors, gates: _staleGates, ...stateWithoutLegacy } = enrichedState;
    // A subflow child is a flow_runs row with parent_invocation_id set —
    // resolve it back to the parent's flowId/nodeId/subflowKey for a "Parent
    // run" link, plus the child's REAL top-level pipelineId (its own
    // stateJson.pipelineId is the synthetic "parent::NODE_ID" form loader.js
    // registers subflow nodes under).
    let parentFlowId = null, parentNodeId = null, parentSubflowKey = null;
    let resolvedPipelineId = flow.stateJson?.pipelineId ?? null;
    if (flow.parentInvocationId) {
      const invocation = await nodeExecutionsStore.getInvocation(flow.parentInvocationId);
      if (invocation) {
        parentFlowId = invocation.flowId;
        parentNodeId = invocation.nodeId;
        parentSubflowKey = invocation.subflowKey;
        const parentFlow = await readStore.get(parentFlowId);
        if (parentFlow) resolvedPipelineId = parentFlow.pipelineId ?? resolvedPipelineId;
      }
    }
    const payload = {
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
      // Normalized error history from failure_events — sibling to stateJson
      // (not nested) so flow-level and subflow-level shapes stay symmetric.
      errorEntries: await failureStore.queryErrorsByFlow(flow.flowId),
      createdAtMs: flow.createdAt ?? null,
      updatedAtMs: flow.updatedAt ?? null,
      endedAtMs: flow.endedAt ?? null,
      parentFlowId,
      nodeId: parentNodeId,
      subflowKey: parentSubflowKey,
    };
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify(payload));
    return true;
  }

  async function sendArtifactsList(res, flow) {
    const { readdir, stat } = await import("node:fs/promises");
    const pathMod = await import("node:path");
    const pipelineId = flow.stateJson?.pipelineId;
    const runId = flow.stateJson?.runId;
    if (!pipelineId || !runId) {
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ artifacts: [] }));
      return true;
    }
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline?._dir) {
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ artifacts: [] }));
      return true;
    }
    const runDir = pathMod.join(pipeline._dir, "runs", runId);
    try {
      const entries = await readdir(runDir, { withFileTypes: true });
      const out = [];
      for (const ent of entries) {
        if (!ent.isFile()) continue;
        const full = pathMod.join(runDir, ent.name);
        const s = await stat(full);
        out.push({ name: ent.name, size: s.size, mtimeMs: s.mtimeMs });
      }
      out.sort((a, b) => a.name.localeCompare(b.name));
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ artifacts: out }));
    } catch (err) {
      if (err.code === "ENOENT") {
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ artifacts: [] }));
        return true;
      }
      throw err;
    }
    return true;
  }

  async function sendArtifactFile(res, flow, name) {
    const { readFile, stat } = await import("node:fs/promises");
    const pathMod = await import("node:path");
    const pipelineId = flow.stateJson?.pipelineId;
    const runId = flow.stateJson?.runId;
    if (!pipelineId || !runId) {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "no run artifacts" }));
      return true;
    }
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline?._dir) {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "pipeline dir unknown" }));
      return true;
    }
    const runDir = pathMod.join(pipeline._dir, "runs", runId);
    const resolved = pathMod.resolve(runDir, name);
    if (!resolved.startsWith(runDir + pathMod.sep)) {
      res.statusCode = 403;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "forbidden" }));
      return true;
    }
    try {
      const s = await stat(resolved);
      if (!s.isFile()) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "not a file" }));
        return true;
      }
      if (s.size > 5 * 1024 * 1024) {
        res.statusCode = 413;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "artifact too large", size: s.size }));
        return true;
      }
      const body = await readFile(resolved);
      const ext = pathMod.extname(resolved).toLowerCase();
      const mime =
        ext === ".json"  ? "application/json"     :
        ext === ".jsonl" ? "application/x-ndjson" :
        ext === ".md"    ? "text/markdown"        :
        ext === ".txt"   ? "text/plain"           :
        ext === ".log"   ? "text/plain"           :
        ext === ".html"  ? "text/html"            :
        ext === ".csv"   ? "text/csv"             :
                           "application/octet-stream";
      res.statusCode = 200;
      res.setHeader("Content-Type", mime);
      res.setHeader("Content-Length", String(s.size));
      res.end(body);
    } catch (err) {
      if (err.code === "ENOENT") {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "not found" }));
        return true;
      }
      throw err;
    }
    return true;
  }

  async function sendSubflowsList(res, flow) {
    const grouped = await getAllSubflowGroups({
      flowId: flow.flowId, runStore, nodeExecutionsStore: getNodeExecutionsStore(),
    });
    const subflows = Object.values(grouped).flatMap((group) => Object.values(group));
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ subflows }));
    return true;
  }

  // Node tree: unifies this flow's own node timeline (Steps) with the
  // child-flow fan-out (Subflows) into one recursively-expandable tree.
  // type:"node" — one row per invocation (not per attempt); expandable only
  //   when kind==="subflow" with at least one child.
  // type:"flow" — one row per child flow spawned from a subflow-kind node;
  //   expandable once it has dispatched a node of its own.
  // Expanding a "node" returns "flow" children; expanding a "flow" returns
  // that flow's own "node" list (same shape /tree itself returns).
  async function buildTreeNodeRows(flow) {
    const pipelineId = flow.stateJson?.pipelineId;
    const nodeExecutionsStore = getNodeExecutionsStore();
    const invocations = await nodeExecutionsStore.listInvocationsForFlow(flow.flowId);
    // A node's own status can only be told apart from a failed one by
    // cross-referencing failure_events (flow_node_executions carries no error
    // content). A subflow item's own errors live under ITS OWN flow_id, not
    // this one's, so this set naturally only matches root-level invocations.
    const failedExecutionIds = new Set(
      (await failureStore.queryErrorsByFlow(flow.flowId)).map((e) => e.executionId).filter(Boolean)
    );

    // Point-in-time kind/agent per invocation: a node whose pipeline
    // definition has since changed — or been deleted — still renders its
    // ORIGINAL kind/agent, keyed by invokedAt rather than the live single-copy
    // definition. Batched in one call, not one per invocation.
    const versionRequests = invocations.map((inv) => ({ pipelineId, nodeName: inv.nodeId, atMs: inv.invokedAt }));
    const versions = await pipelineRegistryStore.getNodeVersionsBatch(versionRequests);
    const versionByInvocationId = new Map(invocations.map((inv, i) => [inv.invocationId, versions[i]]));

    // A subflow-kind row's OWN invocation (subflow_key=null) is never a
    // child's parent_invocation_id — each branch item gets its OWN separate
    // invocation keyed by the item's own subflow_key. Resolve those per-nodeId
    // via listInvocationIdsForNode, batched once per distinct nodeId.
    const subflowNodeIds = [
      ...new Set(invocations.filter((inv) => versionByInvocationId.get(inv.invocationId)?.kind === "subflow").map((inv) => inv.nodeId)),
    ];
    const childrenByNodeId = new Map();
    if (subflowNodeIds.length > 0) {
      const invIdsByNodeId = await nodeExecutionsStore.listInvocationIdsForNodes(flow.flowId, subflowNodeIds);
      const allInvIds = [...invIdsByNodeId.values()].flat();
      const allChildren = await runStore.listForParentInvocations(allInvIds);
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
      // A node's execution row carries no success/failure signal; absent a
      // failedExecutionId, recover status only from the FLOW's status — which
      // applies solely to the LAST invocation (invocations ascend by
      // invoked_at; an earlier node can't be mid-dispatch or skipped). No
      // isTerminal gate: gating would drop a terminal flow's real status from
      // its own last node whenever that execution recorded no error,
      // defaulting a failed flow to "succeeded".
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
        // execKind is the LATEST execution only — for a subflow node with
        // finished children that's always "recheck", masking an earlier
        // retry/revival. Independent signal, not a replacement for execKind.
        everRetriedOrRevived: inv.everRetriedOrRevived,
        hasChildren: isSubflowNode && kids.length > 0,
      };
      if (isSubflowNode) row.rollup = bucketChildFlowsByStatus(kids);
      return row;
    });
  }

  // Shallow rollup (immediate children only, not a recursive subtree walk) —
  // same status-bucket shape summarizeChildrenByFlowIds already uses.
  function bucketChildFlowsByStatus(childFlowRows) {
    const rollup = { succeeded: 0, failed: 0, pending: 0, skipped: 0, cancelled: 0 };
    for (const c of childFlowRows) {
      if (c.status === "succeeded") rollup.succeeded++;
      else if (c.status === "failed") rollup.failed++;
      else if (c.status === "skipped") rollup.skipped++;
      else if (c.status === "cancelled") rollup.cancelled++;
      else rollup.pending++; // running | blocked | queued
    }
    return rollup;
  }

  // The human-readable "why" behind a non-succeeded child flow: a human-gate
  // wait carries a pipeline-authored title in
  // waitJson.items[0].requestPayload.title — far more useful than the generic
  // statusReason ("awaiting 1 approval at GAP_GATE") next to it. Every other
  // wait kind, and failures, fall back to statusReason (the executor's own
  // short human summary).
  function extractAttentionReason(child) {
    if (child.waitJson?.kind === "human-gate") {
      const title = child.waitJson.items?.[0]?.requestPayload?.title;
      if (typeof title === "string" && title) return title;
    }
    return child.statusReason ?? null;
  }

  // Maps child flow_runs rows (spawned from one subflow-kind node's
  // invocation) into the tree's type:"flow" row shape.
  async function buildTreeFlowRows(childFlowRows) {
    const nodeExecutionsStore = getNodeExecutionsStore();
    const withInvocations = await nodeExecutionsStore.hasInvocations(childFlowRows.map((c) => c.flowId));
    const derived = await Promise.all(childFlowRows.map((child) => perChildDerivedFields(child, nodeExecutionsStore)));
    return childFlowRows.map((child, idx) => ({
      type: "flow",
      flowId: child.flowId,
      status: child.status,
      startedAt: child.createdAt,
      updatedAt: child.updatedAt,
      endedAt: child.endedAt,
      display: child.stateJson?._display ?? null,
      reason: child.status === "succeeded" ? null : extractAttentionReason(child),
      hasChildren: withInvocations.has(child.flowId),
      cascadeAttempt: derived[idx].cascadeAttempt,
      // Deliberately per-child, not rolled onto the parent's row: a spawned
      // child can retry at its own dispatch level without the subflow node
      // that spawned it ever retrying (different table, no execKind there).
      everRetriedOrRevived: derived[idx].everRetriedOrRevived,
    }));
  }

  // One listInvocationsForFlow per child, shared by both derived fields —
  // cascadeAttempt only needs it for cascade children (short-circuiting
  // otherwise), everRetriedOrRevived needs it unconditionally.
  async function perChildDerivedFields(child, nodeExecutionsStore) {
    const invocations = await nodeExecutionsStore.listInvocationsForFlow(child.flowId);
    return {
      cascadeAttempt: await cascadeAttemptFor(child, invocations),
      everRetriedOrRevived: invocations.some((inv) => inv.everRetriedOrRevived),
    };
  }

  // A cascade-kind pipeline retries its root nodes in sequence until one
  // succeeds; each attempt gets its own invocation row, so >1 rows means the
  // first node did NOT resolve it — worth flagging even on eventual success.
  // Non-cascade pipelines return null: ordinary sequential nodes would
  // false-positive if this weren't gated on the declared kind. Point-in-time
  // (pipeline registry keyed by the child's createdAt, not live definitions).
  async function cascadeAttemptFor(child, invocations) {
    const version = await pipelineRegistryStore.getPipelineVersionAt({
      pipelineId: child.stateJson?.pipelineId, atMs: child.createdAt,
    });
    if (version?.kind !== "cascade") return null;
    const attempts = invocations.length;
    return attempts > 1 ? attempts : null;
  }

  async function sendTreeRoot(res, flow) {
    const items = await buildTreeNodeRows(flow);
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ items }));
    return true;
  }

  async function sendTreeExpand(res, flow, url) {
    const type = url.searchParams.get("type");
    if (type === "node") {
      const invocationId = url.searchParams.get("invocationId");
      if (!invocationId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "invocationId required for type=node" }));
        return true;
      }
      const invocation = await getNodeExecutionsStore().getInvocation(invocationId);
      if (!invocation || invocation.flowId !== flow.flowId) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "invocation not found on this flow" }));
        return true;
      }
      // invocationId here is the subflow-kind node's OWN dispatch
      // (subflow_key=null), never a child's parent_invocation_id — resolve
      // the real per-item invocations for this nodeId first.
      const itemInvocationIds = await getNodeExecutionsStore().listInvocationIdsForNode(flow.flowId, invocation.nodeId);
      const children = await runStore.listForParentInvocations(itemInvocationIds);
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: await buildTreeFlowRows(children) }));
      return true;
    }
    if (type === "flow") {
      const childFlowId = url.searchParams.get("childFlowId");
      if (!childFlowId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "childFlowId required for type=flow" }));
        return true;
      }
      const childFlow = await readStore.get(childFlowId);
      if (!childFlow) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "child flow not found", childFlowId }));
        return true;
      }
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: await buildTreeNodeRows(childFlow) }));
      return true;
    }
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: false, error: `invalid type "${type}" — expected "node" or "flow"` }));
    return true;
  }

  // Full execution-attempt history for one invocation (rows with
  // executionCount > 1 link here).
  async function sendInvocationDetail(res, flow, invocationId) {
    const invocation = await getNodeExecutionsStore().getInvocation(invocationId);
    if (!invocation || invocation.flowId !== flow.flowId) {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "invocation not found on this flow" }));
      return true;
    }
    const version = await pipelineRegistryStore.getNodeVersionAt({
      pipelineId: flow.stateJson?.pipelineId, nodeName: invocation.nodeId, atMs: invocation.invokedAt,
    });
    const rawExecutions = await getNodeExecutionsStore().listExecutionsForInvocation(invocationId);
    // Per-attempt error content lives in failure_events — at most one row per
    // execution_id (upsert conflict target), so this is a clean 1:1 lookup.
    const messageByExecutionId = new Map(
      (await failureStore.queryErrorsByFlow(flow.flowId))
        .filter((e) => e.invocationId === invocationId)
        .map((e) => [e.executionId, e.message])
    );
    const executions = rawExecutions.map((e) => ({ ...e, error: messageByExecutionId.get(e.executionId) ?? null }));
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({
      invocationId: invocation.id,
      nodeId: invocation.nodeId,
      kind: version?.kind ?? null,
      agent: version?.agent ?? null,
      subflowKey: invocation.subflowKey,
      visitSeq: invocation.visitSeq,
      executions,
    }));
    return true;
  }

  // Transcripts read manifold's own stores only: discuss threads from
  // discuss-store, dispatch agent turns from node-executions resultJson
  // (a single assistant turn when the result carries reply text — content
  // is never invented). Attribution uses stored columns; the session id
  // passes through opaquely and is never parsed.
  // lite=1 → turnCount only (list views); session=X → one full transcript.
  async function sendTranscriptsList(res, flow, url) {
    const lite       = url.searchParams.get("lite") === "1";
    const singleSession = url.searchParams.get("session") ?? null;

    const sessionRows = await Promise.resolve(sessionIndex?.listForFlow?.(flow.flowId)).catch(() => []) ?? [];
    const discussThreads = await Promise.resolve(discussStore?.listForFlow?.(flow.flowId)).catch(() => []) ?? [];
    const nodeExecutionsStore = getNodeExecutionsStore();
    const results = await Promise.resolve(nodeExecutionsStore.listResultsForFlow(flow.flowId)).catch(() => []);
    const resultByNode = new Map((results ?? []).map((r) => [r?.nodeId, r]));
    // Agent display name from the loaded pipeline's node def; null when the
    // pipeline is not loaded — never guessed from key shape.
    const pipelineNodes = pipelinesRef?.value?.get?.(flow.stateJson?.pipelineId)?.nodes ?? {};

    const transcripts = [];
    for (const row of sessionRows) {
      // Discuss identity rows (branchItemId set) are covered by the discuss
      // loop below, which joins them to full turns from discuss-store.
      if (row.branchItemId != null) continue;
      if (singleSession && row.session !== singleSession) continue;
      const result = row.nodeId != null ? resultByNode.get(row.nodeId) : null;
      const turns = lite ? null : dispatchTurns(result);
      transcripts.push({
        session: row.session, agent: pipelineNodes?.[row.nodeId]?.agent ?? null,
        nodeId: row.nodeId ?? null, itemKey: row.branchItemId ?? null,
        startedAt: row.spawnedAt ?? null,
        updatedAt: result?.resultWrittenAt ?? row.spawnedAt ?? null,
        ...(lite ? { turnCount: turnsForCount(result) } : { turns }),
      });
    }
    for (const thread of discussThreads) {
      // A thread predating session-index recording has turns but no stored
      // identity row — it still lists, with null identity fields.
      let identity = null;
      for (const row of sessionRows) {
        if (row.branchItemId === thread.itemId && (!identity || (row.spawnedAt ?? 0) > (identity.spawnedAt ?? 0))) identity = row;
      }
      const session = identity?.session ?? null;
      if (singleSession && session !== singleSession) continue;
      const turns = lite ? null : discussTurns(thread.messages);
      transcripts.push({
        session, agent: null,
        nodeId: identity?.nodeId ?? "discuss", itemKey: thread.itemId ?? null,
        startedAt: thread.messages?.[0]?.ts ?? thread.updatedAt ?? null,
        updatedAt: thread.updatedAt ?? null,
        ...(lite ? { turnCount: Array.isArray(thread.messages) ? thread.messages.length : 0 } : { turns }),
      });
    }
    transcripts.sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ transcripts }));
    return true;
  }

  // Stored discuss messages use operator/agent roles; the transcript wire
  // shape (and its dashboard renderer) uses user/assistant.
  function discussTurns(messages) {
    const turns = [];
    if (!Array.isArray(messages)) return turns;
    for (const m of messages) {
      const { text, truncated } = capText(typeof m?.text === "string" ? m.text : "");
      turns.push({
        role: m?.role === "agent" ? "assistant" : "user",
        text, thinking: null, truncated, timestamp: m?.ts ?? null,
      });
    }
    return turns;
  }

  // The only dispatch content observed durably is resultJson: a bare string,
  // or an object carrying reply text in `text`. Anything else yields no turn
  // — the entry lists with metadata only.
  function dispatchTurns(result) {
    const text = dispatchTurnText(result?.resultJson);
    if (text == null) return [];
    const capped = capText(text);
    return [{
      role: "assistant", text: capped.text, thinking: null,
      truncated: capped.truncated, timestamp: result?.resultWrittenAt ?? null,
    }];
  }

  function turnsForCount(result) {
    return dispatchTurnText(result?.resultJson) == null ? 0 : 1;
  }

  function dispatchTurnText(resultJson) {
    if (typeof resultJson !== "string") return null;
    let parsed = resultJson;
    try { parsed = JSON.parse(resultJson); } catch { return null; }
    if (typeof parsed === "string") return parsed;
    if (parsed && typeof parsed === "object" && typeof parsed.text === "string") return parsed.text;
    return null;
  }

  function capText(text) {
    const cap = 8 * 1024;
    if (text.length <= cap) return { text, truncated: false };
    return { text: text.slice(0, cap), truncated: true };
  }

  async function sendCompareToPeers(res, flow) {
    const pipelineId = flow.stateJson?.pipelineId ?? "";
    const peers = await readStore.listPeers({ pipelineId, excludeFlowId: flow.flowId });
    const comparison = peers.length === 0 ? null : computeCompareToMedian(flow, peers);
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, comparison }));
    return true;
  }

  function computeCompareToMedian(run, peers) {
    const dur = (r) => {
      const end = r?.endedAt ?? r?.updatedAt ?? null;
      const start = r?.createdAt ?? null;
      return end != null && start != null ? end - start : null;
    };
    const median = (arr) => {
      const xs = arr.filter((v) => v != null && v >= 0).sort((a, b) => a - b);
      if (xs.length === 0) return null;
      const mid = Math.floor(xs.length / 2);
      return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
    };
    const peerDurations = peers.map(dur);
    const medianMs = median(peerDurations);
    const peerRevs = peers.map((r) => r.revision ?? 0);
    const medianRev = median(peerRevs);
    const runDur = dur(run);
    return {
      peerCount: peers.length,
      durationMs: runDur,
      durationMedianMs: medianMs,
      durationRatio: runDur != null && medianMs ? runDur / medianMs : null,
      revision: run.revision ?? 0,
      revisionMedian: medianRev,
      revisionRatio: medianRev != null && medianRev > 0 ? (run.revision ?? 0) / medianRev : null,
    };
  }

  async function handleRerun(req, res, sourceFlow) {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    let body;
    try { body = raw ? JSON.parse(raw) : {}; }
    catch { res.statusCode = 400; res.end("invalid json"); return true; }
    const argsOverride = (body && typeof body === "object" && "args" in body) ? body.args : undefined;
    if (argsOverride !== undefined && (argsOverride === null || typeof argsOverride !== "object" || Array.isArray(argsOverride))) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "args must be a JSON object" }));
      return true;
    }
    try {
      const missingIdentity = missingIdentityFields(body, ["session", "agentId"]);
      if (missingIdentity.length > 0) { refuseMissingIdentity(res, missingIdentity); return true; }
      const result = await rerunElevated({ sourceFlow, argsOverride });
      res.statusCode = result.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(result.body));
    } catch (err) {
      api.logger.warn(`manifold rerun handler error: ${err?.message ?? err}`);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
    }
    return true;
  }
}
