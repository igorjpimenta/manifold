import { getNodeExecutionsStore } from "../state/node-executions.js";
import { getAllSubflowGroups } from "../state/subflow-state.js";
import { assembleState } from "../engine/executor-helpers.js";

export function registerRunRoutes({
  api,
  readStore,
  costForAllPipelines,
  runStore,
  pipelinesRef,
  pipelineRegistryStore,
  invokeElevated,
  discussStore,
  failureStore,
  // discuss handlers — destructured from registerDiscussHandlers() return value
  handleDiscuss,
  handleDiscussHistory,
  handleDiscussApplyDraft,
  handleDiscussAudio,
  handleDiscussAudioGet,
  handleDiscussAudioRetry,
}) {

  // ── GET /pipeclaw/cost ─────────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/pipeclaw/cost",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
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
        api.logger.warn(`pipeclaw cost handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── GET /pipeclaw/runs ─────────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/pipeclaw/runs",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const limitRaw = url.searchParams.get("limit");
      const limit = Math.max(1, Math.min(500, Number(limitRaw) || 50));
      const pipelineIdFilter = url.searchParams.get("pipelineId");
      const beforeMs = url.searchParams.has("before_ms")
        ? parseInt(url.searchParams.get("before_ms"), 10) : null;
      const beforeId = url.searchParams.get("before_id") ?? null;
      try {
        // "waiting" is never actually written anywhere (confirmed repo-wide)
        // — gate pauses are always persisted as "blocked" — so no special-
        // case expansion is needed for that filter value.
        const statusParam = url.searchParams.get("status");
        // Two targeted queries: GROUP BY for tab counts (unfiltered), and a
        // paginated fetch for the current page (sorted + cursor in SQL).
        const statusCounts = await readStore.listStatusCounts({ pipelineId: pipelineIdFilter });
        const totalKnown = statusParam
          ? (statusCounts[statusParam] ?? 0)
          : Object.values(statusCounts).reduce((s, n) => s + n, 0);
        // Fetch limit+1 so hasMore detection needs no extra COUNT query.
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
        // Dual-mode (Phase 2): a flow's subflows are either old-model
        // (flow_branches) or new-model (real flow_runs children) never
        // both — see state/subflow-state.js's own header comment. Prefer the
        // childFlows summary; a flow with no real children (the common
        // case: hasn't reached a subflow node yet, or predates the
        // migration) naturally falls through to the old-model map, then
        // to EMPTY_SUBFLOWS. Without this, every childFlows-migrated run
        // in the list (i.e. every run post-migration) would report
        // subflowTotal:0 even though its own /subflows endpoint has real
        // data — the exact mismatch that made RunDetailView's Subflows
        // section populate while PipelineDetailView's inline subflow peek
        // silently never offered itself.
        const subflowSummaryMap = new Map();
        const childSummaryMap = await getNodeExecutionsStore().summarizeChildrenByFlowIds(page.map((r) => r.flowId));
        const EMPTY_SUBFLOWS = { total: 0, pending: 0, failed: 0, skipped: 0, succeeded: 0 };
        // listRunsPage already delivers final field names and extracted
        // display fields — route only merges in the secondary-service data.
        const runs = page.map((r) => {
          const discuss = discussSummaries[r.flowId] ?? null;
          const subflows = childSummaryMap.get(r.flowId) ?? subflowSummaryMap.get(r.flowId) ?? EMPTY_SUBFLOWS;
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
        api.logger.warn(`pipeclaw runs handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── GET /pipeclaw/errors?flowIds=id1,id2,... ──────────────────────────
  // Returns error entries for the given flow IDs.
  // Used by the dashboard to enrich its run lists and stats without direct
  // DB access. flowIds is a comma-separated list; max 500 entries.

  api.registerHttpRoute({
    path: "/pipeclaw/errors",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
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
        api.logger.warn(`pipeclaw errors handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── GET /pipeclaw/queue ────────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/pipeclaw/queue",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      try {
        // Lean, purpose-built mapper — deliberately not the /runs list's
        // .map() above, which pulls in bulk discuss/subflow-summary fetches
        // that mean nothing for a queued item's glance view and would slow
        // this endpoint down for no benefit.
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
        api.logger.warn(`pipeclaw queue handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── GET /pipeclaw/tree-summary?flowIds=id1,id2,... ──────────────────────
  // Batched "which top-level nodes did each of these flows visit" —
  // backing the dashboard auto-funnel's per-node visit check without one
  // round-trip per run. Same shape/convention as /pipeclaw/errors above
  // (comma-separated flowIds, capped at 500, sibling top-level resource
  // rather than nested under /pipeclaw/runs/ — that path is a
  // prefix-matched route below and this avoids any ambiguity with it).

  api.registerHttpRoute({
    path: "/pipeclaw/tree-summary",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
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
        api.logger.warn(`pipeclaw tree-summary handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── GET+POST /pipeclaw/runs/<flowId>/... ────────────────────────────────

  api.registerHttpRoute({
    path: "/pipeclaw/runs/",
    auth: "plugin",
    match: "prefix",
    handler: async (req, res) => {
      if (req.method !== "GET" && req.method !== "POST") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET, POST");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const tail = url.pathname.slice("/pipeclaw/runs/".length);
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
        api.logger.warn(`pipeclaw run-detail handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
        return true;
      }
    },
  });

  // ── sendFullRun ───────────────────────────────────────────────────────────

  async function sendFullRun(res, flow) {
    const nodeExecutionsStore = getNodeExecutionsStore();
    // B2.5 — enriched via assembleState() (each node's own resultJson
    // replayed on top of the still-live stateJson) instead of trusting the
    // raw stateJson blob directly, matching how the engine's own dispatch
    // loop and routes/discuss.js already read state.
    const enrichedState = await assembleState(flow.flowId, {
      runStore, nodeExecutionsStore, baseState: flow.stateJson ?? {},
    });
    // .gates must NEVER come from assembleState()'s own generic per-key
    // merge — every human-gate node writes a FULL SNAPSHOT of state.gates
    // into its own resultJson (executor-nodes.js: `gates: {
    // ...(state.gates ?? {}), [nodeId]: record }`), so replaying rows in
    // invokedAt order can resolve to a stale snapshot if a later gate opens
    // out of the order assembleState() assumes. gate_records (a dedicated
    // per-flow accessor, one row per gate, no merge) is the staleness-safe
    // source — same fix already applied to the engine's own human-gate
    // reads when B2 landed.
    const gates = await nodeExecutionsStore.listGateRecordsForFlow(flow.flowId);
    // Strip `loop` from the stateJson payload — subflow data is served via
    // GET /pipeclaw/runs/:flowId/subflows (childFlows, not old-model branchService).
    // Strip `errors` too: failure_events is now the sole store for error
    // history ("Eradicate soft errors" Phase 1.5) — errorEntries below is
    // the normalized replacement. Only historical rows persisted before
    // that redirect still carry a baked-in errors[] in their stored
    // stateJson; stripping it here keeps the wire contract clean for those
    // too.
    const { loop: _loop, errors: _errors, gates: _staleGates, ...stateWithoutLegacy } = enrichedState;
    // A subflow child is now just a flow_runs row with parent_invocation_id set —
    // resolve it back to the parent's flowId/nodeId/subflowKey so iOS can
    // render a "Parent run" link. Also resolves this child's REAL top-level
    // pipelineId (its own stateJson.pipelineId is the synthetic
    // "parent::NODE_ID" form loader.js registers subflow nodes under) —
    // same fix listQueued() already applies for the Queues feature.
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
      // Normalized error history sourced from failure_events — sibling to
      // stateJson (not nested inside it) so flow-level and subflow-level
      // shapes stay symmetric.
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

  // ── sendArtifactsList ─────────────────────────────────────────────────────

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

  // ── sendArtifactFile ──────────────────────────────────────────────────────

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

  // ── sendSubflowsList ─────────────────────────────────────────────────────

  async function sendSubflowsList(res, flow) {
    const grouped = await getAllSubflowGroups({
      flowId: flow.flowId, runStore, nodeExecutionsStore: getNodeExecutionsStore(),
    });
    // Strip the legacy `errors` column (Phase 1.6) — errorEntries (Part B's
    // enrichment) is the sole normalized replacement now that
    // executor-branch.js no longer writes this field.
    const subflows = Object.values(grouped).flatMap((group) =>
      Object.values(group).map(({ errors: _errors, ...rest }) => rest)
    );
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ subflows }));
    return true;
  }

  // ── node tree ─────────────────────────────────────────────────────────────
  //
  // Unifies the old Steps (this flow's own node timeline) and Subflows
  // (child-flow fan-out) sections into one recursively-expandable tree.
  // Two row shapes, one recursive expand operation:
  //   type:"node" — one row per invocation (not per execution attempt — see
  //     buildTreeNodeRows) for a single flow's own dispatches. Expandable
  //     only when kind==="subflow" and it has spawned at least one child.
  //   type:"flow" — one row per child flow_runs spawned from a subflow-kind
  //     node's invocation. Expandable once it has dispatched at least one
  //     node of its own.
  // Expanding a "node" row returns "flow" children; expanding a "flow" row
  // returns that flow's own "node" list (same shape /tree itself returns).

  // Builds this flow's own top-level node list — the payload for both the
  // /tree root call and a /tree/expand?type=flow call (a descendant flow's
  // own dispatches use the exact same shape as the root).
  async function buildTreeNodeRows(flow) {
    const pipelineId = flow.stateJson?.pipelineId;
    const nodeExecutionsStore = getNodeExecutionsStore();
    const invocations = await nodeExecutionsStore.listInvocationsForFlow(flow.flowId);
    // A node's own status can only be told apart from a failed one by
    // cross-referencing failure_events (Phase 1.5 — flow_node_executions no
    // longer stores error content). Every failure this flow ever recorded,
    // not just root-level ones, but only executionId matters here, and a
    // subflow item's own errors live under ITS OWN flow_id, not this one's —
    // so this set naturally only ever matches root-level invocations.
    const failedExecutionIds = new Set(
      (await failureStore.queryErrorsByFlow(flow.flowId)).map((e) => e.executionId).filter(Boolean)
    );

    // Point-in-time kind/agent per invocation — replaces reading
    // pipelinesRef.value directly. A node whose pipeline definition has
    // since changed, or whose whole pipeline was deleted, still renders its
    // ORIGINAL kind/agent, keyed by this invocation's own invokedAt rather
    // than whatever the live, single-copy definition currently says (or no
    // longer has at all).
    // Batched in one call, not one query per invocation.
    const versionRequests = invocations.map((inv) => ({ pipelineId, nodeName: inv.nodeId, atMs: inv.invokedAt }));
    const versions = await pipelineRegistryStore.getNodeVersionsBatch(versionRequests);
    const versionByInvocationId = new Map(invocations.map((inv, i) => [inv.invocationId, versions[i]]));

    // A subflow-kind row's OWN invocation (subflow_key=null) is never a
    // child's parent_invocation_id — each branch item gets its OWN separate
    // invocation, keyed by the item's own subflow_key (see
    // listInvocationsForFlow's doc comment). Resolve those per-nodeId via
    // listInvocationIdsForNode first, batched once per distinct nodeId
    // rather than per row.
    //
    // Known limitation: if the same nodeId is visited more than once (a
    // loop revisits it — confirmed to happen, e.g. PROPOSE), there is no
    // schema-level way to partition which items belong to which visit —
    // listInvocationIdsForNode returns every item ever spawned under that
    // (flowId, nodeId) pair, full stop (a real item invocation's own
    // visit_seq/forceNew is never set — see engine/spawn-child.js). All
    // rows sharing that nodeId therefore see the SAME cumulative child
    // list. Rare in practice (most subflow nodes dispatch once per flow);
    // not solved here — would need a schema change (tying an item
    // invocation to its spawning visit) to fix properly.
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
      // A node's own execution row carries no success/failure signal at all
      // (flow_node_executions is a pure identity/timing ledger;
      // failedExecutionIds above is the only way to know a given attempt
      // failed). Absent that, the only way to recover status is by
      // cross-referencing the FLOW's own status — which only ever applies
      // to the LAST invocation in this list (a flow can't be mid-dispatch
      // or have skipped/blocked at an earlier, already-superseded node).
      // invocations is already ordered ascending by invoked_at
      // (listInvocationsForFlow), so idx === lastIdx already and only ever
      // means "the most recently invoked node for this flow" — no extra
      // currentStep/isTerminal gate: comparing a dropped field is always
      // false, and gating on !isTerminal would drop a terminal flow's real
      // status from its own last node whenever that node's execution
      // recorded no error, defaulting a failed flow to "succeeded".
      let status;
      if (inv.latestExecutionId && failedExecutionIds.has(inv.latestExecutionId)) {
        status = "failed";
      } else if (idx === lastIdx) {
        status = flow.status; // live: "running" | "blocked" | "queued"; terminal: whatever flow.status is
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
        // execKind above is the LATEST execution only, which for any
        // subflow node with finished children is always "recheck" — masking
        // a real "retry"/"revival" earlier in the same invocation's
        // history. Independent signal, not a replacement for execKind.
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

  // The human-readable "why" behind a non-succeeded child flow, for the
  // node tree's attention banner. A human-gate wait carries a real,
  // pipeline-authored title in
  // waitJson.items[0].requestPayload.title (e.g. "Cyrano needs 2 things
  // to draft <listing>") — confirmed against live data, far more useful
  // than the generic statusReason ("awaiting 1 approval at GAP_GATE")
  // sitting next to it. Every other wait kind, and failures, fall back to
  // statusReason, which the executor already writes as a short human
  // summary (shortSummary() calls throughout executor.js).
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
      // A spawned child flow can independently retry at its own dispatch
      // level without the SUBFLOW NODE that spawned it (buildTreeNodeRows'
      // own row, above) ever retrying — different table (flow_runs, not
      // flow_node_invocations), no execKind at that level at all.
      // Deliberately per-child, not rolled up onto the parent's own row —
      // matches this file's "shallow, immediate-children only" rollup rule.
      everRetriedOrRevived: derived[idx].everRetriedOrRevived,
    }));
  }

  // One query per child (nodeExecutionsStore.listInvocationsForFlow),
  // shared by both derived fields below rather than each querying
  // independently — cascadeAttempt only used it when pipeline.kind ===
  // "cascade" (short-circuiting otherwise), everRetriedOrRevived needs it
  // unconditionally, so combining costs nothing extra for cascade children
  // and adds exactly the one necessary query for non-cascade ones.
  async function perChildDerivedFields(child, nodeExecutionsStore) {
    const invocations = await nodeExecutionsStore.listInvocationsForFlow(child.flowId);
    return {
      cascadeAttempt: await cascadeAttemptFor(child, invocations),
      everRetriedOrRevived: invocations.some((inv) => inv.everRetriedOrRevived),
    };
  }

  // A cascade-kind pipeline (README.md's "retry-on-failure as a flow
  // characteristic") tries its own root nodes in sequence, one throw at a
  // time, until one succeeds or all are exhausted — every attempt still
  // gets its own ordinary flow_node_invocations row (see
  // buildTreeNodeRows), so "how many root nodes did this child flow
  // dispatch" is a direct, non-heuristic count of those rows. > 1 means
  // the first node did NOT resolve it — something worth flagging, even on
  // an eventual success. Non-cascade pipelines always return null here;
  // ordinary sequential nodes look identical in shape and would false-
  // positive if this weren't gated on the pipeline's own declared kind.
  //
  // Point-in-time (state/pipeline-registry.js) — keyed by this child
  // flow's own createdAt, not the live pipelinesRef.value.
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
      // invocationId here is the subflow-kind node's OWN dispatch (subflow_
      // key=null) — never a child's parent_invocation_id, same reasoning as
      // buildTreeNodeRows. Resolve the real per-item invocations for this
      // nodeId first (same known multi-visit limitation documented there).
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

  // Full execution-attempt history for one invocation — the tree's
  // per-node drill-in (a row with executionCount > 1 links here).
  async function sendInvocationDetail(res, flow, invocationId) {
    const invocation = await getNodeExecutionsStore().getInvocation(invocationId);
    if (!invocation || invocation.flowId !== flow.flowId) {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, error: "invocation not found on this flow" }));
      return true;
    }
    // Point-in-time (state/pipeline-registry.js) — same reasoning as
    // buildTreeNodeRows, keyed by this invocation's own invokedAt rather
    // than the live pipelinesRef.value.
    const version = await pipelineRegistryStore.getNodeVersionAt({
      pipelineId: flow.stateJson?.pipelineId, nodeName: invocation.nodeId, atMs: invocation.invokedAt,
    });
    const rawExecutions = await getNodeExecutionsStore().listExecutionsForInvocation(invocationId);
    // Per-attempt error content lives in failure_events now (Phase 1.5) —
    // cross-reference by executionId. A given execution_id gets at most one
    // failure row (upsertFailureRow's conflict target), so this is a clean
    // 1:1 lookup, not a dedup problem.
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

  // ── sendTranscriptsList ───────────────────────────────────────────────────

  // lite=1  → return turnCount only, no turn text (for the iOS list view)
  // sessionKey=X → return a single transcript with full turns (for TranscriptView drill-in)
  async function sendTranscriptsList(res, flow, url) {
    const { readdir, readFile } = await import("node:fs/promises");
    const pathMod = await import("node:path");
    const osMod = await import("node:os");
    const AGENTS_DIR = pathMod.join(osMod.homedir(), ".openclaw", "agents");
    const flowShort = flow.flowId.slice(0, 8);
    const subagentKeyPattern = new RegExp(`^agent:([^:]+):subagent:gf-${flowShort}-(.+)$`);
    const discussKeyPattern  = new RegExp(`^agent:([^:]+):(?:gf-discuss-|pipeclaw-parrot:direct:)${flowShort}-(.+)$`);
    const lite       = url.searchParams.get("lite") === "1";
    const singleKey  = url.searchParams.get("sessionKey") ?? null;

    let agentDirs = [];
    try { agentDirs = await readdir(AGENTS_DIR, { withFileTypes: true }); }
    catch {
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ transcripts: [] }));
      return true;
    }

    const transcripts = [];
    for (const ent of agentDirs) {
      if (!ent.isDirectory()) continue;
      const agent = ent.name;
      const indexPath = pathMod.join(AGENTS_DIR, agent, "sessions", "sessions.json");
      let index;
      try {
        const raw = await readFile(indexPath, "utf-8");
        index = JSON.parse(raw);
      } catch { continue; }
      if (!index || typeof index !== "object") continue;
      for (const [sessionKey, meta] of Object.entries(index)) {
        // sessionKey filter: skip non-matching entries fast
        if (singleKey && sessionKey !== singleKey) continue;
        let nodeId = null;
        let itemKey = null;
        const subM = subagentKeyPattern.exec(sessionKey);
        if (subM && subM[1] === agent) {
          const tail = subM[2];
          const dashIdx = tail.indexOf("-");
          nodeId = dashIdx === -1 ? tail : tail.slice(0, dashIdx);
          itemKey = dashIdx === -1 ? null : tail.slice(dashIdx + 1);
        } else {
          const dM = discussKeyPattern.exec(sessionKey);
          if (!dM || dM[1] !== agent) continue;
          nodeId = "discuss";
          itemKey = dM[2];
        }
        const sessionId = meta?.sessionId;
        if (!sessionId) continue;
        if (lite) {
          const turnCount = await countSessionTurns(agent, sessionId, AGENTS_DIR, pathMod, readFile).catch(() => 0);
          transcripts.push({
            sessionKey, agent, nodeId, itemKey, sessionId,
            startedAt: meta?.sessionStartedAt ?? null,
            updatedAt: meta?.updatedAt ?? null,
            turnCount,
          });
        } else {
          const turns = await parseSessionTurns(agent, sessionId, AGENTS_DIR, pathMod, readFile).catch(() => []);
          transcripts.push({
            sessionKey, agent, nodeId, itemKey, sessionId,
            startedAt: meta?.sessionStartedAt ?? null,
            updatedAt: meta?.updatedAt ?? null,
            turns,
          });
        }
        if (singleKey) break; // found the one we need
      }
      if (singleKey && transcripts.length > 0) break;
    }
    transcripts.sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ transcripts }));
    return true;
  }

  async function countSessionTurns(agent, sessionId, agentsDir, pathMod, readFile) {
    const file = pathMod.join(agentsDir, agent, "sessions", `${sessionId}.jsonl`);
    let raw;
    try { raw = await readFile(file, "utf-8"); }
    catch { return 0; }
    let count = 0;
    for (const line of raw.split("\n")) {
      if (!line.trim()) continue;
      let evt;
      try { evt = JSON.parse(line); } catch { continue; }
      if (evt?.type !== "message") continue;
      const role = evt.message?.role;
      if (role === "user" || role === "assistant") count++;
    }
    return count;
  }

  async function parseSessionTurns(agent, sessionId, agentsDir, pathMod, readFile) {
    const file = pathMod.join(agentsDir, agent, "sessions", `${sessionId}.jsonl`);
    let raw;
    try { raw = await readFile(file, "utf-8"); }
    catch { return []; }
    const turns = [];
    for (const line of raw.split("\n")) {
      if (!line.trim()) continue;
      let evt;
      try { evt = JSON.parse(line); } catch { continue; }
      if (evt?.type !== "message") continue;
      const msg = evt.message ?? {};
      const role = msg.role;
      if (role !== "user" && role !== "assistant") continue;
      let text = "";
      let thinking = "";
      const content = Array.isArray(msg.content) ? msg.content : [];
      for (const part of content) {
        if (part?.type === "text" && typeof part.text === "string") {
          text += (text ? "\n" : "") + part.text;
        } else if (part?.type === "thinking" && typeof part.thinking === "string") {
          thinking += (thinking ? "\n" : "") + part.thinking;
        }
      }
      const cap = 8 * 1024;
      const truncated = text.length > cap;
      if (truncated) text = text.slice(0, cap);
      turns.push({ role, text, thinking: thinking || null, truncated, timestamp: evt.timestamp ?? null });
    }
    return turns;
  }

  // ── sendCompareToPeers ────────────────────────────────────────────────────

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

  // ── handleRerun ───────────────────────────────────────────────────────────

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
      const elev = await invokeElevated("/pipeclaw/_elev/rerun", {
        sourceFlowId: sourceFlow.flowId,
        args: argsOverride,
      });
      res.statusCode = elev.status || 500;
      res.setHeader("Content-Type", "application/json");
      res.end(elev.body || JSON.stringify({ ok: false, code: "elevation_no_body" }));
    } catch (err) {
      api.logger.warn(`pipeclaw rerun handler error: ${err?.message ?? err}`);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
    }
    return true;
  }
}
