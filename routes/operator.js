import { createRetryFailure } from "./operator-retry.js";
import { createDiscussHandler, createDiscussHistoryHandler } from "./operator-discuss.js";
import { createStateStore } from "../state/state-store.js";
import { completeCancellation } from "../engine/executor-cancel.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { admitAndSetRunning } from "../engine/child-resume-trigger.js";

export function registerOperatorRoutes({
  api,
  store,
  readStore,
  executor,
  gateResolver,
  failureStore,
  pipelinesRef,
  ensurePipelinesLoaded,
  invokeElevated,
  ownerSessionKey,
  runStore,
  admissionQueueStore,
  resumeParentRef,
  // from discuss.js
  stampDiscussSession,
  normalizePendingFields,
  // from pending-store.js
  readPending,
  mergePendingFields,
  setPendingText,
  // component emitter for child-session proposal apply
  componentEmitter,
  getBroadcast,
}) {
  const { retryFailureBranch } = createRetryFailure({
    store, readStore, failureStore, pipelinesRef, executor, runStore, ownerSessionKey,
    admissionQueueStore,
    logger: api.logger,
    api,
  });

  const { handleDiscuss } = createDiscussHandler({
    api, stampDiscussSession, normalizePendingFields, readPending, mergePendingFields,
    setPendingText, componentEmitter,
  });

  const { handleDiscussHistory } = createDiscussHistoryHandler({ api });

  // ── POST /manifold/gate/resolve ───────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/gate/resolve",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      api.logger.info(`manifold gate/resolve: via ${authz.via}`);
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      try {
        // recordItemDecision can trigger executor.resume(), which may
        // dispatch a kind:"agent" node (e.g. SETTLE_GAP_ANSWER) — that
        // needs operator.write scope, which this auth:"plugin" route never
        // has ambient. Loop back through invokeElevated to a sibling
        // auth:"gateway" route that does, same pattern as /manifold/start.
        const elev = await invokeElevated("/manifold/_elev/gate-resolve", {
          flowId: body.flowId,
          itemId: body.itemId,
          decision: body.decision,
          source: body.source,
          editedText: body.editedText,
          reason: body.reason,
        });
        res.statusCode = elev.status || 500;
        res.setHeader("Content-Type", "application/json");
        res.end(elev.body || JSON.stringify({ ok: false, code: "elevation_no_body" }));
      } catch (err) {
        api.logger.warn(`manifold gate/resolve handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/_elev/gate-resolve ─────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/gate-resolve",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      try {
        const result = await gateResolver.recordItemDecision({
          flowId: body.flowId,
          itemId: body.itemId,
          decision: body.decision,
          source: body.source,
          editedText: body.editedText,
          reason: body.reason,
        });
        const status = result.ok ? 200 : (result.code === "flow_not_found" ? 404 : 409);
        res.statusCode = status;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify(result));
      } catch (err) {
        api.logger.warn(`manifold gate-resolve (elev) handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/cancel ─────────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/cancel",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const flowId = typeof body.flowId === "string" ? body.flowId.trim() : "";
      const reason = typeof body.reason === "string" && body.reason.trim() ? body.reason.trim() : null;
      if (!flowId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flowId required" }));
        return true;
      }
      try {
        let result = await executor.cancel(flowId, reason);
        if (!result?.cancelled && result?.reason === "Flow not found.") {
          const sqlFlow = await readStore?.get(flowId);
          if (sqlFlow && !["succeeded", "failed", "cancelled", "skipped"].includes(sqlFlow.status)) {
            // Use a store bound to the flow's actual ownerKey. Legacy flows were
            // created under "graph-flow-owner"; the default store is bound to
            // "manifold-owner". Mismatched ownerKey → setRunning returns applied=false.
            //
            // Must carry the same dependency set as the primary `store` and
            // index.js's own createCancelStore factory (nodeExecutionsStore,
            // resumeParentRef, pipelinesRef, failureStore, admissionQueueStore):
            // state-store.js's cancel()/finish()/fail() silently no-op
            // triggerAdmissionRelease without admissionQueueStore and
            // triggerParentResume without nodeExecutionsStore/resumeParentRef —
            // a fallback missing either leaks the flow's start/child-dispatch
            // admission slot permanently and leaves a waiting parent on
            // "children-pending" forever.
            const flowOwnerKey = sqlFlow.ownerKey ?? ownerSessionKey;
            const cancelStore =
              flowOwnerKey === ownerSessionKey
                ? store
                : createStateStore(api, flowOwnerKey, {
                    runStore, nodeExecutionsStore: getNodeExecutionsStore(), resumeParentRef,
                    pipelinesRef, failureStore, admissionQueueStore, logger: api.logger,
                  });
            // Pre-load into the in-process Map so the runtime's cancel() can find it.
            const setResult = await cancelStore.setRunning({
              flowId,
              expectedRevision: sqlFlow.revision,
              lastStep: sqlFlow.lastStep,
              stateJson: sqlFlow.stateJson ?? {},
            });
            if (setResult.applied) {
              // completeCancellation stamps branches + writes stateJson to DB.
              // Pass revision+1 because setRunning just incremented it.
              result = await completeCancellation({
                store: cancelStore,
                flowId,
                stateJson: sqlFlow.stateJson ?? {},
                lastStep: sqlFlow.lastStep,
                revision: sqlFlow.revision + 1,
                cfg: api.config,
                runStore,
                nodeExecutionsStore: getNodeExecutionsStore(),
                reason,
              });
            }
          }
        }
        if (!result?.cancelled) {
          res.statusCode = 409;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: false, code: "cancel_not_applied", reason: result?.reason ?? "unknown" }));
          return true;
        }
        api.logger.info(`manifold cancel: flow=${flowId.slice(0, 8)} via=${authz.via}`);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: true, cancelled: true }));
      } catch (err) {
        api.logger.warn(`manifold cancel handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/start ──────────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/start",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const pipelineId = typeof body.pipelineId === "string" ? body.pipelineId.trim() : "";
      if (!pipelineId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "pipelineId required" }));
        return true;
      }
      try {
        await ensurePipelinesLoaded?.();
        if (!pipelinesRef.value.has(pipelineId)) {
          res.statusCode = 404;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: false, error: `unknown pipeline: ${pipelineId}`, known: [...pipelinesRef.value.keys()] }));
          return true;
        }
        const args = (body.args && typeof body.args === "object") ? body.args : {};
        const elev = await invokeElevated("/manifold/_elev/start", { pipelineId, args, via: authz.via });
        res.statusCode = elev.status || 500;
        res.setHeader("Content-Type", "application/json");
        res.end(elev.body || JSON.stringify({ ok: false, code: "elevation_no_body" }));
      } catch (err) {
        api.logger.warn(`manifold start handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/_elev/start ────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/start",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const pipelineId = typeof body.pipelineId === "string" ? body.pipelineId.trim() : "";
      const args = (body.args && typeof body.args === "object") ? body.args : {};
      const via = typeof body.via === "string" ? body.via : "elev";
      const parentSessionKey = typeof body.parentSessionKey === "string" && body.parentSessionKey.trim()
        ? body.parentSessionKey.trim() : null;
      if (!pipelineId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "pipelineId required" }));
        return true;
      }
      try {
        await ensurePipelinesLoaded?.();
        if (!pipelinesRef.value.has(pipelineId)) {
          res.statusCode = 404;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: false, error: `unknown pipeline: ${pipelineId}`, known: [...pipelinesRef.value.keys()] }));
          return true;
        }
        const triggeredBy = {
          kind: via.includes("app") ? "operator_http" : via.includes("cron") ? "cron" : via.includes("cli") ? "cli" : "operator_http",
          label: `via ${via}`,
          ts: Date.now(),
        };
        const record = await executor.start(pipelineId, args, triggeredBy);
        try {
          if (parentSessionKey) {
            await api.runtime?.lineage?.recordTrigger?.({
              flowId: record.flowId,
              runId: record.stateJson?.runId ?? null,
              pipelineId,
              parentSessionKey,
            });
          } else {
            const source = triggeredBy.kind === "cli" ? "cli" : triggeredBy.kind === "operator_http" ? "app" : "operator";
            const synthSession = `agent:operator:${source}:${record.flowId}`;
            await api.runtime?.lineage?.recordTrigger?.({
              flowId: record.flowId,
              runId: record.stateJson?.runId ?? null,
              pipelineId,
              parentSessionKey: synthSession,
              rootKind: "explicit",
              rootLabel: pipelineId,
            });
          }
        } catch (err) {
          api.logger.warn(`manifold start: lineage edge skipped: ${err?.message ?? err}`);
          try {
            const freshForLineage = await store.get(record.flowId);
            if (freshForLineage) {
              await store.setRunning({
                flowId: record.flowId,
                expectedRevision: freshForLineage.revision,
                lastStep: freshForLineage.lastStep,
                stateJson: { ...freshForLineage.stateJson, _lineageError: { ts: Date.now(), message: err?.message ?? String(err) } },
              });
            }
          } catch { /* non-fatal; lineage attribution already lost */ }
        }
        setImmediate(() => {
          executor.runFlow(record.flowId, "fresh").catch((err) => {
            api.logger.warn(`manifold start: runFlow [${record.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
          });
        });
        if (triggeredBy.kind === "cron") {
          try { getBroadcast?.()?.("cron.job.fired", { flowId: record.flowId, pipelineId, firedAt: Date.now() }); } catch {}
        }
        api.logger.info(`manifold start: pipeline=${pipelineId} flow=${record.flowId.slice(0, 8)} via=${via} (elev)`);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: true, pipelineId, flowId: record.flowId }));
      } catch (err) {
        api.logger.warn(`manifold start (elev) handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/resume-from-gate ──────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/resume-from-gate",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true; }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const flowId = typeof body.flowId === "string" ? body.flowId : "";
      if (!flowId) {
        res.statusCode = 400; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flowId required" })); return true;
      }
      const flow = (await store.get(flowId)) ?? (await readStore?.get(flowId)) ?? null;
      if (!flow) {
        res.statusCode = 404; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flow not found" })); return true;
      }
      // Re-requests this flow's own admission before resuming — it was
      // released the moment the flow blocked on this budget-gate
      // (state-store.js's setWaiting hook). If the pool is full, the flow
      // is left at status:"queued" instead of running; admission-queue-
      // store's own release()/promotion cycle resumes it later.
      const result = await admitAndSetRunning({
        flowId, expectedRevision: flow.revision, lastStep: flow.lastStep, stateJson: flow.stateJson ?? {},
        parentInvocationId: flow.parentInvocationId, store, admissionQueueStore,
        runStore, resume: executor.resume, logger: api.logger, api,
      });
      if (!result.applied) {
        res.statusCode = 409; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: `setRunning failed: ${result.code}` })); return true;
      }
      if (!result.queued) {
        setImmediate(() => {
          executor.resume(flowId).catch((err) => {
            api.logger.warn(`manifold resume-from-gate: runFlow [${flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
          });
        });
      }
      api.logger.info(`manifold: resume-from-gate flow=${flowId.slice(0, 8)} node=${body.nodeId ?? "?"} decision=${body.decision ?? "?"}${result.queued ? " (queued for admission)" : ""}`);
      res.statusCode = 200; res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, flowId, queued: result.queued })); return true;
    },
  });

  // ── POST /manifold/_elev/rerun ────────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/rerun",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const sourceFlowId = typeof body.sourceFlowId === "string" ? body.sourceFlowId.trim() : "";
      if (!sourceFlowId) {
        res.statusCode = 400; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "sourceFlowId required" })); return true;
      }
      const argsOverride = (body && "args" in body) ? body.args : undefined;
      if (argsOverride !== undefined && (argsOverride === null || typeof argsOverride !== "object" || Array.isArray(argsOverride))) {
        res.statusCode = 400; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "args must be a JSON object" })); return true;
      }
      const sourceFlow = await readStore.get(sourceFlowId);
      if (!sourceFlow) {
        res.statusCode = 404; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: `source flow not found: ${sourceFlowId}` })); return true;
      }
      let record;
      try {
        record = executor.rerunFromSource(sourceFlow, argsOverride);
      } catch (err) {
        const msg = err?.message ?? String(err);
        if (msg.includes("active flow")) { res.statusCode = 409; res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ ok: false, error: msg })); return true; }
        if (msg.includes("not found")) { res.statusCode = 404; res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ ok: false, error: msg })); return true; }
        api.logger.warn(`manifold rerun (elev) handler error: ${msg}`);
        res.statusCode = 500; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: msg })); return true;
      }
      setImmediate(() => {
        executor.runFlow(record.flowId, "fresh").catch((err) => {
          api.logger.warn(`manifold rerun (elev): runFlow [${record.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
        });
      });
      api.logger.info(`manifold rerun (elev): source=${sourceFlow.flowId.slice(0, 8)} → new=${record.flowId.slice(0, 8)} pipeline=${record.stateJson?.pipelineId ?? "?"}`);
      res.statusCode = 200; res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, flowId: record.flowId, pipelineId: record.stateJson?.pipelineId ?? null, rerunOf: sourceFlow.flowId }));
      return true;
    },
  });

  // ── POST /manifold/_elev/retry-failure ────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/retry-failure",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const id = typeof body.id === "string" ? body.id.trim() : "";
      if (!id) {
        res.statusCode = 400; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "id required" })); return true;
      }
      const via = typeof body.via === "string" ? body.via : "elev";
      const result = await retryFailureBranch({ id, via });
      res.statusCode = result.code === "ok" ? 200 : result.code === "not_found" ? 404 : result.code === "conflict" ? 409 : 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(result));
      return true;
    },
  });

  // ── POST /manifold/_elev/discuss ──────────────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/discuss",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const { status, body: responseBody } = await handleDiscuss(body);
      res.statusCode = status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(responseBody));
      return true;
    },
  });

  // ── POST /manifold/_elev/discuss_history ──────────────────────────────────

  api.registerHttpRoute({
    path: "/manifold/_elev/discuss_history",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const { status, body: responseBody } = await handleDiscussHistory(body);
      res.statusCode = status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(responseBody));
      return true;
    },
  });
}
