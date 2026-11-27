import { createRetryFailure } from "./operator-retry.js";
import { createStateStore } from "../state/state-store.js";
import { completeCancellation } from "../engine/executor-cancel.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { resumeFlowFromGate } from "./operator-resume.js";
import { authorizeBearerRequest, sendUnauthorized } from "./bearer-auth.js";

// An operator action carries a stamped harness identity (session + agentId)
// so it can be attributed; refuse unstamped calls instead of letting them run
// with nothing. The guard belongs on the HTTP boundary, not inside the work
// functions, so in-process callers (the budget queue) never have to invent a
// stamp.
export function missingIdentityFields(body, fields) {
  return fields.filter((f) => typeof body?.[f] !== "string" || !body[f].trim());
}

export function refuseMissingIdentity(res, missing) {
  res.statusCode = 400;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ ok: false, code: "missing_identity", error: `${missing.join(", ")} required` }));
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
}

export function registerOperatorRoutes({
  api,
  store,
  readStore,
  executor,
  gateResolver,
  failureStore,
  pipelinesRef,
  ensurePipelinesLoaded,
  ownerSessionKey,
  runStore,
  admissionQueueStore,
    resumeParentRef,
    getBroadcast,
}) {
  const { retryFailureBranch } = createRetryFailure({
    store, readStore, failureStore, pipelinesRef, executor, runStore, ownerSessionKey,
    admissionQueueStore,
    logger: api.logger,
    api,
  });

  // Work the public routes (and in-process callers such as the budget queue)
  // run directly — the caller owns the HTTP shape, the identity guard and the
  // bearer check.
  async function gateResolveElevated({ flowId, itemId, decision, source, editedText, reason }) {
    try {
      const result = await gateResolver.recordItemDecision({ flowId, itemId, decision, source, editedText, reason });
      const status = result.ok ? 200 : (result.code === "flow_not_found" ? 404 : 409);
      return { status, body: result };
    } catch (err) {
      api.logger.warn(`manifold gate-resolve handler error: ${err?.message ?? err}`);
      return { status: 500, body: { ok: false, code: "internal", error: err?.message ?? String(err) } };
    }
  }

  async function startElevated({ pipelineId, args, via, parentSessionKey }) {
    const effectiveVia = typeof via === "string" ? via : "operator";
    try {
      await ensurePipelinesLoaded?.();
      if (!pipelinesRef.value.has(pipelineId)) {
        return { status: 404, body: { ok: false, error: `unknown pipeline: ${pipelineId}`, known: [...pipelinesRef.value.keys()] } };
      }
      const triggeredBy = {
        kind: effectiveVia.includes("app") ? "operator_http" : effectiveVia.includes("cron") ? "cron" : "operator_http",
        label: `via ${effectiveVia}`,
        ts: Date.now(),
      };
      const record = await executor.start(pipelineId, args ?? {}, triggeredBy);
      try {
        if (parentSessionKey) {
          await api.runtime?.lineage?.recordTrigger?.({
            flowId: record.flowId,
            runId: record.stateJson?.runId ?? null,
            pipelineId,
            parentSessionKey,
          });
        } else {
          const source = triggeredBy.kind === "operator_http" ? "app" : "operator";
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
      api.logger.info(`manifold start: pipeline=${pipelineId} flow=${record.flowId.slice(0, 8)} via=${effectiveVia}`);
      return { status: 200, body: { ok: true, pipelineId, flowId: record.flowId } };
    } catch (err) {
      api.logger.warn(`manifold start handler error: ${err?.message ?? err}`);
      return { status: 500, body: { ok: false, code: "internal", error: err?.message ?? String(err) } };
    }
  }

  async function rerunElevated({ sourceFlow, argsOverride }) {
    let record;
    try {
      record = executor.rerunFromSource(sourceFlow, argsOverride);
    } catch (err) {
      const msg = err?.message ?? String(err);
      if (msg.includes("active flow")) return { status: 409, body: { ok: false, error: msg } };
      if (msg.includes("not found")) return { status: 404, body: { ok: false, error: msg } };
      api.logger.warn(`manifold rerun handler error: ${msg}`);
      return { status: 500, body: { ok: false, code: "internal", error: msg } };
    }
    setImmediate(() => {
      executor.runFlow(record.flowId, "fresh").catch((err) => {
        api.logger.warn(`manifold rerun: runFlow [${record.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    });
    api.logger.info(`manifold rerun: source=${sourceFlow.flowId.slice(0, 8)} → new=${record.flowId.slice(0, 8)} pipeline=${record.stateJson?.pipelineId ?? "?"}`);
    return { status: 200, body: { ok: true, flowId: record.flowId, pipelineId: record.stateJson?.pipelineId ?? null, rerunOf: sourceFlow.flowId } };
  }

  async function retryFailureElevated({ id, via }) {
    const result = await retryFailureBranch({ id, via });
    const status = result.code === "ok" ? 200 : result.code === "not_found" ? 404 : result.code === "conflict" ? 409 : 500;
    return { status, body: result };
  }

  api.registerHttpRoute({
    path: "/manifold/gate/resolve",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      api.logger.info("manifold gate/resolve: bearer ok");
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body;
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      try {
        const missingGateIdentity = missingIdentityFields(body, ["flowId", "session", "agentId"]);
        if (missingGateIdentity.length > 0) { refuseMissingIdentity(res, missingGateIdentity); return true; }
        const result = await gateResolveElevated({
          flowId: body.flowId,
          itemId: body.itemId,
          decision: body.decision,
          source: body.source,
          editedText: body.editedText,
          reason: body.reason,
        });
        sendJson(res, result.status, result.body);
      } catch (err) {
        api.logger.warn(`manifold gate/resolve handler error: ${err?.message ?? err}`);
        sendJson(res, 500, { ok: false, code: "internal", error: err?.message ?? String(err) });
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/cancel",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
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
            // Cancel store bound to the flow's actual ownerKey — a mismatched
            // ownerKey makes setRunning return applied=false. Must carry the
            // same dependency set as index.js's createCancelStore:
            // state-store's cancel()/finish()/fail() silently no-op
            // triggerAdmissionRelease without admissionQueueStore and
            // triggerParentResume without nodeExecutionsStore/resumeParentRef
            // — a fallback missing either leaks the admission slot forever
            // and leaves a waiting parent on "children-pending" forever.
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
              // completeCancellation stamps branches + writes stateJson; its
              // revision must be setRunning's increment (sqlFlow.revision + 1).
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

  api.registerHttpRoute({
    path: "/manifold/start",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
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
        const missingStartIdentity = missingIdentityFields(body, ["session", "agentId"]);
        if (missingStartIdentity.length > 0) { refuseMissingIdentity(res, missingStartIdentity); return true; }
        const result = await startElevated({
          pipelineId,
          args: (body.args && typeof body.args === "object") ? body.args : {},
          via: authz.via,
          parentSessionKey: typeof body.parentSessionKey === "string" && body.parentSessionKey.trim()
            ? body.parentSessionKey.trim() : null,
        });
        sendJson(res, result.status, result.body);
      } catch (err) {
        api.logger.warn(`manifold start handler error: ${err?.message ?? err}`);
        sendJson(res, 500, { ok: false, code: "internal", error: err?.message ?? String(err) });
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/resume-from-gate",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true; }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
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
      const result = await resumeFlowFromGate({
        store, readStore, admissionQueueStore, runStore, executor,
        logger: api.logger, api, flowId, nodeId: body.nodeId, decision: body.decision,
      });
      if (!result.ok && result.code === "flow_not_found") {
        res.statusCode = 404; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "flow not found" })); return true;
      }
      if (!result.ok) {
        res.statusCode = 409; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: result.code })); return true;
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

  return { gateResolveElevated, startElevated, rerunElevated, retryFailureElevated };
}
