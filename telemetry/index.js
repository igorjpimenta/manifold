// telemetry/index.js — session lineage + cost attribution: hook writes,
// /manifold/telemetry/* routes, recordTrigger/recordSpawn, `openclaw lineage`.

import { openStore, defaultDbPath } from "./store.js";
import { makeResolver } from "./resolver.js";
import { registerCliCommands } from "./cli.js";
import { resolveCost } from "./pricer.js";
import { checkBudget, initCapStore } from "./budget-alert.js";
import { registerHttpRoutes } from "./http.js";
import { applyTelemetryEvent } from "./ingest.js";
import { authorizeBearerRequest, sendUnauthorized } from "../routes/bearer-auth.js";
import { createEmitter } from "../events/emit.js";
// Module-level so repeated register() calls (config reload) share one
// store/resolver; only the first service start opens the DB.
let SHARED_STORE = null;
let SHARED_RESOLVER = null;

// Null until the lineage-service start() hook has run.
export function getSharedResolver() {
  return SHARED_RESOLVER;
}

// Calls arriving before the store is ready (gateway boot revival) are buffered
// and flushed once the service starts.
let PENDING_CALLS = [];

export function registerTelemetry(api, { getBroadcast, _dbPath } = {}) {
  const dbPath = _dbPath ?? defaultDbPath();

  const logger = api.logger;

  const { emit } = createEmitter({ getBroadcast, logger });

  // Must load before checkBudget()/the /budget/cap routes see traffic, or they
  // would read the DEFAULT_CAP_USD fallback instead of the operator's value.
  api.registerService({
    id: "budget-cap-store",
    start: async () => {
      try {
        await initCapStore({ dbPath });
        logger.info(`telemetry: budget-cap store ready`);
      } catch (err) {
        logger.error(`telemetry: budget-cap store init failed: ${err?.message ?? err}`);
      }
    },
  });

  api.registerService({
    id: "lineage-service",
    start: async () => {
      if (SHARED_STORE) { logger.info(`telemetry: store already initialized — skipping`); return; }
      try {
        SHARED_STORE = await openStore({ dbPath });
        SHARED_RESOLVER = makeResolver({ store: SHARED_STORE });
        logger.info(`telemetry: store ready at ${dbPath}`);
        if (PENDING_CALLS.length) {
          const pending = PENDING_CALLS.splice(0);
          logger.info(`telemetry: flushing ${pending.length} queued pre-ready call(s)`);
          for (const { type, args } of pending) {
            try {
              if (type === "trigger") await api.runtime.lineage.recordTrigger(args);
              else if (type === "spawn") await api.runtime.lineage.recordSpawn(args);
            } catch (e) {
              logger.warn(`telemetry: pending flush error (${type}): ${e?.message ?? e}`);
            }
          }
        }
      } catch (err) {
        logger.error(`telemetry: service start failed: ${err?.message ?? err}`);
        SHARED_STORE = null; SHARED_RESOLVER = null;
      }
    },
  });

  // Adapters post semantic events (lineage.session, lineage.spawn,
  // cost.usage); ingest.js derives the rows.
  const ingestToken = process.env.MANIFOLD_INGEST_TOKEN || "";

  api.registerHttpRoute({
    path: "/manifold/telemetry/event",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405;
        res.setHeader("Allow", "POST");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger, secret: ingestToken });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      let size = 0;
      const chunks = [];
      for await (const chunk of req) {
        size += chunk.length;
        if (size > 256 * 1024) { res.statusCode = 413; res.end("body too large"); return true; }
        chunks.push(chunk);
      }
      let event;
      try { event = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); }
      catch { res.statusCode = 400; res.end(JSON.stringify({ ok: false, error: "invalid json body" })); return true; }
      const result = await applyTelemetryEvent(event, {
        getStore: () => SHARED_STORE,
        getResolver: () => SHARED_RESOLVER,
        getBroadcast, logger, emit,
      });
      res.statusCode = result.ok ? 202 : 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(result));
      return true;
    },
  });

  registerHttpRoutes({ api, getStore: () => SHARED_STORE, logger, getBroadcast });

  api.runtime = api.runtime ?? {};
  api.runtime.lineage = {
    async recordTrigger({ flowId, runId, pipelineId, parentSessionKey, rootKind: rkHint, rootLabel: rlHint }) {
      try {
        const store = SHARED_STORE, resolver = SHARED_RESOLVER;
        if (!store || !resolver) {
          PENDING_CALLS.push({ type: "trigger", args: { flowId, runId, pipelineId, parentSessionKey, rootKind: rkHint, rootLabel: rlHint } });
          return { ok: false, reason: "not_ready", queued: true };
        }
        if (!flowId || !parentSessionKey) return { ok: false, reason: "missing_args" };

        const WALK_THROUGH = new Set(["agent_tool", "cli"]);
        let rootKey = parentSessionKey, rootKind = rkHint ?? "unknown", rootLabel = rlHint ?? parentSessionKey;
        if (!rkHint || WALK_THROUGH.has(rkHint)) {
          try {
            const chain = await resolver.walk(parentSessionKey);
            if (chain.length) {
              const r = chain[chain.length - 1];
              rootKey = r.key; rootKind = r.kind; rootLabel = r.rootLabel ?? r.key;
            }
          } catch { /* self-root defaults */ }
        }

        // "explicit" and "operator-trigger" roots have no durable identity
        // worth grouping by, so the pipeline being dispatched is the better
        // label (same convention routes/operator.js uses). pipelineId may be a
        // composite `<pipeline>::NODE[::NODE]` child id; splitting on "::" keeps
        // every subflow of one run on the same root_label, so http-tree.js's
        // GROUP BY merges them instead of fragmenting the run into N rows.
        if (pipelineId && (rootKind === "explicit" || rootKind === "operator-trigger")) {
          rootLabel = pipelineId.split("::")[0];
        }

        await store.upsertLineage({
          child_session_key:  `flow:${flowId}`,
          parent_session_key: parentSessionKey,
          root_session_key:   rootKey,
          kind:               "flow-triggered",
          root_kind:          rootKind,
          root_label:         rootLabel,
          agent_id:           null,
          pipeline_id:        pipelineId ?? null,
          flow_id:            flowId,
          run_id:             runId ?? null,
          node_id:            null,
          item_key:           null,
          first_seen_at:      Date.now(),
          source:             "trigger_hook",
        });
        return { ok: true };
      } catch (err) {
        logger.warn(`telemetry.recordTrigger: ${err?.message ?? err}`);
        return { ok: false, reason: err?.message ?? String(err) };
      }
    },

    async recordSpawn({ childSession, flowId, nodeId, itemKey, pipelineId, agentId, kind, source = "spawn_hook" }) {
      try {
        const store = SHARED_STORE, resolver = SHARED_RESOLVER;
        if (!store) {
          PENDING_CALLS.push({ type: "spawn", args: { childSession, flowId, nodeId, itemKey, pipelineId, agentId, kind, source } });
          return { ok: false, reason: "not_ready", queued: true };
        }
        if (!childSession || !flowId) return { ok: false, reason: "missing_args" };

        // Root comes from walking the flow's trigger edge, not a hardcoded
        // "flow-node": a flow-node subagent's cost should attribute to whatever
        // triggered the flow (cron, operator, …). `kind` is only the fallback
        // when the trigger edge hasn't landed yet — the HTTP ATTR_JOIN
        // re-attribution recovers those.
        const parentKey = `flow:${flowId}`;
        let rootKind = kind ?? "flow-node", rootLabel = null, rootSessionKey = childSession;
        if (resolver) {
          const chain = await resolver.walk(parentKey);
          const rootFrame = chain[chain.length - 1];
          if (rootFrame) {
            rootKind = rootFrame.rootKind ?? rootFrame.kind ?? rootKind;
            rootLabel = rootFrame.rootLabel ?? null;
            rootSessionKey = rootFrame.rootKey ?? rootFrame.key ?? rootSessionKey;
          }
        }

        await store.upsertLineage({
          child_session_key:  childSession,
          parent_session_key: parentKey,
          root_session_key:   rootSessionKey,
          kind:               kind ?? "flow-node",
          root_kind:          rootKind,
          root_label:         rootLabel,
          agent_id:           agentId ?? null,
          pipeline_id:        pipelineId ?? null,
          flow_id:            flowId,
          node_id:            nodeId ?? null,
          item_key:           itemKey ?? null,
          first_seen_at:      Date.now(),
          source,
        });
        return { ok: true };
      } catch (err) {
        logger.warn(`telemetry.recordSpawn: ${err?.message ?? err}`);
        return { ok: false, reason: err?.message ?? String(err) };
      }
    },
  };

  async function ensureStore() {
    if (SHARED_STORE) return SHARED_STORE;
    try {
      SHARED_STORE = await openStore({ dbPath });
      SHARED_RESOLVER = makeResolver({ store: SHARED_STORE });
    } catch (err) {
      logger.error(`telemetry CLI: lazy store init failed: ${err?.message ?? err}`);
    }
    return SHARED_STORE;
  }
  api.registerCli(
    (ctx) => registerCliCommands({
      program: ctx.program,
      logger,
      getStore: () => ensureStore(),
      getResolver: async () => { await ensureStore(); return SHARED_RESOLVER; },
      dbPath,
    }),
    {
      commands: ["lineage"],
      descriptors: [{
        name: "lineage",
        description: "Session lineage + per-call cost attribution (lookup / stats / cost)",
        hasSubcommands: true,
      }],
    },
  );

  logger.info("telemetry: registered (subagent_spawned + session_start + before_message_write + CLI)");
}

function parseTs(t) {
  if (typeof t === "number") return t;
  if (typeof t === "string") {
    const ms = Date.parse(t);
    return Number.isFinite(ms) ? ms : null;
  }
  return null;
}
