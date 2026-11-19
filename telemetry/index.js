// telemetry/index.js — lineage + cost attribution wired into manifold.
// Absorbed from extensions/lineage/; exposes registerTelemetry(api) instead of
// definePluginEntry so manifold's own plugin entry calls it inline.
//
// Responsibilities (unchanged from lineage):
//   - session_lineage + cost_by_call writes via before_agent_start /
//     subagent_spawned / session_start / before_message_write hooks
//   - /manifold/telemetry/* HTTP read-only endpoints (cost, tree, budget cap)
//   - api.runtime.lineage.recordTrigger / recordSpawn cross-plugin API
//   - `openclaw lineage` CLI subcommands

import { openStore, defaultDbPath } from "./store.js";
import { makeResolver } from "./resolver.js";
import { registerCliCommands } from "./cli.js";
import { resolveCost } from "./pricer.js";
import { checkBudget, initCapStore } from "./budget-alert.js";
import { registerHttpRoutes } from "./http.js";
import { applyTelemetryEvent } from "./ingest.js";
import { authorizeBearerRequest, sendUnauthorized } from "../routes/bearer-auth.js";
import { createEmitter } from "../events/emit.js";
// Hoisted to module-level so every call's hook closures share the same
// store/resolver. The SDK can call register() repeatedly (config reload,
// hot-reload); only the first service start actually opens the DB.
let SHARED_STORE = null;
let SHARED_RESOLVER = null;

// Live accessor for manifold modules that need to resolve a session's
// flow/item lineage without opening their own resolver instance. Returns
// null until the lineage-service's start() hook has run.
export function getSharedResolver() {
  return SHARED_RESOLVER;
}

// Calls that arrive before the store is ready (gateway boot revival) are
// buffered and flushed once the service starts.
let PENDING_CALLS = [];

export function registerTelemetry(api, { getBroadcast, _dbPath } = {}) {
  const dbPath = _dbPath ?? defaultDbPath();

  const logger = api.logger;

  // Canonical-kind fan-out for cost.record.added below: canonical wire
  // name, envelope adds kind.
  const { emit } = createEmitter({ getBroadcast, logger });

  // ── Budget-cap store service ────────────────────────────────────────────────
  // Must load before checkBudget()/the /budget/cap HTTP routes see traffic —
  // otherwise they'd read the DEFAULT_CAP_USD fallback instead of the
  // operator-set value.

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

  // ── Store service ──────────────────────────────────────────────────────────

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

  // ── Ingest ─────────────────────────────────────────────────────────────────
  // Harnesses observe their own lifecycle; core decides what those
  // observations mean. Adapters post semantic events (lineage.session,
  // lineage.spawn, cost.usage) and ingest.js derives the rows.
  const ingestToken = process.env.MANIFOLD_INGEST_TOKEN || process.env.MANIFOLD_BEARER || "";

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

  // ── HTTP routes ────────────────────────────────────────────────────────────

  registerHttpRoutes({ api, getStore: () => SHARED_STORE, logger, getBroadcast });

  // ── Runtime API ────────────────────────────────────────────────────────────

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
          } catch { /* fall through with self-root defaults */ }
        }

        // An "explicit" root is a hand-typed/ad hoc session purpose string —
        // it has no durable identity worth grouping by, and labels.js collapses
        // it to "Ad-hoc sessions" once it's UUID-suffixed. The pipeline this
        // trigger is dispatching is the far more useful attribution — same
        // convention routes/operator.js already uses for its parent-less
        // synthetic trigger path (rootKind: "explicit", rootLabel: pipelineId).
        //
        // "operator-trigger" hits the identical problem via a different route:
        // routes/operator.js's synthetic `agent:operator:(cli|operator):
        // <flowId>` key is never itself a stored child_session_key (only
        // `flow:<id>` rows get upserted), so a subflow dispatched from an
        // agent session whose chain reaches that key directly — rather than
        // through the seeded flow:<topFlowId> row — gets classified fresh by
        // grammar.js from the key shape alone, landing on resolver.js's
        // generic "Operator run <shortid>" / "Operator runs" fallback (no
        // pipelineId access there). Same fix applies: prefer the pipeline
        // we're actually dispatching over an uninformative fallback label.
        //
        // pipelineId may be a composite child id (engine/loader.js's
        // `<pipeline>::NODE[::NODE...]` shape for a subflow dispatch, e.g.
        // `freelance_freelancer-proposal::PROPOSE::SUBMIT`) — every subflow of
        // one manually-triggered run independently calls recordTrigger with
        // its own composite id, but they must all collapse to the SAME root
        // label so http-tree.js's GROUP BY root_label merges them into one
        // instance instead of fragmenting one run into N rows, one per node.
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

        // Resolve the real root by walking the flow's own trigger edge
        // (flow:<flowId>) instead of hardcoding "flow-node" — a flow-node
        // subagent's cost should attribute to whatever triggered the flow
        // (cron, operator, etc.), not to a made-up kind of its own. Falls
        // back to `kind ?? "flow-node"` only if the resolver isn't ready or
        // the trigger edge hasn't landed yet (race between flow start and
        // recordTrigger) — the HTTP-layer ATTR_JOIN re-attribution still
        // recovers those.
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

  // ── CLI ────────────────────────────────────────────────────────────────────

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
