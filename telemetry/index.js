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

import { openStore, DEFAULT_DB_PATH } from "./store.js";
import { makeResolver } from "./resolver.js";
import { registerCliCommands } from "./cli.js";
import { resolveCost } from "./pricer.js";
import { checkBudget, initCapStore } from "./budget-alert.js";
import { registerHttpRoutes } from "./http.js";
import { createEmitter } from "../events/emit.js";
// Hoisted to module-level so every call's hook closures share the same
// store/resolver. The SDK can call register() repeatedly (config reload,
// hot-reload); only the first service start actually opens the DB.
let SHARED_STORE = null;
let SHARED_RESOLVER = null;

// Live accessor for manifold modules that need to resolve a session's
// flow/item lineage (e.g. propose_draft's gate-context auto-resolve)
// without opening their own resolver instance. Returns null until the
// lineage-service's start() hook has run.
export function getSharedResolver() {
  return SHARED_RESOLVER;
}

// Calls that arrive before the store is ready (gateway boot revival) are
// buffered and flushed once the service starts.
let PENDING_CALLS = [];

export function registerTelemetry(api, { getBroadcast, _dbPath } = {}) {
  const dbPath = _dbPath ?? DEFAULT_DB_PATH;

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
        await initCapStore();
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

  // ── before_agent_start ─────────────────────────────────────────────────────

  api.on("before_agent_start", async (event, ctx) => {
    const store = SHARED_STORE, resolver = SHARED_RESOLVER;
    try {
      if (!store || !resolver) return;
      const key = event?.sessionKey || ctx?.sessionKey;
      if (!key) return;
      const existing = await store.getLineage(key);
      if (existing && existing.source === "spawn_hook") return;
      const row = await resolver.resolve(key, { source: "start_hook" });
      if (!row) return;
      row.agent_id = row.agent_id || event?.agentId || ctx?.agentId || null;
      if (ctx?.requesterSessionKey) row.parent_session_key = ctx.requesterSessionKey;
      await store.upsertLineage(row);
    } catch (err) {
      logger.warn(`telemetry[before_agent_start]: ${err?.message ?? err}`);
    }
  });

  // ── subagent_spawned ───────────────────────────────────────────────────────

  api.on("subagent_spawned", async (event, ctx) => {
    logger.info(`telemetry[subagent_spawned] FIRED child=${event?.childSessionKey ?? "?"}`);
    const store = SHARED_STORE, resolver = SHARED_RESOLVER;
    try {
      if (!store || !resolver) return;
      const childKey = event?.childSessionKey;
      const parentKey = ctx?.requesterSessionKey || null;
      if (!childKey) return;

      const row = (await resolver.resolve(childKey, { source: "spawn_hook" })) || {};
      row.child_session_key = childKey;
      row.parent_session_key = parentKey || row.parent_session_key || null;
      row.agent_id = row.agent_id || event.agentId || null;

      if (parentKey) {
        const pChain = await resolver.walk(parentKey);
        if (pChain.length) {
          const pRoot = pChain[pChain.length - 1];
          row.root_session_key = pRoot.key;
          row.root_kind = pRoot.kind;
          row.root_label = pRoot.rootLabel ?? pRoot.key;
        }
      }
      row.kind = row.kind || "unknown";
      row.root_kind = row.root_kind || row.kind;
      row.root_session_key = row.root_session_key || childKey;
      row.source = "spawn_hook";
      await store.upsertLineage(row);
    } catch (err) {
      logger.warn(`telemetry[subagent_spawned]: ${err?.message ?? err}`);
    }
  });

  // ── session_start ──────────────────────────────────────────────────────────

  api.on("session_start", async (event, ctx) => {
    logger.info(`telemetry[session_start] FIRED sid=${event?.sessionId ?? "?"} key=${(event?.sessionKey ?? ctx?.sessionKey ?? "?").slice(0, 80)}`);
    const store = SHARED_STORE, resolver = SHARED_RESOLVER;
    try {
      if (!store || !resolver) return;
      const key = event?.sessionKey || ctx?.sessionKey;
      if (!key) return;
      const existing = await store.getLineage(key);
      if (existing && existing.source === "spawn_hook") return;
      const row = await resolver.resolve(key, { source: "session_start_hook" });
      if (!row) return;
      row.agent_id = row.agent_id || event.agentId || ctx?.agentId || null;
      await store.upsertLineage(row);
    } catch (err) {
      logger.warn(`telemetry[session_start]: ${err?.message ?? err}`);
    }
  });

  // ── before_message_write ───────────────────────────────────────────────────

  api.on("before_message_write", async (event, ctx) => {
    const hasCost = event?.message?.usage?.cost?.total != null;
    const store = SHARED_STORE, resolver = SHARED_RESOLVER;
    if (!hasCost) return;
    try {
      if (!store || !resolver) { logger.warn(`telemetry[bmw] store/resolver not ready`); return; }
      const msg = event?.message;
      if (!msg || typeof msg !== "object") return;
      const usage = msg.usage || msg?.message?.usage;
      if (!usage) return;
      const provider = msg.provider ?? msg?.message?.provider ?? null;
      const model = msg.model ?? msg?.message?.model ?? null;
      const cost = resolveCost({ provider, model, usage });
      if (!cost || typeof cost.total !== "number") return;

      const sessionKey = event.sessionKey ?? ctx?.sessionKey ?? null;
      const sessionId = ctx?.sessionId ?? msg.sessionId ?? sessionKey ?? "unknown";
      const msgId = msg.id ?? msg.messageId ?? `${sessionId}-${msg.timestamp ?? Date.now()}`;
      const ts = parseTs(msg.timestamp) ?? Date.now();

      let lineage = sessionKey ? await store.getLineage(sessionKey) : null;
      if (!lineage && sessionKey) {
        const row = await resolver.resolve(sessionKey, { source: "lazy_resolve" });
        if (row) { await store.upsertLineage(row); lineage = row; }
      }

      await store.insertCost({
        session_id: sessionId,
        msg_id: msgId,
        ts,
        provider, model,
        input_tokens: usage.input ?? null,
        output_tokens: usage.output ?? null,
        cache_read_tokens: usage.cacheRead ?? null,
        cache_write_tokens: usage.cacheWrite ?? null,
        total_tokens: usage.totalTokens ?? usage.total ?? null,
        cost_input: cost.input ?? null,
        cost_output: cost.output ?? null,
        cost_cache_read: cost.cacheRead ?? null,
        cost_cache_write: cost.cacheWrite ?? null,
        cost_total: cost.total,
        session_key: sessionKey,
        agent_id: lineage?.agent_id ?? ctx?.agentId ?? null,
        root_session_key: lineage?.root_session_key ?? sessionKey,
        root_kind: lineage?.root_kind ?? lineage?.kind ?? null,
        root_label: lineage?.root_label ?? null,
        pipeline_id: lineage?.pipeline_id ?? null,
        flow_id: lineage?.flow_id ?? null,
        node_id: lineage?.node_id ?? null,
        item_key: lineage?.item_key ?? null,
      });
      try { await emit("cost.record.added", { costTotal: cost.total, flowId: lineage?.flow_id ?? null }); } catch {}
      checkBudget({ store, logger, justWroteUsd: cost.total, notify: (payload) => getBroadcast?.()?.("plugin.manifold.budget.alert", payload) });
    } catch (err) {
      logger.warn(`telemetry[before_message_write] ERROR: ${err?.message ?? err}`);
    }
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

    async recordSpawn({ childSessionKey, flowId, nodeId, itemKey, pipelineId, agentId, kind, source = "spawn_hook" }) {
      try {
        const store = SHARED_STORE, resolver = SHARED_RESOLVER;
        if (!store) {
          PENDING_CALLS.push({ type: "spawn", args: { childSessionKey, flowId, nodeId, itemKey, pipelineId, agentId, kind, source } });
          return { ok: false, reason: "not_ready", queued: true };
        }
        if (!childSessionKey || !flowId) return { ok: false, reason: "missing_args" };

        // Resolve the real root by walking the flow's own trigger edge
        // (flow:<flowId>) instead of hardcoding "flow-node" — a flow-node
        // subagent's cost should attribute to whatever triggered the flow
        // (cron, operator, etc.), not to a made-up kind of its own. Falls
        // back to `kind ?? "flow-node"` only if the resolver isn't ready or
        // the trigger edge hasn't landed yet (race between flow start and
        // recordTrigger) — the HTTP-layer ATTR_JOIN re-attribution still
        // recovers those.
        const parentKey = `flow:${flowId}`;
        let rootKind = kind ?? "flow-node", rootLabel = null, rootSessionKey = childSessionKey;
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
          child_session_key:  childSessionKey,
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
