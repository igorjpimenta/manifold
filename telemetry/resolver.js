// resolver — session → parent + root + breadcrumbs, via three layers tried in
// order: the lineage table, the session grammar, then external joins
// (flow_runs, cron jobs.json).

import path from "node:path";
import { getSharedDb } from "../state/db.js";
import os from "node:os";
import fs from "node:fs";
import { resolveDataDir, runtimeDbPath } from "../state/paths.js";
import { classify, agentIdOf } from "./grammar.js";
import { execAll } from "./http-helpers.js";

// The cron path is the harness's own jobs.json — an external join, not a
// manifold store.
const FLOWS_DB_PATH = runtimeDbPath(resolveDataDir());
const CRON_JSON_PATH = path.join(os.homedir(), ".openclaw", "cron", "jobs.json");

export function makeResolver({ store, flowsDbPath = FLOWS_DB_PATH, cronJsonPath = CRON_JSON_PATH } = {}) {
  let flowsByPrefix = null;
  let cronJobs = null;
  let lastFlowsLoadTs = 0;
  let lastCronLoadTs = 0;
  const FLOWS_TTL_MS = 30_000;  // flow_runs grows quickly
  const CRON_TTL_MS = 60_000;

  async function loadFlows() {
    if (flowsByPrefix && Date.now() - lastFlowsLoadTs < FLOWS_TTL_MS) return;
    if (!fs.existsSync(flowsDbPath)) { flowsByPrefix = new Map(); return; }
    const { client } = await getSharedDb(flowsDbPath, { readonly: true });
    // flow_runs has pipeline_id as a direct column and no controller_id.
    // Callers swallow resolver errors, so a schema surprise surfaces as
    // missing lineage rather than a throw.
    const rows = await execAll(client,
      `SELECT flow_id, owner_key, pipeline_id, status, created_at FROM flow_runs`
    );
    const map = new Map();
    for (const r of rows) {
      const k = String(r.flow_id).slice(0, 8);
      if (!map.has(k)) map.set(k, []);
      map.get(k).push(r);
    }
    flowsByPrefix = map;
    lastFlowsLoadTs = Date.now();
  }

  function loadCron() {
    if (cronJobs && Date.now() - lastCronLoadTs < CRON_TTL_MS) return;
    cronJobs = new Map();
    if (!fs.existsSync(cronJsonPath)) return;
    try {
      const doc = JSON.parse(fs.readFileSync(cronJsonPath, "utf8"));
      for (const j of (doc?.jobs || [])) if (j?.id) cronJobs.set(j.id, j);
      lastCronLoadTs = Date.now();
    } catch { /* treat as empty */ }
  }

  async function flowFor(flow8) {
    await loadFlows();
    const bucket = flowsByPrefix.get(flow8) || [];
    // Prefix-hash collision: prefer the most recent.
    return bucket.length ? bucket.reduce((a, b) => (a.created_at >= b.created_at ? a : b)) : null;
  }

  function cronFor(cronId) { loadCron(); return cronJobs.get(cronId) || null; }

  async function classifyWithExternal(session, opts = {}) {
    // `flow:<uuid>` is synthetic, minted here as the parent of all flow
    // subagents; the grammar only knows `agent:*` keys. Read the trigger-edge
    // row (written by recordTrigger) and return its root, stopping the walk.
    // If the edge isn't written yet, emit a "flow-triggered" placeholder so
    // the walk stops instead of returning "unknown".
    if (typeof session === "string" && session.startsWith("flow:") && store) {
      const flowId = session.slice("flow:".length);
      const edge = await store.getLineage(session);
      if (edge) {
        return {
          kind: edge.kind ?? "flow-triggered",
          agentId: null,
          flowId,
          pipelineId: edge.pipeline_id ?? opts.pipelineId ?? null,
          nodeId: null,
          itemKey: null,
          parentKey: null,
          rootKey:   edge.root_session_key ?? null,
          rootKind:  edge.root_kind  ?? null,
          rootLabel: edge.root_label ?? null,
        };
      }
      // Race: trigger edge not written yet — best-effort pipeline name from
      // flow_runs.
      const fr = await flowFor(flowId.slice(0, 8));
      return {
        kind: "flow-triggered",
        agentId: null,
        flowId,
        pipelineId: fr?.pipeline_id ?? opts.pipelineId ?? null,
        nodeId: null,
        itemKey: null,
        parentKey: null,
        rootKey: null,
        rootKind: null,
        rootLabel: fr?.pipeline_id ?? null,
      };
    }

    const g = classify(session);
    // Forward scope-specific fields (cronName, telegramChatId, …) so
    // rootLabelOf can label without re-parsing.
    const out = {
      ...g,
      pipelineId: opts.pipelineId ?? null,
      flowId: opts.flowId ?? null,
      nodeId: g.nodeId ?? null,
      itemKey: g.itemKey ?? null,
      parentKey: null,
    };
    delete out.parentHint;

    if (!g.parentHint) return out;

    if (g.parentHint.kind === "flow") {
      const fr = await flowFor(g.parentHint.flowShort);
      if (fr) {
        // Parent is the flow's trigger edge (`flow:<id>`), not owner_key: the
        // edge carries the genuine root (cron-tick / operator-trigger), while
        // owner_key names the owner session and would mis-attribute discuss
        // sessions and gate-spawned subagents.
        out.parentKey = `flow:${fr.flow_id}`;
        out.flowId = fr.flow_id;
        out.pipelineId = fr.pipeline_id;
      } else if (store) {
        // Flow pruned from flow_runs — probe session_lineage for the trigger
        // edge, matched by the 8-char prefix (that's all we hold).
        const edge = await execAll(store._client,
          `SELECT child_session_key FROM session_lineage
           WHERE child_session_key LIKE ?
             AND source IN ('trigger_hook', 'trigger_backfill')
           LIMIT 1`,
          [`flow:${g.parentHint.flowShort}%`]
        ).then((rows) => rows[0] ?? null);
        if (edge) {
          const fullFlowId = edge.child_session_key.slice(5); // strip 'flow:'
          out.parentKey = edge.child_session_key;
          out.flowId = fullFlowId;
        }
      }
      return out;
    }
    if (g.parentHint.kind === "cron-id") {
      const job = cronFor(g.parentHint.cronId);
      out.parentKey = job?.sessionKey || null;
      return out;
    }
    if (g.parentHint.kind === "memory-synth") {
      // No on-disk session for the synth run itself — the chunk IS the leaf.
      return out;
    }
    return out;
  }

  /** Leaf-to-root frames, stopping on cycle or depth limit; the lineage table
   *  short-circuits the walk. */
  async function walk(session, { maxDepth = 16, useCache = true } = {}) {
    const chain = [];
    const seen = new Set();
    let cur = session;
    while (cur && chain.length < maxDepth) {
      if (seen.has(cur)) { chain.push({ key: cur, kind: "cycle" }); break; }
      seen.add(cur);

      if (useCache && store) {
        const cached = await store.getLineage(cur);
        if (cached) {
          // Stale miss: root_kind "unknown" + a synthetic flow:* root means the
          // trigger edge landed after this row was written — don't propagate
          // the stale root; the live walk re-derives it.
          const staleFlowRoot = cached.root_kind === "unknown" &&
            typeof cached.root_session_key === "string" &&
            cached.root_session_key.startsWith("flow:");
          chain.push({
            key: cur,
            kind: cached.kind,
            agentId: cached.agent_id,
            pipelineId: cached.pipeline_id,
            flowId: cached.flow_id,
            nodeId: cached.node_id,
            itemKey: cached.item_key,
            rootKey:   staleFlowRoot ? undefined : cached.root_session_key,
            rootKind:  staleFlowRoot ? undefined : cached.root_kind,
            rootLabel: staleFlowRoot ? undefined : cached.root_label,
          });
          if (!cached.parent_session_key || cached.parent_session_key === cur) break;
          cur = cached.parent_session_key;
          continue;
        }
      }

      // Push the whole classifier output so rootLabelOf sees its scope fields.
      const c = await classifyWithExternal(cur);
      chain.push({ ...c, key: cur });
      if (!c.parentKey || c.parentKey === cur) break;
      cur = c.parentKey;
    }
    const last = chain[chain.length - 1];
    if (last) {
      const rootKey = last.rootKey ?? last.key;
      const rootKind = last.rootKind ?? last.kind;
      const rootLabel = last.rootLabel ?? rootLabelOf(last);
      for (const f of chain) {
        f.rootKey = f.rootKey ?? rootKey;
        f.rootKind = f.rootKind ?? rootKind;
        f.rootLabel = f.rootLabel ?? rootLabel;
      }
    }
    return chain;
  }

  async function resolve(session, opts = {}) {
    const chain = await walk(session, opts);
    if (!chain.length) return null;
    const leaf = chain[0], root = chain[chain.length - 1];
    // Inherit pipeline/flow/node/item from the nearest ancestor carrying them:
    // a leaf's key often doesn't encode flow context, and without propagation
    // the cost row lands under "Direct (no pipeline)" instead of its flow+node.
    let pipelineId = leaf.pipelineId, flowId = leaf.flowId;
    let nodeId = leaf.nodeId, itemKey = leaf.itemKey;
    for (const frame of chain) {
      pipelineId = pipelineId || frame.pipelineId;
      flowId     = flowId     || frame.flowId;
      nodeId     = nodeId     || frame.nodeId;
      itemKey    = itemKey    || frame.itemKey;
      if (pipelineId && flowId && nodeId && itemKey) break;
    }
    return {
      child_session_key: leaf.key,
      parent_session_key: chain[1]?.key ?? null,
      root_session_key: root.key,
      kind: leaf.kind,
      root_kind: root.kind,
      root_label: root.rootLabel ?? rootLabelOf(root),
      agent_id: leaf.agentId,
      pipeline_id: pipelineId,
      flow_id: flowId,
      node_id: nodeId,
      item_key: itemKey,
      first_seen_at: opts.firstSeenAt ?? Date.now(),
      source: opts.source ?? "lazy_resolve",
    };
  }

  return { resolve, walk, classifyWithExternal, flowFor, cronFor };
}

export function rootLabelOf(frame) {
  if (!frame) return null;
  switch (frame.kind) {
    case "cron-owner":         return frame.cronName || frame.key;
    case "telegram-dm":        return frame.telegramChatId || frame.key;
    case "explicit":           return frame.explicitPurpose || frame.key;
    // The operator-trigger key carries no pipelineId: routes/operator.js's
    // recordTrigger stamps the real pipelineId on the flow/node cost rows
    // directly, so this fallback only labels cost attributed to the bare
    // operator session — at least split by flow.
    case "operator-trigger":   return frame.triggerFlowId ? `Operator run ${frame.triggerFlowId.slice(0, 8)}` : "Operator runs";
    case "agent-default":      return `agent:${frame.agentId}`;
    case "memory-synth-chunk": return `memory-synth:${frame.memorySynthRun || ""}`;
    case "flow-prime":         return `flow-prime:${frame.primeRef || ""}`;
    case "bare-subagent":      return `bare-subagent:${frame.bareUuid?.slice(0, 8) || ""}`;
    default:                   return frame.key;
  }
}
