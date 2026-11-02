// resolver — given a sessionKey, return its parent + root + breadcrumbs.
//
// Three resolution layers tried in order:
//   1. lineage table (fastest; populated by hooks + backfill)
//   2. sessionKey grammar (parses scope kind out of the key string)
//   3. external joins (flow_runs sqlite, cron jobs.json)
//
// `resolve(key)` returns a fully-populated row ready for `store.upsertLineage`,
// even for sessions never seen before. `walk(key)` returns the full chain
// from leaf to root.

import path from "node:path";
import { getSharedDb } from "../state/db.js";
import os from "node:os";
import fs from "node:fs";
import { classify, agentIdOf } from "./grammar.js";
import { execAll } from "./http-helpers.js";

// runtime.sqlite, not flows/registry.sqlite — manifold no longer writes
// flow_runs rows to the latter; runtime.sqlite is the live store (same
// table shape) everywhere else in manifold.
const FLOWS_DB_PATH = path.join(os.homedir(), ".openclaw", "runtime.sqlite");
const CRON_JSON_PATH = path.join(os.homedir(), ".openclaw", "cron", "jobs.json");

// ─── External joins (lazy-loaded, cached per resolver instance) ──────────────

export function makeResolver({ store, flowsDbPath = FLOWS_DB_PATH, cronJsonPath = CRON_JSON_PATH } = {}) {
  let flowsByPrefix = null;     // flow8 → [{ flow_id, owner_key, pipeline_id, status, created_at }]
  let cronJobs = null;          // cronId → { name, sessionKey, agentId, ... }
  let lastFlowsLoadTs = 0;
  let lastCronLoadTs = 0;
  const FLOWS_TTL_MS = 30_000;  // poll fresh data every 30s; flow_runs grows quickly
  const CRON_TTL_MS = 60_000;

  async function loadFlows() {
    if (flowsByPrefix && Date.now() - lastFlowsLoadTs < FLOWS_TTL_MS) return;
    if (!fs.existsSync(flowsDbPath)) { flowsByPrefix = new Map(); return; }
    const { client } = await getSharedDb(flowsDbPath, { readonly: true });
    // flow_runs carries pipeline_id as a direct column (the SELECT below
    // depends on that shape); there is no controller_id column. Failures
    // here are swallowed by the try/catch in every hook that calls
    // resolver.walk/resolve, so a schema surprise shows up as missing
    // lineage rather than a throw.
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
    } catch { /* corrupted/missing — treat as empty */ }
  }

  /** Look up flow record by 8-char prefix. May return null if pruned. */
  async function flowFor(flow8) {
    await loadFlows();
    const bucket = flowsByPrefix.get(flow8) || [];
    // If multiple matches (very rare hash collision), prefer most-recent
    return bucket.length ? bucket.reduce((a, b) => (a.created_at >= b.created_at ? a : b)) : null;
  }

  /** Look up cron job by id. May return null if cron was deleted. */
  function cronFor(cronId) { loadCron(); return cronJobs.get(cronId) || null; }

  // ─── Single-step resolution: given a key, who is its parent? ───────────────

  async function classifyWithExternal(sessionKey, opts = {}) {
    // `flow:<uuid>` is a synthetic key minted by THIS resolver (below) as the
    // parent node for all flow subagents. The grammar only knows `agent:*`
    // keys so classify() returns "unknown" for it. Short-circuit here: read
    // the trigger-edge row from session_lineage (written by recordTrigger).
    // If the row is there, return its root directly and stop walking. If not
    // yet written, emit a "flow-triggered" placeholder so the walk stops
    // gracefully instead of returning "unknown".
    if (typeof sessionKey === "string" && sessionKey.startsWith("flow:") && store) {
      const flowId = sessionKey.slice("flow:".length);
      const edge = await store.getLineage(sessionKey);
      if (edge) {
        return {
          kind: edge.kind ?? "flow-triggered",
          agentId: null,
          flowId,
          pipelineId: edge.pipeline_id ?? opts.pipelineId ?? null,
          nodeId: null,
          itemKey: null,
          parentKey: null,           // treat the trigger-edge as root; stop walking
          rootKey:   edge.root_session_key ?? null,
          rootKind:  edge.root_kind  ?? null,
          rootLabel: edge.root_label ?? null,
        };
      }
      // Trigger edge not yet written (race between flow start and recordTrigger).
      // Use the pipeline name from flow_runs as a best-effort label.
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

    const g = classify(sessionKey);
    // Forward every scope-specific field from the grammar (cronName,
    // telegramChatId, explicitPurpose, memorySynthRun, etc.) so downstream
    // rootLabelOf can produce a human label without re-parsing.
    const out = {
      ...g,
      pipelineId: opts.pipelineId ?? null,
      flowId: opts.flowId ?? null,
      nodeId: g.nodeId ?? null,
      itemKey: g.itemKey ?? null,
      parentKey: null,
    };
    delete out.parentHint;

    if (!g.parentHint) return out;             // already-root scopes

    if (g.parentHint.kind === "flow") {
      const fr = await flowFor(g.parentHint.flowShort);
      if (fr) {
        // Link the child to the flow's TRIGGER EDGE (synthetic `flow:<id>`
        // row in session_lineage), not the flow's owner_key. The trigger
        // edge carries the genuine root (cron-tick / operator-trigger /
        // etc.); owner_key is always graph-flow-owner and would mis-
        // attribute every discuss session and gate-spawned subagent to
        // the "Operator runs" instance.
        out.parentKey = `flow:${fr.flow_id}`;
        out.flowId = fr.flow_id;
        out.pipelineId = fr.pipeline_id;
      } else if (store) {
        // Flow pruned from flow_runs — probe session_lineage for the trigger
        // edge directly. The edge key is 'flow:<full-uuid>' but we only have
        // the 8-char prefix, so use a LIKE prefix match.
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
      // memory-synth chunks belong to a synth-run; treat the synth-run as root.
      // There's no on-disk session for the run itself; the chunk IS the leaf.
      return out;
    }
    return out;
  }

  /**
   * Walk up from `sessionKey` to the root. Returns an array of frames
   * (leaf-to-root). Stops on cycle or depth limit. Uses the lineage table
   * as a short-circuit cache.
   */
  async function walk(sessionKey, { maxDepth = 16, useCache = true } = {}) {
    const chain = [];
    const seen = new Set();
    let cur = sessionKey;
    while (cur && chain.length < maxDepth) {
      if (seen.has(cur)) { chain.push({ key: cur, kind: "cycle" }); break; }
      seen.add(cur);

      // Try cache first
      if (useCache && store) {
        const cached = await store.getLineage(cur);
        if (cached) {
          // Stale-miss detection: if root_kind was cached as "unknown" and the
          // root_session_key is a synthetic flow:* key, the trigger edge hadn't
          // landed yet when this row was first written. Don't propagate the stale
          // root — leave rootKey/rootKind/rootLabel undefined so the annotation
          // re-derives them from the live walk that follows.
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

      // Live resolve — push the whole classifier output so rootLabelOf has
      // access to scope-specific fields (cronName, telegramChatId, etc.).
      const c = await classifyWithExternal(cur);
      chain.push({ ...c, key: cur });
      if (!c.parentKey || c.parentKey === cur) break;
      cur = c.parentKey;
    }
    // Annotate root onto every frame
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

  /**
   * Compute the storeable row for a single sessionKey (full lineage stamp).
   * Used by the hooks and by backfill.
   */
  async function resolve(sessionKey, opts = {}) {
    const chain = await walk(sessionKey, opts);
    if (!chain.length) return null;
    const leaf = chain[0], root = chain[chain.length - 1];
    // Inherit pipeline/flow/node/item from the nearest ancestor that
    // carries them. The leaf's own key may be a bare-subagent UUID
    // (proposal-sender, content-writer side-spawns) or any session
    // whose shape doesn't encode flow context — but its parent
    // (a discuss-gate session, a flow-node subagent, the flow trigger
    // edge itself) almost always does. Without this propagation the
    // cost row written for each LLM call stays NULL on flow_id and
    // node_id, and the cost tree buckets it under "Direct (no
    // pipeline)" instead of under its real flow + node + item.
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

/** Human-readable label for a root session (cron name, telegram chat id, etc.). */
export function rootLabelOf(frame) {
  if (!frame) return null;
  switch (frame.kind) {
    case "cron-owner":         return frame.cronName || frame.key;
    case "telegram-dm":        return frame.telegramChatId || frame.key;
    case "explicit":           return frame.explicitPurpose || frame.key;
    // The operator-trigger session key (agent:operator:(cli|operator):<flowId-uuid>)
    // carries no pipelineId — routes/operator.js's recordTrigger call stamps
    // the real pipelineId onto the flow/node cost rows directly (see
    // labels.js's displayRootLabel case "operator-trigger"), so this fallback
    // only fires for cost attributed to the bare operator session itself.
    // Distinguish by flow at least, rather than collapsing every run together.
    case "operator-trigger":   return frame.triggerFlowId ? `Operator run ${frame.triggerFlowId.slice(0, 8)}` : "Operator runs";
    case "agent-default":      return `agent:${frame.agentId}`;
    case "memory-synth-chunk": return `memory-synth:${frame.memorySynthRun || ""}`;
    case "flow-prime":         return `flow-prime:${frame.primeRef || ""}`;
    case "bare-subagent":      return `bare-subagent:${frame.bareUuid?.slice(0, 8) || ""}`;
    case "manifold-subagent":   return `manifold:${frame.flowShort || ""}`;
    default:                   return frame.key;
  }
}
