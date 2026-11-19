// ingest.js — telemetry event generation and reconciliation.
//
// Harnesses observe their own lifecycle; core owns what those observations
// mean. A harness adapter posts semantic events here (lineage.session,
// lineage.spawn, cost.usage) and this module derives the lineage/cost rows,
// resolves ancestry and applies the budget check. No harness field names
// appear below — the adapter's translation layer carries those.
//
// Events are best-effort: a malformed event is logged and dropped, never
// thrown back at the poster.

import { resolveCost } from "./pricer.js";
import { checkBudget } from "./budget-alert.js";

const KINDS = new Map([
  ["lineage.session", applyLineageSession],
  ["lineage.spawn", applyLineageSpawn],
  ["cost.usage", applyCostUsage],
]);

export function ingestKinds() {
  return [...KINDS.keys()];
}

export async function applyTelemetryEvent(event, deps) {
  const apply = KINDS.get(event?.kind);
  if (!apply) return { ok: false, code: "unknown_kind" };
  const { getStore, getResolver, logger = null } = deps;
  try {
    const store = getStore?.();
    const resolver = getResolver?.();
    if (!store || !resolver) return { ok: false, code: "store_not_ready" };
    await apply(event, { ...deps, store, resolver, logger });
    return { ok: true };
  } catch (err) {
    logger?.warn?.(`telemetry[${event?.kind}]: ${err?.message ?? err}`);
    return { ok: false, code: "apply_failed", error: err?.message ?? String(err) };
  }
}

async function applyLineageSession(event, { store, resolver }) {
  const session = event?.session;
  if (!session) return;
  const existing = await store.getLineage(session);
  if (existing && existing.source === "spawn_hook") return;
  const source = event?.source === "session_start" ? "session_start_hook" : "start_hook";
  const row = await resolver.resolve(session, { source });
  if (!row) return;
  row.agent_id = row.agent_id || event?.agentId || null;
  if (event?.parentSession) row.parent_session_key = event.parentSession;
  await store.upsertLineage(row);
}

async function applyLineageSpawn(event, { store, resolver }) {
  const childSession = event?.session;
  if (!childSession) return;
  const parentKey = event?.parentSession || null;
  const row = (await resolver.resolve(childSession, { source: "spawn_hook" })) || {};
  row.child_session_key = childSession;
  row.parent_session_key = parentKey || row.parent_session_key || null;
  row.agent_id = row.agent_id || event?.agentId || null;

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
  row.root_session_key = row.root_session_key || childSession;
  row.source = "spawn_hook";
  await store.upsertLineage(row);
}

async function applyCostUsage(event, { store, resolver, getBroadcast, logger, emit }) {
  const usage = event?.usage;
  if (!usage || typeof usage !== "object") return;
  const provider = event?.provider ?? null;
  const model = event?.model ?? null;
  const cost = resolveCost({ provider, model, usage });
  if (!cost || typeof cost.total !== "number") return;

  const session = event?.session ?? null;
  const sessionId = event?.sessionId ?? session ?? "unknown";
  const msgId = event?.messageId ?? `${sessionId}-${event?.timestamp ?? Date.now()}`;
  const ts = parseTs(event?.timestamp) ?? Date.now();

  let lineage = session ? await store.getLineage(session) : null;
  if (!lineage && session) {
    const row = await resolver.resolve(session, { source: "lazy_resolve" });
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
    session_key: session,
    agent_id: lineage?.agent_id ?? event?.agentId ?? null,
    root_session_key: lineage?.root_session_key ?? session,
    root_kind: lineage?.root_kind ?? lineage?.kind ?? null,
    root_label: lineage?.root_label ?? null,
    pipeline_id: lineage?.pipeline_id ?? null,
    flow_id: lineage?.flow_id ?? null,
    node_id: lineage?.node_id ?? null,
    item_key: lineage?.item_key ?? null,
  });
  try { await emit?.("cost.record.added", { costTotal: cost.total, flowId: lineage?.flow_id ?? null }); } catch {}
  checkBudget({ store, logger, justWroteUsd: cost.total, notify: (payload) => getBroadcast?.()?.("plugin.manifold.budget.alert", payload) });
}

function parseTs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms : null;
  }
  return null;
}
