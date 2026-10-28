// events/flow-presence.js — harness presence over the event queue.
//
// A harness subscriber connects to the WS event server and immediately
// receives the currently-open flows for its harness, followed by the
// live queue. Presence, not revival: the queue replays history on every
// connect, so a missed frame costs a re-read, never a lost event.
//
// Single-harness invariant: one manifold instance serves one harness, so
// the open-flows snapshot is the instance's non-terminal set, labelled
// with the subscribing harness. Per-entry filtering keys off
// entry.session.harness when present; untagged entries are instance-wide
// and visible to every subscriber.
import { TERMINAL_STATUS_SET } from "../state/run-store.js";

export function entryHarness(entry) {
  const h = entry?.session?.harness;
  if (typeof h !== "string") return null;
  const trimmed = h.trim();
  return trimmed !== "" ? trimmed : null;
}

// No harness on the subscriber: the legacy verbatim feed, everything
// passes. A harness-scoped subscriber sees its own tagged entries plus
// untagged instance-wide ones — never another harness's tagged entries.
export function visibleTo(entry, harness) {
  if (!harness) return true;
  const h = entryHarness(entry);
  return h === null || h === harness;
}

export function filterEntries(entries, harness) {
  if (!harness || !Array.isArray(entries)) return entries;
  return entries.filter((e) => visibleTo(e, harness));
}

function openFlowRow(flow, harness) {
  return {
    id: flow.flowId,
    pipelineId: flow.pipelineId ?? null,
    nodeId: flow.waitJson?.nodeId ?? flow.lastStep ?? null,
    status: flow.status,
    harness,
  };
}

// The connect-time snapshot: every non-terminal flow row. Prefers the
// store's own active-rows query; falls back to a full list filtered
// against the terminal set so stub stores stay usable.
export async function collectOpenFlows({ runStore, harness = null } = {}) {
  if (!runStore) return [];
  try {
    if (typeof runStore.listActive === "function") {
      const rows = await runStore.listActive();
      return (Array.isArray(rows) ? rows : []).map((f) => openFlowRow(f, harness));
    }
    const all = typeof runStore.list === "function"
      ? await runStore.list({ includeChildren: true })
      : [];
    return (Array.isArray(all) ? all : [])
      .filter((f) => f && !TERMINAL_STATUS_SET.has(f.status))
      .map((f) => openFlowRow(f, harness));
  } catch {
    return [];
  }
}

// Live queue entry for a flow transition. Terminal rows are included on
// purpose: the snapshot covers the open set, the stream covers movement,
// and a harness learns about closes the same way it learns about opens.
export function presenceEvent(flow, harness = null) {
  if (!flow) return null;
  return {
    event: "flow.changed",
    at: Date.now(),
    flow: {
      id: flow.flowId,
      pipelineId: flow.pipelineId ?? null,
      nodeId: flow.waitJson?.nodeId ?? flow.lastStep ?? null,
      status: flow.status ?? null,
    },
    session: { harness: harness ?? null },
  };
}

// Producer wiring for serve --ws: every flow-state change becomes one
// queue entry. Delivery faults never throw into the producer — a failed
// lookup or append only logs. Returns an unsubscribe function.
export function attachPresenceProducer({ stream, runStore, harness = null, subscribe, logger = null } = {}) {
  if (!stream || typeof stream.append !== "function") throw new Error("flow-presence: attachPresenceProducer requires a stream");
  if (!runStore || typeof runStore.get !== "function") throw new Error("flow-presence: attachPresenceProducer requires a runStore");
  if (typeof subscribe !== "function") throw new Error("flow-presence: attachPresenceProducer requires a subscribe function");
  const onChange = (flowId) => {
    (async () => {
      try {
        const flow = await runStore.get(flowId);
        const entry = presenceEvent(flow, harness);
        if (entry) stream.append(entry);
      } catch (err) {
        try { logger?.warn?.(`flow-presence: producer failed: ${err?.message ?? err}`); } catch {}
      }
    })().catch(() => {});
  };
  return subscribe(onChange);
}
