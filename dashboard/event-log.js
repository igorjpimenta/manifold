// event-log.js — synthesise a flow-change event stream for the dashboard.
//
// Driven by flowStateEmitter (state-store.js fires after every write) so
// each change lands within a single process.nextTick — no polling lag.
// Emits (canonical kinds):
//   - run.created : first time we see a flowId
//   - run.updated : revision advanced; carries {prevStep, currStep, prevStatus, currStatus, stateKeysAdded, stateKeysRemoved}
//   - run.ended   : status moved to a terminal state
//
// stateKeysAdded/stateKeysRemoved: diffed off which node_ids have a
// flow_node_invocations row for this flow (nodeExecutionsStore.
// listVisitedNodeIdsByFlow, same store method dashboard/stats.js's
// funnel-visit check uses), not off Object.keys(stateJson) — a raw
// key-diff goes silent for node dispatches that never grow state_json.
// Node-invocation existence is the better signal anyway: it fires the
// instant a node is dispatched, not only once its output merges into
// state_json.
//
// Each event is delivered to live WS subscribers AND appended to a per-flow
// ring buffer (cap 256 events) so the run-detail page can replay history on
// first load.
//
// When `dbPath` is supplied (wired from index.js), events are also written to a
// `flow_events` SQLite table so history survives gateway restarts. getEventsFor()
// falls back to SQL when the in-memory buffer is empty (cold read after restart).

import { mkdirSync } from "node:fs";
import { openDb } from "../state/db.js";
import { enqueueRaw } from "../state/batch-writer.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { resolveDataDir, runtimeDbPath } from "../state/paths.js";
import { flowStateEmitter } from "../events.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "../state/migrate.js";

const DEFAULT_EVENTS_DB_PATH = runtimeDbPath(resolveDataDir());

async function openEventsDb(dbPath, logger) {
  try {
    mkdirSync(path.dirname(dbPath), { recursive: true });
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    const { client } = await openDb(dbPath);
    return client;
  } catch (err) {
    logger?.warn?.(`manifold event-log: failed to open events DB: ${err?.message ?? err}`);
    return null;
  }
}

const RING_CAP = 256;

const INSERT_SQL = `INSERT OR IGNORE INTO flow_events (id, flow_id, event_type, prev_step, curr_step, prev_status, curr_status, wait_kind, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

export function createEventBus({ store, nodeExecutionsStore, logger, dbPath = DEFAULT_EVENTS_DB_PATH }) {
  let _client = null;
  if (dbPath) openEventsDb(dbPath, logger).then(c => { _client = c; });
  // Map<flowId, { revision, status, lastStep, stateKeys: Set<string>, terminal: boolean }>
  const last = new Map();

  // Map<flowId, Event[]>  (ring buffer per flow)
  const buffers = new Map();

  // Set<(event) => void>
  const subscribers = new Set();

  let running = false;
  // Bound listener stored so we can remove it on stop().
  let changeListener = null;

  function emit(event) {
    const list = buffers.get(event.flowId) ?? [];
    list.push(event);
    if (list.length > RING_CAP) list.splice(0, list.length - RING_CAP);
    buffers.set(event.flowId, list);
    // Persist to SQLite so history survives restarts (best-effort, fire-and-
    // forget — errors already swallowed, nothing re-reads the row in the
    // same call). Routed through batch-writer's enqueueRaw rather than a
    // bare client.execute(): this fires on every run.created/run.updated/
    // run.ended transition anywhere in the system — the single
    // highest-frequency write in the app — so an unretried write here both
    // risked silently dropping history rows under SQLITE_BUSY contention
    // and meant every one of those transitions issued its own individual
    // round-trip instead of coalescing. enqueueRaw resolves its own shared
    // connection via dbPath independently of this bus's own `_client` (used
    // only for reads/close below), so this needs no dependency on `_client`
    // having resolved yet either.
    if (dbPath && event.flowId && event.kind) {
      enqueueRaw(dbPath, INSERT_SQL, [randomUUID(), event.flowId, event.kind, event.prevStep ?? null, event.currStep ?? null, event.prevStatus ?? null, event.currStatus ?? null, event.waitKind ?? null, event.at ?? Date.now()]);
    }
    for (const cb of subscribers) {
      try { cb(event); } catch (err) {
        logger?.warn?.(`manifold dashboard event-bus subscriber error: ${err?.message ?? err}`);
      }
    }
  }

  async function visitedNodeIdSet(flowId) {
    try {
      const map = await nodeExecutionsStore?.listVisitedNodeIdsByFlow?.([flowId]);
      return map?.get(flowId) ?? new Set();
    } catch {
      return new Set();
    }
  }

  function diffKeys(prevSet, currSet) {
    const added = [];
    const removed = [];
    for (const k of currSet) if (!prevSet.has(k)) added.push(k);
    for (const k of prevSet) if (!currSet.has(k)) removed.push(k);
    return { added, removed };
  }

  // Terminal set matches the WS run listener exactly (succeeded, failed,
  // cancelled, skipped) — the two streams must agree on what "ended" means
  // or the dashboard and companion clients diverge on skipped runs.
  function isTerminal(status) {
    return status === "succeeded" || status === "failed" || status === "cancelled" || status === "skipped";
  }

  // Process a single flow row — diff against last snapshot and emit if changed.
  async function tickOne(row) {
    const prev = last.get(row.flowId);
    const currKeys = await visitedNodeIdSet(row.flowId);
    if (!prev) {
      last.set(row.flowId, {
        revision: row.revision,
        status: row.status,
        lastStep: row.lastStep,
        stateKeys: currKeys,
        terminal: isTerminal(row.status),
      });
      emit({
        flowId: row.flowId,
        kind: "run.created",
        at: row.createdAt ?? Date.now(),
        status: row.status,
        lastStep: row.lastStep,
        pipelineId: row.stateJson?.pipelineId ?? null,
      });
      return;
    }
    if (prev.revision === row.revision) return;
    const { added, removed } = diffKeys(prev.stateKeys, currKeys);
    emit({
      kind: "run.updated",
      flowId: row.flowId,
      at: row.updatedAt ?? Date.now(),
      prevStatus: prev.status,
      currStatus: row.status,
      prevStep: prev.lastStep,
      currStep: row.lastStep,
      revision: row.revision,
      stateKeysAdded: added,
      stateKeysRemoved: removed,
      waitKind: row.waitJson?.kind ?? null,
    });
    if (!prev.terminal && isTerminal(row.status)) {
      emit({
        kind: "run.ended",
        flowId: row.flowId,
        at: row.endedAt ?? row.updatedAt ?? Date.now(),
        status: row.status,
      });
    }
    last.set(row.flowId, {
      revision: row.revision,
      status: row.status,
      lastStep: row.lastStep,
      stateKeys: currKeys,
      terminal: isTerminal(row.status),
    });
  }

  return {
    async start() {
      if (running) return;
      running = true;
      // Prime the snapshot without emitting flow_created for pre-existing rows.
      // One batched listVisitedNodeIdsByFlow() call for every row instead of
      // one query per flow — same batching rationale as stats.js's
      // tree-summary batching.
      try {
        const rows = await store.list();
        const visitedByFlow = (await nodeExecutionsStore?.listVisitedNodeIdsByFlow?.(rows.map((r) => r.flowId))) ?? new Map();
        for (const row of rows) {
          last.set(row.flowId, {
            revision: row.revision,
            status: row.status,
            lastStep: row.lastStep,
            stateKeys: visitedByFlow.get(row.flowId) ?? new Set(),
            terminal: isTerminal(row.status),
          });
        }
      } catch (err) {
        logger?.warn?.(`manifold dashboard event-bus prime failed: ${err?.message ?? err}`);
      }
      // Subscribe to trigger events from state-store — fires within a
      // process.nextTick after each write, replacing the 1 Hz poll.
      changeListener = async (flowId) => {
        if (!running || !flowId) return;
        let row;
        try { row = await store.get(flowId); } catch { return; }
        if (row) {
          await tickOne(row);
        } else {
          // Flow was deleted — prune from tracking (rare; test-run cleanup).
          last.delete(flowId);
          buffers.delete(flowId);
        }
      };
      flowStateEmitter.on("change", changeListener);
    },
    stop() {
      running = false;
      if (changeListener) {
        flowStateEmitter.off("change", changeListener);
        changeListener = null;
      }
      subscribers.clear();
      try { _client?.close?.(); } catch { /* ignore */ }
    },
    subscribe(cb) {
      subscribers.add(cb);
      return () => subscribers.delete(cb);
    },
    async getEventsFor(flowId) {
      const inMemory = buffers.get(flowId);
      if (inMemory && inMemory.length > 0) return [...inMemory];
      // Fall back to SQLite after a restart when the ring buffer is cold.
      if (!_client) return [];
      try {
        const { rows } = await _client.execute({
          sql: "SELECT * FROM flow_events WHERE flow_id = ? ORDER BY occurred_at ASC LIMIT 500",
          args: [flowId],
        });
        return rows.map((r) => ({
          // event_type stores the canonical kind outright; no read-time
          // translation.
          kind: r.event_type,
          flowId: r.flow_id,
          at: r.occurred_at,
          prevStep: r.prev_step ?? undefined,
          currStep: r.curr_step ?? undefined,
          prevStatus: r.prev_status ?? undefined,
          currStatus: r.curr_status ?? undefined,
          waitKind: r.wait_kind ?? undefined,
        }));
      } catch {
        return [];
      }
    },
    // Push a non-flow event straight to live subscribers without ring-buffering
    // it. Used for pipelines_changed and any other "side-channel" notification
    // the dashboard wants to surface live.
    broadcast(event) {
      for (const cb of subscribers) {
        try { cb(event); } catch (err) {
          logger?.warn?.(`manifold dashboard event-bus broadcast error: ${err?.message ?? err}`);
        }
      }
    },
    snapshot() {
      return { tracked: last.size, subscribers: subscribers.size, buffered: [...buffers.values()].reduce((a, b) => a + b.length, 0) };
    },
  };
}
