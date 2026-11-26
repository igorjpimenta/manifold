// event-log.js — flow-change event stream for the dashboard.
//
// Kinds: run.created (first sight of a flowId), run.updated (revision
// advanced; carries prev/curr step+status, stateKeysAdded/Removed, waitKind),
// run.ended (status moved terminal). Every event goes to live WS subscribers
// and a per-flow ring buffer (cap 256); with a dbPath it is also written to
// flow_events so history survives restarts (getEventsFor falls back to SQL
// when the buffer is cold).
//
// stateKeysAdded/Removed diff flow_node_invocations presence, not
// Object.keys(stateJson): a raw key-diff is silent for node dispatches that
// never grow state_json, and an invocation row exists the instant a node
// dispatches.

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
  const last = new Map();
  const buffers = new Map();
  const subscribers = new Set();

  let running = false;
  let changeListener = null;

  function emit(event) {
    const list = buffers.get(event.flowId) ?? [];
    list.push(event);
    if (list.length > RING_CAP) list.splice(0, list.length - RING_CAP);
    buffers.set(event.flowId, list);
    // Routed through batch-writer, not a bare execute(): this fires on every
    // run transition system-wide — the highest-frequency write in the app —
    // so it must coalesce and survive SQLITE_BUSY. enqueueRaw resolves its own
    // shared connection from dbPath, independent of this bus's `_client`
    // (reads/close only) and of whether it has resolved yet.
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

  // Must match the WS run listener's terminal set exactly (succeeded, failed,
  // cancelled, skipped) or the dashboard and companion clients diverge on
  // skipped runs.
  function isTerminal(status) {
    return status === "succeeded" || status === "failed" || status === "cancelled" || status === "skipped";
  }

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
      // Prime without emitting run.created for pre-existing rows; one batched
      // visit call instead of a query per flow.
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
      changeListener = async (flowId) => {
        if (!running || !flowId) return;
        let row;
        try { row = await store.get(flowId); } catch { return; }
        if (row) {
          await tickOne(row);
        } else {
          // Flow deleted — stop tracking it (test-run cleanup).
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
      if (!_client) return [];
      try {
        const { rows } = await _client.execute({
          sql: "SELECT * FROM flow_events WHERE flow_id = ? ORDER BY occurred_at ASC LIMIT 500",
          args: [flowId],
        });
        return rows.map((r) => ({
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
    // Live-only push for side-channel notifications (e.g. pipelines_changed);
    // never ring-buffered.
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
