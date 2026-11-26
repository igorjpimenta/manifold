// tool-results.js — in-process rendezvous for a turn's structured result.
//
// A turn only ever returns { text, runId, toolResult? }, so a tool needs a
// channel of its own to reach the node that spawned it. That channel is the
// turn itself: a tool posts to the harness route (index.js registers one) and
// run-turn reads it back, both inside this process. The buffer dies with the
// process that owns the turn, exactly like the session it is keyed by — the
// durable copy of the data is the flow state apply() writes afterwards.
//
// Latest wins per session: one result per turn at most.

function keyOf(session) {
  return typeof session === "string" ? session.trim() : "";
}

export function createToolResults({ now = Date.now } = {}) {
  const rows = new Map();
  const waiters = new Map();

  return {
    /** Store one result; wakes a waiter for the same session. False when the write is unusable. */
    write({ session, runId = null, agentId = null, kind = null, payload } = {}) {
      const key = keyOf(session);
      if (!key || payload === undefined) return false;
      const row = { session: key, runId, agentId, kind, payload, updatedAt: now() };
      rows.set(key, row);
      const pending = waiters.get(key);
      if (pending) {
        waiters.delete(key);
        for (const resolve of pending) resolve(row);
      }
      return true;
    },

    /** The result for a session, or null. */
    read(session) {
      const key = keyOf(session);
      return key ? rows.get(key) ?? null : null;
    },

    /** Forget a session's result. Idempotent. */
    clear(session) {
      const key = keyOf(session);
      if (key) rows.delete(key);
      return true;
    },

    /**
     * { promise, cancel }: resolves with an existing row immediately, otherwise
     * with the next write for that session. The caller MUST cancel() when its
     * own race decides, or the promise keeps a waiter entry alive.
     */
    wait(session) {
      const key = keyOf(session);
      let settle;
      const promise = new Promise((resolve) => { settle = resolve; });
      if (!key) return { promise, cancel: () => {} };
      const existing = rows.get(key);
      if (existing) { settle(existing); return { promise, cancel: () => {} }; }
      const pending = waiters.get(key) ?? new Set();
      pending.add(settle);
      waiters.set(key, pending);
      const cancel = () => {
        const set = waiters.get(key);
        if (!set) return;
        set.delete(settle);
        if (set.size === 0) waiters.delete(key);
      };
      return { promise, cancel };
    },
  };
}
