// wait-for-event.js — await the state transition a test actually cares about,
// instead of sleeping a fixed interval and hoping it already happened.
//
// The engine E2E tests used `await flush()` — a fixed setTimeout — to bridge
// the gap between triggering work and asserting on its result. That is wrong in
// both directions at once: too long on a fast machine (a settle taking 3ms
// still costs the full 60ms) and too short on a loaded CI runner (the suite
// runs ~4x slower there, and the assertion fires while the flow is still
// running). The constant had already been bumped 20ms -> 60ms for exactly that
// reason, and it flaked again in CI at 60ms — bumping it a third time only
// moves the threshold.
//
// None of that guessing is necessary: state-store already announces every
// transition on process.nextTick. The signal exists; the tests just weren't
// listening.
//
//   gate.opened        a flow blocked at a human gate
//   flow.terminal      a flow reached succeeded/failed/skipped/cancelled
//   flow.step.changed  a flow advanced a step and is running
//
// USAGE — subscribe BEFORE triggering the work:
//
//   const blocked = waitForGateOpen((p) => p.flowId !== parentId);
//   await executor.runFlow(parentId);
//   await blocked;
//
// Subscribing afterwards races the emit and hangs until the timeout, because
// these fire on nextTick. That ordering is the one thing to get right.

import { gateEmitter, flowEmitter, flowStateEmitter } from "../../events.js";

// Long enough that a slow CI runner never trips it, short enough that a
// genuine hang fails inside the per-test timeout with a named condition —
// "timed out waiting for gate.opened" beats "'running' !== 'blocked'".
const DEFAULT_TIMEOUT_MS = 10_000;

export function onceMatching(emitter, eventName, predicate, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      emitter.off(eventName, handler);
      reject(new Error(`timed out after ${timeoutMs}ms waiting for ${eventName}`));
    }, timeoutMs);
    function handler(payload) {
      if (predicate && !predicate(payload)) return;   // not the flow we mean
      clearTimeout(timer);
      emitter.off(eventName, handler);
      resolve(payload);
    }
    emitter.on(eventName, handler);
  });
}

/** Resolves when some flow matching `predicate` blocks at a gate. */
export function waitForGateOpen(predicate, timeoutMs) {
  return onceMatching(gateEmitter, "gate.opened", predicate, timeoutMs);
}

/** Resolves when some flow matching `predicate` reaches a terminal status. */
export function waitForTerminal(predicate, timeoutMs) {
  return onceMatching(flowEmitter, "flow.terminal", predicate, timeoutMs);
}

/**
 * Resolves once `count` distinct flows have matched — for fan-outs where the
 * test cares that every child got there, not just the first. Keyed by flowId so
 * a flow emitting twice cannot satisfy the count on its own.
 */
export function waitForAll(emitter, eventName, predicate, count, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const seen = new Map();
    const timer = setTimeout(() => {
      emitter.off(eventName, handler);
      reject(new Error(
        `timed out after ${timeoutMs}ms waiting for ${count} ${eventName} (saw ${seen.size})`,
      ));
    }, timeoutMs);
    function handler(payload) {
      if (predicate && !predicate(payload)) return;
      seen.set(payload?.flowId ?? Symbol("anon"), payload);
      if (seen.size < count) return;
      clearTimeout(timer);
      emitter.off(eventName, handler);
      resolve([...seen.values()]);
    }
    emitter.on(eventName, handler);
  });
}

/**
 * Wait until a flow's ROW satisfies `predicate` — the strongest of the three,
 * and the one to reach for when a point event isn't enough.
 *
 * gate.opened tells you *a* gate opened; it does not tell you which, nor that
 * the row still reads blocked by the time you look. That is exactly why the gap
 * loop-back rounds could not be converted to waitForGateOpen: the child opens a
 * gap gate, gets decided, then opens the real gate, and "a gate opened" matches
 * the wrong one.
 *
 * state-store calls notifyChange on every write path, so subscribing to
 * `change` and re-reading is still fully event-driven — no interval, no sleep —
 * while testing the actual condition rather than a proxy for it. The predicate
 * receives the whole row, so it can assert on waitJson, lastStep or stateJson,
 * not just status.
 *
 *   await waitForStatus(runStore, childFlowId,
 *     (r) => r.status === "blocked" && r.waitJson?.items?.[0]?.itemId === "job-2");
 *
 * Checks once on entry too, so a condition already true does not hang waiting
 * for a write that has already happened.
 */
export function waitForStatus(runStore, flowId, predicate, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      flowStateEmitter.off("change", onChange);
      fn(arg);
    };
    const timer = setTimeout(
      () => finish(reject, new Error(`timed out after ${timeoutMs}ms waiting for ${flowId} to match`)),
      timeoutMs,
    );
    async function check() {
      if (settled) return;
      try {
        const row = await runStore.get(flowId);
        if (row && predicate(row)) finish(resolve, row);
      } catch {
        // A read can fail transiently while a write is in flight; the next
        // change event retries, and the timeout is the backstop.
      }
    }
    function onChange(id) { if (id === flowId) check(); }
    flowStateEmitter.on("change", onChange);
    check();
  });
}
