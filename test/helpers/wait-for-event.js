
import { gateEmitter, flowEmitter, flowStateEmitter } from "../../events.js";

const DEFAULT_TIMEOUT_MS = 10_000;

export function onceMatching(emitter, eventName, predicate, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      emitter.off(eventName, handler);
      reject(new Error(`timed out after ${timeoutMs}ms waiting for ${eventName}`));
    }, timeoutMs);
    function handler(payload) {
      if (predicate && !predicate(payload)) return;
      clearTimeout(timer);
      emitter.off(eventName, handler);
      resolve(payload);
    }
    emitter.on(eventName, handler);
  });
}


export function waitForGateOpen(predicate, timeoutMs) {
  return onceMatching(gateEmitter, "gate.opened", predicate, timeoutMs);
}


export function waitForTerminal(predicate, timeoutMs) {
  return onceMatching(flowEmitter, "flow.terminal", predicate, timeoutMs);
}

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
      }
    }
    function onChange(id) { if (id === flowId) check(); }
    flowStateEmitter.on("change", onChange);
    check();
  });
}
