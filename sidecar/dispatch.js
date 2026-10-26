// sidecar/dispatch.js — the sidecar half of agent dispatch.
//
// The engine resolves its subagent to api.runtime.subagent (see
// engine/executor.js's resolveSubagent); in the sidecar that object is a
// remote subagent built here. Each op (run | waitForRun | getSessionMessages)
// publishes a dispatch.request on the hub's dispatch channel and resolves
// when the linked harness runner POSTs the outcome back:
//
//   harness -> POST /manifold/internal/dispatch/ack { id }
//     pickup receipt; without one the request fails fast (no harness
//     linked) instead of hanging the engine's run loop.
//   harness -> POST /manifold/internal/dispatch/result { id, ok, result?, error? }
//     fulfills the op.
//
// Wire field names are duplicated in companion/runner.js (the companion
// vendors standalone — it cannot import this file). Keep them in sync.
import { randomUUID } from "node:crypto";

export const DISPATCH_OPS = ["run", "waitForRun", "getSessionMessages"];

const DEFAULT_ACCEPT_TIMEOUT_MS = 30_000;
const DEFAULT_OP_TIMEOUT_MS = 30 * 60_000;

export function createDispatchHub({ publish, logger = null, acceptTimeoutMs = DEFAULT_ACCEPT_TIMEOUT_MS, opTimeoutMs = DEFAULT_OP_TIMEOUT_MS } = {}) {
  const pending = new Map(); // id -> { op, resolve, reject, ackTimer, opTimer }

  function fail(id, err) {
    const p = pending.get(id);
    if (!p) return;
    pending.delete(id);
    clearTimeout(p.ackTimer);
    clearTimeout(p.opTimer);
    p.reject(err);
  }

  return {
    pendingCount() {
      return pending.size;
    },
    request(op, args, { timeoutMs = opTimeoutMs } = {}) {
      if (!DISPATCH_OPS.includes(op)) return Promise.reject(new Error(`sidecar dispatch: unknown op ${op}`));
      const id = randomUUID();
      return new Promise((resolve, reject) => {
        const ackTimer = setTimeout(() => {
          fail(id, new Error("sidecar dispatch: no harness subscribed (accept timeout)"));
        }, acceptTimeoutMs);
        // Unref where available so an idle sidecar with a forgotten
        // dispatch can't hold the process open past SIGTERM handling.
        ackTimer.unref?.();
        const opTimer = setTimeout(() => {
          fail(id, new Error(`sidecar dispatch: op ${op} timed out`));
        }, timeoutMs);
        opTimer.unref?.();
        pending.set(id, { op, resolve, reject, ackTimer, opTimer });
        try {
          publish("dispatch", { type: "dispatch.request", id, op, args: args ?? {} });
        } catch (err) {
          fail(id, err);
        }
      });
    },
    ack(id) {
      const p = pending.get(id);
      if (!p) return false;
      clearTimeout(p.ackTimer);
      return true;
    },
    fulfill(id, outcome) {
      const p = pending.get(id);
      if (!p) return false;
      pending.delete(id);
      clearTimeout(p.ackTimer);
      clearTimeout(p.opTimer);
      if (outcome?.ok) p.resolve(outcome.result);
      else p.reject(new Error(outcome?.error ?? `sidecar dispatch: op ${p.op} failed`));
      return true;
    },
  };
}

// Remote subagent with exactly the three methods the executor's gate calls
// (engine/gateway-subagent.js's GATED_METHODS). waitForRun carries the
// caller's own timeout (WAIT_UNTIL_RUN_ENDS_MS) plus margin; run/get resolve
// at op completion.
export function createRemoteSubagent({ dispatchHub, logger = null } = {}) {
  return {
    async run(args) {
      logger?.info?.(`sidecar dispatch: run (session ${(args?.sessionKey ?? "?").slice?.(0, 16) ?? "?"})`);
      return dispatchHub.request("run", args);
    },
    async waitForRun(args) {
      const timeoutMs = Number(args?.timeoutMs) > 0 ? Number(args.timeoutMs) + 60_000 : undefined;
      return dispatchHub.request("waitForRun", args, timeoutMs ? { timeoutMs } : {});
    },
    async getSessionMessages(args) {
      return dispatchHub.request("getSessionMessages", args, { timeoutMs: 120_000 });
    },
  };
}
