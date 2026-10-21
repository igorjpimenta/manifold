// tool-handoff.js — in-process bridge between a dispatched subagent's tool
// call and the dispatching node's apply().
//
// runSubagentTurn (executor-subagent.js) only ever returns {text, runId} to
// apply() — never a tool call's structured payload — so a tool has no other
// channel to hand data back to the node that spawned it.
//
// This is deliberately in-memory, not persisted: the tool's execute() and
// the engine's executor.js loop that calls apply() run in the same Node
// process (api.registerTool wires the tool into the same in-process plugin
// registry the executor loop lives in — no IPC/RPC boundary between them).
// Nothing here needs to survive a gateway restart either — an orphaned
// agent-kind node is revived by re-dispatching it from scratch with a
// continuation prompt (engine/executor.js's reviveAgentKindOrphan /
// boot/flow-revival.js), never by recovering an in-flight tool result. So a
// plain Map is sufficient, and anything left in it when the process exits
// just vanishes with it instead of becoming an orphaned file.
//
// State lives on globalThis, NOT as plain module-scope `const` bindings:
// extensions/manifold/** loads through jiti's CJS transpilation
// (engine/executor-subagent.js, agent/tools/submit-score.js — see
// extensions/manifold/node_modules/.cache/jiti/*.cjs), while pipeline files
// under workspace/.pipelines/** load via native ESM `await import(url)`
// (engine/loader.js). Both import this file by the same file:// URL, but
// jiti and native ESM do not share a module registry — each produces its
// OWN separate instance of this module, each with its own plain `const`
// Maps. submit-score.js (jiti) wrote a pending result that score-llm.js's
// apply() (native ESM) could never see: same process, two disconnected
// copies of "the same" module. globalThis is the one thing both loaders
// actually share, so the real Maps live there — every export below is a
// thin wrapper over a lazily-initialized globalThis slot.
//
// Lifecycle:
//   registerDispatch(sessionKey, invocationId) — dispatchAgentEntry
//     (executor-nodes.js), right before spawning the subagent session
//   resolveInvocationId(sessionKey)            — a tool's execute(), to
//     find its own invocationId
//   writePendingResult(invocationId, payload)  — the tool's execute()
//   readPendingResult(invocationId)            — the node's apply()
//   clearPendingResult(invocationId)           — apply(), unconditionally,
//     so a stale value from a prior attempt/retry can never be silently
//     reused
//   clearDispatch(sessionKey)                  — dispatchAgentEntry, once
//     the dispatch settles (success or throw), so the sessionKey ->
//     invocationId map doesn't grow unbounded over gateway uptime
//
// notifyToolFired/waitForToolSignal is a separate, runId-keyed wake-up
// mechanism, not a data channel — it carries no payload (the payload still
// only ever flows through writePendingResult/readPendingResult above). It
// exists to fix a specific race: runSubagentTurn's dispatchAgentWithRetry
// call only finds out a tool result exists when waitForRun settles — either
// the whole underlying agent run reaches its own natural end, or timeoutMs
// expires — even when the ONE tool call the node actually cares about
// already fired long before either of those. A hook (e.g.
// hooks/score-tool-wake.js) calls notifyToolFired(runId) the instant the
// relevant tool call completes; runSubagentTurn races waitForRun against
// waitForToolSignal(runId) so it can proceed the moment the real answer
// exists instead of waiting on — or timing out against — the rest of the
// turn. runId (not invocationId/sessionKey) is the correlation key here
// because it's what before_tool_call/after_tool_call events carry natively
// (see hooks/profile-write-lock.js's identical event.runId usage) and it's
// already returned by subagent.run() before the wait even starts — no
// extra registration/resolution step needed the way invocationId requires.

const GLOBAL_KEY = "__manifold_tool_handoff_state__";

function state() {
  if (!globalThis[GLOBAL_KEY]) {
    globalThis[GLOBAL_KEY] = {
      invocationBySessionKey: new Map(),
      pendingResultByInvocation: new Map(),
      toolSignalWaitersByRunId: new Map(),
    };
  }
  return globalThis[GLOBAL_KEY];
}

export function notifyToolFired(runId) {
  if (!runId) return;
  const { toolSignalWaitersByRunId } = state();
  const waiters = toolSignalWaitersByRunId.get(runId);
  if (!waiters) return;
  toolSignalWaitersByRunId.delete(runId);
  for (const resolve of waiters) resolve();
}

// Returns { promise, cancel } rather than a bare promise — the loser of a
// Promise.race is never cleaned up on its own, so the caller MUST call
// cancel() once the race is decided (whichever side wins) or this entry
// leaks in toolSignalWaitersByRunId until notifyToolFired happens to fire
// for that exact runId (which, if the race was already won by waitForRun,
// may never happen).
export function waitForToolSignal(runId) {
  if (!runId) return { promise: new Promise(() => {}), cancel: () => {} };
  const { toolSignalWaitersByRunId } = state();
  let resolveFn;
  const promise = new Promise((resolve) => { resolveFn = resolve; });
  if (!toolSignalWaitersByRunId.has(runId)) toolSignalWaitersByRunId.set(runId, new Set());
  toolSignalWaitersByRunId.get(runId).add(resolveFn);
  return {
    promise,
    cancel: () => {
      const waiters = toolSignalWaitersByRunId.get(runId);
      if (!waiters) return;
      waiters.delete(resolveFn);
      if (waiters.size === 0) toolSignalWaitersByRunId.delete(runId);
    },
  };
}

export function registerDispatch(sessionKey, invocationId) {
  if (sessionKey && invocationId) state().invocationBySessionKey.set(sessionKey, invocationId);
}

export function resolveInvocationId(sessionKey) {
  if (!sessionKey) return null;
  return state().invocationBySessionKey.get(sessionKey) ?? null;
}

export function clearDispatch(sessionKey) {
  if (sessionKey) state().invocationBySessionKey.delete(sessionKey);
}

export function writePendingResult(invocationId, payload) {
  if (!invocationId) return;
  state().pendingResultByInvocation.set(invocationId, payload);
}

export function readPendingResult(invocationId) {
  if (!invocationId) return null;
  return state().pendingResultByInvocation.get(invocationId) ?? null;
}

export function clearPendingResult(invocationId) {
  if (invocationId) state().pendingResultByInvocation.delete(invocationId);
}
