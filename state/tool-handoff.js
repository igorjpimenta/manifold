// tool-handoff.js — in-process bridge between a claiming agent's tool
// call and the parked node invocation it claims.
//
// executePullNode (engine/executor-nodes.js) parks every agent node as a
// claim ticket and registers the ticket here; the claiming agent learns the
// ticket from its claim instructions and passes it explicitly to tools
// (e.g. submit_score), which resolve their own invocationId from it and
// hand structured data back via the pending-result map below.
//
// This is deliberately in-memory, not persisted: the tool's execute() and
// the engine's executor.js loop run in the same Node process
// (api.registerTool wires the tool into the same in-process plugin
// registry the executor loop lives in — no IPC/RPC boundary between them).
// Nothing here needs to survive a gateway restart either — a restarted
// gateway re-parks interrupted agent nodes as fresh tickets through the
// normal runFlow path, never by recovering an in-flight tool result. So a
// plain Map is sufficient, and anything left in it when the process exits
// just vanishes with it instead of becoming an orphaned file.
//
// State lives on globalThis, NOT as plain module-scope `const` bindings:
// extensions/manifold/** loads through jiti's CJS transpilation
// (agent/tools/submit-score.js — see
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
//   registerTicket(ticket, invocationId) — executePullNode
//     (engine/executor-nodes.js), when the ticket parks
//   resolveInvocationId(ticket)          — a tool's execute(), to
//     find its own invocationId from its explicit ticket param
//   writePendingResult(invocationId, payload)  — the tool's execute()
//   readPendingResult(invocationId)            — a result consumer
//   clearPendingResult(invocationId)           — the consumer,
//     unconditionally, so a stale value from a prior attempt can never be
//     silently reused
//   clearTicket(ticket)                        — the report path, once
//     the ticket's result is stored, so the ticket -> invocationId map
//     doesn't grow unbounded over gateway uptime
//
const GLOBAL_KEY = "__manifold_tool_handoff_state__";

function state() {
  if (!globalThis[GLOBAL_KEY]) {
    globalThis[GLOBAL_KEY] = {
      invocationByTicket: new Map(),
      pendingResultByInvocation: new Map(),
    };
  }
  return globalThis[GLOBAL_KEY];
}

export function registerTicket(ticket, invocationId) {
  if (ticket && invocationId) state().invocationByTicket.set(ticket, invocationId);
}

export function resolveInvocationId(ticket) {
  if (!ticket) return null;
  return state().invocationByTicket.get(ticket) ?? null;
}

export function clearTicket(ticket) {
  if (ticket) state().invocationByTicket.delete(ticket);
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
