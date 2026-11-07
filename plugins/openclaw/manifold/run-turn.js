// run-turn.js — the openclaw side of an agent dispatch: one message→reply
// round-trip against the gateway's agent runtime.
//
// Core owns the engine (nodes, edges, state, results); the harness owns the
// agents. Everything harness-shaped lives here: the runtime call shape
// (run/waitForRun/getSessionMessages), the gateway-readiness gate, and the
// early exit against a delivered tool result. Core injects the resulting
// function and only ever sees { text, runId, toolResult? }.
//
// The readiness gate exists because the gateway refuses agent RPCs during
// startup: for the first seconds after every start, agent.wait is refused
// while the RPC that starts a run already works, so a dispatch landing in
// that window starts its agent run and then can't wait on it. Every call
// below awaits the shared readiness promise first — settled once the
// gateway is up, so free after boot. A call refused anyway surfaces as an
// ordinary error; it is not retried.

import { GatewayHandoffNeeded, isRequestScopedSubagentError, fetchSessionError } from "../../../engine/executor-errors.js";
import { waitForToolResult } from "../../../state/tool-handoff.js";

// A lapsed agent.wait window answers with the same bare "timeout" status a
// timed-out run gets — waitForRun drops endedAt, so the two can't be told
// apart to re-wait. Asking for the gateway's maximum timer delay
// (setSafeTimeout clamps anything larger) makes the run's own end the only
// thing that settles the wait; the runtime's agents.defaults.timeoutSeconds
// still bounds the run itself.
const WAIT_UNTIL_RUN_ENDS_MS = 2_147_483_647;

const GATED_METHODS = ["run", "waitForRun", "getSessionMessages"];

/**
 * Returns gate(rawRuntime) → a runtime whose run/waitForRun/getSessionMessages
 * wait for gateway readiness. Other members pass through to the raw runtime
 * unchanged; a missing runtime stays missing (null / undefined), so "no
 * runtime here" keeps its own handling downstream.
 */
export function createGatewaySubagentGate({ whenGatewayReady } = {}) {
  // One wrapper per raw runtime object, so repeated resolves don't allocate.
  const wrappers = new WeakMap();

  return function gate(raw) {
    if (!raw) return raw;
    let gated = wrappers.get(raw);
    if (!gated) {
      gated = Object.create(raw);
      for (const method of GATED_METHODS) {
        if (typeof raw[method] === "function") {
          gated[method] = async (...args) => {
            if (whenGatewayReady) await whenGatewayReady();
            return raw[method](...args);
          };
        }
      }
      wrappers.set(raw, gated);
    }
    return gated;
  };
}

/**
 * Builds the turn function core injects.
 *
 * `getRuntime` resolves the harness's agent runtime per call (it can
 * appear late — the gateway binds it during startup) and may return null.
 */
export function createRunTurn({ getRuntime, whenGatewayReady = null, logger = null } = {}) {
  const gate = createGatewaySubagentGate({ whenGatewayReady });

  async function runTurn({ sessionKey, message, skill, agentId }) {
    const runtime = gate(typeof getRuntime === "function" ? getRuntime() : getRuntime);
    if (!runtime?.run) throw new GatewayHandoffNeeded("agent runtime unavailable on this api instance");
    let runResult;
    try {
      runResult = await runtime.run({ sessionKey, message, skill, agentId });
    } catch (err) {
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    // Race waitForRun against a tool-result poll (state/tool-handoff.js): an
    // MCP deliver_result call for this session lands a row the waiter picks
    // up — so a turn whose structured result already arrived finishes early
    // instead of waiting out the model's own natural end. The row is read
    // through the shared store, so the delivering side and this waiter may
    // live in different processes.
    //
    // There is deliberately no manifold-side deadline here: a dispatched
    // run can sit silently in the gateway's own lane queue
    // (agents.defaults.maxConcurrent) for arbitrarily long, and neither
    // silence nor wall-clock time there means the run is hung.
    const toolWait = waitForToolResult(sessionKey);
    let waitResult;
    try {
      waitResult = await Promise.race([
        runtime.waitForRun({ runId: runResult.runId, timeoutMs: WAIT_UNTIL_RUN_ENDS_MS }),
        toolWait.promise.then((row) => ({ status: "ok", toolFired: true, toolRow: row })),
      ]);
    } catch (err) {
      toolWait.cancel();
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    toolWait.cancel();
    if (waitResult.status !== "ok") {
      const errDetail = waitResult.error ?? (await fetchSessionError(sessionKey)) ?? "no error message";
      throw new Error(`agent turn ${waitResult.status}: ${errDetail}`);
    }
    // Tool already delivered everything apply() needs (state/tool-handoff.js's
    // session-keyed row, written by the MCP deliver_result call) — skip
    // fetching the model's own wrap-up reply, which is often just a
    // "submitted" confirmation with nothing useful in it. output.text is
    // only ever a fallback for apply()s that find no tool result.
    if (waitResult.toolFired) {
      const toolRow = waitResult.toolRow ?? null;
      return {
        text: "",
        runId: runResult.runId,
        toolResult: toolRow
          ? { kind: toolRow.kind ?? null, payload: toolRow.payload ?? null }
          : null,
      };
    }
    let msgsResult;
    try {
      msgsResult = await runtime.getSessionMessages({ sessionKey, limit: 5 });
    } catch (err) {
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    const msgs = msgsResult.messages ?? [];
    let lastText = "";
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      if (m?.role !== "assistant") continue;
      if (typeof m.content === "string") { lastText = m.content; break; }
      if (Array.isArray(m.content)) {
        const text = m.content
          .filter((c) => c?.type === "text" || typeof c?.text === "string")
          .map((c) => c.text ?? "")
          .join("\n");
        if (text) { lastText = text; break; }
      }
    }
    logger?.info?.(`manifold: agent turn ${runResult.runId} returned ${lastText.length} chars`);
    return { text: lastText, runId: runResult.runId };
  }

  return { runTurn };
}
