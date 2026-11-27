// The openclaw side of an agent dispatch: one message→reply round-trip
// against the gateway's agent runtime. Core injects the resulting function
// and only ever sees { text, runId, toolResult? } — everything
// harness-shaped (runtime call shape, readiness gate, tool-result early
// exit) lives here.
//
// The readiness gate exists because the gateway refuses agent RPCs during
// startup: for the first seconds after every start, agent.wait is refused
// while the RPC that starts a run already works, so a dispatch landing in
// that window starts its agent run and then can't wait on it. Every call
// below awaits the shared readiness promise first — settled once the
// gateway is up, so free after boot. A call refused anyway surfaces as an
// ordinary error; it is not retried.

import { GatewayHandoffNeeded, isRequestScopedSubagentError, fetchSessionError } from "@igorjpimenta/manifold/errors";

// A lapsed agent.wait window answers with the same bare "timeout" status a
// timed-out run gets — waitForRun drops endedAt, so the two can't be told
// apart to re-wait. Asking for the gateway's maximum timer delay
// (setSafeTimeout clamps anything larger) makes the run's own end the only
// thing that settles the wait; the runtime's agents.defaults.timeoutSeconds
// still bounds the run itself.
const WAIT_UNTIL_RUN_ENDS_MS = 2_147_483_647;
// A tool's POST trails the run's own end by milliseconds (the harness ends the
// run at the call). Bounded so a text-only turn is not delayed beyond this.
const TOOL_RESULT_GRACE_MS = 2_000;

// This gateway lowercases session keys. Node ids arrive SCREAMING_CASE, so the
// turn must address the runtime, the tool's ctx.sessionKey and the hand-off row
// with the spelling the gateway will actually store; the hand-off stays
// entirely inside the harness, and core only ever sees the returned toolResult.
function gatewaySessionKey(session) {
  return String(session ?? "").trim().toLowerCase();
}

const GATED_METHODS = ["run", "waitForRun", "getSessionMessages"];

// Returns gate(rawRuntime) → a runtime whose run/waitForRun/getSessionMessages
// wait for gateway readiness. Other members pass through unchanged; a missing
// runtime stays missing so "no runtime here" keeps its own handling downstream.
export function createGatewaySubagentGate({ whenGatewayReady } = {}) {
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

// Builds the turn function core injects. getRuntime resolves the harness's
// agent runtime per call (it can appear late — the gateway binds it during
// startup) and may return null.
export function createRunTurn({ getRuntime, whenGatewayReady = null, logger = null, toolResults, toolResultGraceMs = TOOL_RESULT_GRACE_MS }) {
  const gate = createGatewaySubagentGate({ whenGatewayReady });

  async function runTurn({ session, message, skill, agentId }) {
    const sessionKey = gatewaySessionKey(session);
    const startedAt = Date.now();
    const runtime = gate(typeof getRuntime === "function" ? getRuntime() : getRuntime);
    if (!runtime?.run) throw new GatewayHandoffNeeded("agent runtime unavailable on this api instance");
    let runResult;
    try {
      // Adapter edge: the harness runtime names its own field `sessionKey`;
      // core passes its single `session` as that field.
      runResult = await runtime.run({ sessionKey, message, skill, agentId });
    } catch (err) {
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    // Race waitForRun against the session's tool result: a tool that already
    // delivered finishes the turn early instead of waiting out the model's
    // natural end. No manifold-side deadline: a dispatched run can sit
    // silently in the gateway's own lane queue (agents.defaults.maxConcurrent)
    // for arbitrarily long, and neither silence nor wall-clock time there
    // means the run is hung.
    const toolWait = toolResults.wait(sessionKey);
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
    // Tool already delivered everything apply() needs — skip fetching the
    // model's own wrap-up reply, often just a "submitted" confirmation.
    // output.text is only ever a fallback for apply()s with no tool result.
    // A result stamped before this turn began belongs to a previous attempt
    // and must not answer this one; fall through to the model's own reply.
    if (waitResult.toolFired && (waitResult.toolRow?.updatedAt ?? 0) >= startedAt) {
      const toolRow = waitResult.toolRow ?? null;
      if (toolRow) toolResults.clear(sessionKey);
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
    // A tool call can land after the run's own end: the harness ends the run
    // at the call and the tool's POST arrives milliseconds later, so a plain
    // read here loses a result that was already on its way. Wait a bounded
    // grace period for a row stamped at or after this turn's start — an older
    // stamp belongs to a previous attempt and must not be resurrected.
    const late = await toolResults.wait(sessionKey, { since: startedAt, timeoutMs: toolResultGraceMs }).promise;
    if (late) {
      toolResults.clear(sessionKey);
      return {
        text: "",
        runId: runResult.runId,
        toolResult: { kind: late.kind ?? null, payload: late.payload ?? null },
      };
    }
    logger?.info?.(`manifold: agent turn ${runResult.runId} returned ${lastText.length} chars`);
    return { text: lastText, runId: runResult.runId };
  }

  return { runTurn };
}
