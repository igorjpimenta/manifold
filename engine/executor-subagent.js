import { GatewayHandoffNeeded, isRequestScopedSubagentError, fetchSessionError } from "./executor-errors.js";
import { usesNamedParams } from "./executor-helpers.js";
import { waitForToolSignal } from "../state/tool-handoff.js";

// A lapsed agent.wait window answers with the same bare "timeout" status a
// timed-out run gets — waitForRun drops endedAt, so the two can't be told
// apart to re-wait. Asking for the gateway's maximum timer delay
// (setSafeTimeout clamps anything larger) makes the run's own end the only
// thing that settles the wait; the runtime's agents.defaults.timeoutSeconds
// still bounds the run itself.
const WAIT_UNTIL_RUN_ENDS_MS = 2_147_483_647;

export function createSubagentDispatch({ getSubagent, sessionIndex, logger, emit }) {
  /**
   * Dispatch one subagent turn against an existing or fresh session key,
   * await its completion, fetch the last assistant message.
   */
  async function runSubagentTurn({ sessionKey, message, skill, agentId }) {
    const subagent = getSubagent();
    if (!subagent?.run) throw new GatewayHandoffNeeded("subagent runtime unavailable on this api instance");
    let runResult;
    try {
      runResult = await subagent.run({ sessionKey, message, skill, agentId });
    } catch (err) {
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    // Race waitForRun against a tool-fired wake-up (tool-handoff.js,
    // notified by e.g. hooks/score-tool-wake.js) — waitForRun only settles
    // when the WHOLE underlying run reaches its own natural end, even when
    // the one tool call a node actually cares about already landed well
    // before that. This lets the common, already-succeeded case finish
    // early instead of waiting out the model's redundant post-tool-call
    // wrap-up reply.
    //
    // There is deliberately no manifold-side deadline, idle or absolute: a
    // dispatched run can sit silently in the gateway's own lane queue
    // (agents.defaults.maxConcurrent) for arbitrarily long, and neither
    // silence nor wall-clock time there means the run is hung.
    const toolSignal = waitForToolSignal(runResult.runId);
    let waitResult;
    try {
      waitResult = await Promise.race([
        subagent.waitForRun({ runId: runResult.runId, timeoutMs: WAIT_UNTIL_RUN_ENDS_MS }),
        toolSignal.promise.then(() => ({ status: "ok", toolFired: true })),
      ]);
    } catch (err) {
      toolSignal.cancel();
      if (isRequestScopedSubagentError(err)) throw new GatewayHandoffNeeded(err.message);
      throw err;
    }
    toolSignal.cancel();
    if (waitResult.status !== "ok") {
      const errDetail = waitResult.error ?? (await fetchSessionError(sessionKey)) ?? "no error message";
      throw new Error(`subagent ${waitResult.status}: ${errDetail}`);
    }
    // Tool already fired with everything apply() needs (tool-handoff.js's
    // pending-result map) — skip fetching the model's own wrap-up reply,
    // which is often just a "score submitted" confirmation with nothing
    // useful in it. output.text is only ever a fallback for apply()s that
    // don't find a pending result.
    if (waitResult.toolFired) return { text: "", runId: runResult.runId };
    let msgsResult;
    try {
      msgsResult = await subagent.getSessionMessages({ sessionKey, limit: 5 });
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
    return { text: lastText, runId: runResult.runId };
  }

  /**
   * Run an `agent`-kind dispatch — a single attempt, no automatic retry.
   *
   * A failed dispatch throws immediately, gets recorded as a normal failure
   * event the same way every other failure class does, and the operator
   * retries manually via the flow's existing Retry action once whatever was
   * wrong is actually fixed — same rule for every agent node, no exceptions.
   * `node.retry` config, if still present on any node, is inert and ignored.
   */
  async function dispatchAgentWithRetry({ node, nodeId, sessionKey, initialMessage, applyCtx, applyExtraArg, applyMeta, flowId, needsCtx }) {
    if (sessionIndex?.record) await sessionIndex.record({ flowId, sessionKey, nodeId });
    const output = await runSubagentTurn({
      sessionKey,
      message: initialMessage,
      skill: node.skill,
      agentId: node.agent,
    });
    // Transcript event: without it the reply is visible only inside apply().
    // visible only inside apply(). Tool-fired path carries no reply text — skipped,
    // since the contract requires text and an empty event helps nobody.
    // Nullable like sessionIndex: callers without an emitter (CLI, tests)
    // dispatch exactly as before.
    if (emit && output?.text) {
      try { await emit("session.transcript.completed", { scope: "node", nodeId, sessionKey, text: output.text }); } catch {}
    }
    // Option B "needs:" — needsCtx threaded through both calling
    // conventions as a new field/5th positional arg. Additive: every
    // existing apply() function ignores an argument/key it doesn't
    // ask for, so this is a no-op for every apply() that hasn't
    // opted in yet.
    return usesNamedParams(node.apply)
      ? await Promise.resolve(node.apply({ state: applyCtx, output, item: applyExtraArg ?? undefined, meta: applyMeta, needs: needsCtx }))
      : applyExtraArg !== undefined
        ? await Promise.resolve(node.apply(applyCtx, output, applyExtraArg, applyMeta, needsCtx))
        : await Promise.resolve(node.apply(applyCtx, output, undefined, applyMeta, needsCtx));
  }

  return { runSubagentTurn, dispatchAgentWithRetry };
}
