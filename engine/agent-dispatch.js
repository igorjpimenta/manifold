import { usesNamedParams } from "./executor-helpers.js";
import { clearPendingResult, readPendingResult } from "../state/tool-handoff.js";

// agent-dispatch.js — the engine's agent-node dispatch: run one turn
// through the injected harness runtime, then reduce the result with the
// node's apply().
//
// The turn itself is harness work — which runtime, which call shape, how a
// reply comes back — so core injects it as `runTurn` and only ever sees
// { text, runId, toolResult? }. A harness that cannot run a turn throws
// GatewayHandoffNeeded from there, and core's failure path records it like
// any other.

export function createAgentDispatch({ runTurn, sessionIndex, logger, emit }) {
  /**
   * Run an `agent`-kind dispatch — a single attempt, no automatic retry.
   *
   * A failed dispatch throws immediately, gets recorded as a normal failure
   * event the same way every other failure class does, and the operator
   * retries manually via the flow's existing Retry action once whatever was
   * wrong is actually fixed — same rule for every agent node, no exceptions.
   * `node.retry` config, if still present on any node, is inert and ignored.
   */
  async function dispatchAgentWithRetry({ node, nodeId, session, initialMessage, applyCtx, applyExtraArg, applyMeta, flowId, needsCtx }) {
    // The harness-owned session id stores verbatim.
    if (sessionIndex?.record) await sessionIndex.record({ flowId, session, nodeId, agent: node.agent ?? null });
    if (typeof runTurn !== "function") {
      throw new Error("manifold: no agent runtime bound — the harness must inject runTurn");
    }
    // `session` is core's whole contract here — the harness adapter
    // (run-turn.js) maps it to the runtime's own `sessionKey` field.
    const before = await readPendingResult(session).catch(() => null);
    const output = await runTurn({
      session,
      message: initialMessage,
      skill: node.skill,
      agentId: node.agent,
    });
    // Transcript event: without it the reply is visible only inside apply().
    // The tool-fired path carries no reply text — skipped, since the
    // contract requires text and an empty event helps nobody.
    // Nullable like sessionIndex: callers without an emitter (CLI, tests)
    // dispatch exactly as before.
    if (emit && output?.text) {
      try { await emit("session.transcript.completed", { scope: "node", nodeId, session, text: output.text }); } catch {}
    }
    // needsCtx is threaded through both calling conventions as a new field /
    // 5th positional arg. Additive: every existing apply() function ignores
    // an argument/key it doesn't ask for, so this is a no-op for every apply()
    // that hasn't opted in yet. output.toolResult carries the delivered row
    // (when a tool fired); the row is consumed here so a retried turn never
    // rereads a stale result.
    // A tool call can land before the wait starts or after the turn's own run
    // ends (the model retries a rejected call); the harness races only up to
    // the run's end. Read the session's row one last time so apply() still sees
    // a payload that arrived outside that window.
    // Only a row that appeared or changed during this turn counts: an unchanged
    // one belongs to a previous attempt and apply() must not resurrect it.
    const pending = output?.toolResult ? null : await readPendingResult(session).catch(() => null);
    const fresh = pending && (pending.updatedAt ?? 0) > (before?.updatedAt ?? 0) ? pending : null;
    const delivered = output?.toolResult ?? fresh;
    const merged = output?.toolResult || !delivered ? output : { ...output, toolResult: delivered };
    try {
      return usesNamedParams(node.apply)
        ? await Promise.resolve(node.apply({ state: applyCtx, output: merged, item: applyExtraArg ?? undefined, meta: applyMeta, needs: needsCtx }))
        : applyExtraArg !== undefined
          ? await Promise.resolve(node.apply(applyCtx, merged, applyExtraArg, applyMeta, needsCtx))
          : await Promise.resolve(node.apply(applyCtx, merged, undefined, applyMeta, needsCtx));
    } finally {
      if (merged?.toolResult) await clearPendingResult(session).catch(() => {});
    }
  }

  return { dispatchAgentWithRetry };
}
