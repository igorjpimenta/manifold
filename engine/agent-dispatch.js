import { usesNamedParams } from "./executor-helpers.js";
import { clearPendingResult } from "../state/tool-handoff.js";

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
  async function dispatchAgentWithRetry({ node, nodeId, sessionKey, initialMessage, applyCtx, applyExtraArg, applyMeta, flowId, needsCtx }) {
    // The harness-owned session id stores verbatim.
    if (sessionIndex?.record) await sessionIndex.record({ flowId, sessionKey, nodeId, agent: node.agent ?? null });
    if (typeof runTurn !== "function") {
      throw new Error("manifold: no agent runtime bound — the harness must inject runTurn");
    }
    const output = await runTurn({
      sessionKey,
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
      try { await emit("session.transcript.completed", { scope: "node", nodeId, sessionKey, text: output.text }); } catch {}
    }
    // Option B "needs:" — needsCtx threaded through both calling
    // conventions as a new field/5th positional arg. Additive: every
    // existing apply() function ignores an argument/key it doesn't
    // ask for, so this is a no-op for every apply() that hasn't
    // opted in yet. output.toolResult carries the delivered row (when a
    // tool fired); the row is consumed here so a retried turn never
    // rereads a stale result.
    try {
      return usesNamedParams(node.apply)
        ? await Promise.resolve(node.apply({ state: applyCtx, output, item: applyExtraArg ?? undefined, meta: applyMeta, needs: needsCtx }))
        : applyExtraArg !== undefined
          ? await Promise.resolve(node.apply(applyCtx, output, applyExtraArg, applyMeta, needsCtx))
          : await Promise.resolve(node.apply(applyCtx, output, undefined, applyMeta, needsCtx));
    } finally {
      if (output?.toolResult) await clearPendingResult(sessionKey).catch(() => {});
    }
  }

  return { dispatchAgentWithRetry };
}
