import { usesNamedParams } from "./executor-helpers.js";
import { clearPendingResult } from "../state/tool-handoff.js";

// agent-dispatch.js — the engine's agent-node dispatch: run one turn
// through the injected harness runtime, then reduce the result with the
// node's apply(). The turn itself is harness work; core injects it as
// `runTurn` and only ever sees { text, runId, toolResult? }.

export function createAgentDispatch({ runTurn, sessionIndex, logger, emit }) {
  // Single attempt, no automatic retry: a failed dispatch records as a
  // normal failure event and the operator retries manually via the flow's
  // Retry action. `node.retry` config is inert.
  async function dispatchAgentWithRetry({ node, nodeId, session, initialMessage, applyCtx, applyExtraArg, applyMeta, flowId, needsCtx }) {
    if (sessionIndex?.record) await sessionIndex.record({ flowId, session, nodeId });
    if (typeof runTurn !== "function") {
      throw new Error("manifold: no agent runtime bound — the harness must inject runTurn");
    }
    const output = await runTurn({
      session,
      message: initialMessage,
      skill: node.skill,
      agentId: node.agent,
    });
    // Emit the reply as a transcript event, or it's visible only inside
    // apply(); skipped when there's no text (tool-fired turn). emit is
    // optional like sessionIndex — CLI/tests dispatch without it.
    if (emit && output?.text) {
      try { await emit("session.transcript.completed", { scope: "node", nodeId, session, text: output.text }); } catch {}
    }
    // needsCtx is additive on both apply() calling conventions — existing
    // apply() functions ignore the extra arg/key. toolResult is consumed
    // in finally so a retried turn never rereads a stale result.
    try {
      return usesNamedParams(node.apply)
        ? await Promise.resolve(node.apply({ state: applyCtx, output, item: applyExtraArg ?? undefined, meta: applyMeta, needs: needsCtx }))
        : applyExtraArg !== undefined
          ? await Promise.resolve(node.apply(applyCtx, output, applyExtraArg, applyMeta, needsCtx))
          : await Promise.resolve(node.apply(applyCtx, output, undefined, applyMeta, needsCtx));
    } finally {
      if (output?.toolResult) await clearPendingResult(session).catch(() => {});
    }
  }

  return { dispatchAgentWithRetry };
}
