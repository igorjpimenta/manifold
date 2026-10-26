// extensions/manifold/hooks/score-tool-wake.js — after_tool_call hook that
// wakes a waiting SCORE_LLM dispatch the instant submit_score fires.
//
// Without this, dispatchAgentWithRetry (via runSubagentTurn's
// subagent.waitForRun call) only finds out submit_score produced a valid
// score when the WHOLE underlying agent run reaches its own natural end,
// or when timeoutMs expires — even though the tool call already carried
// everything apply() needs the moment it landed. The answer does land in
// flight — well after the node has already been marked failed — with
// nothing listening for it.
//
// Mirrors hooks/profile-write-lock.js's after_tool_call registration and
// its event.runId correlation (that file's own header comment: output.runId
// from a kind:"agent" dispatch and this hook's event.runId are the same
// identifier, via subagent.run()'s return value).
// Carries no payload of its own — the score itself still flows through
// tool-handoff.js's writePendingResult/readPendingResult, written by
// submit-score.js's execute() before this hook ever fires. This hook is
// purely the wake-up signal race-listening code (executor-subagent.js)
// waits on.

import { notifyToolFired } from "../../../../state/tool-handoff.js";

const SCORE_TOOL_NAME = "submit_score";

export function registerScoreToolWakeHook(api) {
  api.on("after_tool_call", (event) => {
    if (event?.toolName !== SCORE_TOOL_NAME) return;
    if (!event?.runId) return;
    // Only wake on a call that actually stored a pending result.
    // submit-score.js's own validation (score range, fitFlags shape,
    // hard-gate cap) returns toolErr()'s { isError: true, ... } shape
    // WITHOUT writing anything — and the model can and does self-correct a
    // rejected call within the same turn. Waking on that failed attempt
    // would end the race prematurely, before the corrected retry ever gets
    // a chance to fire, leaving apply() to fall through to output.text
    // with nothing useful in it — exactly the failure this hook exists to
    // prevent, just moved earlier.
    //
    // event.error does NOT reflect this — openclaw core's isToolError
    // classification (agent-loop.js's executePreparedToolCall) is set only
    // on a THROWN exception, never on an { isError: true } RETURN value,
    // so a toolErr() rejection is indistinguishable from success via
    // event.error alone. event.result carries the tool's actual raw
    // return value (aliased from sanitizedResult) — that top-level
    // isError flag is what toolErr() sets, so check it directly.
    if (event.result?.isError) return;
    notifyToolFired(event.runId);
  });
}
