// submit-score.js — registers the submit_score agent tool.
//
// Replaces the "scorer" agent's old contract (reply with a single fenced
// ```json``` block, parsed free-text via extractJsonBlock in
// score-llm.js's apply()) — that free-text parsing has been failing at a
// high rate as the model increasingly emits malformed content instead of
// clean JSON. Modeled on submit-draft.js's structured-tool-call pattern,
// but far simpler: no file writing, no frontmatter — just structural
// validation plus a hand-off write to tool-handoff.js's in-memory pending-
// result map, keyed by this SCORE_LLM dispatch's own invocationId
// (resolveInvocationId, tool-handoff.js — resolved from ctx.sessionKey via
// the registration dispatchAgentEntry makes right before spawning this
// session; see that module's header comment for why this doesn't need to
// touch disk or a DB).

import { toolErr } from "./shared.js";
import { resolveInvocationId, writePendingResult } from "../../../../../state/tool-handoff.js";

export function registerSubmitScoreTool({ api }) {
  api.registerTool((ctx) => ({
    name: "submit_score",
    label: "Manifold: submit a listing's fit score",
    description:
      "Use to submit your finished score instead of replying with a fenced JSON block. Call this exactly once per listing, as your final action — no prose reply before or after it. `score` must be an integer 0-100 respecting the rubric's hard-gate cap (≤30 if any hard gate failed). `fitFlags` and `reasons` follow the same schema SKILL.md's rubric describes.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["score", "reasons", "fitFlags"],
      properties: {
        score: {
          type: "number",
          description: "Final rubric score, an integer 0-100. Must be ≤30 if any hard gate (language/budget/domain) failed.",
        },
        reasons: {
          type: "array",
          items: { type: "string" },
          description: "2-5 short strings explaining the score — one positive driver, one negative driver minimum.",
        },
        fitFlags: {
          type: "object",
          additionalProperties: false,
          required: ["language", "budget", "domain", "time"],
          properties: {
            language: { type: "boolean" },
            budget: { type: "boolean" },
            domain: { type: "boolean" },
            time: { type: "boolean" },
          },
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const score = params?.score;
      if (typeof score !== "number" || !Number.isInteger(score) || score < 0 || score > 100) {
        return toolErr("score must be an integer 0-100");
      }
      const reasons = Array.isArray(params?.reasons) ? params.reasons.filter((r) => typeof r === "string" && r.trim()) : [];
      if (reasons.length === 0) return toolErr("reasons must be a non-empty array of strings");
      const fitFlagsRaw = params?.fitFlags;
      if (!fitFlagsRaw || typeof fitFlagsRaw !== "object") return toolErr("fitFlags is required");
      const fitFlags = {};
      for (const k of ["language", "budget", "domain", "time"]) {
        if (typeof fitFlagsRaw[k] !== "boolean") return toolErr(`fitFlags.${k} must be a boolean`);
        fitFlags[k] = fitFlagsRaw[k];
      }
      const hardGateFailed = fitFlags.language === false || fitFlags.budget === false || fitFlags.domain === false;
      if (hardGateFailed && score > 30) {
        return toolErr(`score must be ≤30 when a hard gate failed (language=${fitFlags.language}, budget=${fitFlags.budget}, domain=${fitFlags.domain}), got ${score}`);
      }

      const invocationId = resolveInvocationId(ctx?.sessionKey);
      if (!invocationId) {
        return toolErr("could not resolve this session's dispatch context — this tool must be called from within a SCORE_LLM node dispatch");
      }

      writePendingResult(invocationId, { score, reasons, fitFlags });
      try { api?.logger?.info?.(`submit_score tool: score=${score} for invocation ${invocationId.slice(0, 8)}`); } catch {}

      return {
        content: [{ type: "text", text: `score submitted: ${score}` }],
      };
    },
  }), { name: "submit_score" });
}
