// report-draft-blocked.js — registers the report_draft_blocked agent tool.
//
// The structured sibling of submit_draft's failure path: when a drafting
// agent (e.g. content-writer's write-workana-proposal skill) can't draft
// honestly because the profile has no evidence for something the listing
// needs, it calls this instead of replying with a free-text "BLOCKED: ..."
// sentinel. A bare-text sentinel is fragile to parse (regex truncates
// multi-line replies, a legitimate mention of "blocked" in draft prose
// could false-positive, no real structure for more than one question) and
// the platform doesn't expose structured tool-call arguments to a pipeline
// node's apply() — only { text, runId }. This
// tool sidesteps both problems the same way submit_draft already does:
// write a structured file as the tool's side effect, let the calling
// pipeline's apply() infer what happened from that file's existence and
// content instead of parsing prose.
//
// `questions` is an array specifically so a drafting agent that already
// sees multiple gaps up front can report all of them in one call instead
// of dripping them one at a time across separate blocked rounds.
//
// Each entry may be a plain string (legacy shape, still fully supported)
// or a {question, hint} object. `hint` is optional, agent-supplied extra
// context — the specific listing detail that made the gap concrete (a
// quoted requirement line, a stack name) — surfaced to the operator
// alongside the question so they don't have to re-open the listing to
// answer. Normalized to {question, hint} objects on disk so every
// downstream reader (DRAFT.apply, GAP_GATE) sees one consistent shape.

import { writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { toolErr, resolveGateContext } from "./shared.js";

export function registerReportDraftBlockedTool({ api }) {
  api.registerTool((ctx) => ({
    name: "report_draft_blocked",
    label: "Manifold: report a draft blocked on missing profile info",
    description:
      "Use when you cannot draft honestly because the profile/portfolio has no evidence for something the listing requires — do NOT fabricate experience, and do NOT reply with free text describing the block. Call this tool instead. If you already see more than one gap, report ALL of them in one call via the `questions` array rather than calling this tool once per gap — that costs the operator an extra round-trip for no benefit. For each question, include `hint` when the listing gives you a concrete detail for free (a quoted requirement, a named stack) — it's shown to the operator right next to the question so they don't have to re-open the listing. `outPath` must be copied verbatim from your brief. `jobId` is optional — it's auto-resolved from your session's own listing context; only pass it explicitly if you have a reason to override that.\n\nIf the operator already declined a prior gap question (dealbreaker/skip/no), do NOT use this tool to ask something else instead — call `report_draft_skip` instead.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["outPath", "questions"],
      properties: {
        outPath: {
          type: "string",
          description: "Absolute output path for the blocked-report, copied verbatim from your brief.",
        },
        jobId: {
          type: "string",
          description: "The job/item identifier. Optional — auto-resolved from your session context if omitted. Pass it explicitly only if auto-resolve fails or you need to override it; never derive it from a filename or URL.",
        },
        questions: {
          type: "array",
          items: {
            oneOf: [
              { type: "string" },
              {
                type: "object",
                additionalProperties: false,
                required: ["question"],
                properties: {
                  question: { type: "string" },
                  hint: {
                    type: "string",
                    description: "Optional — the specific listing detail that makes this gap concrete (a quoted requirement, a named stack/tool). Omit if you have nothing beyond the question itself.",
                  },
                },
              },
            ],
          },
          minItems: 1,
          description: "One or more gaps the operator needs to answer before you can draft — plain strings, or {question, hint} objects. Report every currently-apparent gap in one call.",
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const outPath = typeof params?.outPath === "string" ? params.outPath.trim() : "";
      let jobId = typeof params?.jobId === "string" ? params.jobId.trim() : "";
      if (!jobId) {
        const resolved = await resolveGateContext({ ctx });
        if (resolved?.itemId) jobId = resolved.itemId;
      }
      const questions = Array.isArray(params?.questions)
        ? params.questions
            .map((q) => {
              if (typeof q === "string") return { question: q.trim(), hint: null };
              if (q && typeof q === "object" && typeof q.question === "string") {
                const hint = typeof q.hint === "string" ? q.hint.trim() : "";
                return { question: q.question.trim(), hint: hint || null };
              }
              return { question: "", hint: null };
            })
            .filter((q) => q.question)
        : [];

      if (!outPath || !path.isAbsolute(outPath) || !outPath.endsWith(".json")) {
        return toolErr("outPath must be an absolute path ending in .json, copied verbatim from your brief");
      }
      if (!jobId) return toolErr("jobId was not provided and could not be auto-resolved from session context — pass it explicitly, copied verbatim from your brief");
      if (questions.length === 0) return toolErr("questions must be a non-empty array of plain-language question strings");

      const content = JSON.stringify({ jobId, questions, reportedAt: new Date().toISOString() }, null, 2);

      await mkdir(path.dirname(outPath), { recursive: true });
      await writeFile(outPath, content, "utf-8");

      try { api?.logger?.info?.(`report_draft_blocked tool: wrote ${questions.length} question(s) to ${outPath} (job-id=${jobId})`); } catch {}

      return {
        content: [
          { type: "text", text: `blocked-report: ${outPath}` },
        ],
      };
    },
  }), { name: "report_draft_blocked" });
}
