// report-draft-skip.js — registers the report_draft_skip agent tool.
//
// Sibling of report_draft_blocked, deliberately a separate tool rather than
// a `skip` flag bolted onto it: report_draft_blocked's job is "I need more
// info before I can draft" (asks a question); this tool's job is "the
// operator already declined a prior gap question — stop, don't ask again,
// don't draft around it." Conflating the two under one tool with a boolean
// flag left `questions` doing double duty (new question vs. record of what
// was declined) and a drafting agent could set the flag without saying
// anything real. Splitting them gives the skip path its own required
// `reason` field instead.
//
// Writes to the SAME blockedReportPath report_draft_blocked uses — DRAFT.apply
// (nodes/propose/draft.js) has one file to check, distinguished by shape:
// `{ questions: [...] }` (report_draft_blocked) vs `{ skip: true, reason }`
// (this tool). No new path convention, no brief changes needed.

import { writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { toolErr, resolveGateContext } from "./shared.js";

export function registerReportDraftSkipTool({ api }) {
  api.registerTool((ctx) => ({
    name: "report_draft_skip",
    label: "Manifold: report the operator declined this listing — skip it",
    description:
      "Use ONLY after the operator's answer to a gap question you already asked was an explicit decline — \"dealbreaker\", \"skip\", \"skip it\", \"no\", or equivalent. Ends this listing immediately: no draft is written, no further gap rounds happen. Do NOT call report_draft_blocked with a different question instead (that buries the decline under an unrelated ask), and do NOT draft around the declined requirement (that silently overrides the operator's decision). `reason` should name what was declined, in your own words, from the operator's actual answer — not invented. `outPath` must be copied verbatim from your brief (same blockedReportPath report_draft_blocked uses). `jobId` is optional — auto-resolved from your session's own listing context.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["outPath", "reason"],
      properties: {
        outPath: {
          type: "string",
          description: "Absolute output path for the blocked-report, copied verbatim from your brief (the same blockedReportPath report_draft_blocked uses).",
        },
        jobId: {
          type: "string",
          description: "The job/item identifier. Optional — auto-resolved from your session context if omitted. Pass it explicitly only if auto-resolve fails or you need to override it; never derive it from a filename or URL.",
        },
        reason: {
          type: "string",
          description: "What the operator declined, in your own words drawn from their actual answer — e.g. \"No Italian-language capability; operator confirmed this is a dealbreaker.\" Do not invent or embellish beyond what they said.",
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
      const reason = typeof params?.reason === "string" ? params.reason.trim() : "";

      if (!outPath || !path.isAbsolute(outPath) || !outPath.endsWith(".json")) {
        return toolErr("outPath must be an absolute path ending in .json, copied verbatim from your brief");
      }
      if (!jobId) return toolErr("jobId was not provided and could not be auto-resolved from session context — pass it explicitly, copied verbatim from your brief");
      if (!reason) return toolErr("reason must be a non-empty description of what the operator declined");

      const content = JSON.stringify({ jobId, skip: true, reason, reportedAt: new Date().toISOString() }, null, 2);

      await mkdir(path.dirname(outPath), { recursive: true });
      await writeFile(outPath, content, "utf-8");

      try { api?.logger?.info?.(`report_draft_skip tool: wrote skip report to ${outPath} (job-id=${jobId})`); } catch {}

      return {
        content: [
          { type: "text", text: `skip-report: ${outPath}` },
        ],
      };
    },
  }), { name: "report_draft_skip" });
}
