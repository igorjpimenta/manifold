// submit-draft.js — registers the submit_draft agent tool.
//
// Drafting agents (e.g. content-writer's write-workana-proposal skill) call
// this instead of hand-writing a markdown file with YAML frontmatter via
// shell heredoc. Hand-typed frontmatter has repeatedly drifted — most
// often the `job-id` field, which agents used to have to infer from
// filenames instead of copying the literal value their prompt supplies.
// This tool takes structured params and stamps the frontmatter itself,
// deterministically.
//
// jobId defaults to an auto-resolved value (resolveGateContext, shared.js)
// derived from the calling session's own lineage record — the same
// mechanism propose-draft.js uses for flowId/itemId — rather than requiring
// the agent to transcribe it. An explicit jobId param still overrides.
//
// Identity correctness (does jobId actually match the intended item) is
// NOT validated here — this tool only does structural checks. The
// calling pipeline's own node `apply()` is responsible for validating
// jobId against its own state and retrying the agent if it's wrong (see
// workspace/.pipelines/freelance_workana-proposal/nodes/propose/draft.js's
// DRAFT node for the reference implementation).
//
// Delivery-estimate field requirement (timeframe vs period) is resolved
// per-pipeline, not hardcoded: `timeframe` is intentionally NOT in the
// static JSON-schema `required` array below, because that schema is
// process-cached independent of the calling session (confirmed against
// openclaw core's tool-descriptor cache — the cache key has no
// session/flow component) and can't reliably flex per pipeline. The real
// enforcement happens in execute(), which does see fresh per-call ctx: it
// resolves pipelineId, looks up that pipeline's own declared
// `proposalMeta.requiredDeliveryField` from the live pipeline registry
// (pipelinesRef — same dependency-injection pattern pipeclaw-start.js
// already uses), and defaults to "timeframe" when a pipeline declares
// nothing (Workana today) or resolution fails for any reason (unresolved
// session, boot-time race) — a pipeline only ever gets a DIFFERENT
// required field by explicitly opting in via its own index.js, never as a
// side effect of another pipeline's config.
//
// pipelineId resolution deliberately uses resolveFlowIdOnly, not
// resolveGateContext (used below only for the jobId auto-resolve
// fallback): a per-item agent node like DRAFT is dispatched via a
// subflow with itemKey set, which spawns one CHILD FLOW PER ITEM, so
// flow_id alone already disambiguates the item — item_key is never
// stamped on this session's lineage row (executor-nodes.js's recordSpawn
// for a plain kind:"agent" dispatch doesn't pass one), so
// resolveGateContext's stricter flow_id-AND-item_key requirement always
// returned null here, silently defaulting every Freelancer.com draft to
// "timeframe" regardless of its own proposalMeta opt-in. Confirmed live
// 2026-09-17 (jobs 40717459/40717651: the agent correctly omitted
// timeframe per its skill, submit_draft rejected it, no draft was ever
// written).

import { writeFile, readFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { toolErr, resolveGateContext, resolveFlowIdOnly, looksLikeNumericRange } from "./shared.js";

function parseFrontmatter(raw) {
  const m = raw.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/);
  if (!m) return [{}, raw];
  const fm = {};
  for (const line of m[1].split("\n")) {
    const kv = line.match(/^([a-zA-Z][\w-]*)\s*:\s*(.*)$/);
    if (!kv) continue;
    let v = kv[2].trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    fm[kv[1]] = v;
  }
  return [fm, m[2] ?? ""];
}

function formatFrontmatterValue(v) {
  const s = String(v);
  const needsQuotes = /[:#@]|^\s|\s$|^$/.test(s);
  return needsQuotes ? JSON.stringify(s) : s;
}

export function registerSubmitDraftTool({ api, pipelinesRef }) {
  api.registerTool((ctx) => ({
    name: "submit_draft",
    label: "Pipeclaw: write a structured draft to disk",
    description:
      "Use to submit a finished draft instead of writing the file yourself with a heredoc. `outPath` must be copied verbatim from your brief. `jobId` is optional — it's auto-resolved from your session's own listing context; only pass it explicitly if you have a reason to override that (never derive it from a filename, URL, or listing content). This tool stamps the frontmatter deterministically (status is always set to 'pending' — you cannot mark a draft approved or sent) and will refuse to overwrite a draft already marked approved or sent. Exactly one of `timeframe`/`period` is required — which one depends on the pipeline you're running under (most platforms use `timeframe`; Freelancer.com uses `period`). If you're unsure, supply both.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["outPath", "body", "language", "budget"],
      properties: {
        outPath: {
          type: "string",
          description: "Absolute output path, copied verbatim from your brief.",
        },
        jobId: {
          type: "string",
          description: "The job/item identifier. Optional — auto-resolved from your session context if omitted. Pass it explicitly only if auto-resolve fails or you need to override it; never derive it from a filename or URL.",
        },
        body: {
          type: "string",
          description: "The draft's prose body only — no YAML frontmatter, no surrounding code fences.",
        },
        language: {
          type: "string",
          description: "Language code for the draft body, e.g. 'pt-BR', 'en', 'es'.",
        },
        budget: {
          type: ["string", "number"],
          description: "Numeric budget the submission form should receive. Pass as a number or numeric string.",
        },
        timeframe: {
          type: "string",
          description: "Delivery duration as a short human-readable string in the same language as the body.",
        },
        sourceListing: {
          type: "string",
          description: "Path to the source listing file this draft was written from, if any.",
        },
        duration: {
          type: "string",
          description: "Platform-specific project-duration bucket for a milestone form's \"How long will this project take?\" dropdown. One of 'Less than 1 month', '1 to 3 months', '3 to 6 months', 'More than 6 months' — copy one of these four exactly, not a free-text estimate. Omit entirely on platforms with no such field (e.g. Workana).",
        },
        period: {
          type: ["string", "number"],
          description: "Freelancer.com-only: the bid's delivery period in whole days (maps directly to placeBid's `period` field). Pass a positive integer, e.g. 14. Omit entirely on platforms with no such field (other platforms use `timeframe`/`duration` instead).",
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const outPath = typeof params?.outPath === "string" ? params.outPath.trim() : "";
      let jobId = typeof params?.jobId === "string" ? params.jobId.trim() : "";
      const resolved = await resolveGateContext({ ctx });
      if (!jobId && resolved?.itemId) jobId = resolved.itemId;
      const flowResolved = resolved ?? await resolveFlowIdOnly({ ctx });
      const body = typeof params?.body === "string" ? params.body.trim() : "";
      const language = typeof params?.language === "string" ? params.language.trim() : "";

      if (!outPath || !path.isAbsolute(outPath) || !outPath.endsWith(".md")) {
        return toolErr("outPath must be an absolute path ending in .md, copied verbatim from your brief");
      }
      if (!jobId) return toolErr("jobId was not provided and could not be auto-resolved from session context — pass it explicitly, copied verbatim from your brief");
      if (!body) return toolErr("body is required");
      if (!language) return toolErr("language is required");

      // No auto-stripping of currency symbols/separators/ranges here --
      // silently "fixing" malformed input is exactly how a real budget
      // range ("R$ 2.600 - 3.500") once got mangled into a single 8-figure
      // number. Reject and ask the agent for a clean number instead.
      let budget;
      {
        const raw = params?.budget;
        if (typeof raw === "number") {
          budget = raw;
        } else {
          const trimmed = String(raw ?? "").trim();
          if (looksLikeNumericRange(trimmed)) {
            return toolErr(`budget must be a single number, not a range — got ${JSON.stringify(raw)}; pick one value (e.g. the minimum or midpoint) and pass it as a plain number`);
          }
          if (!/^\d+$/.test(trimmed)) {
            return toolErr(`budget must be a plain number with no currency symbols or formatting, got: ${JSON.stringify(raw)}`);
          }
          budget = Number(trimmed);
        }
        if (!Number.isFinite(budget) || budget <= 0) {
          return toolErr(`budget must be a positive number, got: ${JSON.stringify(raw)}`);
        }
        budget = Math.round(budget);
      }

      const timeframe = typeof params?.timeframe === "string" ? params.timeframe.trim() : "";
      let period = null;
      if (params?.period != null) {
        if (typeof params.period === "number") {
          period = params.period;
        } else {
          const trimmed = String(params.period).trim();
          if (looksLikeNumericRange(trimmed)) {
            return toolErr(`period must be a single number, not a range — got ${JSON.stringify(params.period)}; pick one value and pass it as a plain integer number of days`);
          }
          if (!/^\d+$/.test(trimmed)) {
            return toolErr(`period must be a plain integer number of days with no formatting, got: ${JSON.stringify(params.period)}`);
          }
          period = Number(trimmed);
        }
        if (!Number.isFinite(period) || period <= 0) {
          return toolErr(`period must be a positive integer number of days, got: ${JSON.stringify(params.period)}`);
        }
        period = Math.round(period);
      }
      // See this file's header — which field is required is a per-pipeline
      // choice, declared by the pipeline itself and read from the live
      // registry, never hardcoded here. Unresolved/undeclared → "timeframe",
      // today's exact behavior for every pipeline that doesn't opt in.
      const pipelineDef = pipelinesRef?.value?.get(flowResolved?.pipelineId ?? "");
      const requiredDeliveryField = pipelineDef?.proposalMeta?.requiredDeliveryField ?? "timeframe";
      if (requiredDeliveryField === "period") {
        if (period == null) return toolErr("period is required (this pipeline uses a numeric delivery period, not a free-text timeframe)");
      } else if (!timeframe) {
        return toolErr("timeframe is required");
      }

      if (existsSync(outPath)) {
        try {
          const existingRaw = await readFile(outPath, "utf-8");
          const [existingFm] = parseFrontmatter(existingRaw);
          if (existingFm.status === "approved" || existingFm.status === "sent") {
            return toolErr(`${outPath} is already ${existingFm.status} — refusing to overwrite. Choose a different path or stop and report to the operator.`);
          }
        } catch {}
      }

      const fm = {
        status: "pending",
        language,
        "job-id": jobId,
        "drafted-at": new Date().toISOString(),
        budget,
      };
      if (timeframe) fm.timeframe = timeframe;
      if (params?.sourceListing) fm["source-listing"] = String(params.sourceListing).trim();
      if (params?.duration) fm.duration = String(params.duration).trim();
      if (period != null) fm.period = period;

      const fmLines = Object.entries(fm).map(([k, v]) => `${k}: ${formatFrontmatterValue(v)}`);
      const content = `---\n${fmLines.join("\n")}\n---\n\n${body}\n`;

      await mkdir(path.dirname(outPath), { recursive: true });
      await writeFile(outPath, content, "utf-8");

      try { api?.logger?.info?.(`submit_draft tool: wrote ${content.length} chars to ${outPath} (job-id=${jobId})`); } catch {}

      return {
        content: [
          { type: "text", text: `draft: ${outPath}` },
        ],
      };
    },
  }), { name: "submit_draft" });
}
