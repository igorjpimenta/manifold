// propose-draft-meta.js — registers the propose_draft_meta agent tool.
//
// Sibling of propose_draft for METADATA changes — budget / timeframe /
// language updates that don't touch the prose body. Lets the operator
// say "bump budget to R$ 20k" in chat and have Cyrano register just
// that change without re-drafting. Same operator-gated semantics: the
// proposal stashes via mergePendingFields; the iOS Apply button
// commits the merge into the .md file's frontmatter (preserving the
// body).
//
// Field whitelist is the submission-form set Hermes reads from
// frontmatter: budget, timeframe, language. Lifecycle fields
// (status, approval-token, drafted-at) and identity fields (job-id,
// source-listing) are pipeline-owned and rejected here.
//
// Factory-form registration (like propose_draft / manifold_start) —
// needs `ctx.sessionKey` / `ctx.agentId` to auto-resolve flowId/itemId
// when the caller is a subagent that was never given them directly.
// See resolveGateContext in shared.js.

import { mergePendingFields, readPending } from "../../../../../state/pending-store.js";
import { toolErr, META_FIELD_WHITELIST, normalizePendingFields, resolveGateContext, looksLikeNumericRange } from "./shared.js";

export function registerProposeDraftMetaTool({ api, readStore, componentEmitter, discussStore }) {
  api.registerTool((ctx) => ({
    name: "propose_draft_meta",
    label: "Manifold: propose frontmatter metadata changes for the operator's gate",
    description:
      "Use during a discuss-with-the-operator session to propose budget / timeframe / language changes on the pending gate's draft (without changing the prose body). The operator sees an 'Apply' button on your reply — they decide whether to commit. Pair with `propose_draft` when the operator wants both body AND metadata changed; either tool alone when only one dimension is in play. You do NOT need to pass `flowId`/`itemId` — the gateway resolves the active gate from your session context automatically. Pass them explicitly only if you already have them (e.g. from your discussion system prompt) or if auto-resolve fails.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["fields"],
      properties: {
        flowId: {
          type: "string",
          description: "Full flowId of the run with the pending gate. Optional — auto-resolved from your session context if omitted or wrong.",
        },
        itemId: {
          type: "string",
          description: "Gate itemId you're proposing the metadata change for. Optional — auto-resolved from your session context if omitted or wrong.",
        },
        fields: {
          type: "object",
          additionalProperties: false,
          description: "Partial frontmatter update — include only the fields you want to change. Unknown fields are rejected.",
          properties: {
            budget: {
              type: ["string", "number"],
              description: "Numeric budget (BRL) the submission form should receive. Pass as number or numeric string. Example: 20000.",
            },
            timeframe: {
              type: "string",
              description: "Delivery duration in the same language as the draft body. Examples: '4 semanas', '8-12 weeks'.",
            },
            language: {
              type: "string",
              description: "Language code for the draft body. One of 'pt-BR', 'en', 'es'.",
            },
            duration: {
              type: "string",
              description: "Upwork-only project-duration bucket for the milestone form's \"How long will this project take?\" dropdown. One of 'Less than 1 month', '1 to 3 months', '3 to 6 months', 'More than 6 months' — copy one of these four exactly, not a free-text estimate.",
            },
            period: {
              type: ["string", "number"],
              description: "Freelancer.com-only: the bid's delivery period in whole days (maps directly to placeBid's `period` field). Pass a positive integer, e.g. 14.",
            },
          },
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const rawFields = params?.fields;
      if (!rawFields || typeof rawFields !== "object") return toolErr("fields object is required");
      const fields = {};
      for (const [k, v] of Object.entries(rawFields)) {
        if (!META_FIELD_WHITELIST.has(k)) {
          return toolErr(`unknown field: ${k} (allowed: ${[...META_FIELD_WHITELIST].join(", ")})`);
        }
        if (v == null || v === "") continue;
        // Budget/period must be a positive integer, passed as a real number
        // or a plain digit string. No auto-stripping of currency symbols,
        // separators, or ranges ("R$ 6.000", "6,000", "2600-3500") -- silently
        // "fixing" malformed input is exactly how a real budget range once
        // got mangled into a single 8-figure number (see looksLikeNumericRange
        // usage below). Reject and ask the agent to pass a clean number
        // instead of guessing what it meant.
        if (k === "budget") {
          let n;
          if (typeof v === "number") {
            n = v;
          } else {
            const raw = String(v).trim();
            if (looksLikeNumericRange(raw)) {
              return toolErr(`budget must be a single number, not a range — got ${JSON.stringify(v)}; pick one value (e.g. the minimum or midpoint) and pass it as a plain number like 5000`);
            }
            if (!/^\d+$/.test(raw)) {
              return toolErr(`budget must be a plain number with no currency symbols or formatting — got ${JSON.stringify(v)}; pass it as a plain number like 5000, not "R$ 5.000"`);
            }
            n = Number(raw);
          }
          if (!Number.isFinite(n) || n <= 0) return toolErr(`budget must be a positive integer (e.g. 5000), got: ${JSON.stringify(v)} — pass a plain number like 5000`);
          fields[k] = Math.round(n);
        } else if (k === "period") {
          let n;
          if (typeof v === "number") {
            n = v;
          } else {
            const raw = String(v).trim();
            if (looksLikeNumericRange(raw)) {
              return toolErr(`period must be a single number, not a range — got ${JSON.stringify(v)}; pick one value and pass it as a plain integer number of days, like 14`);
            }
            if (!/^\d+$/.test(raw)) {
              return toolErr(`period must be a plain integer number of days with no formatting — got ${JSON.stringify(v)}; pass it as a plain number like 14`);
            }
            n = Number(raw);
          }
          if (!Number.isFinite(n) || n <= 0) return toolErr(`period must be a positive integer number of days (e.g. 14), got: ${JSON.stringify(v)}`);
          fields[k] = Math.round(n);
        } else {
          const s = String(v).trim();
          if (!s) continue;
          fields[k] = s;
        }
      }
      if (Object.keys(fields).length === 0) return toolErr("no valid fields supplied");

      let flowId = typeof params?.flowId === "string" ? params.flowId.trim() : "";
      let itemId = typeof params?.itemId === "string" ? params.itemId.trim() : "";
      let flow = flowId ? await readStore.get(flowId) : null;
      if (!flow) {
        const resolved = await resolveGateContext({ ctx });
        if (resolved) {
          flowId = resolved.flowId;
          itemId = resolved.itemId;
          flow = await readStore.get(flowId);
        }
      }
      if (!flowId || !itemId) {
        return toolErr("flowId/itemId were not provided and could not be auto-resolved from session context — pass them explicitly, or call this from an active discuss session.");
      }
      if (!flow) return toolErr(`flow not found: ${flowId}`);
      if (flow.waitJson?.kind !== "human-gate") return toolErr(`flow ${flowId.slice(0, 8)} is not paused on a gate`);
      const items = Array.isArray(flow.waitJson?.items) ? flow.waitJson.items : [];
      const currentGateItem = items.find((it) => it?.itemId === itemId);
      if (!currentGateItem) {
        return toolErr(`gate item not found: ${itemId} (pending items: ${items.map((it) => it.itemId).join(", ") || "none"})`);
      }
      // No-op guard: fail if every proposed field is identical to the current
      // state (gate editableFields baseline merged with any pending store fields).
      {
        const pending = readPending(flowId, itemId) ?? {};
        const pendingFields = pending.fields ?? {};
        const gateEditableFields = Array.isArray(currentGateItem?.requestPayload?.editableFields)
          ? currentGateItem.requestPayload.editableFields : [];
        const baseline = {};
        for (const ef of gateEditableFields) {
          if (ef.editable !== false && ef.id && META_FIELD_WHITELIST.has(ef.id) && ef.value != null) {
            baseline[ef.id] = ef.value;
          }
        }
        const current = { ...baseline, ...pendingFields };
        const allUnchanged = Object.entries(fields).every(([k, v]) => {
          const cur = current[k];
          if (cur == null) return false;
          return Number.isFinite(Number(cur)) && Number.isFinite(Number(v))
            ? Number(cur) === Number(v)
            : String(cur).trim() === String(v).trim();
        });
        if (allUnchanged) {
          const proposed = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(", ");
          return toolErr(`No changes to apply — proposed fields (${proposed}) are identical to the current draft values. Revise before calling propose_draft_meta.`);
        }
      }
      // Shallow-merge with any prior fields stashed for this
      // (flowId, itemId). Lets Cyrano build up changes across
      // multiple tool calls in the same turn, and also lets a later
      // call to propose_draft_meta refine without resetting earlier
      // operator-acknowledged fields.
      mergePendingFields(flowId, itemId, fields);
      if (discussStore) {
        try { await discussStore.mergePendingFields(flowId, itemId, fields); } catch {}
      }
      try { api?.logger?.info?.(`propose_draft_meta tool: stashed ${Object.keys(fields).join("+")} for ${flowId.slice(0, 8)}/${itemId.slice(0, 40)}`); } catch {}
      try {
        if (componentEmitter) {
          await componentEmitter({ flowId, itemId, component: { kind: "proposed_meta", fields: normalizePendingFields(fields), updatedAt: Date.now() } });
        }
      } catch {}
      const summary = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(", ");
      return {
        content: [
          { type: "text", text: `Metadata change proposed (${summary}). The operator will see an "Apply" button on this reply — they decide whether to commit.` },
        ],
      };
    },
  }), { name: "propose_draft_meta" });
}
