// propose-draft.js — registers the propose_draft agent tool.
//
// Tool the discuss-chat agent (typically `main`/Herculano) calls to
// propose a revised draft for a pending gate. Replaces the brittle
// <draft>...</draft> text-tag protocol — structured tool calls are
// more reliable than text formatting and survive markdown/quoting
// quirks. Same operator-gated semantics: the proposal surfaces as an
// "Apply as draft" button on the operator's iOS Inbox; nothing is
// auto-committed.
//
// Reads (readStore — cross-process sqlite) to validate the flow + gate
// item exist before stashing. Writes through the on-disk pending-store
// (sidecar JSON per flow+item) so the proposal survives gateway
// restart AND is readable on cold-open of the iOS app via the discuss
// history GET.
//
// Factory-form registration (like manifold_start) — needs `ctx.sessionKey`
// / `ctx.agentId` to auto-resolve flowId/itemId when the caller is a
// subagent (e.g. Cyrano) that was never given them directly. See
// resolveGateContext in shared.js.

import { setPendingText } from "../../state/pending-store.js";
import { toolErr, resolveGateContext } from "./shared.js";

export function registerProposeDraftTool({ api, readStore, componentEmitter, discussStore }) {
  api.registerTool((ctx) => ({
    name: "propose_draft",
    label: "Manifold: propose a draft revision for the operator's gate",
    description:
      "Use during a discuss-with-the-operator session to propose a new draft for the pending gate. The operator sees an 'Apply as draft' button on your reply — they decide whether to commit. This is the PRIMARY mechanism; do not write <draft>...</draft> tags inline if you call this tool. Pass ONLY the literal reply body inside `text` (the message the operator would actually send — no headings, no Before/After commentary, no narration). You do NOT need to pass `flowId`/`itemId` — the gateway resolves the active gate from your session context automatically. Pass them explicitly only if you already have them (e.g. from your discussion system prompt) or if auto-resolve fails. Pass `id` only when the gate has multiple draft slots (rare — e.g. subject + body) and you want to target a specific one; defaults to the gate's first/sole draft.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["text"],
      properties: {
        flowId: {
          type: "string",
          description: "Full flowId of the run with the pending gate. Optional — auto-resolved from your session context if omitted or wrong.",
        },
        itemId: {
          type: "string",
          description: "Gate itemId you're proposing the draft for. Optional — auto-resolved from your session context if omitted or wrong.",
        },
        text: {
          type: "string",
          description: "Literal new reply body. Just the message — no headings, no comparisons, no commentary.",
        },
        id: {
          type: "string",
          description: "Optional draft slot id when the gate has multiple drafts in its layout (e.g. 'subject' + 'body'). Omit for single-draft gates — defaults to the gate's first/sole draft slot.",
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const text = typeof params?.text === "string" ? params.text.trim() : "";
      if (!text) return toolErr("text is required");

      let flowId = typeof params?.flowId === "string" ? params.flowId.trim() : "";
      let itemId = typeof params?.itemId === "string" ? params.itemId.trim() : "";
      const draftId = typeof params?.id === "string" && params.id.trim()
        ? params.id.trim()
        : null;

      // Validate target before recording — surface a clear error to
      // the agent if they got the IDs wrong rather than silently
      // accepting a useless proposal.
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
      const resolveItemId = (raw) => typeof raw === "string" ? raw : (raw?.id ?? null);
      const currentItem = items.find((it) => resolveItemId(it?.itemId) === itemId);
      if (!currentItem) {
        return toolErr(`gate item not found: ${itemId} (pending items: ${items.map((it) => resolveItemId(it?.itemId) ?? it?.itemId).join(", ") || "none"})`);
      }
      const currentText = typeof currentItem?.requestPayload?.editableText === "string"
        ? currentItem.requestPayload.editableText.trim()
        : null;
      if (currentText !== null && text === currentText) {
        return toolErr("No changes to apply — the proposed text is identical to the current draft. Revise the draft before calling propose_draft.");
      }
      setPendingText(flowId, itemId, text, draftId);
      if (discussStore) {
        try { await discussStore.setPending(flowId, itemId, text, draftId); } catch {}
      }
      const slotTag = draftId ? `/${draftId}` : "";
      try { api?.logger?.info?.(`propose_draft tool: stashed ${text.length} chars for ${flowId.slice(0, 8)}/${itemId.slice(0, 40)}${slotTag}`); } catch {}
      try {
        if (componentEmitter) {
          await componentEmitter({ flowId, itemId, component: { kind: "proposed_draft", text, draftId: draftId ?? null, updatedAt: Date.now() } });
        }
      } catch {}
      return {
        content: [
          { type: "text", text: `Draft proposed (${text.length} chars). The operator will see an "Apply as draft" button on this reply — they decide whether to commit.` },
        ],
      };
    },
  }), { name: "propose_draft" });
}
