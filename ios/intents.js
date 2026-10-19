// Inbound intent router — handles POST /pipeclaw/ios/intent dispatched by
// the iOS app's LiveActivityIntent handlers (Approve / Deny buttons in
// Dynamic Island and Lock Screen).
//
// Two activity flavors hit this router:
//   1. needs-info  → activityId = "agent-approval:<flowId>"
//   2. gate item → activityId = "agent-approval:<flowId>:<itemId>"
//
// needs-info can no longer be produced by any live pipeline (the implicit
// `_kind` default was removed from engine/executor.js, and the
// pipeclaw_respond/pipeclaw_notify_block/pipeclaw_list_blocked tools that
// only served needs-info flows have been deleted). handleApprove / handleReply
// below now just throw — they only fire if a stale in-flight Live Activity
// from before that change still sends an Approve/Reply with no itemId. New
// activities only ever carry an itemId (gate item form), routed to
// handleGateDecision instead.
//
// needs-info Deny still works fine (no pipeclaw_respond dependency): goes
// direct via `api.runtime.tasks.managedFlows.bindSession(...).fail(...)`.
//
// gate item (either decision): routes through invokeElevated to
// /pipeclaw/_elev/gate-resolve rather than calling
// gateResolver.recordItemDecision() in-process. recordItemDecision can
// trigger executor.resume(), which may dispatch a kind:"agent" node (e.g.
// SETTLE_GAP_ANSWER) — that needs operator.write scope, which this route
// (auth:"plugin") never has ambient. See elevation.js's header for the
// full mechanism; routes/operator.js's /pipeclaw/gate/resolve loops back
// through the exact same sibling route.
//
// cancel + apply-draft still route via HTTP loopback to the pipeclaw routes
// (same process, same port) — their logic lives in operator.js and is too
// stateful (readStore fallback, store.setRunning) to safely inline here.
// apply-draft is ONLY taken when the item has a real draftPath (loop-branch
// items with an upstream sub-node that wrote a draft file to disk, e.g.
// PROPOSE) — for a plain gate with no draft file (e.g. PRIME_FILL), the
// edit goes straight to recordItemDecision with decision:"edit", since
// apply-draft's on-disk sync would silently no-op and discard the text.

import { findDraftPathForItem } from "../routes/discuss-helpers.js";

const PIPECLAW_OWNER_SESSION_KEY = "agent:main:explicit:graph-flow-owner";
const DENY_STATUS_REASON = "denied-via-live-activity";
const CANCEL_URL = "http://127.0.0.1:18789/pipeclaw/cancel";
const APPLY_DRAFT_URL = (flowId) => `http://127.0.0.1:18789/pipeclaw/runs/${encodeURIComponent(flowId)}/discuss/apply-draft`;

// Parses both activity-id shapes:
//   "agent-approval:<flowId>"          → { flowId, itemId: null }
//   "agent-approval:<flowId>:<itemId>" → { flowId, itemId }
// flowId is a 36-char UUID (8-4-4-4-12), so we can recognize the boundary
// by length — the second ':' (if present) starts at offset 37 of the
// slice-after-prefix. Exported for unit testing.
export function parseActivityId(activityId) {
  if (typeof activityId !== "string") return null;
  if (!activityId.startsWith("agent-approval:")) return null;
  const rest = activityId.slice("agent-approval:".length);
  if (!rest) return null;
  if (rest.length === 36) return { flowId: rest, itemId: null };
  if (rest.length > 37 && rest[36] === ":") {
    return { flowId: rest.slice(0, 36), itemId: rest.slice(37) };
  }
  return { flowId: rest, itemId: null };
}

export function createIntentRouter({ api, logger, invokeElevated }) {
  const tasks = api.runtime.tasks.managedFlows.bindSession({
    sessionKey: PIPECLAW_OWNER_SESSION_KEY,
  });

  // Only reachable from a stale in-flight Live Activity predating the
  // needs-info removal (see file header) — pipeclaw_respond no longer
  // exists, so there's nothing to dispatch to. Fail loudly and immediately
  // instead of spawning a subagent turn that would fail on a missing tool.
  async function handleApprove({ flowId, activityId }) {
    throw new Error(`needs-info activities are no longer supported (flowId=${flowId.slice(0, 8)}, activityId=${activityId}) — this is a stale Live Activity; dismiss it.`);
  }

  async function handleReply({ flowId, activityId }) {
    throw new Error(`needs-info activities are no longer supported (flowId=${flowId.slice(0, 8)}, activityId=${activityId}) — this is a stale Live Activity; dismiss it.`);
  }

  async function handleDeny({ flowId, activityId }) {
    const flow = tasks.get(flowId);
    if (!flow) throw new Error(`flow not found: ${flowId}`);
    const result = tasks.fail({
      flowId,
      expectedRevision: flow.revision,
      stateJson: flow.stateJson,
      statusReason: DENY_STATUS_REASON,
    });
    if (!result.applied) throw new Error(`tasks.fail() did not apply: ${result.code}`);
    logger?.info?.(
      `ios intent: deny applied (flowId=${flowId.slice(0, 8)}, activityId=${activityId})`
    );
    return { ok: true, action: "deny", flowId };
  }

  // Gate item resolution — HTTP loopback to /pipeclaw/_elev/gate-resolve
  // (auth:"gateway"), not a direct in-process call to
  // gateResolver.recordItemDecision(). See module header.
  async function handleGateDecision({ flowId, itemId, activityId, decision, editedText }) {
    let effectiveDecision = decision;
    if (decision === "edit") {
      if (typeof editedText !== "string" || !editedText.trim()) {
        throw new Error(`gate edit intent missing editedText (flowId=${flowId.slice(0, 8)}, itemId=${itemId})`);
      }
      // Only take the apply-draft detour when there's an actual draft file
      // to sync it to (loop-branch items whose upstream sub-node wrote a
      // draftPath, e.g. PROPOSE's DRAFT step). For a plain gate with no
      // draft file, apply-draft's on-disk sync silently no-ops — fall
      // through and let recordItemDecision carry the text directly instead.
      const hasDraftTarget = findDraftPathForItem([], itemId) != null;
      if (hasDraftTarget) {
        const token = process.env.OPENCLAW_GATEWAY_TOKEN;
        if (!token) {
          throw new Error("OPENCLAW_GATEWAY_TOKEN unset; bridge cannot call apply-draft");
        }
        const applyRes = await fetch(APPLY_DRAFT_URL(flowId), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`,
          },
          body: JSON.stringify({ text: editedText, itemId, source: "ios" }),
        });
        const applyBody = await applyRes.json().catch(() => ({}));
        if (!applyRes.ok || applyBody?.ok === false) {
          throw new Error(`apply-draft ${applyRes.status}: ${applyBody?.code ?? "unknown"}`);
        }
        effectiveDecision = "approve";
        logger?.info?.(
          `ios intent: iOS edit rewritten as apply-draft+approve ` +
          `(flowId=${flowId.slice(0, 8)}, itemId=${itemId}, ${editedText.length}ch)`
        );
      }
      // else: fall through — effectiveDecision stays "edit", editedText
      // carries through to recordItemDecision below.
    }
    const elev = await invokeElevated("/pipeclaw/_elev/gate-resolve", {
      flowId,
      itemId,
      decision: effectiveDecision,
      source: "ios",
      ...(effectiveDecision === "edit" ? { editedText } : {}),
    });
    let body;
    try { body = elev.body ? JSON.parse(elev.body) : {}; }
    catch { body = {}; }
    if (!elev.ok || !body?.ok) {
      throw new Error(`gate/resolve: ${body?.code ?? elev.error ?? "unknown"}`);
    }
    logger?.info?.(
      `ios intent: gate ${effectiveDecision} dispatched (flowId=${flowId.slice(0, 8)}, ` +
      `itemId=${itemId}, status=${body.status ?? "?"}, activityId=${activityId})`
    );
    return { ok: true, action: effectiveDecision, flowId, itemId, status: body.status };
  }

  async function handleCancel({ flowId, activityId }) {
    const token = process.env.OPENCLAW_GATEWAY_TOKEN;
    if (!token) {
      throw new Error("OPENCLAW_GATEWAY_TOKEN unset; bridge cannot call /pipeclaw/cancel");
    }
    const res = await fetch(CANCEL_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ flowId, reason: "cancelled from Live Activity" }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body?.ok === false) {
      throw new Error(`cancel ${res.status}: ${body?.code ?? body?.error ?? "unknown"}`);
    }
    logger?.info?.(
      `ios intent: cancel dispatched (flowId=${flowId.slice(0, 8)}, activityId=${activityId})`
    );
    return { ok: true, action: "cancel", flowId };
  }

  async function handle(payload) {
    if (payload?.type !== "live_activity.intent") {
      throw new Error("expected payload.type=live_activity.intent");
    }
    const parsed = parseActivityId(payload.activityId);
    if (!parsed) throw new Error(`unrecognized activityId format: ${payload.activityId}`);
    const { flowId, itemId } = parsed;
    if (!["approve", "deny", "reply", "edit", "cancel", "confirm-sent", "confirm-not-sent"].includes(payload.intentId)) {
      throw new Error(`unknown intentId: ${payload.intentId}`);
    }
    if (payload.intentId === "cancel") {
      return await handleCancel({ flowId, activityId: payload.activityId });
    }
    if (itemId) {
      if (payload.intentId === "reply") {
        throw new Error("reply intent not supported for gate items; use 'edit' to change the draft text");
      }
      if (payload.intentId === "edit") {
        const text = typeof payload.text === "string" ? payload.text : "";
        if (!text.trim()) {
          throw new Error("edit intent requires non-empty text");
        }
        return await handleGateDecision({
          flowId, itemId, activityId: payload.activityId, decision: "edit", editedText: text,
        });
      }
      const decision = (payload.intentId === "approve" || payload.intentId === "confirm-sent") ? "approve" : "reject";
      return await handleGateDecision({ flowId, itemId, activityId: payload.activityId, decision });
    }
    if (payload.intentId === "approve") {
      return await handleApprove({ flowId, activityId: payload.activityId });
    }
    if (payload.intentId === "reply") {
      return await handleReply({ flowId, activityId: payload.activityId, text: payload.text });
    }
    return await handleDeny({ flowId, activityId: payload.activityId });
  }

  return { handle };
}
