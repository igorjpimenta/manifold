// Pure stateless helpers — no closure deps, no factory required.

import { getAllSubflowGroups } from "../state/subflow-state.js";
import { replayResults } from "./result-replay.js";

// Edge functions come in two modes, dispatched by declared arity:
//   - direct edge (1 param):  (state) => nextNodeId | null.
//   - edge function (2 params): (state, error) => nextNodeId | null — opts
//     into receiving the node's thrown error ({message, errorType}, or null
//     if it succeeded / hasn't run), so a cascade's edge can decide
//     retry-vs-fallback-vs-give-up from the REAL error.
//   CAVEAT: a default on the 2nd param (`(state, error = null) =>`) or a
//   rest param collapses fn.length and silently falls back to direct-edge
//   mode — no error ever passed. Don't do that.
export function resolveEdge(edges, fromNode, state, ctx = null) {
  const edge = edges[fromNode];
  if (typeof edge === "string") return edge;
  if (typeof edge === "function") {
    try {
      return edge.length >= 2 ? edge(state, ctx?.error ?? null) : edge(state);
    } catch (err) {
      ctx?.logger?.warn?.(`manifold [${(ctx?.flowId ?? "?").slice(0, 8)}] edge ${fromNode} threw: ${err?.message ?? err}`);
      return null;
    }
  }
  return null;
}

export function isTerminal(status) {
  return status === "succeeded" || status === "failed" || status === "cancelled" || status === "skipped";
}

export function isBlocked(status) {
  // Despite its name, setWaiting() always writes status='blocked' — it
  // never writes "waiting", so that's the only value checked here.
  return status === "blocked";
}

// Waiting on an internal resource (admission-pool slot, cross-process
// lock), not an operator decision — kept distinct from isBlocked so
// "waiting on a human" callers don't also match resource-queued flows.
export function isQueued(status) {
  return status === "queued";
}

// Whether entering `nodeId` is a fresh pass or a continuation of the SAME
// dispatch (retry, reentrant loop-wait re-check) — feeds write()'s
// forceNew param. `lastNodeId` must come from findCurrentStep(flowId)
// called BEFORE this dispatch decision, never re-read after execution
// starts: a concurrent mutation there would reflect something unrelated to
// what led to THIS dispatch. findCurrentStep reflects the node entered
// before this one (this node's row is created only once the decision is
// made) — a match means re-entering the exact node we just came from
// (e.g. an operator retry that rewinds lastStep without a new invocation);
// anything else, including null, is a fresh pass.
export function isFreshNodeEntry(lastNodeId, nodeId) {
  return lastNodeId !== nodeId;
}

export function mergeForConflict(fresh, mine) {
  if (!fresh || typeof fresh !== "object") return mine;
  const out = { ...fresh, ...mine };
  for (const [k, v] of Object.entries(fresh)) {
    if (v && typeof v === "object" && !Array.isArray(v)
        && mine[k] && typeof mine[k] === "object" && !Array.isArray(mine[k])) {
      out[k] = { ...v, ...mine[k] };
    }
  }
  return out;
}

export function withEditableTextDefault(payload, branchData) {
  if (!payload || typeof payload !== "object") return payload;
  if (payload.editableText !== undefined) return payload;
  if (!branchData || typeof branchData !== "object") return payload;
  let candidate;
  for (const v of Object.values(branchData)) {
    if (v && typeof v === "object" && typeof v.text === "string" && v.text.length > 0) {
      candidate = v.text;
    }
  }
  if (candidate === undefined) return payload;
  return { ...payload, editableText: candidate };
}

export function shortSummary(text) {
  if (!text) return "";
  const oneLine = String(text).split("\n")[0];
  return oneLine.length > 240 ? oneLine.slice(0, 237) + "..." : oneLine;
}

export function deepClone(v) {
  return JSON.parse(JSON.stringify(v));
}

// Fallback reason for a blocked node whose result is a plain returned
// waitJson (no blockedInfo/throw). Only human-gate and children-pending
// route through this generic path — every other wait kind carries a real
// reason by construction.
export function describeWaitFallback(waitJson, nodeId) {
  switch (waitJson?.kind) {
    case "human-gate": {
      const n = waitJson.pending?.length ?? waitJson.items?.length ?? 0;
      return `awaiting ${n} approval${n === 1 ? "" : "s"} at ${nodeId}`;
    }
    case "children-pending": {
      const n = waitJson.childFlowIds?.length ?? 0;
      return `awaiting ${n} child flow${n === 1 ? "" : "s"} at ${nodeId}`;
    }
    default:
      return `blocked at ${nodeId}`;
  }
}

// Fail-fast before persisting any waitJson batch: every gate declares its
// own requestPayload.layout (no auto-synthesis from editableFields/
// editableText/etc.), and a kind:"deck" must be the layout's ONLY entry —
// GateDeckView pages the whole gate, leaving nowhere for sibling
// components. Both are always pipeline-author bugs, so throw loudly at the
// source instead of shipping broken content to the client.
export function validateGateLayout(items) {
  if (!Array.isArray(items)) return;
  for (const item of items) {
    const layout = item?.requestPayload?.layout;
    const label0 = item?.itemId ?? "?";
    if (!Array.isArray(layout)) {
      throw new Error(`item ${label0}: requestPayload.layout must be an array — every gate declares its own layout explicitly, there is no editableFields/editableText/chatThread/references auto-synthesis`);
    }
    const deckItems = layout.filter((li) => li?.kind === "deck");
    if (deckItems.length === 0) continue;
    const label = item?.itemId ?? "?";
    if (layout.length > 1) {
      throw new Error(`item ${label}: layout mixes kind:"deck" with ${layout.length - 1} other item(s) — a deck must be the only layout entry`);
    }
    const deck = deckItems[0];
    if (!Array.isArray(deck.questions) || deck.questions.length === 0) {
      throw new Error(`item ${label}: deck "${deck.id}" must have a non-empty questions array`);
    }
    deck.questions.forEach((q, i) => {
      if (!q || typeof q.question !== "string" || !q.question.trim()) {
        throw new Error(`item ${label}: deck "${deck.id}" question ${i} is missing a non-empty "question" string`);
      }
      if (q.hint != null && typeof q.hint !== "string") {
        throw new Error(`item ${label}: deck "${deck.id}" question ${i} has a non-string "hint"`);
      }
    });
  }
}

export function detectBlockedSentinel(result) {
  if (result && typeof result === "object" && result._blocked && typeof result._blocked === "object") {
    return result._blocked;
  }
  return null;
}

export function usesNamedParams(fn) {
  if (typeof fn !== "function" || fn.length !== 1) return false;
  const src = fn.toString();
  const paren = src.indexOf("(");
  return paren >= 0 && /^\s*\{/.test(src.slice(paren + 1));
}

export function detectSkippedSentinel(result) {
  if (result && typeof result === "object" && result._skipped && typeof result._skipped === "object") {
    return result._skipped;
  }
  return null;
}

// Reconstructs the working `state` a node would see by replaying
// flow_node_invocations' resultJson rows ON TOP of `baseState` (the flow's
// current stateJson).
//
// `baseState` is required: state_json carries entry/bookkeeping fields
// (pipelineId, args, runId, cost, approvedIds, _cascadeError, …) that no
// node's stateUpdate ever produces, so replay alone can never reproduce
// them — passing baseState:{} silently drops them (fine for comparing
// node-output keys in isolation, fatal for real dispatch).
//
// Overlay is additive: every node-output key resultJson can contribute is
// already present in state_json. Each row replays in chronological order
// via the same shallow-merge the live engine uses ({...state, ...update})
// — a merge, not a per-node-key lookup, because a node's resultJson is
// whatever run()/apply() returned (conventionally {[nodeId]: …}, not
// guaranteed).
//
// Deliberately NOT done: no synthesized aggregate for a subflow-kind node's
// own state[nodeId] (dispatchSubflowChildren always returns stateUpdate:{}
// — there is no such value to fabricate); subflow results come only
// through _subflowGroups, folded into the same getAllSubflowGroups call.
export async function assembleState(flowId, { runStore, nodeExecutionsStore, baseState = {} } = {}) {
  const results = (await nodeExecutionsStore?.listResultsForFlow?.(flowId)) ?? [];
  let state = replayResults(baseState, results);
  const groups = await getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore });
  if (Object.keys(groups).length > 0) state._subflowGroups = groups;
  return state;
}
