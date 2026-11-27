// Pure stateless helpers — no closure deps, no factory required.

import { getAllSubflowGroups } from "../state/subflow-state.js";
import { replayResults } from "./result-replay.js";

// Edge functions come in two modes, dispatched by declared arity:
//   - direct edge (1 param):  (state) => nextNodeId | null
//   - edge function (2 params): (state, error) => nextNodeId | null — opts
//     into receiving the node's thrown error ({message, errorType} or null if
//     it succeeded / hasn't run) as its 2nd arg, so a cascade's edge can
//     decide retry-vs-fallback-vs-give-up from the real thrown error.
// CAVEAT: a default on the 2nd param (`(state, error = null) =>`) or a rest
// param collapses fn.length and silently falls back to direct-edge mode — no
// error ever passed. Don't do that.
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
  // setWaiting() always writes status='blocked' — it never writes "waiting"
  // (confirmed repo-wide), so that's the only value checked here.
  return status === "blocked";
}

// A flow waiting on an internal resource (shared admission-pool slot —
// child-dispatch/flow-start — or a cross-process lock) rather than an
// operator decision. Distinct from isBlocked so callers meaning "waiting on
// a human" don't accidentally match resource-queued flows, and vice versa.
export function isQueued(status) {
  return status === "queued";
}

// Whether entering `nodeId` is a genuinely fresh pass or a continuation of
// the SAME dispatch (retry, reentrant loop-wait re-check) — feeds
// node-executions.js write()/findOrCreateInvocation()'s forceNew param.
// `lastNodeId` must come from findCurrentStep(flowId) called BEFORE this
// node's own dispatch decision; never re-read after execution starts.
// findCurrentStep reflects the node entered before this one (this node's row is
// only created once the dispatch decision is made): if it matches nodeId,
// we're re-entering the exact node we just came from (continuation — e.g. an
// operator retry that rewinds lastStep without a new invocation, or a
// reentrant loop-wait re-check); anything else, including no prior
// invocation (lastNodeId === null), is a fresh pass.
export function isFreshNodeEntry(lastNodeId, nodeId) {
  return lastNodeId !== nodeId;
}

export function mergeForConflict(fresh, mine) {
  // Cheap merge: prefer "my" values, but keep any slot-keyed objects fresh
  // already has and I don't. Good enough for slot-per-item parallel writes.
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
// waitJson (no blockedInfo/throw — see executor.js's waitJson-persist branch).
// node.renderForOperator is checked first; no pipeline in this codebase
// implements it, so this kind-aware synthesis is what actually fires. Every
// other wait kind (resource-lock, needs-gateway-handoff, pipeline-load-pending,
// budget-gate-approval) already carries a real reason by construction and never
// reaches this; only human-gate and children-pending route through the
// generic waitJson path, so those are the two kinds worth a specific message.
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

// Fail-fast check for one waitJson batch's items, called right before any
// waitJson (root-level human-gate, branch-internal loop aggregate, or a
// node's own `_blocked.items`) gets persisted. Every gate must declare its own
// `requestPayload.layout` array — no auto-synthesis from editableFields/
// editableText/chatThread/references, so a forgotten `layout` fails loudly
// here instead of shipping an empty gate. A layout with kind:"deck" must have
// that as its ONLY entry (mixing a deck with siblings is ambiguous —
// GateDeckView pages the whole gate), and each deck question must be a
// well-formed {question, hint?}. Throws rather than degrading silently — both
// are pipeline-author bugs, not runtime data shapes.
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

// Whether an `apply` function was written with a single destructured
// parameter (`apply({ state, item, output })`) rather than positional args.
// Detected by checking the first char after `(` is `{`.
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
// flow_node_invocations' resultJson rows ON TOP OF `baseState` (the flow's
// own current stateJson), instead of relying purely on the persisted
// accumulated blob.
//
// baseState matters: state_json carries entry/bookkeeping fields (pipelineId,
// args, runId, startedAt, cost, _display, approvedIds/rejectedIds/editedTexts/
// approvalPending/approvalResults, _cascadeError, triggeredBy) that NO node's
// own stateUpdate produces, so resultJson replay alone can never reproduce
// them. Real call sites need these (dispatchSubflowChildren reads
// state.pipelineId to build childPipelineId and clones the whole state into
// every spawned child); baseState:{} (kept for the validation script/tests)
// drops them, fine for comparing node-output keys in isolation but would break
// real dispatch.
//
// The overlay is additive, not a behaviour change: every node-output key
// resultJson can contribute is already present in state_json (verified
// byte-for-byte against a real flow). It isolates node contributions; the
// reconstructed state currently matches the blob it was derived from.
//
// Replays each top-level node's own contributed stateUpdate in chronological
// order via the same shallow-merge applyUpdate already does
// (`{...state, ...update}`) — it replays the run's own node contributions in
// order, not a lookup of "the value of state[nodeId]": a node's resultJson is
// whatever object it returned from run()/apply(), conventionally
// `{[itsOwnNodeId]: {...}}` but not a guaranteed shape, so merge exactly as
// the live engine already merges rather than assuming a per-node key.
//
// Deliberately does NOT:
//   - synthesize an aggregate value for a subflow-kind node's own state[nodeId]
//     (dispatchSubflowChildren returns stateUpdate:{} — no such value today).
//   - read subflow results via anything but _subflowGroups, computed by the
//     same getAllSubflowGroups call folded into this function.
//
// writeResult() (state/node-executions.js) does not cap results (see its own
// header) — a node's real value is always what lands here. A
// `{ _resultTooLarge: true, bytes }` marker at a node's own key surfaces only
// for a row whose resultJson is null and resultTooLargeBytes is set — a result
// never stored, not recoverable.
export async function assembleState(flowId, { runStore, nodeExecutionsStore, baseState = {} } = {}) {
  const results = (await nodeExecutionsStore?.listResultsForFlow?.(flowId)) ?? [];
  let state = replayResults(baseState, results);
  const groups = await getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore });
  if (Object.keys(groups).length > 0) state._subflowGroups = groups;
  return state;
}
