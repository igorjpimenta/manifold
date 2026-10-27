// Pure stateless helpers — no closure deps, no factory required.

import { getAllSubflowGroups } from "../state/subflow-state.js";
import { replayResults } from "./result-replay.js";

// Edge functions come in two modes, dispatched by declared arity (same
// technique this file's own usesNamedParams() uses elsewhere):
//   - direct edge (1 param):  (state) => nextNodeId | null — unchanged,
//     every edge in the codebase before this mode existed.
//   - edge function (2 params): (state, error) => nextNodeId | null — opts
//     into receiving the node's thrown error (a {message, errorType}
//     object, or null if the node succeeded / hasn't run) as its 2nd arg.
//     Lets a cascade's edge decide retry-vs-fallback-vs-give-up from the
//     REAL thrown error instead of a soft-returned state field, per the
//     "Eradicate soft errors" push.
//   CAVEAT: a default value on the 2nd param (`(state, error = null) =>`)
//   or a rest param collapses fn.length back down and silently falls back
//   to direct-edge mode — no error ever passed. Don't do that.
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
  // never actually writes "waiting" (confirmed repo-wide), so that's the
  // only value checked here.
  return status === "blocked";
}

// A flow waiting on an internal resource (a shared admission-pool slot —
// child-dispatch, flow-start — or a cross-process lock)
// rather than an operator decision. Distinct from
// isBlocked so callers that mean "waiting on a human" don't accidentally
// also match resource-queued flows, and vice versa.
export function isQueued(status) {
  return status === "queued";
}

// Determines whether entering `nodeId` is a genuinely fresh pass through
// this node, or a continuation of the SAME dispatch (a retry, a reentrant
// loop-wait re-check) — feeds node-executions.js's
// write()/findOrCreateInvocation()'s forceNew param.
// `lastNodeId` must come from nodeExecutions.findCurrentStep(flowId)
// called BEFORE this node's own dispatch decision was made — never a
// value re-read after execution starts, since a concurrent mutation there
// would reflect something unrelated to what led to THIS dispatch.
// findCurrentStep always reflects the PREVIOUSLY entered node (this
// node's own row is only created once the dispatch decision below is
// made) — if it matches nodeId itself, we're re-entering the exact node
// we just came from (continuation, e.g. an operator retry that rewinds
// lastStep but never creates a new invocation for the failed node, or a
// reentrant loop-wait re-check); anything else, including no prior
// invocation at all (lastNodeId === null), is a fresh pass (a real
// loop-back edge, or the first-ever entry).
export function isFreshNodeEntry(lastNodeId, nodeId) {
  return lastNodeId !== nodeId;
}

export function mergeForConflict(fresh, mine) {
  // Cheap merge: prefer "my" values, but keep any slot-keyed objects that
  // fresh already has and I don't. Good enough for slot-per-item parallel writes.
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


// Core-namespace handle for one agent invocation; the adapter maps it
// to a gateway key at the edge. Explicit entry.sessionKey /
// sessionKeyBuilder overrides pass through verbatim, never minted here.
export function mintAgentSessionKey(flowId, nodeId, itemKey) {
  const seed = itemKey ? `${nodeId.toLowerCase()}-${itemKey}` : nodeId.toLowerCase();
  return `mf-${flowId.slice(0, 8)}-${seed}`;
}

// Opaque core handle shape (`mf-<flow8>-<node>[-<item>]`), verbatim.
const OPAQUE_SESSION_KEY_RE = /^mf-[0-9a-f]{8}-/;
// Gateway shape the adapter produces for an opaque handle:
// `agent:<agent>:subagent:mf-<flow8>-<node>[-<item>]`. Matched so boot-time
// orphan classification keeps working — it only ever sees gateway-shaped
// keys from task_runs rows.
const GATEWAY_SESSION_KEY_RE = /:subagent:mf-[0-9a-f]{8}-/;

// Convention-based check, same fallback role as before — see
// boot/taskflow-revive.js's isManifoldOwnedSession(), which prefers the
// session store's own pluginOwnerId field and only falls back to this
// when that lookup is unavailable.
export function isManifoldOwnedSessionKey(sessionKey) {
  return typeof sessionKey === "string"
    && (OPAQUE_SESSION_KEY_RE.test(sessionKey) || GATEWAY_SESSION_KEY_RE.test(sessionKey));
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
// waitJson (no blockedInfo/throw — see executor.js's own waitJson-persist
// branch). node.renderForOperator is checked first; since no pipeline in
// this codebase implements it, this kind-aware synthesis is what actually
// fires in practice. Every OTHER wait kind (resource-lock,
// needs-gateway-handoff, pipeline-load-pending, budget-gate-approval)
// already carries a real reason by construction and never reaches this —
// only human-gate and children-pending route through the generic waitJson
// path, so those are the two kinds worth a specific message here.
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

// Fail-fast check for one waitJson batch's items — called right before any
// waitJson (root-level human-gate, branch-internal loop aggregate, or a
// node's own `_blocked.items`) gets persisted. Every gate must declare its
// own `requestPayload.layout` array — no auto-synthesis from legacy
// editableFields/editableText/chatThread/references fields exists anymore,
// so an author who forgets `layout` gets a loud error here instead of an
// empty gate on iOS. A `layout` containing kind:"deck" must have that as
// its ONLY entry (mixing a deck with other components is ambiguous —
// GateDeckView pages the WHOLE gate, there's nowhere for sibling
// components to render), and each deck question must be a well-formed
// {question, hint?} pair. Throws rather than degrading silently — both of
// these are always pipeline-author bugs, never legitimate runtime data
// shapes, so they should fail the node loudly at the source instead of
// shipping broken content to the client.
export function validateGateLayout(items) {
  if (!Array.isArray(items)) return;
  for (const item of items) {
    const layout = item?.requestPayload?.layout;
    const label0 = item?.itemId ?? "?";
    if (!Array.isArray(layout)) {
      throw new Error(`item ${label0}: requestPayload.layout must be an array — every gate declares its own layout explicitly, there is no legacy editableFields/editableText/chatThread/references auto-synthesis`);
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

// Returns true when an `apply` function was written with a single destructured
// parameter — `apply({ state, item, output }) => ...` — rather than positional args.
// Detected by checking that the first char after `(` is `{`.
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

// Reconstructs the working `state` a node would see, by replaying
// flow_node_invocations' resultJson rows ON TOP OF `baseState` (the flow's
// own current stateJson), instead of relying purely on the persisted
// accumulated blob.
//
// `baseState` matters: state_json carries a fixed list of entry/bookkeeping
// fields (pipelineId, args, runId, startedAt, cost, _display, approvedIds/
// rejectedIds/editedTexts/approvalPending/approvalResults, _cascadeError,
// triggeredBy) that NO node's own stateUpdate ever produces, so resultJson
// replay alone can never reproduce them. Every real call site needs these
// (e.g. dispatchSubflowChildren reads state.pipelineId to build
// childPipelineId, and clones the whole state into every spawned child) —
// passing baseState:{} (kept for the existing validation script/tests)
// silently drops them, which is fine for comparing node-output keys in
// isolation but would break real dispatch.
//
// The overlay is additive, not a behaviour change: every node-output key
// resultJson can contribute is already present in state_json, verified
// byte-for-byte against a real flow. So nothing here depends on the replay
// to fix a wrong value today — it isolates node contributions, and the
// reconstructed state currently matches the blob it was derived from.
//
// Replays each top-level node's own contributed stateUpdate in
// chronological order via the exact same shallow-merge applyUpdate already
// does today (`{...state, ...update}`) — this is a replay of history, not
// a lookup of "the value of state[nodeId]": a node's resultJson is
// whatever object it returned from run()/apply(), which by convention is
// almost always `{[itsOwnNodeId]: {...}}` but isn't a guaranteed shape, so
// this merges it exactly the way the live engine already merges it rather
// than assuming a per-node key.
//
// Two things this deliberately does NOT do:
//   - It does not try to synthesize a merged/aggregate value for a
//     subflow-kind node's own state[nodeId]. dispatchSubflowChildren
//     (executor-subflow-children.js) always returns stateUpdate: {} —
//     there is no such value today, so assembly doesn't fabricate one.
//   - Subflow results are read exclusively through _subflowGroups,
//     computed the same way enrichWithSubflows already computes it —
//     folded into this one function instead of a second call site, not
//     replaced by a different mechanism.
//
// writeResult() (state/node-executions.js) no longer caps results at all
// (see its own header comment) — a node's real value is always what lands
// here now. A `{ _resultTooLarge: true, bytes }` marker at a node's own key
// can still surface, but only by replaying a row written before that
// removal, whose real value was never stored and isn't recoverable — not a
// live risk for anything writing today.
export async function assembleState(flowId, { runStore, nodeExecutionsStore, baseState = {} } = {}) {
  const results = (await nodeExecutionsStore?.listResultsForFlow?.(flowId)) ?? [];
  let state = replayResults(baseState, results);
  const groups = await getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore });
  if (Object.keys(groups).length > 0) state._subflowGroups = groups;
  return state;
}
