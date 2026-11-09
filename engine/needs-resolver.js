// needs-resolver.js — per-node "needs:": a node declares
// `needs: { varName: {hops, nodeId} }`, keyed by the local variable name
// it'll be read as (an ORM-style relation declaration, not an array) —
// and resolves each one lazily, only when `ctx.needs(varName)` is actually
// awaited inside that node's own run()/promptBuilder()/selector(), not
// pre-fetched in bulk before dispatch. A branch of code that never calls
// ctx.needs("x") never pays for or blocks on that declared need.
//
// True per-property-access laziness (a bare `state._needs.x` read
// transparently blocking) was considered and rejected: this codebase's
// only SQLite driver (@libsql/client) is fully async, and the only way to
// make an async read look synchronous from inside a plain property getter
// is Atomics.wait blocking the ENTIRE process's event loop — which would
// stall every other concurrently-dispatching flow and every HTTP request
// on this gateway for the duration of the read. `ctx.needs(varName)` as an
// async function call gets the same lazy-only-if-touched property safely,
// using async/await machinery this engine already has (node.run() is
// already awaited today) instead of inventing dangerous new plumbing.
//
// Deliberately its own module, not folded into executor-helpers.js:
// resolveOneNeed needs areSiblingsAllTerminal (child-resume-trigger.js),
// which itself imports isTerminal from executor-helpers.js — importing it
// back the other way would be a cycle.
//
// hops:1 = immediate parent flow, hops:2 = grandparent, etc. Walked via
// the same two tables/columns the design doc's original multi-hop
// description already used — flow_runs.parent_invocation_id ->
// flow_node_invocations.getInvocation().flowId -> repeat — bounded by the
// caller's own declared (small, explicit) hop count, so no new denormalized
// parent_invocation_id column on flow_node_invocations is needed even for
// hops > 1: a bounded loop of existing point lookups is exactly as cheap.
//
// hops:0 = same-flow — the declaring node's own flow, no walk at all
// (walkUpHops runs zero loop iterations and returns flowId unchanged).
// A node can opt into the same explicit, lazy, distinguishes-"not-ready"-
// from-"never" accessor for same-flow reads too, instead of two different
// idioms depending on whether the source is same-flow or cross-flow.
//
// A second declaration shape, `{hops, entry}` (mutually exclusive with
// `nodeId`/`config`): reaches an ancestor's own ENTRY/bookkeeping field —
// runId, args, pipelineId, cost, etc. — set once at that flow's own
// start() (or updated directly by gate-resolver.js), never via any node's
// own resultJson. This is a different read path entirely from
// `nodeId`-based needs: no resultJson replay, no "not ready yet" state (an
// entry field exists the instant its flow does), just a direct read of
// that ancestor flow's own persisted stateJson at the target hop. `entry`
// is validated at load time (engine/loader.js) against ENTRY_FIELDS below
// — `gates` is deliberately excluded even though it's a real stateJson
// key, since it has its own dedicated, staleness-safe accessor
// (gate_records) and reading it through here would reopen exactly the bug
// that mechanism was built to close.
//
// A third shape, `{hops, config}` (mutually exclusive with the other two):
// reaches a pipeline-DECLARED static config field (index.js's own `state:`
// block — e.g. sourceFilter, minClusterSize) at the target hop. Config is
// code, not data: it's never written to stateJson (toPersistableState
// never carries it — see executor-persist.js), never subject to that
// narrowing, and resolved live off the currently-loaded pipeline
// definition via pipelinesRef, same source start() itself reads from.
// Always "ready" the instant the target flow exists, same as `entry` — it
// doesn't depend on any node having run. Unlike `entry`, not validated
// against a fixed key set at load time (pipelines declare arbitrary
// `state:` shapes, no schema) — an unknown key just resolves `undefined`.
import { getSubflowGroup } from "../state/subflow-state.js";
import { areSiblingsAllTerminal } from "./child-resume-trigger.js";
import { NodeBlocked } from "./executor-errors.js";

export const ENTRY_FIELDS = new Set([
  "runId", "startedAt", "args", "triggeredBy", "pipelineId",
  "cost", "_display",
  "approvedIds", "rejectedIds", "editedTexts", "approvalPending", "approvalResults",
  "_cascadeError",
]);

async function walkUpHops(flowId, hops, runStore, nodeExecutionsStore) {
  let currentFlowId = flowId;
  for (let i = 0; i < hops; i++) {
    const flow = await runStore.get(currentFlowId);
    const parentInvocationId = flow?.parentInvocationId ?? null;
    if (!parentInvocationId) return null; // walked off the top — fewer real ancestors than declared
    const inv = await nodeExecutionsStore.getInvocation(parentInvocationId);
    if (!inv?.flowId) return null;
    currentFlowId = inv.flowId;
  }
  return currentFlowId;
}

// Three-state result per declared need:
//   "not-found" — walked off the top, OR the target flow has no
//     invocation/subflow-fan-out for nodeId at all (never dispatched
//     there) — a needs: entry naming the wrong hop count or a nodeId
//     that hasn't run yet both land here; the caller treats both as
//     "not ready, recheck later" the same way it treats an ordinary
//     not-ready result, since a not-yet-dispatched ancestor node is not
//     distinguishable from "give it more time" without knowing that
//     ancestor's own future dispatch order, which this accessor has no
//     way to see.
//   "not-ready" — invocation row (or subflow fan-out) exists but isn't
//     complete yet: resultWrittenAt still null for an ordinary node, or
//     not every sibling is terminal yet for a subflow-sourced one.
//   "ready" — value is the resolved data. A subflow-sourced need's value
//     is the same { [subflowKey]: row } aggregate shape _subflowGroups
//     already exposes elsewhere, not a different mechanism. An over-cap
//     ordinary result that kept no value carries the same
//     { _resultTooLarge, bytes } marker assembleState() already uses.
export async function resolveOneNeed({ flowId, hops, nodeId, runStore, nodeExecutionsStore }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };

  // Subflow-sourced check first: a real subflow node's own dispatch never
  // writes an ordinary resultJson row for itself (dispatchSubflowChildren
  // always returns stateUpdate:{}, per this doc's own corrected "SCORE is
  // itself a subflow" resolution), so an ordinary getResultForNode lookup
  // for a subflow node's id would always read as "not-found" even once its
  // fan-out is real and complete. getSubflowGroup only ever returns real
  // rows for a node that has actually spawned childFlows children, so
  // checking it first (rather than guessing from a pipeline definition
  // this accessor doesn't have on hand) is unambiguous.
  const group = await getSubflowGroup({ flowId: targetFlowId, groupId: nodeId, runStore, nodeExecutionsStore });
  if (group && Object.keys(group).length > 0) {
    const { allTerminal, reason } = await areSiblingsAllTerminal({ flowId: targetFlowId, nodeId, runStore, nodeExecutionsStore });
    if (reason === "no_siblings") return { status: "not-found" };
    if (!allTerminal) return { status: "not-ready" };
    return { status: "ready", value: group };
  }

  const row = await nodeExecutionsStore.getResultForNode(targetFlowId, nodeId);
  if (!row) return { status: "not-found" };
  if (row.resultWrittenAt == null) return { status: "not-ready" };
  if (row.resultTooLargeBytes != null && row.resultJson == null) {
    // writeResult() (state/node-executions.js) does not cap results, so this
    // fires only for a row that kept nothing at all — the real value isn't
    // recoverable.
    return { status: "ready", value: { _resultTooLarge: true, bytes: row.resultTooLargeBytes } };
  }
  try {
    // A node's resultJson is its own raw stateUpdate — by convention almost
    // always `{[nodeId]: value}` (assembleState's own doc comment notes
    // this isn't a GUARANTEED shape, just the near-universal one), so
    // unwrap it here: a needs: author writing needs:{x:{hops,nodeId:"SCAN"}}
    // expects ctx.needs("x") to resolve to what state.SCAN means in SCAN's
    // own flow, not the wrapper object around it. assembleState() itself
    // doesn't unwrap because it's replaying MANY nodes' updates onto one
    // shared object via spread, where the wrapper IS the update; this
    // accessor fetches exactly one node's row, so unwrapping is correct
    // here and would be wrong there.
    const parsed = JSON.parse(row.resultJson ?? "null");
    return { status: "ready", value: parsed && typeof parsed === "object" ? parsed[nodeId] : undefined };
  } catch {
    return { status: "not-found" }; // corrupted row — same treatment assembleState() gives a bad row
  }
}

// Resolves a {hops, entry} need — always "ready" once the target ancestor
// flow itself exists (an entry field is set the instant its flow is
// created; there is no "dispatched but not complete" state the way a
// node's own resultJson has). "not-found" only when the walk itself fails
// (fewer real ancestors than declared, or the target flow vanished).
export async function resolveOneEntry({ flowId, hops, entry, runStore, nodeExecutionsStore }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };
  const flow = await runStore.get(targetFlowId);
  if (!flow) return { status: "not-found" };
  return { status: "ready", value: flow.stateJson?.[entry] };
}

// Resolves a {hops, config} need — reads a pipeline-declared static config
// field off the target flow's own pipeline DEFINITION, not its stateJson.
// "not-found" when the walk fails, the target flow vanished, or its
// pipeline isn't currently loaded in pipelinesRef (mirrors runFlow()'s own
// "pipeline not loaded" handling in executor.js — a transient loader race,
// not a real absence). Otherwise always "ready", same as `entry` — an
// unknown config key simply resolves to `undefined`.
export async function resolveOneConfig({ flowId, hops, config, runStore, nodeExecutionsStore, pipelinesRef }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };
  const flow = await runStore.get(targetFlowId);
  if (!flow) return { status: "not-found" };
  const pipeline = pipelinesRef?.value?.get(flow.stateJson?.pipelineId);
  if (!pipeline) return { status: "not-found" };
  return { status: "ready", value: pipeline.state?.[config] };
}

// Builds the `needs(varName)` async function passed to a declaring node's
// own run()/promptBuilder()/selector() as part of its ctx. Each call is an
// independent, fresh resolveOneNeed — no bulk pre-fetch, no caching across
// calls within the same dispatch (consistent with this engine's "no cache
// layer, direct read" convention every other store accessor already
// follows). A not-ready/not-found result throws NodeBlocked with `_kind:
// "needs-not-ready"` explicitly stamped onto `_blocked` — mirroring the
// established pattern the budget-gate pause already uses
// (executor-nodes.js's dispatchAgentEntry) — a bare NodeBlocked with no
// `_kind` is treated as a hard FAILURE by executor.js's blockedInfo
// handling, not a wait, so this must never be omitted.
export function makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef }) {
  return async function needs(varName) {
    const spec = node?.needs?.[varName];
    if (!spec) {
      throw new Error(`ctx.needs("${varName}"): not declared in this node's .needs — check node.needs`);
    }
    const result = spec.entry !== undefined
      ? await resolveOneEntry({ flowId, hops: spec.hops, entry: spec.entry, runStore, nodeExecutionsStore })
      : spec.config !== undefined
      ? await resolveOneConfig({ flowId, hops: spec.hops, config: spec.config, runStore, nodeExecutionsStore, pipelinesRef })
      : await resolveOneNeed({ flowId, hops: spec.hops, nodeId: spec.nodeId, runStore, nodeExecutionsStore });
    if (result.status !== "ready") {
      const target = spec.entry !== undefined ? `entry:"${spec.entry}"` : spec.config !== undefined ? `config:"${spec.config}"` : `nodeId:"${spec.nodeId}"`;
      const err = new NodeBlocked({
        reason: `waiting on needs.${varName} (hops:${spec.hops}, ${target}, ${result.status})`,
        needed: [spec.entry ?? spec.config ?? spec.nodeId],
        hint: "declared needs: not ready yet — will recheck on next dispatch",
      });
      err._blocked = { ...err._blocked, _kind: "needs-not-ready" };
      throw err;
    }
    // Optional field-pick, so an author can ask for exactly the sub-key(s)
    // it actually reads instead of the whole ancestor node's output — the
    // same "scoped read, not the whole blob" reasoning this file's own
    // Option A/B design is built on, just applied at the single-node
    // granularity ctx.needs() operates at. `fields` is always an array,
    // even for a single key — no separate "unwrap to a bare value" mode,
    // so ctx.needs()'s return shape is always a dict, never sometimes-a-
    // dict-sometimes-a-scalar depending on how many fields were picked.
    // Not declared -> today's default, the whole resolved value, unchanged.
    if (Array.isArray(spec.fields)) {
      return Object.fromEntries(spec.fields.map((f) => [f, result.value?.[f]]));
    }
    return result.value;
  };
}
