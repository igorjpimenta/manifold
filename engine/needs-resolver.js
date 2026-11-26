// needs-resolver.js — per-node "needs:": a node declares
// `needs: { varName: {hops, nodeId|entry|config} }`, keyed by the local
// variable name ctx.needs(varName) resolves it as. Resolution is lazy —
// only when ctx.needs() is awaited inside that node's own
// run()/promptBuilder()/selector(); a branch that never calls it never
// blocks on it.
//
// True per-property-access laziness (`state._needs.x` transparently
// blocking) was rejected: the only SQLite driver is fully async, and
// making an async read look synchronous in a getter requires Atomics.wait,
// which blocks the ENTIRE event loop — stalling every other in-flight flow
// and every HTTP request for the duration of the read.
//
// Its own module (not executor-helpers.js) to avoid an import cycle:
// resolveOneNeed → child-resume-trigger → executor-helpers.
//
// hops:1 = immediate parent, hops:2 = grandparent, … walked via
// flow_runs.parent_invocation_id → invocation.flowId → repeat, bounded by
// the declared hop count — point lookups only, no denormalized parent
// column needed. hops:0 skips the walk (same-flow), giving one idiom for
// same- and cross-flow reads.
//
// `{hops, entry}` reaches an ancestor's own bookkeeping field (runId,
// args, …) — set at that flow's start(), never via any node's resultJson,
// so it's always ready the instant the flow exists (no "not ready yet"
// state). `{hops, config}` reaches a pipeline-declared static field
// (index.js `state:` block): config is code, never written to stateJson,
// resolved live off the loaded pipeline definition — also always ready,
// and unvalidated at load time because pipelines declare arbitrary
// `state:` shapes.
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

// Three-state result: "not-found" (walk failed, or nodeId never dispatched
// there — a wrong hop count and a not-yet-run ancestor are indistinguishable
// here, so both are treated as "recheck later"), "not-ready" (invocation or
// fan-out exists but incomplete), "ready" (value — subflow-sourced values
// use the same { [subflowKey]: row } aggregate shape _subflowGroups exposes).
export async function resolveOneNeed({ flowId, hops, nodeId, runStore, nodeExecutionsStore }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };

  // Subflow nodes never write an ordinary resultJson row (their dispatch
  // returns stateUpdate:{}), so an ordinary lookup for their id would read
  // "not-found" forever — check the subflow group first; it only returns
  // real rows once children actually exist.
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
  try {
    // resultJson is the node's raw stateUpdate, conventionally
    // {[nodeId]: value} — unwrap so ctx.needs() returns what state[nodeId]
    // means in that node's own flow, not the wrapper. assembleState
    // doesn't unwrap because it spreads MANY nodes' updates onto one object
    // where the wrapper IS the update; this reads exactly one row.
    const parsed = JSON.parse(row.resultJson ?? "null");
    return { status: "ready", value: parsed && typeof parsed === "object" ? parsed[nodeId] : undefined };
  } catch {
    return { status: "not-found" }; // corrupted row
  }
}

// An entry field exists the instant its flow does, so this is "ready"
// whenever the walk lands; "not-found" only when the walk itself fails.
export async function resolveOneEntry({ flowId, hops, entry, runStore, nodeExecutionsStore }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };
  const flow = await runStore.get(targetFlowId);
  if (!flow) return { status: "not-found" };
  return { status: "ready", value: flow.stateJson?.[entry] };
}

// Reads the config field off the target flow's pipeline DEFINITION (not
// stateJson). "not-found" when the walk fails, the flow vanished, or its
// pipeline isn't loaded yet — a transient loader race, not a real absence.
export async function resolveOneConfig({ flowId, hops, config, runStore, nodeExecutionsStore, pipelinesRef }) {
  const targetFlowId = await walkUpHops(flowId, hops, runStore, nodeExecutionsStore);
  if (!targetFlowId) return { status: "not-found" };
  const flow = await runStore.get(targetFlowId);
  if (!flow) return { status: "not-found" };
  const pipeline = pipelinesRef?.value?.get(flow.stateJson?.pipelineId);
  if (!pipeline) return { status: "not-found" };
  return { status: "ready", value: pipeline.state?.[config] };
}

// Builds ctx.needs(varName): each call is a fresh resolve, no bulk fetch
// and no caching across calls in one dispatch. Not-ready/not-found throws
// NodeBlocked with `_kind: "needs-not-ready"` stamped on `_blocked` — a
// bare NodeBlocked with no _kind is treated as a hard FAILURE (not a wait)
// by executor.js's blockedInfo handling, so the stamp must never be
// omitted.
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
    // Optional field-pick: always an array (even for one key), no
    // bare-value mode — the return shape stays a dict regardless of how
    // many fields were picked. Without `fields`: the whole resolved value.
    if (Array.isArray(spec.fields)) {
      return Object.fromEntries(spec.fields.map((f) => [f, result.value?.[f]]));
    }
    return result.value;
  };
}
