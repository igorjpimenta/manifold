// wait-for-flow-settlement.js — recursively waits for a flow AND every
// descendant it spawns via any childFlows:true subflow node, at any depth,
// to reach a stopping point (genuinely terminal, or a real human-gate
// wait). Exists specifically for the one-shot CLI (bin/cli.js) — the
// live gateway process never needs this: its own event loop keeps
// draining pending setImmediate/timer callbacks for as long as it runs,
// so children spawned by dispatchSubflowChildren settle naturally over
// time. A one-shot CLI invocation has no such guarantee: `withExit`'s
// `process.exit(0)` kills any pending setImmediate outright.
//
// `bin/cli.js`'s start/rerun/retry/approve commands each `await` exactly
// one `executor.runFlow()`/`resume()` call for the ONE flow they were
// given, documented as "blocks until the flow reaches terminal or
// human-gate" — but that await resolves the moment THAT flow itself goes
// blocked/terminal, without ever waiting on any children it spawned along
// the way (dispatchSubflowChildren fires each child via a bare
// `setImmediate(() => runFlow(childFlowId, "fresh"))` and returns
// immediately).
//
// Deliberately NOT a fix inside dispatchSubflowChildren/executor.js
// itself — that fire-and-forget dispatch is correct and load-bearing for
// the live gateway (a subflow node's own step must return promptly so the
// executor isn't stuck waiting on one flow's children before it can
// process anything else). This module only changes what the CLI does
// with its own already-live process before letting it exit.

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

// waitJson.kind values that will NEVER resolve on their own no matter how
// long this process keeps polling — each needs a genuinely different
// resumption path than "wait":
//   - "human-gate": resumed later via the gate-resolve API, against
//     whatever process an operator/dashboard/Telegram calls it from —
//     not by this one-shot CLI process staying alive.
//   - "needs-gateway-handoff": thrown by executor-errors.js's
//     GatewayHandoffNeeded when a kind:"agent" node dispatches outside a
//     live gateway request scope — which the one-shot CLI process always
//     is. That file's own docstring confirms there is no automatic
//     resume for this wait kind anywhere in the codebase; it needs either
//     the scope gap fixed at the dispatch's own call site or manual
//     task-revival. Without excluding it here, any childFlows:true
//     descendant containing a kind:"agent" node would make this module's
//     unbounded poll loop hang forever instead of surfacing the (already
//     already-set, already-expected) blocked status promptly.
// Anything else non-terminal (running, queued on the shared admission pool,
// blocked on its own children-pending) needs this process to keep running
// blocked on its own children-pending) needs this process to keep running
// for it to ever resolve, since nothing else will wake it.
const NEVER_AUTO_RESOLVES = new Set(["human-gate", "needs-gateway-handoff"]);

function isRestPoint(flow) {
  if (!flow) return true; // gone/not found — nothing left to wait on
  if (TERMINAL_STATUSES.has(flow.status)) return true;
  return flow.status === "blocked" && NEVER_AUTO_RESOLVES.has(flow.waitJson?.kind);
}

async function collectDirectChildren(flowId, { runStore, nodeExecutionsStore }) {
  const subflowNodeIds = await nodeExecutionsStore.listSubflowNodeIdsForFlow(flowId);
  if (subflowNodeIds.length === 0) return [];
  const byNode = await nodeExecutionsStore.listInvocationIdsForNodes(flowId, subflowNodeIds);
  const invocationIds = [...byNode.values()].flat();
  if (invocationIds.length === 0) return [];
  return runStore.listForParentInvocations(invocationIds);
}

// One pass over the whole current tree (re-discovered fresh every call,
// since a resumed flow can route to nodes/children that didn't exist on
// the previous pass). Returns the list of flowIds still needing this
// process to keep running — empty means safe to exit.
async function findPending(rootFlowId, ctx) {
  const visited = new Set();
  const pending = [];
  const queue = [rootFlowId];
  while (queue.length > 0) {
    const flowId = queue.shift();
    if (visited.has(flowId)) continue;
    visited.add(flowId);
    const flow = await ctx.store.get(flowId);
    if (!isRestPoint(flow)) pending.push(flowId);
    const children = await collectDirectChildren(flowId, ctx);
    for (const child of children) queue.push(child.flowId);
  }
  return pending;
}

/**
 * @param {string} rootFlowId
 * @param {object} ctx — { store, runStore, nodeExecutionsStore, logger }
 * @param {object} [opts]
 * @param {number} [opts.pollMs] — default 300
 * @param {number} [opts.logEveryMs] — progress log cadence while waiting, default 5000
 * @returns {Promise<{settled: true}>} — resolves once every flow in the
 *   tree is terminal or genuinely gate-blocked. No timeout: unbounded,
 *   same philosophy the CLI's single-flow await already has
 *   ("ctrl-c to stop" is the documented escape hatch, not a silent give-up).
 */
export async function waitForFlowSettlement(rootFlowId, ctx, { pollMs = 300, logEveryMs = 5000 } = {}) {
  let lastLogAt = 0;
  while (true) {
    const pending = await findPending(rootFlowId, ctx);
    if (pending.length === 0) return { settled: true };
    const now = Date.now();
    if (now - lastLogAt >= logEveryMs) {
      lastLogAt = now;
      ctx.logger?.info?.(`manifold: waiting on ${pending.length} still-active flow(s) beneath ${rootFlowId.slice(0, 8)}: ${pending.map((id) => id.slice(0, 8)).join(", ")}`);
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}
