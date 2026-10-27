// engine/pull-claims.js — pull-claim report path for agent nodes.
//
// Park runs inside the dispatch loop (engine/executor-nodes.js's pull
// branch); this module is the other half: verifying a report against the
// ticket's claimant binding, storing the result, and resuming the parked
// flow through the gate resume path (routes/operator-resume.js's
// resumeFlowFromGate — the same admitAndSetRunning + executor.resume every
// gate decision uses, not a fork of it).
import { resumeFlowFromGate } from "../routes/operator-resume.js";
import { clearTicket } from "../state/tool-handoff.js";

export async function reportPullClaim(
  { nodeExecutionsStore, store, admissionQueueStore, runStore, executor, logger, api },
  { ticket, harness, agent, session, result },
) {
  if (!nodeExecutionsStore?.reportClaim) {
    throw new Error("pull-claim: reportPullClaim requires a claim-aware nodeExecutionsStore (reportClaim missing)");
  }
  // Binding check + result store + reported mark commit inside the store;
  // resume only fires after all three succeed, never on a refusal.
  const recorded = await nodeExecutionsStore.reportClaim({ ticket, harness, agent, session, result });
  clearTicket(ticket);
  const resumed = await resumeFlowFromGate({
    store, admissionQueueStore, runStore, executor, logger, api,
    flowId: recorded.flowId, nodeId: recorded.nodeId, decision: "reported",
  });
  if (!resumed?.ok) {
    throw new Error(`pull-claim: ticket ${ticket} reported but resume failed (${resumed?.code ?? "unknown"})`);
  }
  return { ok: true, ticket, flowId: recorded.flowId, nodeId: recorded.nodeId, queued: resumed.queued ?? false };
}
