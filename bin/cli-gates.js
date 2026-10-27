// cli-gates.js — approve and node-health status/reset subcommands.
// Deps keys used: store, executor, pipelinesRef, logger, admissionQueueStore, runStore, nodeExecutionsStore, nodeHealthStore, resolvePipelinesDir.
import { createGateResolver } from "../gates/gate-resolver.js";
import { waitForFlowSettlement } from "../engine/wait-for-flow-settlement.js";
import { PIPELINES_DIR, ensurePipelinesLoaded, withExit } from "./cli.js";

export function registerGateCommands(root, deps) {
  const { store, executor, pipelinesRef, logger, admissionQueueStore, runStore, nodeExecutionsStore, nodeHealthStore, resolvePipelinesDir } = deps;
  // Adapter-resolved pipelines dir for every CLI load below (table →
  // env → config → default); standalone one-shot use keeps PIPELINES_DIR.
  const cliPipelinesDir = (() => {
    try { return resolvePipelinesDir?.()?.dir ?? PIPELINES_DIR; }
    catch { return PIPELINES_DIR; }
  })();
  async function ensureCliLoaded() {
    await ensurePipelinesLoaded(pipelinesRef, logger, cliPipelinesDir);
  }

  // manifold node-health status [pipelineId]
  // manifold node-health reset <pipelineId> <nodeId>
  //
  // Both live under one `node-health` parent command — Commander's
  // `.command(nameAndArgs)` only reads the first whitespace-delimited
  // token as the command name, so two top-level `.command("node-health
  // ...")` calls would collide ("cannot add command 'node-health' as
  // already have command 'node-health'"), crashing CLI registration for
  // every `manifold` subcommand, not just these two.
  const nodeHealthCmd = root
    .command("node-health")
    .description("Inspect or reset a node's agent-dispatch circuit breaker.");

  // Lists nodes currently blocked by the circuit breaker — any "agent"-kind
  // node that opts in via node.circuitBreaker: true is eligible (see
  // executor-nodes.js's dispatchAgentEntry), including cascade sub-nodes
  // dispatched as part of a kind:"subflow" node's own child flow. --all
  // includes resolved history instead of only active blocks.
  nodeHealthCmd
    .command("status [pipelineId]")
    .description("List nodes currently blocked (unhealthy) by the circuit breaker.")
    .option("--all", "Include resolved history, not just active blocks", false)
    .action(withExit(async (pipelineId, options) => {
      const rows = await nodeHealthStore.list(pipelineId, options.all ? {} : { status: "blocked" });
      if (rows.length === 0) {
        console.log(pipelineId ? `manifold: no node-health blocks for ${pipelineId}` : "manifold: no node-health blocks");
        return;
      }
      for (const r of rows) {
        const when = new Date(r.created_at).toISOString();
        console.log(`${r.status.padEnd(9)} ${r.pipeline_id}/${r.node_id}  (${when})`);
        console.log(`  reason: ${r.reason}`);
        if (r.blocked_by_flow_id) console.log(`  flow:   ${r.blocked_by_flow_id.slice(0, 8)}`);
        if (r.status === "resolved") console.log(`  resolved via: ${r.resolved_via ?? "?"}`);
      }
    }));

  // Flags the active block resolved (row is kept for history, not deleted
  // — see state/node-health-store.js). Does not itself retry any flow;
  // the node simply becomes eligible for dispatch again on its next visit.
  nodeHealthCmd
    .command("reset <pipelineId> <nodeId>")
    .description("Clear an unhealthy block so the node can be tried again.")
    .action(withExit(async (pipelineId, nodeId) => {
      const result = await nodeHealthStore.reset(pipelineId, nodeId, { via: "cli" });
      if (result.reset === 0) {
        console.log(`manifold: ${pipelineId}/${nodeId} was not blocked; nothing to reset`);
        return;
      }
      console.log(`manifold: ${pipelineId}/${nodeId} reset — eligible for dispatch again`);
    }));

  // manifold approve <flowId> <itemId> <decision> [text]
  //
  // Blocks until the resumed flow reaches terminal or blocked again — same
  // reasoning as `retry` above: gates/gate-resolver.js's
  // applyResolutionsAndResume schedules the actual setRunning + executor.
  // resume via setImmediate (and, on sustained revision contention, a
  // further live retry round via setTimeout) and returns before either
  // fires, which is correct for the long-lived HTTP/Telegram/dashboard/iOS
  // callers that share this resolver but fatal for this one-shot CLI
  // process. Passing awaitCompletion:true switches it to await all of that
  // directly instead.
  root
    .command("approve <flowId> <itemId> <decision>")
    .description("Approve, reject, or edit a pending item on a gate node. Blocks until the resumed flow reaches terminal or blocked again.")
    .option("--text <text>", "Edited text (only with decision=edit)")
    .action(withExit(async (flowId, itemId, decision, options) => {
      decision = String(decision).toLowerCase();
      if (!["approve", "reject", "edit"].includes(decision)) {
        console.error(`manifold: decision must be approve|reject|edit`);
        process.exit(1);
      }
      if (decision === "edit" && !options.text) {
        console.error(`manifold: edit requires --text`);
        process.exit(1);
      }

      // Same channel-agnostic resolver the TG callback handler, the
      // dashboard, and the /manifold/gate/resolve route use. Spun up
      // here against this CLI process's own store + executor — the
      // gateway has its own resolver instance, but writes still serialize
      // through the underlying SQL store so cross-process resolution is
      // safe. recordItemDecision drives setRunning + executor.resume
      // internally once all items decide.
      const gateResolver = createGateResolver({
        api: { logger },
        store,
        executor,
        pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
        admissionQueueStore,
      });
      // executor.resume (fired from inside applyResolutionsAndResume) needs
      // the pipeline definition loaded in this CLI process's pipelinesRef.
      await ensureCliLoaded();

      const result = await gateResolver.recordItemDecision({
        flowId,
        itemId,
        decision,
        source: "cli",
        editedText: options.text,
        awaitCompletion: true,
      });
      if (!result.ok) {
        console.error(`manifold: ${result.code}`);
        process.exit(1);
      }
      // recordItemDecision's own awaitCompletion:true only awaits the ONE
      // executor.resume() call for THIS flow — if resuming re-enters a
      // childFlows:true subflow node, freshly spawned children fire via a
      // bare setImmediate and are still in flight here. See
      // wait-for-flow-settlement.js's header.
      await waitForFlowSettlement(flowId, { store, runStore, nodeExecutionsStore, logger });
      const final = await store.get(flowId);
      console.log(
        `manifold: ${decision} "${itemId}" (${result.status})` +
        (final ? ` — flow → ${final.status ?? "?"} (step=${final.lastStep ?? "?"})` : "")
      );
    }));
}
