// cli.js — both CLI surfaces in one module: runCli(argv) serves MCP/WS
// in a standalone process (bin/manifold.js's thin
// entry calls it), and registerCliCommands(...) registers the
// `openclaw manifold` subcommands on the gateway's commander program.
// Neither surface imports the openclaw SDK, so this module loads under
// plain node for standalone use and node:test.
import { loadPipelines } from "../engine/loader.js";
import { resolveStorePaths } from "../state/paths.js";
import { listPaused, pause as pauseFlow, resume as resumeFlow } from "../gates/paused-flows.js";
import { createGateResolver } from "../gates/gate-resolver.js";
import { spawn } from "node:child_process";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";
import { createFailureStore } from "../state/failure-store.js";
import { createRetryFailure } from "../routes/operator-retry.js";
import { waitForFlowSettlement } from "../engine/wait-for-flow-settlement.js";
import { flipToRunningAndResume } from "../engine/child-resume-trigger.js";
import { wakePromotedRelease, drainDetachedResumes } from "../engine/admission-promotion-resume.js";
import { parseArgs } from "node:util";
import { startMcpServer } from "../mcp/stdio.js";
import { createRealOps } from "../mcp/wire-ops.js";
import { createEventStream } from "../events/stream.js";
import { startWsServer, DEFAULT_WS_PORT } from "../events/serve-ws.js";

// cli.js — registers the `openclaw manifold <subcommand>` CLI surface.
//
// IMPORTANT: CLI handlers run in a separate openclaw process from the gateway,
// so the `pipelinesRef` populated by the gateway's loader-service is NOT
// visible here. Every CLI handler that needs pipeline definitions must
// re-load them from disk on demand via ensurePipelinesLoaded(). Similarly,
// the executor loop runs in-process here (sync await), so CLI commands
// `start`, `rerun`, `retry`, and `approve` all block until the flow reaches
// terminal or blocked.


// Precedence: explicit --pipelines-dir flag wins, then the adapter seam
// (resolvePipelinesDir: table -> env -> config -> default), then the store
// seam (resolveStorePaths().pipelinesDir, honoring MANIFOLD_PIPELINES_DIR).
// Throws when nothing resolves — callers fail loudly naming the flag/env
// instead of silently falling back to a harness layout.
export function resolveCliPipelinesDir({ flagDir = null, resolvePipelinesDir = null, storePaths = null, env = process.env } = {}) {
  if (flagDir) return { dir: flagDir, source: "flag" };
  try {
    const adapter = resolvePipelinesDir?.();
    if (adapter?.dir) return adapter;
  } catch { /* adapter unavailable — fall through to the store seam */ }
  const fromStore = storePaths?.pipelinesDir ?? resolveStorePaths({ env }).pipelinesDir;
  if (fromStore) return { dir: fromStore, source: env?.MANIFOLD_PIPELINES_DIR && !storePaths?.pipelinesDir ? "env" : "store" };
  throw new Error("manifold: no pipelines directory resolved — pass --pipelines-dir <dir> or set MANIFOLD_PIPELINES_DIR");
}

async function ensurePipelinesLoaded(pipelinesRef, logger, dir) {
  if (pipelinesRef.value && pipelinesRef.value.size > 0) return;
  const loaded = await loadPipelines(dir, logger);
  pipelinesRef.value = loaded;
}

// No subcommand exits on its own after a successful run: something opened
// during the plugin boot phase, before any subcommand-specific code even
// runs, holds the process open regardless of which subcommand ran, and it
// isn't a leftover socket or timer that Node's own introspection exposes,
// so waiting it out isn't an option. An explicit exit is the only reliable
// fix.
//
// Only safe to wrap an action once its own async work is FULLY done —
// fire-and-forget follow-on work scheduled as setImmediate/setTimeout (fine
// for a long-lived HTTP/Telegram/dashboard/companion-app caller) is dropped by the
// exit below. Every command must await its own work to completion: retry
// via routes/operator-retry.js's awaitCompletion, approve via
// gates/gate-resolver.js's awaitCompletion (which also covers
// applyResolutionsAndResume's own multi-round setTimeout-based retry chain
// on sustained revision contention, not just its setImmediate resume).
// Drains detached promotion resumes before exiting: any command that
// releases an admission can promote another flow, whose run is scheduled on
// a later tick (engine/admission-promotion-resume.js) — process.exit()
// right after would drop it with the row already flipped to 'running'.
function withExit(action) {
  return async (...args) => {
    await action(...args);
    await drainDetachedResumes();
    process.exit(0);
  };
}

export function registerCliCommands({ program, executor, store, pipelinesRef, logger, dashboardUrl, api, nodeHealthStore, ownerSessionKey, runStore, nodeExecutionsStore, admissionQueueStore, pipelineDirsStore = null, resolvePipelinesDir = null }) {
  // Pipelines dir for every CLI load below, resolved lazily per command
  // (the --pipelines-dir flag is only known after parse): flag wins, then
  // the adapter seam, then the store seam. Refusal to resolve is fatal —
  // there is no harness-layout fallback.
  function resolveCliDir(harness = null) {
    return resolveCliPipelinesDir({
      flagDir: root.opts()?.pipelinesDir ?? null,
      resolvePipelinesDir: resolvePipelinesDir ? () => resolvePipelinesDir(harness) : null,
      env: process.env,
    });
  }
  async function ensureCliLoaded() {
    let resolved;
    try {
      resolved = resolveCliDir();
    } catch (err) {
      console.error(`manifold: ${err.message}`);
      process.exit(1);
    }
    await ensurePipelinesLoaded(pipelinesRef, logger, resolved.dir);
  }
  const root = program.command("manifold");
  root.option("--pipelines-dir <dir>", "Pipeline definitions directory (wins over MANIFOLD_PIPELINES_DIR and the store default).");
  root.description("Run state-graph pipelines from the resolved pipelines directory.");
  const failureStore = createFailureStore({ logger, nodeExecutionsStore });

  // manifold list
  root
    .command("list")
    .description("List loaded pipelines (from the resolved pipelines directory)")
    .action(withExit(async () => {
      await ensureCliLoaded();
      const all = [...pipelinesRef.value.values()];
      if (all.length === 0) {
        console.log("manifold: no pipelines loaded.");
        return;
      }
      for (const p of all) {
        console.log(`  ${p.id}`);
        if (p.goal) console.log(`    goal: ${p.goal}`);
        console.log(`    nodes: ${Object.keys(p.nodes).length}, file: ${p._filepath}`);
      }
    }));

  // manifold pipelines register --harness <h> --path <dir> — upsert a
  // per-harness override (dir must exist). Takes effect on next gateway
  // boot; `list` shows the currently effective dir.
  // manifold pipelines list [--harness <h>] — table rows + effective dir.
  const pipelinesCmd = root
    .command("pipelines")
    .description("Inspect or override per-harness pipeline directories.");
  pipelinesCmd
    .command("register")
    .description("Register a per-harness pipeline directory override (upsert; dir must exist).")
    .requiredOption("--harness <h>", "Harness this override applies to")
    .requiredOption("--path <dir>", "Pipeline directory (must exist)")
    .action(withExit(async (options) => {
      if (!pipelineDirsStore) {
        console.error("manifold: pipeline dir store unavailable in this process.");
        process.exit(1);
      }
      try {
        const reg = pipelineDirsStore.register({ harness: options.harness, dirPath: options.path });
        console.log(`manifold: registered pipelines dir for harness "${reg.harness}": ${reg.path}`);
      } catch (err) {
        console.error(`manifold: ${err?.message ?? err}`);
        process.exit(1);
      }
    }));
  pipelinesCmd
    .command("list")
    .description("List registered per-harness pipeline dirs and the effective dir.")
    .option("--harness <h>", "Harness to resolve the effective dir for")
    .action(withExit(async (options) => {
      if (!pipelineDirsStore) {
        console.error("manifold: pipeline dir store unavailable in this process.");
        process.exit(1);
      }
      const rows = pipelineDirsStore.list();
      if (rows.length === 0) {
        console.log("manifold: no per-harness pipeline dirs registered.");
      } else {
        for (const r of rows) console.log(`  ${r.harness}  ${r.path}`);
      }
      const h = options.harness ?? null;
      let resolved;
      try {
        resolved = resolveCliDir(h);
      } catch (err) {
        console.error(`manifold: ${err.message}`);
        process.exit(1);
      }
      console.log(`manifold: effective dir${h ? ` for harness "${h}"` : ""}: ${resolved.dir} (source: ${resolved.source})`);
    }));

  // manifold start <pipelineId>
  root
    .command("start <pipelineId>")
    .description("Start a new run of a pipeline. Blocks until the flow reaches terminal or human-gate.")
    .option("--args <json>", "JSON-encoded args to merge into initial state", "")
    .action(withExit(async (pipelineId, options) => {
      await ensureCliLoaded();
      const args = options.args ? safeParseJson(options.args) : {};
      if (args === null) {
        console.error(`manifold: --args must be valid JSON`);
        process.exit(1);
      }
      try {
        const triggeredBy = {
          kind: "cli",
          label: `CLI · ${process.env.USER ?? "unknown"}`,
          ts: Date.now(),
        };
        const record = await executor.start(pipelineId, args ?? {}, triggeredBy);
        // Mint a synthetic trigger edge so the lineage tree shows a CLI
        // anchor above this flow — mirrors the HTTP handler's behavior.
        try {
          const synthSession = `agent:operator:cli:${record.flowId}`;
          await api?.runtime?.lineage?.recordTrigger?.({
            flowId: record.flowId,
            runId: record.stateJson?.runId ?? null,
            pipelineId,
            parentSessionKey: synthSession,
            rootKind:  "explicit",
            rootLabel: pipelineId,
          });
        } catch (err) {
          logger?.warn?.(`manifold start (cli): lineage edge skipped: ${err?.message ?? err}`);
        }
        console.log(`manifold: started ${pipelineId} → flowId=${record.flowId}`);
        // The shared admission pool was full at start() time — the flow is
        // durably queued (state/admission-queue-store.js) and will run once
        // a slot frees, but that promotion happens via the long-running
        // GATEWAY process's own resume wiring, not this one-shot CLI
        // process. Nothing left for this invocation to usefully wait on —
        // runFlow() would just no-op immediately (engine/executor.js's own
        // isQueued guard) rather than actually drive it to completion.
        if (record.status === "queued") {
          console.log(`  queued — waiting for a shared admission slot; will run automatically once the gateway has capacity`);
          return;
        }
        console.log(`  driving executor loop in-process; ctrl-c to stop\n`);
        await executor.runFlow(record.flowId, "fresh");
        // The await above only resolves once THIS flow itself goes
        // blocked/terminal — a childFlows:true subflow node fires its
        // children via a bare setImmediate and returns immediately, so
        // without this, withExit's process.exit(0) below would kill those
        // children (and any of their own descendants) before they ever
        // ran. See wait-for-flow-settlement.js's header for the full
        // incident this fixes.
        await waitForFlowSettlement(record.flowId, { store, runStore, nodeExecutionsStore, logger });
        const final = await store.get(record.flowId);
        console.log(`\nmanifold: ${record.flowId} → ${final?.status ?? "?"} (step=${final?.lastStep ?? "?"})`);
      } catch (err) {
        console.error(`manifold: start failed: ${err.message}`);
        process.exit(1);
      }
    }));

  // manifold status <flowId>
  root
    .command("status <flowId>")
    .description("Show current state and pending gates of a flow run")
    .action(withExit(async (flowId) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      const state = flow.stateJson ?? {};
      console.log(`manifold: ${flow.flowId}`);
      console.log(`  pipeline:   ${state.pipelineId ?? "?"}`);
      console.log(`  status:     ${flow.status}`);
      console.log(`  step:       ${flow.lastStep}`);
      console.log(`  revision:   ${flow.revision}`);
      if (flow.waitJson) {
        const kind = flow.waitJson.kind ?? "?";
        const pending = flow.waitJson.pending ?? [];
        console.log(`  waiting:    ${kind} (${pending.length} pending)`);
        if (Array.isArray(pending) && pending.length > 0) {
          for (const id of pending) console.log(`    • ${id}`);
        }
      }
      console.log(`  state keys: ${Object.keys(state).join(", ")}`);
      const flowErrors = await failureStore.queryErrorsByFlow(flow.flowId);
      if (flowErrors.length) {
        console.log(`  errors (${flowErrors.length}):`);
        for (const e of flowErrors.slice(-5)) {
          console.log(`    • ${e.nodeId}: ${e.message}`);
        }
      }
    }));

  // manifold retry <flowId>
  //
  // README.md's CLI table documented this command since before task #267's
  // audit, but nothing ever registered it — the only working path to
  // routes/operator-retry.js's retryFailureBranch (which takes a failure
  // *id*, not a flowId) was the /manifold/_elev/retry-failure HTTP route.
  // Closes that gap: looks up the flow's own most recent OPEN failure_events
  // row (retryFailureBranch's own topology walk resolves the correct node
  // to retry from regardless of which of a flow's open failures is passed,
  // so ties don't matter — see that file's own `rootNode` derivation),
  // then retries it the exact same way the HTTP route does — same code,
  // same "works identically for a root-level flow and a childFlows
  // subflow-item flow" fix this command exists to expose (a subflow item
  // runs as a real flow_runs child with its own flowId, so this needs no
  // special-casing for that case at all).
  //
  // Blocks until every retried flow reaches terminal or blocked — same
  // "drives the executor loop in-process, in this one-shot CLI process"
  // constraint `start` documents at the top of this file, and for the same
  // reason: retryFailureBranch's own default behavior schedules the actual
  // re-run via setImmediate and returns before it fires, which is correct
  // for HTTP/Telegram callers (a long-lived process that keeps going) but
  // fatal for a one-shot CLI process — it would exit before that scheduled
  // work ever ran. Passing awaitCompletion:true switches it to await the
  // retry directly instead.
  root
    .command("retry <flowId>")
    .description("Retry a flow's most recent open failure (root-level or a childFlows subflow item). Blocks until the retried flow(s) reach terminal or blocked.")
    .action(withExit(async (flowId) => {
      await ensureCliLoaded();
      const openFailures = (await failureStore.list({ resolved: false, limit: 1000 }))
        .filter((f) => f.flow_id === flowId);
      if (openFailures.length === 0) {
        console.error(`manifold: no open failure found for flow ${flowId}`);
        process.exit(1);
      }
      const failure = openFailures.reduce((a, b) => (a.failed_at >= b.failed_at ? a : b));
      const { retryFailureBranch } = createRetryFailure({
        store, readStore: runStore, failureStore, pipelinesRef, executor, runStore, ownerSessionKey, admissionQueueStore, logger, api,
      });
      const result = await retryFailureBranch({ id: failure.id, via: "cli", awaitCompletion: true });
      if (result.code !== "ok") {
        console.error(`manifold: retry failed (${result.code}): ${result.reason ?? "unknown"}`);
        process.exit(1);
      }
      // retryFailureBranch's own awaitCompletion:true only awaits the ONE
      // runFlow()/resume() call for the retried flow itself — if that
      // retry re-enters a childFlows:true subflow node, the freshly
      // (re-)spawned children fire via a bare setImmediate and are still
      // in flight here. See wait-for-flow-settlement.js's header.
      if (result.mode === "cascade-redirect") {
        const okResults = result.results.filter((r) => r.code === "ok");
        console.log(`manifold: retry for ${flowId} — redirected to ${result.redirectedTo.length} underlying failure(s), ${okResults.length} retried`);
        for (const r of okResults) {
          await waitForFlowSettlement(r.flowId, { store, runStore, nodeExecutionsStore, logger });
          const final = await store.get(r.flowId);
          console.log(`  • ${r.flowId} → ${final?.status ?? "?"} (failureNode=${r.failureNode}, rootNode=${r.rootNode}, cleared=[${r.cleared.join(",")}])`);
        }
      } else {
        await waitForFlowSettlement(flowId, { store, runStore, nodeExecutionsStore, logger });
        const final = await store.get(flowId);
        console.log(`manifold: retry for ${flowId} → ${final?.status ?? "?"} (failureNode=${result.failureNode}, rootNode=${result.rootNode}, cleared=[${result.cleared.join(",")}])`);
      }
    }));

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
  // fires, which is correct for the long-lived HTTP/Telegram/dashboard/companion-app
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

  // manifold rerun <sourceFlowId> — start a fresh run of the same pipeline
  // with the same args (or overridden via --args). Stamps state.rerunOf so
  // the lineage is queryable. Subject to the singleton-per-pipeline guard.
  root
    .command("rerun <sourceFlowId>")
    .description("Start a fresh run of the source flow's pipeline (new flowId, fresh state)")
    .option("--args <json>", "JSON-encoded args; overrides the source flow's args", "")
    .action(withExit(async (sourceFlowId, options) => {
      await ensureCliLoaded();
      const argsOverride = options.args ? safeParseJson(options.args) : undefined;
      if (options.args && argsOverride === null) {
        console.error(`manifold: --args must be valid JSON`);
        process.exit(1);
      }
      let record;
      try { record = await executor.rerun(sourceFlowId, argsOverride); }
      catch (err) {
        console.error(`manifold: rerun failed: ${err.message}`);
        process.exit(1);
      }
      console.log(`manifold: re-ran ${sourceFlowId.slice(0, 8)} → flowId=${record.flowId}`);
      try { await executor.runFlow(record.flowId, "fresh"); }
      catch { /* ignore — error surface is the next status read */ }
      // See wait-for-flow-settlement.js's header — the runFlow() await
      // above only resolves once THIS flow itself goes blocked/terminal,
      // not any childFlows:true children it spawned along the way.
      await waitForFlowSettlement(record.flowId, { store, runStore, nodeExecutionsStore, logger });
      const final = await store.get(record.flowId);
      console.log(`manifold: ${record.flowId.slice(0, 8)} → ${final?.status ?? "?"} (step=${final?.lastStep ?? "?"})`);
    }));

  // manifold cancel <flowId> — terminal transition for a non-terminal flow.
  // Does not preempt in-flight subagent calls; they finish and their results
  // are discarded (the executor's setRunning fails the revision check).
  root
    .command("cancel <flowId>")
    .description("Cancel a running, blocked, or waiting flow (terminal; cannot be undone)")
    .option("--reason <text>", "Why this flow is being cancelled (recorded on the run)")
    .action(withExit(async (flowId, options) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      if (TERMINAL_STATUS_SET.has(flow.status)) {
        console.error(`manifold: flow is already terminal (status=${flow.status}); nothing to cancel`);
        process.exit(1);
      }
      const result = await executor.cancel(flowId, options.reason ?? null);
      if (!result?.cancelled) {
        console.error(`manifold: cancel did not apply: ${result?.reason ?? "unknown"}`);
        process.exit(1);
      }
      // executor.cancel() releases this flow's admission slot (and wakes a
      // waiting parent) via state-store.js's own triggerAdmissionRelease /
      // triggerParentResume before its promise resolves, so no second
      // explicit release is needed here — by the time `cancel()` above
      // returns, the real one has already landed.
      console.log(`manifold: cancelled ${flowId.slice(0, 8)} (was ${flow.status})`);
    }));

  // manifold resume-admitted <flowId> — recovery for an "orphaned
  // promotion": a flow stuck status='queued' whose flow-start/child-dispatch
  // admission row is already 'admitted' (it holds a live pool slot) because
  // some past release() promoted it and the promotion's resume was dropped
  // (see engine/admission-promotion-resume.js's header). Deliberately does
  // NOT call admissionQueueStore.admit() — the flow already holds a valid
  // slot, and every "resume a queued flow" path that admits unconditionally
  // (admitAndSetRunning, used by retry/approve/gate-resolve) would create a
  // SECOND admission row here, permanently shrinking the shared pool by one
  // more slot exactly like the bug this command recovers from. Refuses to
  // run unless it can confirm the flow genuinely already holds that slot —
  // this is a recovery tool for the specific stuck state, not a way to cut
  // the queue.
  root
    .command("resume-admitted <flowId>")
    .description("Force-resume a flow stuck 'queued' that already holds a live flow-start/child-dispatch admission slot (an orphaned promotion). Refuses if the flow doesn't already hold that slot.")
    .action(withExit(async (flowId) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      if (flow.status !== "queued") {
        console.error(`manifold: flow is not queued (status=${flow.status}); nothing to force-resume`);
        process.exit(1);
      }
      const kind = flow.waitJson?.kind;
      if (kind !== "flow-start" && kind !== "child-dispatch") {
        console.error(`manifold: wait kind "${kind ?? "?"}" isn't resumable this way — only flow-start/child-dispatch hold a durable admission with no in-process waiter of their own`);
        process.exit(1);
      }
      if (!admissionQueueStore) {
        console.error("manifold: no admissionQueueStore wired in");
        process.exit(1);
      }
      const admitted = await admissionQueueStore.listByStatus("admitted");
      const held = admitted.find((r) => r.flowId === flowId && r.kind === kind);
      if (!held) {
        console.error(`manifold: flow does not hold a live '${kind}' admission — it's genuinely still queued, not orphaned. Wait for the normal release/promotion cycle instead of forcing this.`);
        process.exit(1);
      }
      const result = await flipToRunningAndResume({ parentFlowId: flowId, runStore, resume: executor.resume, logger, api });
      if (!result.resumed) {
        console.error(`manifold: force-resume did not apply: ${result.reason ?? "unknown"}`);
        process.exit(1);
      }
      await waitForFlowSettlement(flowId, { store, runStore, nodeExecutionsStore, logger });
      // By now the flow is guaranteed terminal or gate-blocked (see
      // waitForFlowSettlement's own contract) — either way its admission
      // release went out via state-store.js's setWaiting/terminal hooks,
      // fire-and-forget. This CLI invocation is short-lived: withExit's
      // process.exit() right after this action returns gives that
      // background release no guarantee of finishing its DB round-trip
      // first, so release explicitly here before exiting. releaseByFlow is
      // a safe no-op for whichever kind doesn't apply, and for the one the
      // internal call already handled.
      if (admissionQueueStore) {
        const results = await Promise.all([
          admissionQueueStore.releaseByFlow({ flowId, kind: "flow-start" }).catch(() => ({ released: false, promoted: null })),
          admissionQueueStore.releaseByFlow({ flowId, kind: "child-dispatch" }).catch(() => ({ released: false, promoted: null })),
        ]);
        await wakePromotedRelease(results, {
          runStore, resume: executor.resume, logger, api, context: "manifold resume-admitted: cleanup-release",
        });
      }
      const final = await store.get(flowId);
      console.log(`manifold: force-resumed ${flowId.slice(0, 8)} → ${final?.status ?? "?"}`);
    }));

  // manifold release-leaked-admission <flowId> — recovery for the OTHER
  // admission-pool failure mode (see engine/admission-promotion-resume.js's
  // header for the orphaned-promotion one resume-admitted above handles): a
  // flow_admission_queue row is only ever supposed to exist while its owning
  // flow is genuinely non-terminal. If a terminal transition's own release
  // (state-store.js's triggerAdmissionRelease) doesn't fire or doesn't land
  // — a bug in the releasing code path, or a process exiting before that
  // fire-and-forget write commits — the row is a leak: a dead flow
  // permanently holding a live pool slot. boot/flow-revival.js sweeps for
  // exactly this at every boot; this is the same check and release, scoped
  // to one caller-specified flowId instead of a full-table scan, for when
  // restarting the gateway isn't the right lever.
  root
    .command("release-leaked-admission <flowId>")
    .description("Release a terminal flow's leaked flow_admission_queue row (status=admitted/queued for a flow that already finished) and resume whatever gets promoted into the freed slot.")
    .action(withExit(async (flowId) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      if (!TERMINAL_STATUS_SET.has(flow.status)) {
        console.error(`manifold: flow is not terminal (status=${flow.status}) — this command only cleans up a leak on an already-finished flow; use resume-admitted or cancel for a live one`);
        process.exit(1);
      }
      if (!admissionQueueStore) {
        console.error("manifold: no admissionQueueStore wired in");
        process.exit(1);
      }
      let releasedCount = 0;
      for (const kind of ["flow-start", "child-dispatch"]) {
        const result = await admissionQueueStore.releaseByFlow({ flowId, kind });
        if (!result.released) continue;
        releasedCount++;
        console.log(`manifold: released leaked ${kind} admission for ${flowId.slice(0, 8)}`);
        if (result.promoted) {
          const r = await wakePromotedRelease(result, {
            runStore, resume: executor.resume, logger, api, context: `manifold release-leaked-admission [${flowId.slice(0, 8)}]`,
          });
          console.log(`  promoted ${result.promoted.flowId.slice(0, 8)} (${result.promoted.kind}) into the freed slot — resumed=${r.resumed}${r.resumed ? "" : ` reason=${r.reason}`}`);
        }
      }
      if (releasedCount === 0) {
        console.log(`manifold: flow ${flowId.slice(0, 8)} held no live admission — nothing to release`);
      }
    }));

  // manifold runs — list our managed runs
  root
    .command("runs")
    .description("List flow runs created by manifold")
    .option("--status <s>", "Filter by status (running, blocked, succeeded, failed, cancelled)")
    .action(withExit(async (options) => {
      const all = await store.list();
      const filtered = options.status ? all.filter((f) => f.status === options.status) : all;
      if (filtered.length === 0) {
        console.log(`manifold: no runs${options.status ? ` with status=${options.status}` : ""}.`);
        return;
      }
      filtered.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
      for (const f of filtered.slice(0, 30)) {
        const pipelineId = f.stateJson?.pipelineId ?? "?";
        const stamp = f.updatedAt ? new Date(f.updatedAt).toISOString().slice(0, 19).replace("T", " ") : "?";
        console.log(`  ${f.flowId.slice(0, 8)}  ${f.status.padEnd(10)} ${pipelineId.padEnd(35)} step=${f.lastStep ?? "?"}  ${stamp}`);
      }
      if (filtered.length > 30) console.log(`  ... and ${filtered.length - 30} more`);
    }));

  // manifold pause <flowId> — stop the before_dispatch hook from claiming
  // operator messages for this flow. Useful when you want to talk to the
  // main agent without disrupting an active Vulcan priming interview.
  root
    .command("pause <flowId>")
    .description("Stop intercepting operator messages for this priming flow — operator can talk to the main agent. Use `resume` to re-engage.")
    .action(withExit(async (flowId) => {
      const record = await store.get(flowId);
      if (!record) {
        const matches = (await store.list()).filter((f) => f.flowId.startsWith(flowId));
        if (matches.length === 1) flowId = matches[0].flowId;
        else if (matches.length > 1) {
          console.error(`manifold: ambiguous prefix "${flowId}" — ${matches.length} matches`);
          process.exit(1);
        } else {
          console.error(`manifold: no flow found with id "${flowId}"`);
          process.exit(1);
        }
      }
      pauseFlow(flowId);
      console.log(`manifold: paused ${flowId.slice(0, 8)} — operator messages will route to the main agent. Resume with: openclaw manifold resume ${flowId.slice(0, 8)}`);
    }));

  // manifold resume <flowId> — re-engage the before_dispatch claim.
  root
    .command("resume <flowId>")
    .description("Re-engage the priming claim for this flow — operator messages route back to the priming session.")
    .action(withExit(async (flowId) => {
      const matches = (await store.list()).filter((f) => f.flowId.startsWith(flowId));
      if (matches.length === 1) flowId = matches[0].flowId;
      else if (matches.length > 1) {
        console.error(`manifold: ambiguous prefix "${flowId}" — ${matches.length} matches`);
        process.exit(1);
      }
      resumeFlow(flowId);
      console.log(`manifold: resumed ${flowId.slice(0, 8)} — operator messages will route to the priming session again.`);
    }));

  // manifold dashboard — print URL and open the browser to the read-only UI.
  // The dashboard service runs in the gateway process; the CLI just opens a
  // window pointing at it. Doesn't check liveness — the user will see a
  // connection error in their browser if the gateway isn't running.
  root
    .command("dashboard")
    .description("Open the read-only manifold dashboard in your browser.")
    .option("--no-open", "Just print the URL; don't launch a browser.")
    .option("--url <url>", "Override the URL to open (defaults to the configured dashboard host/port).")
    .action(withExit((options) => {
      const url = options.url || dashboardUrl || "http://127.0.0.1:7430";
      console.log(`manifold dashboard: ${url}`);
      if (options.open === false) return;
      const { cmd, args } = browserOpenCommand(url);
      if (!cmd) {
        console.log(`manifold dashboard: no platform-appropriate opener; open the URL manually.`);
        return;
      }
      try {
        const child = spawn(cmd, args, { stdio: "ignore", detached: true });
        child.on("error", (err) => console.log(`manifold dashboard: opener "${cmd}" failed (${err.message}); open the URL manually.`));
        child.unref();
      } catch (err) {
        console.log(`manifold dashboard: failed to launch browser (${err.message}); open the URL manually.`);
      }
    }));

  // manifold paused — list paused flows
  root
    .command("paused")
    .description("List flows currently paused (operator side-channel active).")
    .action(withExit(async () => {
      const paused = [...listPaused()];
      if (paused.length === 0) {
        console.log("manifold: no flows are paused.");
        return;
      }
      for (const flowId of paused) {
        const f = await store.get(flowId);
        const pipelineId = f?.stateJson?.pipelineId ?? "(unknown)";
        console.log(`  ${flowId.slice(0, 8)}  ${pipelineId}  status=${f?.status ?? "(missing)"}`);
      }
    }));
}

function safeParseJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function browserOpenCommand(url) {
  // Per-platform opener. macOS `open`, Linux `xdg-open`, Windows `start` via
  // cmd /c. Returns { cmd, args } or { cmd: null } if the platform is unknown.
  if (process.platform === "darwin") return { cmd: "open", args: [url] };
  if (process.platform === "win32")  return { cmd: "cmd",  args: ["/c", "start", "", url] };
  if (process.platform === "linux")  return { cmd: "xdg-open", args: [url] };
  return { cmd: null, args: [] };
}

const USAGE = `usage: manifold <command>

  serve --mcp stdio|http [--port N] [--host H] [--pipelines-dir DIR]
                                       serve the pipeline tools over MCP stdio or Streamable HTTP
                                       (stdio harness from MANIFOLD_HARNESS env only; agent+session stamped per call;
                                       http additionally needs MANIFOLD_BEARER + X-Manifold-Harness per session)
  serve --ws [--port N] [--host H]       stream push events over WebSocket (verbatim + history replay)`;

function fail(msg, code = 2) {
  console.error(msg);
  process.exit(code);
}

function stderrLogger() {
  return {
    info: (...a) => console.error(...a),
    warn: (...a) => console.error(...a),
    error: (...a) => console.error(...a),
  };
}

export async function runCli(argv) {
  const [cmd, ...rest] = argv;
  switch (cmd) {
    case undefined:
    case "--help":
    case "-h":
    case "help":
      console.log(USAGE);
      return;

    case "serve": {
      let values;
      try {
        ({ values } = parseArgs({
          args: rest,
          options: {
            ws: { type: "boolean" },
            mcp: { type: "string" },
            port: { type: "string" },
            host: { type: "string" },
            "pipelines-dir": { type: "string" },
          },
        }));
      } catch (err) {
        if (/Unknown option|ERR_PARSE_ARGS_UNKNOWN_OPTION/.test(String(err?.message ?? err))) {
          fail("manifold serve --mcp takes no --harness flag: set MANIFOLD_HARNESS in the environment.");
        }
        throw err;
      }
      if (values.mcp !== undefined && values.mcp !== "stdio" && values.mcp !== "http") {
        fail(`manifold: --mcp must be stdio or http, got ${JSON.stringify(values.mcp)}`);
      }
      const modes = ["ws", "mcp"].filter((k) => values[k]);
      if (modes.length !== 1) {
        fail("usage: manifold serve (--ws | --mcp stdio|http) [--port N] [--host H]");
      }
      const logger = stderrLogger();
      const host = values.host ?? "127.0.0.1";
      const port = values.port ? Number(values.port) : undefined;
      if (port !== undefined && !(Number.isInteger(port) && port >= 1 && port <= 65535)) {
        fail(`manifold: --port must be 1-65535, got ${JSON.stringify(values.port)}`);
      }
      const servePipelinesDir = values["pipelines-dir"] ?? null;
      if (values.mcp === "stdio") {
        const ops = await createRealOps({ pipelinesDir: servePipelinesDir, logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        // Harness is env-only here by design: the spawner owns identity.
        await startMcpServer({ ops, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      if (values.mcp === "http") {
        const { startMcpHttpServer } = await import("../mcp/http.js");
        const ops = await createRealOps({ pipelinesDir: servePipelinesDir, logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        await startMcpHttpServer({ ops, port, host, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      const stream = createEventStream();
      await startWsServer({ stream, port: port ?? DEFAULT_WS_PORT, host, logger });
      logger.info(`manifold: events on ws://${host}:${port ?? DEFAULT_WS_PORT} (verbatim; subscribers track their own state)`);
      return;
    }

    default:
      fail(`unknown command: ${cmd}\n\n${USAGE}`);
  }
}
