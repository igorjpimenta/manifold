// cli-runs.js — flow lifecycle subcommands: list, start, status, retry, rerun, cancel, runs, pause, resume.
// Deps keys used: executor, store, pipelinesRef, logger, api, ownerSessionKey, runStore, nodeExecutionsStore, admissionQueueStore, resolvePipelinesDir.
import { createFailureStore } from "../state/failure-store.js";
import { createRetryFailure } from "../routes/operator-retry.js";
import { waitForFlowSettlement } from "../engine/wait-for-flow-settlement.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";
import { pause as pauseFlow, resume as resumeFlow } from "../gates/paused-flows.js";
import { PIPELINES_DIR, ensurePipelinesLoaded, withExit, safeParseJson } from "./cli.js";

export function registerRunCommands(root, deps) {
  const { executor, store, pipelinesRef, logger, api, ownerSessionKey, runStore, nodeExecutionsStore, admissionQueueStore, resolvePipelinesDir } = deps;
  // Adapter-resolved pipelines dir for every CLI load below (table →
  // env → config → default); standalone one-shot use keeps PIPELINES_DIR.
  const cliPipelinesDir = (() => {
    try { return resolvePipelinesDir?.()?.dir ?? PIPELINES_DIR; }
    catch { return PIPELINES_DIR; }
  })();
  async function ensureCliLoaded() {
    await ensurePipelinesLoaded(pipelinesRef, logger, cliPipelinesDir);
  }
  const failureStore = createFailureStore({ logger, nodeExecutionsStore });

  // manifold list
  root
    .command("list")
    .description("List loaded pipelines (from workspace/.pipelines/)")
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
}
