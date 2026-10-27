import { checkDb } from "../../../state/db-health.js";
import { writeCorruptionAlert } from "../../../state/db-guard.js";
import { createStateStore } from "../../../state/state-store.js";
import { flushAll as flushBatchWriter } from "../../../state/batch-writer.js";
import { flowStateEmitter, gateEmitter } from "../../../events.js";
import { createWsRunEventsListener } from "../../../events/ws-run-events.js";
import { scanAndCleanup as taskflowScanAndCleanup } from "../../../boot/taskflow-revive.js";
import { reviveOrphanFlows, reconcileStuckAdmissions } from "../../../boot/flow-revival.js";
import { registerPushWebhook } from "./webhook.js";
import { autoregisterPushTarget } from "./autoregister.js";
import { TASKFLOW_RUNS_DB } from "./config.js";

// reviveOrphanFlows only runs once, at service startup — a stuck-promotion
// leak (see reconcileStuckAdmissions's own header in boot/flow-revival.js)
// occurring hours into a long gateway uptime would otherwise go undetected
// until the next restart. 5 minutes matches this codebase's other
// steady-state probe cadences (e.g. model-circuit-breaker's own recovery
// probe).
const STUCK_ADMISSION_RECONCILE_INTERVAL_MS = 5 * 60_000;

// Expired claim leases (engine/pull-claims.js) requeue on their own only
// when something looks: same 5-minute cadence family as the stuck-
// admission reconcile above, separate const so the two ticks stay
// independently tunable.
const EXPIRED_CLAIM_SWEEP_INTERVAL_MS = 5 * 60_000;

export function registerServices(api, { cfg, stores, runtime, pipelinesRef, resumeParentRef }) {
  const { userCfg, taskflowStartupDelayMs } = cfg;
  const {
    ensurePipelinesLoaded, pipelineRegistryStore, admissionQueueStore,
    runStore, store, sessionIndex, nodeExecutionsStore, pushTargetsPromise, storePaths,
    runtimeDbPath, readStore, failureStore, gatewayBroadcastRef, emitCanonical,
  } = stores;
  const { pipelinesWatcher, executor, gateResolver } = runtime;
  // ── Services ────────────────────────────────────────────────────────────
  //
  // openclaw starts these one by one as part of the gateway's own startup
  // and only becomes ready (agent.wait served, gateway_start fired) after
  // the last start() returns. A start() must therefore never await gateway
  // readiness or an agent dispatch — work that dispatches is started
  // without awaiting it, and its dispatches wait at the executor's gate.

  api.registerService({
    id: "manifold-loader",
    start: async (ctx) => {
      const logger = ctx?.logger ?? api.logger;
      logger.info(`manifold: loading pipelines from ${pipelinesDir}`);
      const pipelines = await ensurePipelinesLoaded();
      const ids = [...pipelines.keys()];
      logger.info(`manifold: ${pipelines.size} pipeline(s) loaded${ids.length ? `: ${ids.join(", ")}` : ""}`);
      // First reconcile has to happen here, not from the watcher below —
      // the watcher's own first tick deliberately primes its fingerprint
      // without reloading (this boot-time load already happened).
      await pipelineRegistryStore.reconcilePipelineVersions(pipelinesRef.value, { logger });
    },
  });
  api.registerService({
    id: "manifold-pipelines-watcher",
    start: () => pipelinesWatcher.start(),
    stop:  () => pipelinesWatcher.stop(),
  });
  // batch-writer (state/batch-writer.js) defers and coalesces fire-and-
  // forget DB writes (currently: node-executions.js's write()) behind an
  // in-memory queue flushed on its interval timer or a size threshold —
  // nothing to do at boot (it's lazy, created on first enqueue), but the
  // plugin loader calls every registered service's start() unconditionally,
  // so a service with stop-only work still needs a no-op start (same
  // pattern as manifold-read-store above) or the loader's call throws, the
  // service never reaches its running list, and stop() never fires —
  // meaning the queue's not-yet-flushed buffered writes are silently lost
  // on every shutdown.
  api.registerService({
    id: "manifold-batch-writer",
    start: async () => {},
    stop: () => flushBatchWriter(),
  });
  api.registerService({
    id: "manifold-flow-revival",
    start: async (ctx) => {
      const logger = ctx?.logger ?? api.logger;
      logger.info("manifold revival: start");
      // Not awaited: revival dispatches agents once the gateway is ready,
      // and the gateway isn't ready until this start() returns.
      runBootRevival(logger);

      // Steady-state counterpart to the boot-time revival — same
      // reconciliation, run on a timer for the rest of this process's
      // uptime instead of once. Resuming a flow can dispatch subagent
      // calls, which wait for gateway readiness in the executor's gate;
      // safe to call repeatedly since it only acts on rows already
      // 'admitted' and non-terminal, which the previous run either
      // already fixed or genuinely couldn't.
      setInterval(() => {
        reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, api, logger })
          .catch((err) => logger.warn(`manifold revival: periodic stuck-admission reconcile failed: ${err?.message ?? err}`));
      }, STUCK_ADMISSION_RECONCILE_INTERVAL_MS);
      setInterval(() => {
        nodeExecutionsStore.sweepExpiredClaims()
          .then((swept) => {
            if (swept?.requeued > 0) logger.info(`manifold: expired-claim sweep requeued ${swept.requeued} ticket(s)`);
          })
          .catch((err) => logger.warn(`manifold: periodic expired-claim sweep failed: ${err?.message ?? err}`));
      }, EXPIRED_CLAIM_SWEEP_INTERVAL_MS);
    },
  });
  async function runBootRevival(logger) {
    try {
      // Guarantee pipelines are in memory before reviving orphan flows.
      // Without this, a race between manifold-loader and manifold-flow-revival
      // causes pipelinesRef.value to be empty, the orphan filter passes every flow
      // (undefined pipeline → undefined nodeKind → not "single"), and executor.runFlow
      // immediately fails them with "pipeline not loaded: <id>" via persistFailRetrying.
      await ensurePipelinesLoaded();
      // Read orphan candidates from the SQL store, not store.list() — the
      // in-process Map is empty at boot (runtime only tracks flows created
      // or mutated in the current process lifetime). The SQL store sees all
      // disk-resident rows including those left running by a prior crash.
      // includeChildren:true — a crashed childFlows child must be revived
      // exactly like a crashed top-level flow; list()'s default excludes
      // children (run-store.list()'s default is top-level only).
      const all = await runStore.list({ includeChildren: true });
      await reviveOrphanFlows({
        all,
        pipelines: pipelinesRef.value,
        store,
        logger,
        executor,
        api,
        // Completing an interrupted cancellation needs to cancel flows by
        // their actual ownerKey, not the default manifold-owner key.
        // Legacy graph-flow-owner flows would fail the
        // cancelFlowByIdForOwner equality check otherwise.
        createCancelStore: (ownerKey) => createStateStore(api, ownerKey, {
          runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger: api.logger,
        }),
        runStore,
        nodeExecutionsStore,
        sessionIndex,
        admissionQueueStore,
        // gateResolver is declared further below in this same closure —
        // safe to reference here since this callback only runs once
        // service startup actually reaches this point, well after the
        // synchronous setup code (including gateResolver's own
        // creation) has completed.
        maybeResolveFlow: gateResolver.maybeResolveFlow,
      });
    } catch (err) {
      logger.warn(`manifold revival: failed to enumerate orphans: ${err?.message ?? err}`);
    }
  }
  api.registerService({
    id: "manifold-read-store",
    start: async () => {},
    stop: async () => { sessionIndex?.close?.(); },
  });
  // ── Boot-time task orphan cleanup (absorbed from taskflow-revive) ─────────

  api.registerService({
    id: "manifold-taskflow-revive",
    start: async (ctx) => {
      const logger = ctx?.logger ?? api.logger;
      logger.info(`taskflow-revive: service started (startupDelay=${taskflowStartupDelayMs}ms)`);
      // Not awaited, same as manifold-flow-revival: its dispatches go
      // through the executor's subagent and wait for gateway readiness,
      // which can't come until this start() returns.
      taskflowScanAndCleanup(api, TASKFLOW_RUNS_DB, taskflowStartupDelayMs, logger, {
        // Lets a manifold-owned orphan be delegated to the flow's own
        // revival function (engine/executor.js's reviveAgentKindOrphan,
        // via reviveViaFlowRevival) instead of only being skipped —
        // see boot/taskflow-revive.js's own header for why.
        sessionIndex, runStore, executor,
      }).catch((err) => {
        logger.warn(`taskflow-revive[boot]: scan failed: ${err?.message ?? String(err)}`);
      });
    },
  });
  // ── Run-state WS event broadcaster ──────────────────────────────────────
  // Subscribes to flowStateEmitter to broadcast targeted WS events when
  // a flow starts, enters a gate, or ends. Replaces broad SSE-driven
  // refresh once subscribed clients consume these events.

  let _wsRunEventsListener = null;
  let _gateOpenedListener = null;
  let _gateClosedListener = null;
  api.registerService({
    id: "manifold-ws-run-events",
    start: async () => {
      // Listener body lives in events/ws-run-events.js (unit-covered);
      // this only binds the live stores and subscribes.
      _wsRunEventsListener = createWsRunEventsListener({
        store,
        readStore,
        nodeExecutionsStore,
        pipelineLookup: (id) => pipelinesRef.value.get(id),
        getBroadcast: () => gatewayBroadcastRef.current,
        emit: emitCanonical,
      });
      flowStateEmitter.on("change", _wsRunEventsListener);

      // Gate lifecycle goes out with its full payload (items, waitJson,
      // stateJson) so a consumer can render the gate without a re-fetch;
      // gate.closed tells it to drop the surface. Delivery is the
      // consumer's business — manifold only states what happened.
      _gateOpenedListener = (payload) => {
        try { gatewayBroadcastRef.current?.("manifold.gate.opened", payload); } catch {}
      };
      _gateClosedListener = (payload) => {
        try { gatewayBroadcastRef.current?.("manifold.gate.closed", payload); } catch {}
      };
      gateEmitter.on("gate.opened", _gateOpenedListener);
      gateEmitter.on("gate.closed", _gateClosedListener);
    },
    stop: async () => {
      if (_wsRunEventsListener) {
        flowStateEmitter.off("change", _wsRunEventsListener);
        _wsRunEventsListener = null;
      }
      if (_gateOpenedListener) {
        gateEmitter.off("gate.opened", _gateOpenedListener);
        _gateOpenedListener = null;
      }
      if (_gateClosedListener) {
        gateEmitter.off("gate.closed", _gateClosedListener);
        _gateClosedListener = null;
      }
    },
  });

  // ── Runtime-capture service ──────────────────────────────────────────────

  // Push wake route + boot-time self-registration (best-effort).
  registerPushWebhook({ api, logger: api.logger });
  api.registerService({
    id: "manifold-push-autoregister",
    start: async () => {
      try {
        const ps = await pushTargetsPromise;
        await autoregisterPushTarget({ pushTargetsStore: ps, pluginConfig: userCfg, apiConfig: api.config, logger: api.logger });
      } catch (err) {
        api.logger.warn(`manifold: push autoregister failed: ${err?.message ?? err}`);
      }
    },
  });
  // ── Boot gate: refuse to finish loading on unreadable primaries ──────────
  // A blind manifold mints wrong gate decisions and drops pushes. Fail
  // here, loudly, instead: the alert names the file, launchd respawns
  // into this same refusal until it heals. Synchronous probe — register
  // must stay synchronous, and better-sqlite3 is already a dependency.
  for (const dbPath of [
    runtimeDbPath,
    storePaths.opsDb,
  ]) {
    const check = checkDb(dbPath);
    if (!check.ok) {
      api.logger.error(`manifold: refusing boot — unreadable database ${dbPath}: ${check.error}`);
      writeCorruptionAlert({ dbPath, phase: "boot", error: check.error });
      process.exit(1);
    }
  }
}
