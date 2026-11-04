// manifold — LangGraph-style pipeline executor for openclaw.
//
// Loads pipeline definitions from `workspace/.pipelines/<team>_<name>.js`,
// drives them against managed `flow_runs`, and dispatches each node:
//   - `code`        : pure function (state) => stateUpdate
//   - `single`      : one runtime.subagent.run, output parsed by `apply`
//   - `subflow`     : bounded-parallel fan-out over `.selector(state)`; slot-per-item writes
//   - `human-gate`  : flow.status='blocked' + wait_json; resumed by CLI approve
//   - `parallel`    : fixed set of named `.tasks`, all dispatched concurrently

import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createPipelinesLoader } from "../../../engine/loader.js";
import { createStateStore } from "../../../state/state-store.js";
import { createRunStore } from "../../../state/run-store.js";
import { registerOwnerResolver } from "../../../locks/lock.js";
import { createInvocationOwnerResolver } from "../../../locks/invocation-owner.js";
import { getNodeExecutionsStore } from "../../../state/node-executions.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { configureGlobalConcurrency, getGlobalConcurrencyLimit } from "../../../engine/global-concurrency.js";
import { createFailureStore } from "../../../state/failure-store.js";
import { createNodeHealthStore } from "../../../state/node-health-store.js";
import { createDiscussStore } from "../../../state/discuss-store.js";
import { createSessionIndex } from "../../../state/session-index.js";
import { createPipelineRegistryStore } from "../../../state/pipeline-registry.js";
import { createPipelineDirsStore } from "../../../state/pipeline-dirs.js";
import { wrapSubagentKeys } from "./session-keys.js";
import { createRunTurn } from "./run-turn.js";
import { registerCliCommands } from "../../../bin/cli.js";
import { registerGraphFlowTool } from "./agent/tools/index.js";
import { checkDb } from "../../../state/db-health.js";
import { writeCorruptionAlert } from "../../../state/db-guard.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";
import { createEventBus } from "../../../dashboard/event-log.js";
import { createDashboardServer } from "../../../dashboard/server.js";
import { costForAllPipelines } from "../../../dashboard/cost.js";
import { createPipelinesWatcher } from "../../../engine/pipelines-watcher.js";
import { flushAll as flushBatchWriter } from "../../../state/batch-writer.js";
import { invokeElevated } from "../../../elevation.js";
import { createDialogServer } from "../../../events/ws-dialog.js";

// Route modules extracted from this file.
import { flowStateEmitter, gateEmitter } from "../../../events.js";
import { createEmitter } from "../../../events/emit.js";
import { createWsRunEventsListener } from "../../../events/ws-run-events.js";
import { createCatalogPublisher } from "../../../events/catalog.js";
import { registerDiscussHandlers } from "../../../routes/discuss.js";
import { registerRunRoutes } from "../../../routes/runs.js";
import { registerOperatorRoutes } from "../../../routes/operator.js";
import * as sessions from "./sessions.js";
import { resumeFlowFromGate } from "../../../routes/operator-resume.js";
import { registerFailureRoutes } from "../../../routes/failures.js";
import { registerGraphqlRoutes } from "../../../routes/graphql.js";
import { registerHealthRoutes } from "../../../routes/health.js";
import { registerPipelineRoutes } from "../../../routes/pipelines.js";
// Route auth is winch-style bearer (routes/bearer-auth.js, MANIFOLD_BEARER):
// routes import the guard directly, so no auth object is injected here.
import { registerBudgetRoutes } from "../../../budget/index.js";
import { registerTelemetry } from "../../../telemetry/index.js";
import { reviveOrphanFlows, reconcileStuckAdmissions } from "../../../boot/flow-revival.js";

// reviveOrphanFlows only runs once, at service startup — a stuck-promotion
// leak (see reconcileStuckAdmissions's own header in boot/flow-revival.js)
// occurring hours into a long gateway uptime would otherwise go undetected
// until the next restart. 5 minutes matches this codebase's other
// steady-state probe cadences (e.g. model-circuit-breaker's own recovery
// probe).
const STUCK_ADMISSION_RECONCILE_INTERVAL_MS = 5 * 60_000;
import { registerGatewayReadiness } from "../../../engine/gateway-readiness.js";
import { registerProfileReadGate } from "./hooks/profile-read-gate.js";
import { registerProfileWriteLock } from "./hooks/profile-write-lock.js";
import { registerManifoldMetaHook } from "./meta.js";
import { resolveStorePaths, ensureMigrated, stateFile } from "../../../state/paths.js";
import { setLocksDir } from "../../../locks/lock.js";
import { setStateFile } from "../../../gates/paused-flows.js";

const DEFAULT_OWNER_SESSION_KEY = "agent:main:explicit:manifold-owner";
const DEFAULT_DASHBOARD_PORT = 7430;
const DEFAULT_DASHBOARD_HOST = "127.0.0.1";

// Module-level singleton — captures a working `runtime.subagent` from the
// gateway-bindable plugin load. The `before_dispatch` hook fires outside that
// scope (the dispatcher path), so its bound `api.runtime.subagent.run` throws
// `RequestScopedSubagentRuntimeError`. By saving a reference from the gateway
// load, the hook can forward to Vulcan even though its own `api.runtime` is
// the unavailable variant. Persisted across plugin reloads via module cache.
const moduleState = { workingSubagent: null };


export default definePluginEntry({
  id: "manifold",
  name: "Manifold Pipeline Engine",
  description:
    "LangGraph-style pipeline executor: loads workspace/.pipelines/<team>_<name>.js definitions and runs them against managed flow_runs. Nodes (code / single / subflow / human-gate / parallel) advance via explicit edges with conditional routing. CLI surface: `openclaw manifold list / start / status / approve / runs`. Agent tool: `manifold_start`.",
  configSchema: () => ({
    type: "object",
    additionalProperties: false,
    properties: {
      pipelinesDir: {
        type: "string",
        description: "Directory to scan for pipeline definition files. Default: <dataDir>/workspace/.pipelines (MANIFOLD_DATA_DIR or ~/.manifold).",
      },
      ownerSessionKey: {
        type: "string",
        description: "Session key used to scope flow_run ownership. Default: agent:main:explicit:manifold-owner",
      },
      dashboardEnabled: {
        type: "boolean",
        description: "Whether the read-only HTTP dashboard service should boot. Default: true.",
      },
      dashboardPort: {
        type: "integer",
        minimum: 1024,
        maximum: 65535,
        description: `Port the dashboard listens on (bound to 127.0.0.1 only). Default: ${DEFAULT_DASHBOARD_PORT}.`,
      },
      dashboardHost: {
        type: "string",
        description: "Host/interface to bind. Default: 127.0.0.1 (loopback only). Override at your own risk.",
      },
      discuss: {
        type: "object",
        description: "Discuss defaults. Used when a blocked gate node does not declare its own discuss.agent.",
        additionalProperties: false,
        properties: {
          defaultAgent: {
            type: "string",
            description: "Default agent for gate discuss sessions. Used when the blocked gate node does not declare its own discuss.agent.",
          },
        },
      },
      globalConcurrency: {
        type: "integer",
        minimum: 1,
        description: "Shared ceiling for every flow-run concurrency mechanism — agent dispatch, subflow fan-out, and whole-flow-run concurrency alike (engine/global-concurrency.js). Default: 8.",
      },
      profilePath: {
        type: "string",
        description: "Absolute path of the PROFILE.md file guarded by the before_tool_call read gate and edit lock (hooks/profile-read-gate.js, hooks/profile-write-lock.js). Default: ~/.openclaw/workspace/.shared/freelance/profile/PROFILE.md",
      },
      profileEditResultsDir: {
        type: "string",
        description: "Directory the edit lock writes one <runId>.json outcome marker per guarded PROFILE.md edit into (hooks/profile-write-lock.js). Default: ~/.openclaw/locks/profile-edit-results",
      },
    },
  }),
  register: (api) => {
    const userCfg = api.pluginConfig ?? {};
    // Core owns its store home (state/paths.js): one MANIFOLD_DATA_DIR
    // move relocates every DB, lock, and pending file. Legacy
    // ~/.openclaw/{runtime.sqlite,ops.sqlite} are copied in once, never
    // moved, and never read again afterwards.
    const storePaths = resolveStorePaths();
    ensureMigrated({ dataDir: storePaths.dataDir, logger: api.logger });
    setLocksDir(storePaths.locksDir);
    setStateFile(stateFile(storePaths.dataDir));
    // Pipeline dir resolution (table[harness] → MANIFOLD_PIPELINES_DIR env →
    // config `pipelinesDir` → <dataDir>/workspace/.pipelines). The table lets
    // an operator relocate one harness without touching env or config; sync
    // open-per-op store, so this resolves before any service starts.
    // Single-harness default: the harness name used when a caller doesn't
    // declare one (pipelines-dir resolution, tool defaults). Per-call
    // identity (X-Manifold-Harness, tool args) wins wherever it matters.
    const defaultHarness = "openclaw";
    const pipelineDirsStore = createPipelineDirsStore({ dbPath: runtimeDbPath, logger: api.logger });
    const pipelinesDir = pipelineDirsStore.resolveDir({
      harness: defaultHarness,
      env: process.env,
      dataDir: storePaths.dataDir,
      configDir: userCfg.pipelinesDir || null,
    }).dir;
    const ownerSessionKey = userCfg.ownerSessionKey || DEFAULT_OWNER_SESSION_KEY;
    const discussDefaultAgent = typeof userCfg.discuss?.defaultAgent === "string" ? userCfg.discuss.defaultAgent : null;
    const dashboardEnabled = userCfg.dashboardEnabled !== false;
    const dashboardPort = Number.isInteger(userCfg.dashboardPort) ? userCfg.dashboardPort : DEFAULT_DASHBOARD_PORT;
    const dashboardHost = userCfg.dashboardHost || DEFAULT_DASHBOARD_HOST;

    // One shared ceiling for every concurrency mechanism in the engine —
    // agent dispatch, subflow fan-out, and whole-flow-run concurrency alike
    // (see global-concurrency.js). agentDispatchConcurrency/
    // childFanoutConcurrency were two independent config keys for what are
    // now the same number; globalConcurrency replaces both.
    configureGlobalConcurrency({ maxConcurrent: userCfg.globalConcurrency });

    // Registered here (rather than deferred) since PROFILE.md reads/edits
    // can happen from the very first dispatched pipeline run after boot.
    // Both path options fall through to each hook's own os.homedir() default
    // when the config key is absent or undefined, so a zero-config install
    // acquires byte-identical lock names to the workspace-side PROFILE.md
    // writers (gap-answers.js, prime-nodes.js) it shares that lock with.
    registerProfileReadGate(api, { profilePath: userCfg.profilePath });
    registerProfileWriteLock(api, { profilePath: userCfg.profilePath, resultsDir: userCfg.profileEditResultsDir });
    registerManifoldMetaHook(api);

    const pipelinesRef = { value: new Map() };

    const runtimeDbPath = storePaths.runtimeDb;

    const runStore    = createRunStore({ logger: api.logger, dbPath: runtimeDbPath });
    const nodeExecutionsStore = getNodeExecutionsStore(runtimeDbPath);
    // Locks taken on behalf of a node invocation (executor-nodes.js's
    // executeNode, and browser tool leases of the sessions it dispatches)
    // stay held until the invocation resolves.
    registerOwnerResolver("invocation", createInvocationOwnerResolver({ nodeExecutionsStore, runStore }));
    // The single durable admission pool for flow runs — child-dispatch and
    // flow-start share it; see state/admission-queue-store.js's own header
    // for why it's one combined pool, not one per kind. Reads
    // the same runtimeDbPath every other manifold-owned table lives in;
    // getGlobalConcurrencyLimit is called fresh on every admit(), so a
    // reconfigured limit takes effect immediately, no restart needed.
    const admissionQueueStore = createAdmissionQueueStore({
      dbPath: runtimeDbPath, getLimit: getGlobalConcurrencyLimit,
    });
    // resumeParentRef.resume is set to executor.resume once the executor
    // exists below — store construction has to precede it, so the
    // resume-trigger (state-store.js's finish/skip/fail/cancel) reads
    // through this mutable box rather than a captured closure. Currently
    // dormant: no real flow sets parentInvocationId yet, so this hook never
    // actually fires today.
    const resumeParentRef = { resume: null };
    // Constructed before `store` — createStateStore's own
    // triggerParentResume hook needs failureStore to
    // record a real failure_events row when a subflow's mode isn't
    // satisfied, not just flip flow_runs.status.
    const emitCanonical = createEmitter({ getBroadcast: () => _gatewayBroadcast, logger: api.logger }).emit;
    const failureStore = createFailureStore({ logger: api.logger, dbPath: runtimeDbPath, nodeExecutionsStore, emit: emitCanonical });
    // SCD2 history of node kind/agent/mode (see state/pipeline-registry.js's
    // own header) — separate from failureStore and unrelated to it;
    // constructed here only because runtimeDbPath is already in scope.
    const pipelineRegistryStore = createPipelineRegistryStore(runtimeDbPath);
    const store = createStateStore(api, ownerSessionKey, {
      runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger: api.logger,
    });
    // Memoized single load (engine/loader.js's createPipelinesLoader) —
    // every caller (the loader service, flow-revival, and executor.js's
    // per-flow fallback) awaits this SAME promise instead of each
    // independently checking `pipelinesRef.value.size > 0` and potentially
    // kicking off its own redundant loadPipelines() pass. See that
    // function's docstring for the boot-race this closes.
    const ensurePipelinesLoaded = createPipelinesLoader(pipelinesDir, api.logger, pipelinesRef);
    // createNodeHealthStore is async (opens sqlite + runs migrations) but
    // register() must be synchronous. Same lazy Promise-proxy pattern as
    // sessionIndex below — callers await individual methods without an
    // await here.
    const _nodeHealthPromise = createNodeHealthStore({ logger: api.logger, dbPath: runtimeDbPath });
    const nodeHealthStore = {
      isBlocked(p, n) { return _nodeHealthPromise.then(s => s.isBlocked(p, n)); },
      markUnhealthy(p, n, info) { return _nodeHealthPromise.then(s => s.markUnhealthy(p, n, info)); },
      reset(p, n, opts) { return _nodeHealthPromise.then(s => s.reset(p, n, opts)); },
      list(p, opts) { return _nodeHealthPromise.then(s => s.list(p, opts)); },
      close() { _nodeHealthPromise.then(s => s.close?.()).catch(() => {}); },
    };
    const discussStore = createDiscussStore({ logger: api.logger, dbPath: runtimeDbPath });
    // createSessionIndex is async (opens sqlite + runs migrations) but
    // register() must be synchronous. Wrap the Promise in a thin proxy so
    // callers can await individual methods without an await here — every
    // method below chains off _sessionIndexPromise itself, so a caller is
    // always correctly held until the DB is actually open, regardless of
    // how soon after boot it calls in (e.g. flow-revival's orphan
    // classification, which runs while the gateway is still starting).
    const _sessionIndexPromise = createSessionIndex({ logger: api.logger, dbPath: runtimeDbPath });
    const sessionIndex = {
      // Harness-owned session ids pass through verbatim — stored and read
      // with no edge translation on either path.
      record(data) {
        return _sessionIndexPromise.then((si) => si.record({ ...data }));
      },
      recordThread(data) {
        return _sessionIndexPromise.then((si) => si.recordThread({ ...data }));
      },
      findThread(flowId, gateNodeId, itemId) { return _sessionIndexPromise.then(si => si.findThread(flowId, gateNodeId, itemId)); },
      hasSessionForNode(flowId, nodeId) { return _sessionIndexPromise.then(si => si.hasSessionForNode(flowId, nodeId)); },
      flowIdForSessionKey(sessionKey) { return _sessionIndexPromise.then(si => si.flowIdForSessionKey(sessionKey)); },
      close() { _sessionIndexPromise.then(si => si.close?.()).catch(() => {}); },
    };

    // Process-wide gateway readiness (engine/gateway-readiness.js): settled
    // by gateway_start; resolves at once in a process that never starts
    // manifold's services (the CLI). Awaited by this plugin's run-turn
    // gate, which every agent dispatch passes.
    const gatewayReadiness = registerGatewayReadiness(api, { logger: api.logger });

    // The harness half of an agent dispatch: core injects this function and
    // never touches the gateway's agent runtime itself. The runtime is
    // resolved per call — the gateway binds it during startup, and
    // moduleState.workingSubagent captures the wrapped copy once that
    // happens.
    const { runTurn } = createRunTurn({
      getRuntime: () => moduleState.workingSubagent ?? (api.runtime?.subagent ? wrapSubagentKeys(api.runtime.subagent) : null),
      whenGatewayReady: () => gatewayReadiness.whenReady(),
      logger: api.logger,
    });

    // Assigned in the dashboard section below; the catalog publisher reads
    // it lazily so watcher reloads before dashboard boot stay WS-only.
    let dashboardBusRef = null;
    const publishCatalogChanged = createCatalogPublisher({
      emit: emitCanonical,
      getDashboardBroadcast: () => (dashboardBusRef ? (e) => dashboardBusRef.broadcast(e) : null),
    });

    const executor = createExecutor({
      api,
      config: api.config,
      pipelinesDir,
      store,
      pipelinesRef,
      ensurePipelinesLoaded,
      logger: api.logger,
      failureStore,
      nodeHealthStore,
      sessionIndex,
      runStore,
      nodeExecutionsStore,
      admissionQueueStore,
      // The harness owns the agent runtime: core receives a turn function,
      // never the runtime object (wrapSubagentKeys memoizes, so repeated
      // resolves don't re-wrap).
      runTurn,
      emit: emitCanonical,
    });
    resumeParentRef.resume = executor.resume;

    // ── Services ────────────────────────────────────────────────────────────
    //
    // openclaw starts these one by one as part of the gateway's own startup
    // and only becomes ready (agent.wait served, gateway_start fired) after
    // the last start() returns. A start() must therefore never await gateway
    // readiness or an agent dispatch — work that dispatches is started
    // without awaiting it, and its dispatches wait at the executor's gate.

    // Registered first so it starts first: only the gateway starts plugin
    // services, so this is what tells readiness this process is the gateway
    // (the CLI never gets here, and never waits).
    api.registerService({
      id: "manifold-gateway-readiness",
      start: () => gatewayReadiness.markGatewayProcess(),
    });

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

    const pipelinesWatcher = createPipelinesWatcher({ pipelinesDir, pipelinesRef, logger: api.logger });
    // Single catalog producer (events/catalog.js): identical content to
    // the gateway WS stream and the dashboard bus.
    pipelinesWatcher.onReload((info) => {
      publishCatalogChanged(info).catch(() => {});
    });
    pipelinesWatcher.onReload(() => {
      pipelineRegistryStore.reconcilePipelineVersions(pipelinesRef.value, { logger: api.logger });
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

    // ── CLI + tools ─────────────────────────────────────────────────────────

    api.registerCli(
      (ctx) => registerCliCommands({
        program: ctx.program,
        executor,
        store,
        pipelinesRef,
        logger: ctx.logger ?? api.logger,
        dashboardUrl: `http://${dashboardHost}:${dashboardPort}`,
        api,
        nodeHealthStore,
        ownerSessionKey,
        runStore,
        nodeExecutionsStore,
        admissionQueueStore,
        pipelineDirsStore,
        resolvePipelinesDir: (harness) => pipelineDirsStore.resolveDir({
          harness: harness ?? defaultHarness,
          env: process.env,
          dataDir: storePaths.dataDir,
          configDir: userCfg.pipelinesDir || null,
        }),
      }),
      {
        commands: ["manifold"],
        descriptors: [{
          name: "manifold",
          description: "Run state-graph pipelines from workspace/.pipelines/",
          hasSubcommands: true,
        }],
      },
    );

    registerGraphFlowTool({ api, executor, store, pipelinesRef, ensurePipelinesLoaded });

    const readStore = runStore;
    const gateResolver = createGateResolver({
      api, store, executor, getBroadcast: () => _gatewayBroadcast, readStore, runStore,
      pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
      admissionQueueStore,
    });

    api.registerService({
      id: "manifold-read-store",
      start: async () => {},
      stop: async () => { sessionIndex?.close?.(); },
    });

    // ── componentEmitter + gateway ping ──────────────────────────────────────

    let _gatewayBroadcast = null;

    api.registerGatewayMethod("plugin.manifold.ping", (opts) => {
      if (!_gatewayBroadcast) {
        _gatewayBroadcast = opts.context.broadcast;
        api.logger.info("manifold: WS broadcaster captured");
      }
      opts.respond(true, { ok: true });
    }, { scope: "operator.read" });

    // ── Discuss handlers ─────────────────────────────────────────────────────

    const discussHandlers = registerDiscussHandlers({
      api,
      store,
      pipelinesRef,
      emit: emitCanonical,
      discussDefaultAgent,
      invokeElevated,
      discussStore,
      runStore,
      nodeExecutionsStore,
      // Harness adapter: discuss thread ids come from here
      // (sessions.newDiscussSessionId), never minted in core.
      sessions,
      sessionIndex,
    });

    // ── Route modules ────────────────────────────────────────────────────────

    registerRunRoutes({
      api,
      readStore,
      costForAllPipelines,
      runStore,
      pipelinesRef,
      pipelineRegistryStore,
      invokeElevated,
      discussStore,
      failureStore,
      sessionIndex,
      handleDiscuss:           discussHandlers.handleDiscuss,
      handleDiscussHistory:    discussHandlers.handleDiscussHistory,
      handleDiscussApplyDraft: discussHandlers.handleDiscussApplyDraft,
      handleDiscussAudio:      discussHandlers.handleDiscussAudio,
      handleDiscussAudioGet:   discussHandlers.handleDiscussAudioGet,
      handleDiscussAudioRetry: discussHandlers.handleDiscussAudioRetry,
    });

    registerOperatorRoutes({
      api,
      store,
      readStore,
      executor,
      gateResolver,
      failureStore,
      pipelinesRef,
      ensurePipelinesLoaded,
      invokeElevated,
      ownerSessionKey,
      runStore,
      admissionQueueStore,
      resumeParentRef,
      discussStore,
      getBroadcast: () => _gatewayBroadcast,
      sessions,
    });

    registerFailureRoutes({
      api,
      failureStore,
      invokeElevated,
    });

    registerGraphqlRoutes({
      api,
      failureStore,
      runStore,
    });

    registerHealthRoutes({ api });

    registerPipelineRoutes({
      api,
      pipelinesRef,
      readStore,
      ensurePipelinesLoaded,
    });

    registerBudgetRoutes(api, {
      getBroadcast: () => _gatewayBroadcast,
      // In-process resume for flow-bound budget approvals (no HTTP loopback).
      onFlowResume: ({ flowId, nodeId, decision }) => resumeFlowFromGate({
        store, readStore, admissionQueueStore, runStore, executor,
        logger: api.logger, api, flowId, nodeId, decision,
      }),
    });
    registerTelemetry(api, { getBroadcast: () => _gatewayBroadcast });

    // ── Dialog door: WS message transport into open discuss sessions ───────
    // Any harness client connects with ?bearer= and sends
    // {type:"message",...} frames. Turns run through the same elevation
    // the HTTP discuss route uses.
    let _dialogServer = null;
    api.registerService({
      id: "manifold-dialog",
      start: async () => {
        _dialogServer = createDialogServer({
          port: Number(process.env.MANIFOLD_DIALOG_PORT) || 38295,
          host: process.env.MANIFOLD_DIALOG_HOST ?? "127.0.0.1",
          readFlow: (flowId) => readStore.get(flowId),
          resolveTarget: discussHandlers.resolveDiscussTarget,
          runTurn: async ({ flowId, sessionKey, sessionId, agentId, gateNodeId, modelOverride, itemId, text, systemPrompt }) => {
            const elev = await invokeElevated("/manifold/_elev/discuss", {
              flowId, sessionKey, sessionId, agentId, gateNodeId, modelOverride, text, itemId,
              systemPrompt: typeof systemPrompt === "string" ? systemPrompt : null,
            }, { timeoutMs: 360_000 });
            let payload;
            try { payload = JSON.parse(elev.body || "{}"); } catch { payload = {}; }
            if (!(elev.ok && payload.ok)) {
              return { ok: false, code: payload.code ?? "elev_failed", error: payload.error ?? payload.code ?? elev.error ?? `HTTP ${elev.status}` };
            }
            return {
              ok: true, reply: payload.reply ?? "", proposedDraft: payload.proposedDraft ?? null,
              proposedMeta: payload.proposedMeta ?? null, durationMs: payload.durationMs ?? null,
            };
          },
          discussStore,
          sessionIndex,
          recordLineage: (entry) => api.runtime?.lineage?.recordSpawn?.(entry),
          runStore,
          nodeExecutionsStore,
          emit: emitCanonical,
          logger: api.logger,
        });
        await _dialogServer.start();
      },
      stop: async () => {
        try { await _dialogServer?.stop(); } catch {}
        _dialogServer = null;
      },
    });

    // ── Dashboard ────────────────────────────────────────────────────────────

    if (dashboardEnabled) {
      const dashboardStore = readStore;
      const dashboardBus = createEventBus({ store: dashboardStore, nodeExecutionsStore, logger: api.logger });
      dashboardBusRef = dashboardBus;
      pipelinesWatcher.onReload((info) => {
        publishCatalogChanged(info).catch(() => {});
      });
      const dashboardServer = createDashboardServer({
        writeStore: store,
        readStore,
        failureStore,
        discussStore,
        nodeExecutionsStore,
        pipelinesRef,
        pipelineRegistryStore,
        ensurePipelinesLoaded,
        executor,
        eventBus: dashboardBus,
        port: dashboardPort,
        host: dashboardHost,
        logger: api.logger,
        recordItemDecision: gateResolver.recordItemDecision,
      });
      api.registerService({
        id: "manifold-dashboard",
        start: async () => {
          dashboardBus.start();
          try { await dashboardServer.start(); }
          catch (err) {
            api.logger.warn(`manifold dashboard: failed to start on ${dashboardHost}:${dashboardPort}: ${err?.message ?? err}`);
            dashboardBus.stop();
          }
        },
        stop: async () => {
          dashboardBus.stop();
          await dashboardServer.stop();
        },
      });
    }

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
          nodeExecutionsStore,
          pipelineLookup: (id) => pipelinesRef.value.get(id),
          getBroadcast: () => _gatewayBroadcast,
          emit: emitCanonical,
        });
        flowStateEmitter.on("change", _wsRunEventsListener);

        // Gate lifecycle goes out with its full payload (items, waitJson,
        // stateJson) so a consumer can render the gate without a re-fetch;
        // gate.closed tells it to drop the surface. Delivery is the
        // consumer's business — manifold only states what happened.
        _gateOpenedListener = (payload) => {
          try { broadcast("manifold.gate.opened", payload); } catch {}
        };
        _gateClosedListener = (payload) => {
          try { broadcast("manifold.gate.closed", payload); } catch {}
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

    api.registerService({
      id: "manifold-runtime-capture",
      start: async () => {
        if (!moduleState.workingSubagent && api.runtime?.subagent) {
          // Wrapped ONCE here: every core dispatch below resolves through
          // this reference, so runIds map back at exactly one edge.
          moduleState.workingSubagent = wrapSubagentKeys(api.runtime.subagent);
          api.logger.info("manifold: captured working agent runtime for dispatch use");
        }
      },
      stop: async () => {},
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
  },
});
