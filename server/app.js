// createManifoldApp() assembles the app outside any harness: stores,
// executor, pipelines watcher, revival, HTTP routes, dashboard — returned as
// an ordered service list. Host supplies what only a harness has: logger,
// runTurn, sessions, getBroadcast, recordSpawn. Core never touches a gateway
// API object, so `manifold serve` boots it with no knowledge of a harness.

import { createPipelinesLoader } from "../engine/loader.js";
import { createStateStore } from "../state/state-store.js";
import { createRunStore } from "../state/run-store.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { createAdmissionQueueStore } from "../state/admission-queue-store.js";
import { createExecutor } from "../engine/executor.js";
import { configureGlobalConcurrency, getGlobalConcurrencyLimit } from "../engine/global-concurrency.js";
import { createFailureStore } from "../state/failure-store.js";
import { createNodeHealthStore } from "../state/node-health-store.js";
import { createDiscussStore } from "../state/discuss-store.js";
import { createSessionIndex } from "../state/session-index.js";
import { createPipelineRegistryStore } from "../state/pipeline-registry.js";
import { checkDb } from "../state/db-health.js";
import { writeCorruptionAlert } from "../state/db-guard.js";
import { createGateResolver } from "../gates/gate-resolver.js";
import { createEventBus } from "../dashboard/event-log.js";
import { createDashboardServer } from "../dashboard/server.js";
import { costForAllPipelines } from "../dashboard/cost.js";
import { createPipelinesWatcher } from "../engine/pipelines-watcher.js";
import { flushAll as flushBatchWriter } from "../state/batch-writer.js";
import { createDialogServer, DEFAULT_DIALOG_PORT } from "../events/ws-dialog.js";
import { flowStateEmitter, gateEmitter } from "../events.js";
import { createEmitter } from "../events/emit.js";
import { createWsRunEventsListener } from "../events/ws-run-events.js";
import { createCatalogPublisher } from "../events/catalog.js";
import { registerDiscussHandlers } from "../routes/discuss.js";
import { registerRunRoutes } from "../routes/runs.js";
import { registerOperatorRoutes } from "../routes/operator.js";
import { createDiscussHandler } from "../routes/operator-discuss.js";
import { resumeFlowFromGate } from "../routes/operator-resume.js";
import { registerFailureRoutes } from "../routes/failures.js";
import { registerGraphqlRoutes } from "../routes/graphql.js";
import { registerHealthRoutes } from "../routes/health.js";
import { registerPipelineRoutes } from "../routes/pipelines.js";
import { registerBudgetRoutes, openStore as openBudgetStore } from "../budget/index.js";
import { registerTelemetry } from "../telemetry/index.js";
import { reviveOrphanFlows, reconcileStuckAdmissions } from "../boot/flow-revival.js";
import { resolveStorePaths } from "../state/paths.js";

// reviveOrphanFlows runs once at startup; a stuck-promotion leak hours into
// uptime would otherwise wait for the next restart. 5 minutes matches this
// codebase's other steady-state probe cadences.
const STUCK_ADMISSION_RECONCILE_INTERVAL_MS = 5 * 60_000;

export const DEFAULT_OWNER_SESSION_KEY = "agent:main:explicit:manifold-owner";
export const DEFAULT_DASHBOARD_PORT = 44300;
export const DEFAULT_DASHBOARD_HOST = "127.0.0.1";

export async function startOrWarn(logger, { label, address, start, stop }) {
  try {
    await start();
    return true;
  } catch (err) {
    logger.warn(`${label}: failed to start on ${address}: ${err?.message ?? err}`);
    try { await stop?.(); } catch {}
    return false;
  }
}

export function createManifoldApp({ host, config = {} } = {}) {
  if (!host?.logger) throw new Error("createManifoldApp: host.logger is required");
  if (typeof host.runTurn !== "function") throw new Error("createManifoldApp: host.runTurn is required");
  const logger = host.logger;
  const getBroadcast = host.getBroadcast ?? (() => null);

  const storePaths = resolveStorePaths();

  const pipelinesDir = config.pipelinesDir;
  if (!pipelinesDir) throw new Error("createManifoldApp: config.pipelinesDir is required");
  const ownerSessionKey = config.ownerSessionKey || DEFAULT_OWNER_SESSION_KEY;
  const discussDefaultAgent = typeof config.discussDefaultAgent === "string" ? config.discussDefaultAgent : null;
  const dashboardEnabled = config.dashboardEnabled !== false;
  const dashboardPort = Number.isInteger(config.dashboardPort) ? config.dashboardPort : DEFAULT_DASHBOARD_PORT;
  const dashboardHost = config.dashboardHost || DEFAULT_DASHBOARD_HOST;

  configureGlobalConcurrency({ maxConcurrent: config.globalConcurrency });

  const pipelinesRef = { value: new Map() };
  const runtimeDbPath = storePaths.runtimeDb;

  const runStore = createRunStore({ logger, dbPath: runtimeDbPath });
  const nodeExecutionsStore = getNodeExecutionsStore(runtimeDbPath);
  const admissionQueueStore = createAdmissionQueueStore({
    dbPath: runtimeDbPath, getLimit: getGlobalConcurrencyLimit,
  });
  // Stores must be constructed before the executor exists, but the
  // resume-trigger reads executor.resume through this mutable box rather
  // than a captured closure.
  const resumeParentRef = { resume: null };
  const emitCanonical = createEmitter({ getBroadcast, logger }).emit;
  const failureStore = createFailureStore({ logger, dbPath: runtimeDbPath, nodeExecutionsStore, emit: emitCanonical });
  const pipelineRegistryStore = createPipelineRegistryStore(runtimeDbPath);
  const store = createStateStore({ logger }, ownerSessionKey, {
    runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger,
  });
  const ensurePipelinesLoaded = createPipelinesLoader(pipelinesDir, logger, pipelinesRef);
  // createNodeHealthStore is async (opens sqlite + runs migrations) but app
  // construction must be synchronous — lazy Promise-proxy, as with sessionIndex.
  const _nodeHealthPromise = createNodeHealthStore({ logger, dbPath: runtimeDbPath });
  const nodeHealthStore = {
    isBlocked(p, n) { return _nodeHealthPromise.then(s => s.isBlocked(p, n)); },
    markUnhealthy(p, n, info) { return _nodeHealthPromise.then(s => s.markUnhealthy(p, n, info)); },
    reset(p, n, opts) { return _nodeHealthPromise.then(s => s.reset(p, n, opts)); },
    list(p, opts) { return _nodeHealthPromise.then(s => s.list(p, opts)); },
    close() { _nodeHealthPromise.then(s => s.close?.()).catch(() => {}); },
  };
  const discussStore = createDiscussStore({ logger, dbPath: runtimeDbPath });
  const _sessionIndexPromise = createSessionIndex({ logger, dbPath: runtimeDbPath });
  const sessionIndex = {
    // Harness-owned session ids pass through verbatim — no edge translation.
    record(data) { return _sessionIndexPromise.then((si) => si.record({ ...data })); },
    recordThread(data) { return _sessionIndexPromise.then((si) => si.recordThread({ ...data })); },
    findThread(flowId, gateNodeId, itemId) { return _sessionIndexPromise.then(si => si.findThread(flowId, gateNodeId, itemId)); },
    hasSessionForNode(flowId, nodeId) { return _sessionIndexPromise.then(si => si.hasSessionForNode(flowId, nodeId)); },
    flowIdForSessionKey(session) { return _sessionIndexPromise.then(si => si.flowIdForSessionKey(session)); },
    close() { _sessionIndexPromise.then(si => si.close?.()).catch(() => {}); },
  };

  let dashboardBusRef = null;
  const publishCatalogChanged = createCatalogPublisher({
    emit: emitCanonical,
    getDashboardBroadcast: () => (dashboardBusRef ? (e) => dashboardBusRef.broadcast(e) : null),
  });

  const executor = createExecutor({
    config: host.config ?? null,
    pipelinesDir,
    store,
    pipelinesRef,
    ensurePipelinesLoaded,
    logger,
    failureStore,
    nodeHealthStore,
    sessionIndex,
    runStore,
    nodeExecutionsStore,
    admissionQueueStore,
    // The harness owns the agent runtime and the session namespace: core
    // receives a turn function and asks for ids, never the runtime object.
    runTurn: host.runTurn,
    sessions: host.sessions,
    emit: emitCanonical,
  });
  resumeParentRef.resume = executor.resume;

  const services = [];
  const service = (id, start, stop) => services.push({ id, start, stop });

  service("manifold-loader", async () => {
    logger.info(`manifold: loading pipelines from ${pipelinesDir}`);
    const pipelines = await ensurePipelinesLoaded();
    const ids = [...pipelines.keys()];
    logger.info(`manifold: ${pipelines.size} pipeline(s) loaded${ids.length ? `: ${ids.join(", ")}` : ""}`);
    // The watcher's first tick deliberately primes its fingerprint without
    // reloading, so this boot-time load must do the first reconcile itself.
    await pipelineRegistryStore.reconcilePipelineVersions(pipelinesRef.value, { logger });
  });

  const pipelinesWatcher = createPipelinesWatcher({ pipelinesDir, pipelinesRef, logger });
  pipelinesWatcher.onReload((info) => { publishCatalogChanged(info).catch(() => {}); });
  pipelinesWatcher.onReload(() => {
    pipelineRegistryStore.reconcilePipelineVersions(pipelinesRef.value, { logger });
  });
  service("manifold-pipelines-watcher", () => pipelinesWatcher.start(), () => pipelinesWatcher.stop());

  // Nothing to do at boot, but stop() must flush batch-writer's buffered
  // fire-and-forget writes before the process exits.
  service("manifold-batch-writer", async () => {}, () => flushBatchWriter());

  let revivalTimer = null;
  service("manifold-flow-revival", async (ctx) => {
    const l = ctx?.logger ?? logger;
    l.info("manifold revival: start");
    // Not awaited: start() must return before revival's dispatches complete.
    runBootRevival(l);
    revivalTimer = setInterval(() => {
      reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, logger: l })
        .catch((err) => l.warn(`manifold revival: periodic stuck-admission reconcile failed: ${err?.message ?? err}`));
    }, STUCK_ADMISSION_RECONCILE_INTERVAL_MS);
  }, async () => {
    if (revivalTimer) { clearInterval(revivalTimer); revivalTimer = null; }
  });

  async function runBootRevival(l) {
    try {
      // Pipelines must be in memory before reviving: otherwise a race with
      // the loader makes the orphan filter fail every flow with "pipeline
      // not loaded". Read candidates from the SQL store, not store.list() —
      // the in-process Map is empty at boot. includeChildren:true so a
      // crashed child is revived like a crashed top-level flow.
      await ensurePipelinesLoaded();
      const all = await runStore.list({ includeChildren: true });
      await reviveOrphanFlows({
        all,
        pipelines: pipelinesRef.value,
        store,
        logger: l,
        executor,
        createCancelStore: (ownerKey) => createStateStore({ logger: l }, ownerKey, {
          runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, admissionQueueStore, logger: l,
        }),
        runStore,
        nodeExecutionsStore,
        sessionIndex,
        admissionQueueStore,
        // gateResolver is declared later in this closure — safe here because
        // this callback only runs once startup reaches that point.
        maybeResolveFlow: gateResolver.maybeResolveFlow,
      });
    } catch (err) {
      l.warn(`manifold revival: failed to enumerate orphans: ${err?.message ?? err}`);
    }
  }

  const readStore = runStore;
  // Route modules register their own services; they join this ordered list.
  const routeHost = {
    logger,
    registerHttpRoute: host.registerHttpRoute,
    registerService: (svc) => services.push(svc),
    // A host without these events accepts the registration and never fires it.
    on: (event, handler) => host.on?.(event, handler),
    // The gateway CLI lives with the harness; a standalone core is its own
    // CLI (bin/manifold.js), so this registration is accepted and unused.
    registerCli: (fn, meta) => host.registerCli?.(fn, meta),
    config: host.config ?? {},
    runtime: host.runtime ?? {},
  };
  const gateResolver = createGateResolver({
    api: routeHost, store, executor, getBroadcast, readStore, runStore,
    pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
    admissionQueueStore,
  });

  service("manifold-read-store", async () => {}, async () => { sessionIndex?.close?.(); });

  // The agent-facing half of a discuss turn: it takes no HTTP shape of its
  // own, so the discuss routes and the WS dialog both call it directly.
  const { handleDiscuss: runDiscussTurn } = createDiscussHandler({
    api: routeHost,
    discussStore,
    sessions: host.sessions,
  });

  const { startPipeline, rerunFlow, retryFailure } = registerOperatorRoutes({
    api: routeHost,
    store,
    readStore,
    executor,
    gateResolver,
    failureStore,
    pipelinesRef,
    ensurePipelinesLoaded,
    ownerSessionKey,
    runStore,
    admissionQueueStore,
    resumeParentRef,
    getBroadcast,
  });

  const discussHandlers = registerDiscussHandlers({
    api: routeHost,
    store,
    pipelinesRef,
    emit: emitCanonical,
    discussDefaultAgent,
    runDiscussTurn,
    discussStore,
    runStore,
    nodeExecutionsStore,
    // Discuss thread ids come from the harness, never minted in core.
    sessions: host.sessions,
    sessionIndex,
  });

  registerRunRoutes({
    api: routeHost,
    readStore,
    costForAllPipelines,
    runStore,
    pipelinesRef,
    pipelineRegistryStore,
    rerunFlow,
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

  registerFailureRoutes({ api: routeHost, failureStore, retryFailure });
  registerGraphqlRoutes({ api: routeHost, failureStore, runStore });
  registerHealthRoutes({ api: routeHost });
  registerPipelineRoutes({ api: routeHost, pipelinesRef, readStore, ensurePipelinesLoaded });

  registerBudgetRoutes(routeHost, {
    getBroadcast,
    // The app's store home, not the harness default the module carries.
    openStore: (opts = {}) => openBudgetStore({ dbPath: storePaths.opsDb, runtimeDbPath, ...opts }),
    // In-process resume for flow-bound budget approvals (no HTTP loopback).
    onFlowResume: ({ flowId, nodeId, decision }) => resumeFlowFromGate({
      store, readStore, admissionQueueStore, runStore, executor,
      logger, flowId, nodeId, decision,
    }),
    // In-process start for an approved start-gate item (no HTTP loopback).
    onStartGateApproved: (args) => startPipeline(args),
  });
  registerTelemetry(routeHost, { getBroadcast, _dbPath: storePaths.opsDb });

  let _dialogServer = null;
  const dialogHost = process.env.MANIFOLD_DIALOG_HOST ?? "127.0.0.1";
  const dialogPort = Number(process.env.MANIFOLD_DIALOG_PORT) || DEFAULT_DIALOG_PORT;
  service("manifold-dialog", async () => {
    const server = createDialogServer({
      port: dialogPort,
      host: dialogHost,
      readFlow: (flowId) => readStore.get(flowId),
      resolveTarget: discussHandlers.resolveDiscussTarget,
      runTurn: async ({ flowId, session, agentId, gateNodeId, modelOverride, itemId, text, systemPrompt }) => {
        const result = await runDiscussTurn({
          flowId, session, agentId, gateNodeId, modelOverride, text, itemId,
          systemPrompt: typeof systemPrompt === "string" ? systemPrompt : null,
        });
        const payload = result.body ?? {};
        if (result.status !== 200 || payload.ok !== true) {
          return { ok: false, code: payload.code ?? "discuss_failed", error: payload.error ?? payload.code ?? `HTTP ${result.status}` };
        }
        return {
          ok: true, reply: payload.reply ?? "", proposedDraft: payload.proposedDraft ?? null,
          proposedMeta: payload.proposedMeta ?? null, durationMs: payload.durationMs ?? null,
        };
      },
      discussStore,
      sessionIndex,
      recordLineage: host.recordSpawn,
      runStore,
      nodeExecutionsStore,
      emit: emitCanonical,
      logger,
    });
    const up = await startOrWarn(logger, {
      label: "manifold dialog ws",
      address: `${dialogHost}:${dialogPort}`,
      start: () => server.start(),
      stop: () => server.stop(),
    });
    _dialogServer = up ? server : null;
  }, async () => {
    try { await _dialogServer?.stop(); } catch {}
    _dialogServer = null;
  });

  if (dashboardEnabled) {
    const dashboardStore = readStore;
    const dashboardBus = createEventBus({ store: dashboardStore, nodeExecutionsStore, logger });
    dashboardBusRef = dashboardBus;
    pipelinesWatcher.onReload((info) => { publishCatalogChanged(info).catch(() => {}); });
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
      logger,
      recordItemDecision: gateResolver.recordItemDecision,
      admissionQueueStore,
    });
    service("manifold-dashboard", async () => {
      dashboardBus.start();
      await startOrWarn(logger, {
        label: "manifold dashboard",
        address: `${dashboardHost}:${dashboardPort}`,
        start: () => dashboardServer.start(),
        stop: () => { dashboardBus.stop(); },
      });
    }, async () => {
      dashboardBus.stop();
      await dashboardServer.stop();
    });
  }

  let _wsRunEventsListener = null;
  let _gateOpenedListener = null;
  let _gateClosedListener = null;
  service("manifold-ws-run-events", async () => {
    _wsRunEventsListener = createWsRunEventsListener({
      store,
      nodeExecutionsStore,
      pipelineLookup: (id) => pipelinesRef.value.get(id),
      getBroadcast,
      emit: emitCanonical,
    });
    flowStateEmitter.on("change", _wsRunEventsListener);

    _gateOpenedListener = (payload) => {
      try { getBroadcast()?.("manifold.gate.opened", payload); } catch {}
    };
    _gateClosedListener = (payload) => {
      try { getBroadcast()?.("manifold.gate.closed", payload); } catch {}
    };
    gateEmitter.on("gate.opened", _gateOpenedListener);
    gateEmitter.on("gate.closed", _gateClosedListener);
  }, async () => {
    if (_wsRunEventsListener) { flowStateEmitter.off("change", _wsRunEventsListener); _wsRunEventsListener = null; }
    if (_gateOpenedListener) { gateEmitter.off("gate.opened", _gateOpenedListener); _gateOpenedListener = null; }
    if (_gateClosedListener) { gateEmitter.off("gate.closed", _gateClosedListener); _gateClosedListener = null; }
  });

  let started = [];
  async function start(ctx) {
    started = [];
    for (const s of services) {
      await s.start?.(ctx);
      started.push(s);
    }
  }
  async function stop(ctx) {
    for (const s of started.slice().reverse()) {
      try { await s.stop?.(ctx); } catch (err) { logger.warn(`manifold: stop ${s.id} failed: ${err?.message ?? err}`); }
    }
    started = [];
  }

  const dbCheck = checkDatabases(storePaths, { logger });
  if (!dbCheck.ok) {
    const err = new Error(`unreadable database ${dbCheck.dbPath}: ${dbCheck.error}`);
    err.code = "db_unreadable";
    throw err;
  }

  return {
    services,
    start,
    stop,
    executor,
    store,
    runStore,
    readStore,
    nodeExecutionsStore,
    admissionQueueStore,
    failureStore,
    discussStore,
    sessionIndex,
    pipelineRegistryStore,
    pipelinesRef,
    ensurePipelinesLoaded,
    gateResolver,
    getDashboardBus: () => dashboardBusRef,
    storePaths,
  };
}

// A blind manifold mints wrong gate decisions and drops pushes; refuse to
// boot on an unreadable primary, naming the file. Called after the stores
// have opened (a fresh home creates its files there), so a missing file is
// never mistaken for corruption — see the skip below.
export function checkDatabases(storePaths, { logger = console } = {}) {
  for (const dbPath of [storePaths.runtimeDb, storePaths.opsDb]) {
    const check = checkDb(dbPath);
    // Absent is not corrupt: a fresh home creates its files on first write.
    if (!check.ok && /^missing:/.test(check.error ?? "")) continue;
    if (!check.ok) {
      logger.error(`manifold: refusing boot — unreadable database ${dbPath}: ${check.error}`);
      writeCorruptionAlert({ dbPath, phase: "boot", error: check.error });
      return { ok: false, dbPath, error: check.error };
    }
  }
  return { ok: true };
}
