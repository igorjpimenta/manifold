import { createPipelinesLoader } from "../../../engine/loader.js";
import { createStateStore } from "../../../state/state-store.js";
import { createRunStore } from "../../../state/run-store.js";
import { registerOwnerResolver } from "../../../locks/lock.js";
import { createInvocationOwnerResolver } from "../../../locks/invocation-owner.js";
import { getNodeExecutionsStore } from "../../../state/node-executions.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { configureGlobalConcurrency, getGlobalConcurrencyLimit } from "../../../engine/global-concurrency.js";
import { createFailureStore } from "../../../state/failure-store.js";
import { createNodeHealthStore } from "../../../state/node-health-store.js";
import { createDiscussStore } from "../../../state/discuss-store.js";
import { createSessionIndex } from "../../../state/session-index.js";
import { createPipelineRegistryStore } from "../../../state/pipeline-registry.js";
import { createEmitter } from "../../../events/emit.js";
import { createCatalogPublisher } from "../../../events/catalog.js";
import { resolveStorePaths, ensureMigrated, stateFile } from "../../../state/paths.js";
import { setLocksDir } from "../../../locks/lock.js";
import { setStateFile } from "../../../gates/paused-flows.js";
import { createPushTargetsStore } from "../../../state/push-targets.js";
import { resolvePushTarget, pushPayload, notifyPush } from "../../../events/push.js";
import { createEventStream } from "../../../events/stream.js";

export function createStores(api, cfg, pipelinesRef, resumeParentRef) {
  // Core owns its store home (state/paths.js): one MANIFOLD_DATA_DIR
  // move relocates every DB, lock, and pending file. Legacy
  // ~/.openclaw/{runtime.sqlite,ops.sqlite} are copied in once, never
  // moved, and never read again afterwards.
  const storePaths = resolveStorePaths();
  const runtimeDbPath = storePaths.runtimeDb;
  ensureMigrated({ dataDir: storePaths.dataDir, logger: api.logger });
  setLocksDir(storePaths.locksDir);
  setStateFile(stateFile(storePaths.dataDir));
  // Pipeline dir resolution (table[harness] → MANIFOLD_PIPELINES_DIR env →
  // config `pipelinesDir` → <dataDir>/workspace/.pipelines). The table lets
  // an operator relocate one harness without touching env or config; sync
  // open-per-op store, so this resolves before any service starts.
  const pipelineDirsStore = createPipelineDirsStore({ dbPath: runtimeDbPath, logger: api.logger });
  const pipelinesDir = pipelineDirsStore.resolveDir({
    harness: cfg.pushHarness,
    env: process.env,
    dataDir: storePaths.dataDir,
    configDir: cfg.pipelinesConfigDir,
  }).dir;
  const resolvePipelinesDir = (harness) => pipelineDirsStore.resolveDir({
    harness: harness ?? cfg.pushHarness,
    env: process.env,
    dataDir: storePaths.dataDir,
    configDir: cfg.pipelinesConfigDir,
  });
  // One shared ceiling for every concurrency mechanism in the engine —
  // agent dispatch, subflow fan-out, and whole-flow-run concurrency alike
  // (see global-concurrency.js). agentDispatchConcurrency/
  // childFanoutConcurrency were two independent config keys for what are
  // now the same number; globalConcurrency replaces both.
  configureGlobalConcurrency({ maxConcurrent: cfg.globalConcurrency });

  const runStore    = createRunStore({ logger: api.logger, dbPath: runtimeDbPath });
  const nodeExecutionsStore = getNodeExecutionsStore(runtimeDbPath);
  const readStore = runStore;
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
  // Constructed before `store` — createStateStore's own
  // triggerParentResume hook needs failureStore to
  // record a real failure_events row when a subflow's mode isn't
  // satisfied, not just flip flow_runs.status.
  const gatewayBroadcastRef = { current: null };
  const emitCanonical = createEmitter({ getBroadcast: () => gatewayBroadcastRef.current, logger: api.logger }).emit;
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
    record(data) {
      return _sessionIndexPromise.then((si) => si.record(data));
    },
    hasSessionForNode(flowId, nodeId) { return _sessionIndexPromise.then(si => si.hasSessionForNode(flowId, nodeId)); },
    flowIdForSessionKey(sessionKey) { return _sessionIndexPromise.then(si => si.flowIdForSessionKey(sessionKey)); },
    close() { _sessionIndexPromise.then(si => si.close?.()).catch(() => {}); },
  };
  // Assigned in the dashboard section below; the catalog publisher reads
  // it lazily so watcher reloads before dashboard boot stay WS-only.
  let dashboardBusRef = null;
  const publishCatalogChanged = createCatalogPublisher({
    emit: emitCanonical,
    getDashboardBroadcast: () => (dashboardBusRef ? (e) => dashboardBusRef.broadcast(e) : null),
  });
  // Push delivery (events/push.js): dispatch/gate events POST to the
  // harness's registered target so it can wake the receiver. The store
  // is async-opened but register() is sync — same lazy Promise-proxy
  // shape as nodeHealthStore/sessionIndex above. Best-effort and
  // guarded: a failed lookup or POST never breaks the run.
  const _pushTargetsPromise = createPushTargetsStore({ logger: api.logger, dbPath: runtimeDbPath });
  const pushStream = createEventStream();
  async function notifyPushEvent({ event, flowId = null, nodeId = null, pipelineId = null, sessionKey = null, agent = null }) {
    const payload = pushPayload({ event, flowId, nodeId, pipelineId, sessionKey, agent, harnessId: cfg.pushHarness });
    pushStream.append(payload);
    let dbEntry = null;
    try {
      const row = await _pushTargetsPromise.then((ps) => ps.get(pushHarness));
      if (row) dbEntry = { url: row.url, bearer: row.bearer, timeoutMs: row.timeoutMs };
    } catch {}
    const target = resolvePushTarget({ pushTargets: dbEntry, harnessId: cfg.pushHarness, config: cfg.pushConfig });
    if (!target) return { ok: false, reason: "disabled" };
    return notifyPush({ target, payload, logger: api.logger });
  }
  return {
    storePaths, runtimeDbPath, pipelineDirsStore, pipelinesDir, resolvePipelinesDir,
    runStore, readStore, nodeExecutionsStore, admissionQueueStore, failureStore,
    pipelineRegistryStore, store, ensurePipelinesLoaded, nodeHealthStore, discussStore,
    sessionIndex, gatewayBroadcastRef, emitCanonical,
    publishCatalogChanged, setDashboardBus: (bus) => { dashboardBusRef = bus; },
    pushStream, notifyPushEvent, pushTargetsPromise: _pushTargetsPromise,
  };
}
