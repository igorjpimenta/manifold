import { createExecutor } from "../../../engine/executor.js";
import { wrapSubagentKeys } from "./session-keys.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";
import { createPipelinesWatcher } from "../../../engine/pipelines-watcher.js";

// Module-level singleton — captures a working `runtime.subagent` from the
// gateway-bindable plugin load. The `before_dispatch` hook fires outside that
// scope (the dispatcher path), so its bound `api.runtime.subagent.run` throws
// `RequestScopedSubagentRuntimeError`. By saving a reference from the gateway
// load, the hook can forward to Vulcan even though its own `api.runtime` is
// the unavailable variant. Persisted across plugin reloads via module cache.
const moduleState = { workingSubagent: null };

export function createRuntime(api, cfg, stores, pipelinesRef, resumeParentRef) {
  const {
    pipelinesDir, notifyPushEvent, store, ensurePipelinesLoaded, failureStore,
    nodeHealthStore, sessionIndex, runStore, nodeExecutionsStore, admissionQueueStore,
    gatewayReadiness, emitCanonical, publishCatalogChanged, pipelineRegistryStore,
    readStore, gatewayBroadcastRef,
  } = stores;
  // Wrapped at the single injection edge: raw api.runtime.subagent never
  // reaches core directly (wrapSubagentKeys memoizes, so repeated
  // resolves don't re-wrap).
  const getWorkingSubagent = () => moduleState.workingSubagent ?? (api.runtime?.subagent ? wrapSubagentKeys(api.runtime.subagent) : null);
  const executor = createExecutor({
    api,
    config: api.config,
    pipelinesDir,
    notifyPush: notifyPushEvent,
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
    getWorkingSubagent,
    whenGatewayReady: () => gatewayReadiness.whenReady(),
    emit: emitCanonical,
  });
  resumeParentRef.resume = executor.resume;

  const pipelinesWatcher = createPipelinesWatcher({ pipelinesDir, pipelinesRef, logger: api.logger });
  // Single catalog producer (events/catalog.js): identical content to
  // the gateway WS stream and the dashboard bus.
  pipelinesWatcher.onReload((info) => {
    publishCatalogChanged(info).catch(() => {});
  });
  pipelinesWatcher.onReload(() => {
    pipelineRegistryStore.reconcilePipelineVersions(pipelinesRef.value, { logger: api.logger });
  });

  const gateResolver = createGateResolver({
    api, store, executor, getBroadcast: () => gatewayBroadcastRef.current, readStore, runStore,
    pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
    admissionQueueStore,
  });
  return { executor, moduleState, getWorkingSubagent, pipelinesWatcher, gateResolver };
}
