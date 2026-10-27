import { createExecutor } from "../../../engine/executor.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";
import { createPipelinesWatcher } from "../../../engine/pipelines-watcher.js";

export function createRuntime(api, cfg, stores, pipelinesRef, resumeParentRef) {
  const {
    pipelinesDir, notifyPushEvent, store, ensurePipelinesLoaded, failureStore,
    runStore, nodeExecutionsStore, admissionQueueStore,
    emitCanonical, publishCatalogChanged, pipelineRegistryStore,
    readStore, gatewayBroadcastRef,
  } = stores;
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
    runStore,
    nodeExecutionsStore,
    admissionQueueStore,
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
  return { executor, pipelinesWatcher, gateResolver };
}
