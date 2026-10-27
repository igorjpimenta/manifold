import path from "node:path";
import os from "node:os";
import { createEventBus } from "../../../dashboard/event-log.js";
import { createDashboardServer } from "../../../dashboard/server.js";

export function registerDashboard(api, { cfg, stores, runtime, pipelinesRef }) {
  const { dashboardEnabled, dashboardPort, dashboardHost } = cfg;
  const {
    readStore, store, failureStore, discussStore, nodeExecutionsStore,
    pipelineRegistryStore, ensurePipelinesLoaded, publishCatalogChanged, setDashboardBus,
  } = stores;
  const { pipelinesWatcher, executor, gateResolver } = runtime;
  // ── Dashboard ────────────────────────────────────────────────────────────

  if (dashboardEnabled) {
    const dashboardStore = readStore;
    const dashboardBus = createEventBus({ store: dashboardStore, nodeExecutionsStore, logger: api.logger });
    setDashboardBus(dashboardBus);
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
      agentsDir: process.env.MANIFOLD_AGENTS_DIR
        || path.join(os.homedir(), ".openclaw", "agents"),
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
}
