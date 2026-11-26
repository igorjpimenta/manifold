// Single producer for pipeline-catalog changes: one watcher reload feeds both
// sinks with identical content — the gateway WS stream
// (pipeline.catalog.updated) and the dashboard bus (canonical kind stamped).
// Separate sinks stay separate; only content decisions merge here.
export function createCatalogPublisher({ emit, getDashboardBroadcast }) {
  return async function publishCatalogChanged(info) {
    const now = Date.now();
    const payload = { count: info.size, ids: info.ids, updatedAt: now };
    if (emit) {
      try { await emit("pipeline.catalog.updated", payload); } catch {}
    }
    try {
      const broadcastDashboard = getDashboardBroadcast?.();
      if (broadcastDashboard) {
        await broadcastDashboard({
          kind: "pipeline.catalog.updated",
          at: now,
          size: payload.count,
          ids: payload.ids,
        });
      }
    } catch {}
  };
}
