export function registerPipelineRoutes({ api, pipelinesRef, readStore, ensurePipelinesLoaded }) {
  // GET /pipeclaw/pipelines — catalog of pipeline definitions joined with
  // run-history vitals. Mirrors the dashboard's /api/pipelines but trimmed
  // to what the iPhone Pipelines list view needs at a glance (lastRunAt,
  // todayCount, succeeded/failed today, success-rate over a recent window).
  // No cost (slow JSONL walking lives in the per-pipeline detail route).
  // Historical pipelines (no def on disk but runs in db) are appended last.
  api.registerHttpRoute({
    path: "/pipeclaw/pipelines",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }

      try {
        await ensurePipelinesLoaded?.();
        const nowMs = Date.now();
        // Caller-supplied "today" boundary so the iPhone can bucket by
        // its local-day midnight (the operator thinks in their own TZ).
        // Falls back to UTC midnight to keep behaviour identical for
        // any caller that doesn't send it (parity with the dashboard).
        const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
        const todayParam = url.searchParams.get("todayStartMs");
        const parsedToday = todayParam ? Number(todayParam) : null;
        const todayStartMs = Number.isFinite(parsedToday) && parsedToday > 0
          ? parsedToday
          : (() => { const d = new Date(nowMs); return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime(); })();
        const sevenDaysMs = nowMs - 7 * 86_400_000;

        // SQL GROUP BY + window function replace the JS aggregation loop —
        // two queries instead of iterating 7K rows in JS.
        const agg = await readStore.listAllPipelineStats({ todayStartMs, sevenDaysMs });

        // _synthetic: true — a subflow node's .flow registered as its own
        // addressable pipeline (`${pipelineId}::${nodeId}`) so childFlows
        // can start a real child flow_run against it (see engine/loader.js).
        // Purely an internal dispatch target, never a pipeline an operator
        // runs or sees on its own — must not appear in the catalog.
        const implemented = [...pipelinesRef.value.values()].filter((p) => !p._synthetic).map((p) => {
          const a = agg.get(p.id) ?? null;
          const nodeCount = Object.keys(p.nodes ?? {}).length;
          return {
            id: p.id,
            goal: p.goal ?? null,
            file: p._filepath ? p._filepath.replace(/^.*\//, "") : null,
            nodeCount,
            historical: false,
            cronJobId: typeof p.cronJobId === "string" ? p.cronJobId : null,
            lastRunAtMs: a?.lastRunAtMs ?? null,
            lastRunStatus: a?.lastRunStatus ?? null,
            todayTotal: a?.todayTotal ?? 0,
            todaySucceeded: a?.todaySucceeded ?? 0,
            todayFailed: a?.todayFailed ?? 0,
            todayActive: a?.todayActive ?? 0,
            last7dTotal: a?.last7dTotal ?? 0,
            last7dSucceeded: a?.last7dSucceeded ?? 0,
            last7dFailed: a?.last7dFailed ?? 0,
            totalRuns: a?.totalRuns ?? 0,
          };
        });
        const implementedIds = new Set(implemented.map((p) => p.id));
        const historical = [];
        for (const [pid, a] of agg.entries()) {
          if (implementedIds.has(pid)) continue;
          historical.push({
            id: pid,
            goal: null,
            file: null,
            nodeCount: 0,
            historical: true,
            cronJobId: null,
            lastRunAtMs: a.lastRunAtMs,
            lastRunStatus: a.lastRunStatus,
            todayTotal: a.todayTotal,
            todaySucceeded: a.todaySucceeded,
            todayFailed: a.todayFailed,
            todayActive: a.todayActive,
            last7dTotal: a.last7dTotal,
            last7dSucceeded: a.last7dSucceeded,
            last7dFailed: a.last7dFailed,
            totalRuns: a.totalRuns,
          });
        }

        // Sort by recent activity: implemented-with-runs first (by
        // lastRunAt desc), then implemented-no-runs (alpha), then
        // historical last (lastRunAt desc).
        const sortKey = (p) => p.lastRunAtMs ?? 0;
        const live = implemented.filter((p) => p.lastRunAtMs).sort((a, b) => sortKey(b) - sortKey(a));
        const idle = implemented.filter((p) => !p.lastRunAtMs).sort((a, b) => a.id.localeCompare(b.id));
        historical.sort((a, b) => sortKey(b) - sortKey(a));

        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({
          pipelines: [...live, ...idle, ...historical],
          todayStartMs,
          generatedAtMs: nowMs,
        }));
      } catch (err) {
        api.logger.warn(`pipeclaw pipelines handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });
}
