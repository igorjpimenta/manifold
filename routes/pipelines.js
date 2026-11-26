import { authorizeBearerRequest, sendUnauthorized } from "./bearer-auth.js";

export function registerPipelineRoutes({ api, pipelinesRef, readStore, ensurePipelinesLoaded }) {
  // Catalog of pipeline definitions joined with run-history vitals, trimmed
  // to what a list view needs (no cost — that's the detail route).
  // Historical pipelines (no def on disk but runs in db) append last.
  api.registerHttpRoute({
    path: "/manifold/pipelines",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }

      try {
        await ensurePipelinesLoaded?.();
        const nowMs = Date.now();
      // Caller-supplied "today" boundary so clients can bucket by their own
      // local-day midnight; UTC midnight fallback matches callers that omit it.
        const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
        const todayParam = url.searchParams.get("todayStartMs");
        const parsedToday = todayParam ? Number(todayParam) : null;
        const todayStartMs = Number.isFinite(parsedToday) && parsedToday > 0
          ? parsedToday
          : (() => { const d = new Date(nowMs); return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime(); })();
        const sevenDaysMs = nowMs - 7 * 86_400_000;

        const agg = await readStore.listAllPipelineStats({ todayStartMs, sevenDaysMs });

        // _synthetic: a subflow node's .flow registered as its own
        // addressable pipeline (`${pipelineId}::${nodeId}`) for childFlows
        // dispatch (engine/loader.js) — internal target, never shown to an
        // operator, so it must not appear in the catalog.
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

        // Recent activity first: implemented-with-runs (lastRunAt desc),
        // then implemented-no-runs (alpha), then historical (lastRunAt desc).
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
        api.logger.warn(`manifold pipelines handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
      }
      return true;
    },
  });
}
