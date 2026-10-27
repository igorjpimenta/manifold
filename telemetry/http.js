// HTTP routes for manifold telemetry. Mounted under /manifold/telemetry/*.
//
// All routes are GET/PUT, read-only except budget cap writes, and return JSON.
// Auth: "plugin" — operator session only, matches dashboard semantics.
//
// Routes:
//   /manifold/telemetry/cost/today                       — running total + top movers
//   /manifold/telemetry/cost/by-root  [?since=&until=]   — group by root sessionKey
//   /manifold/telemetry/cost/by-trigger                  — group by root_kind (cron / telegram / explicit / ...)
//   /manifold/telemetry/cost/by-cron-job                 — group by cron job name
//   /manifold/telemetry/cost/by-pipeline                 — group by pipeline_id
//   /manifold/telemetry/cost/by-flow  [?pipeline_id=]    — group by flow_id (drill-down inside a pipeline)
//   /manifold/telemetry/cost/by-node  [?flow_id=]        — group by node_id (drill-down inside a flow)
//   /manifold/telemetry/cost/by-model                    — group by provider+model
//   /manifold/telemetry/cost/by-day   [?days=14]         — group by local day for the last N days
//   /manifold/telemetry/cost/top-calls [?limit=20]       — most expensive single LLM calls
//   /manifold/telemetry/cost/home                        — combined Home-tile payload

import { registerCostRoutes }    from "./http-cost.js";
import { registerTreeRoutes }    from "./http-tree.js";
import { registerBudgetRoutes }  from "./http-budget.js";
import { registerSessionRoutes } from "./http-sessions.js";

export function registerHttpRoutes({ api, getStore, logger, getBroadcast, sources, lcmDbPath } = {}) {
  const json = (res, body, status = 200) => {
    res.statusCode = status;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(body));
  };
  const bad = (res, msg, status = 503) => json(res, { error: msg }, status);

  const parseRange = (urlParams) => {
    const since = urlParams.get("since");
    const until = urlParams.get("until");
    const args = [];
    const where = [];
    if (since) { const ms = Date.parse(since); if (Number.isFinite(ms)) { where.push("ts >= ?"); args.push(ms); } }
    if (until) { const ms = Date.parse(until); if (Number.isFinite(ms)) { where.push("ts <= ?"); args.push(ms); } }
    return { where, args };
  };

  registerCostRoutes({ api, getStore, logger, json, bad, parseRange });
  registerTreeRoutes({ api, getStore, logger, json, bad, parseRange });
  registerBudgetRoutes({ api, getStore, logger, json, bad, getBroadcast });
  registerSessionRoutes({ api, logger, json, bad, sources, lcmDbPath });
}
