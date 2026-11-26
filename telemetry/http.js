// Telemetry HTTP routes — cost, tree, budget — under /manifold/telemetry/*.

import { registerCostRoutes }    from "./http-cost.js";
import { registerTreeRoutes }    from "./http-tree.js";
import { registerBudgetRoutes }  from "./http-budget.js";

export function registerHttpRoutes({ api, getStore, logger, getBroadcast }) {
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
}
