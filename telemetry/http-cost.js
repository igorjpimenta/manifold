// /manifold/telemetry/cost/* routes — aggregate and drill-down cost views.
import {
  displayRootLabel, displayRootKind, stripTeamPrefix,
  UNATTRIBUTED_LABEL, UNATTRIBUTED_FILTER,
} from "./labels.js";
import { currentCapUSD } from "./budget-alert.js";
import {
  ATTR_JOIN, ATTR_ROOT_KIND, ATTR_ROOT_LABEL,
  bootstrapForecast, hasFlowsAttached, execAll, execGet,
} from "./http-helpers.js";

export function registerCostRoutes({ api, getStore, logger, json, bad, parseRange }) {
  const makeGroupHandler = (groupExpr, label, extraCols = []) => async (req, res) => {
    const store = getStore();
    if (!store) return bad(res, "lineage store not ready");
    try {
      const url = new URL(req.url, "http://x");
      const { where, args } = parseRange(url.searchParams);
      const limit = Math.min(Number(url.searchParams.get("limit") || 50), 500);
      for (const col of ["pipeline_id", "flow_id", "node_id", "root_kind", "root_label", "model", "provider"]) {
        const v = url.searchParams.get(col);
        if (v != null && v !== "") { where.push(`${col} = ?`); args.push(v); }
      }
      const w = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const extra = extraCols.length ? `, ${extraCols.join(", ")}` : "";
      const sql = `
        SELECT ${groupExpr} AS ${label}${extra},
               COUNT(*) AS calls,
               SUM(input_tokens)        AS input_tokens,
               SUM(output_tokens)       AS output_tokens,
               SUM(cache_read_tokens)   AS cache_read_tokens,
               SUM(cache_write_tokens)  AS cache_write_tokens,
               ROUND(SUM(cost_total), 6) AS cost
        FROM cost_by_call
        ${w}
        GROUP BY 1${extraCols.length ? ", 2" : ""}
        ORDER BY cost DESC NULLS LAST
        LIMIT ${limit}
      `;
      const rows = await execAll(store._client, sql, args);
      json(res, { dimension: label, rows });
    } catch (err) {
      logger?.warn?.(`lineage[http ${label}]: ${err?.message ?? err}`);
      json(res, { error: String(err?.message ?? err) }, 500);
    }
  };

  // ─── /cost/today ───────────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET",
    path: "/manifold/telemetry/cost/today",
    auth: "plugin",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      const client = store._client;
      const dayClause = `date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')`;
      const summary = await execGet(client, `
        SELECT COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
        FROM cost_by_call WHERE ${dayClause}
      `);
      const byRoot = await execAll(client, `
        SELECT root_label, root_kind, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
        FROM cost_by_call WHERE ${dayClause} AND root_label IS NOT NULL
        GROUP BY root_label, root_kind ORDER BY cost DESC LIMIT 10
      `);
      const byModel = await execAll(client, `
        SELECT model, provider, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
        FROM cost_by_call WHERE ${dayClause} AND model IS NOT NULL
        GROUP BY model, provider ORDER BY cost DESC
      `);
      const _n = new Date();
      const _today = `${_n.getFullYear()}-${String(_n.getMonth()+1).padStart(2,'0')}-${String(_n.getDate()).padStart(2,'0')}`;
      json(res, { today: _today, summary, byRoot, byModel });
    },
  });

  // ─── uniform group-by handlers ─────────────────────────────────────────────
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-root",    auth: "plugin", handler: makeGroupHandler("root_label", "root_label", ["root_kind"]) });
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-trigger", auth: "plugin", handler: makeGroupHandler("root_kind",  "root_kind") });
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-pipeline",auth: "plugin", handler: makeGroupHandler("pipeline_id","pipeline_id") });
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-flow",    auth: "plugin", handler: makeGroupHandler("flow_id",    "flow_id", ["pipeline_id"]) });
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-node",    auth: "plugin", handler: makeGroupHandler("node_id",    "node_id", ["flow_id"]) });

  // ─── /cost/by-session ──────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/by-session", auth: "plugin",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const url = new URL(req.url, "http://x");
        const flowId  = url.searchParams.get("flow_id");
        const nodeId  = url.searchParams.get("node_id");
        if (!flowId || !nodeId) return bad(res, "flow_id and node_id required");
        const rows = await execAll(store._client, `
          SELECT c.session_key, c.item_key,
                 (SELECT sl.agent_id FROM session_lineage sl
                  WHERE sl.child_session_key = c.session_key LIMIT 1) AS agent_id,
                 COUNT(*) AS calls,
                 ROUND(SUM(c.cost_total), 6) AS cost,
                 datetime(MIN(c.ts)/1000, 'unixepoch', 'localtime') AS started_at
          FROM cost_by_call c
          WHERE c.flow_id = ? AND c.node_id = ?
          GROUP BY c.session_key
          ORDER BY MIN(c.ts) ASC
        `, [flowId, nodeId]);
        json(res, { rows });
      } catch (err) {
        logger?.warn?.(`lineage[http by-session]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /cost/runs ────────────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/runs", auth: "plugin",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const url = new URL(req.url, "http://x");
        const limit = Math.min(Number(url.searchParams.get("limit") || 50), 200);
        const pipelineId = url.searchParams.get("pipeline_id");
        const { where: rangeWhere, args } = parseRange(url.searchParams);
        if (await hasFlowsAttached(store._client)) {
          const where = ["c.flow_id IS NOT NULL", ...rangeWhere.map((w) => `c.${w}`)];
          if (pipelineId) { where.push("c.pipeline_id = ?"); args.push(pipelineId); }
          const sql = `
            SELECT c.flow_id, c.pipeline_id,
                   COUNT(*) AS calls,
                   ROUND(SUM(c.cost_total), 6) AS cost,
                   f.status,
                   datetime(MIN(c.ts)/1000, 'unixepoch', 'localtime') AS started_at,
                   MIN(c.ts) AS ts_first, MAX(c.ts) AS ts_last
            FROM cost_by_call c
            LEFT JOIN flows.flow_runs f ON f.flow_id = c.flow_id
            WHERE ${where.join(" AND ")}
            GROUP BY c.flow_id ORDER BY ts_first DESC LIMIT ${limit}
          `;
          json(res, { rows: await execAll(store._client, sql, args) });
        } else {
          const where = ["flow_id IS NOT NULL", ...rangeWhere];
          if (pipelineId) { where.push("pipeline_id = ?"); args.push(pipelineId); }
          const sql = `
            SELECT flow_id, pipeline_id,
                   COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost,
                   NULL AS status,
                   datetime(MIN(ts)/1000, 'unixepoch', 'localtime') AS started_at,
                   MIN(ts) AS ts_first, MAX(ts) AS ts_last
            FROM cost_by_call
            WHERE ${where.join(" AND ")}
            GROUP BY flow_id ORDER BY ts_first DESC LIMIT ${limit}
          `;
          json(res, { rows: await execAll(store._client, sql, args) });
        }
      } catch (err) {
        logger?.warn?.(`lineage[http /runs]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-model",   auth: "plugin", handler: makeGroupHandler("model", "model", ["provider"]) });

  // ─── /cost/by-cron-job ─────────────────────────────────────────────────────
  api.registerHttpRoute({ method: "GET", path: "/manifold/telemetry/cost/by-cron-job", auth: "plugin", handler: async (req, res) => {
    const store = getStore();
    if (!store) return bad(res, "lineage store not ready");
    try {
      const url = new URL(req.url, "http://x");
      const { where, args } = parseRange(url.searchParams);
      where.push(`root_kind = 'cron-owner'`);
      const sql = `
        SELECT root_label AS cron_job, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
        FROM cost_by_call WHERE ${where.join(" AND ")}
        GROUP BY 1 ORDER BY cost DESC NULLS LAST LIMIT 50
      `;
      json(res, { dimension: "cron_job", rows: await execAll(store._client, sql, args) });
    } catch (err) { json(res, { error: String(err?.message ?? err) }, 500); }
  } });

  // ─── /cost/by-day ──────────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/by-day", auth: "plugin",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const sp = new URL(req.url, "http://x").searchParams;
        const since = sp.get("since");
        const until = sp.get("until");
        let rows, days;
        if (since) {
          const sinceMs = new Date(since).getTime();
          const untilMs = until ? new Date(until).getTime() : Date.now();
          days = Math.round((untilMs - sinceMs) / (24 * 3600 * 1000));
          rows = await execAll(store._client, `
            SELECT date(ts/1000, 'unixepoch', 'localtime') AS day,
                   COUNT(*) AS calls, ROUND(SUM(cost_total), 4) AS cost
            FROM cost_by_call WHERE ts >= ? AND ts < ?
            GROUP BY day ORDER BY day ASC
          `, [sinceMs, untilMs]);
        } else {
          days = Math.min(Number(sp.get("days") || 90), 90);
          rows = await execAll(store._client, `
            SELECT date(ts/1000, 'unixepoch', 'localtime') AS day,
                   COUNT(*) AS calls, ROUND(SUM(cost_total), 4) AS cost
            FROM cost_by_call
            WHERE ts >= strftime('%s', date('now', 'localtime', '-' || ? || ' days')) * 1000
            GROUP BY day ORDER BY day ASC
          `, [days]);
        }
        json(res, { days, rows, cap: currentCapUSD() });
      } catch (err) {
        logger?.warn?.(`lineage[http /by-day]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /cost/by-hour ─────────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/by-hour", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const now = new Date();
        const sodMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
        const fullDayMs = 24 * 3600 * 1000;
        const currentHour = now.getHours();

        const rawRows = await execAll(store._client, `
          SELECT CAST(strftime('%H', ts/1000, 'unixepoch', 'localtime') AS INTEGER) AS hour,
                 COUNT(*) AS calls,
                 ROUND(SUM(cost_total), 6) AS cost
          FROM cost_by_call
          WHERE date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')
          GROUP BY hour ORDER BY hour ASC
        `);
        const rowMap = new Map(rawRows.map((r) => [r.hour, r]));
        const rows = [];
        for (let h = 0; h <= currentHour; h++) {
          rows.push(rowMap.get(h) ?? { hour: h, calls: 0, cost: 0 });
        }

        const rawHist = await execAll(store._client, `
          SELECT
            date(ts/1000, 'unixepoch', 'localtime') AS day,
            CAST(strftime('%H', ts/1000, 'unixepoch', 'localtime') AS INTEGER) AS hour,
            SUM(cost_total) AS hourly_cost
          FROM cost_by_call
          WHERE ts >= ? AND ts < ?
          GROUP BY day, hour ORDER BY day, hour
        `, [sodMs - 14 * fullDayMs, sodMs]);

        const byDay = new Map();
        for (const r of rawHist) {
          if (!byDay.has(r.day)) byDay.set(r.day, new Map());
          byDay.get(r.day).set(r.hour, r.hourly_cost);
        }
        const dayCumuls = [];
        for (const [, hourMap] of byDay) {
          let sum = 0;
          const cumul = [];
          for (let h = 0; h <= 23; h++) { sum += hourMap.get(h) ?? 0; cumul.push(sum); }
          dayCumuls.push(cumul);
        }

        const histBands = [];
        for (let h = 0; h <= 23; h++) {
          const vals = dayCumuls.map((c) => c[h]).sort((a, b) => a - b);
          if (!vals.length) { histBands.push({ hour: h, p50: 0, p95: 0 }); continue; }
          const p50 = vals[Math.floor((vals.length - 1) * 0.50)];
          const p95 = vals[Math.floor((vals.length - 1) * 0.95)];
          histBands.push({ hour: h, p50: Number(p50.toFixed(6)), p95: Number(p95.toFixed(6)) });
        }

        const rawBand = bootstrapForecast(byDay, currentHour, rows);
        const currentCumul = rows.reduce((s, r) => s + r.cost, 0);
        const forecastBand = rawBand ? [
          { hour: currentHour, p10: currentCumul, p50: currentCumul, p90: currentCumul },
          ...rawBand,
        ] : null;

        json(res, { rows, currentHour, histBands, cap: currentCapUSD(), forecastBand });
      } catch (err) {
        logger?.warn?.(`lineage[http /by-hour]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /cost/top-calls ───────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/top-calls", auth: "plugin",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const url = new URL(req.url, "http://x");
        const limit = Math.min(Number(url.searchParams.get("limit") || 20), 500);
        const { where, args } = parseRange(url.searchParams);
        const filterCols = ["pipeline_id", "flow_id", "node_id", "session_key", "root_kind", "root_label", "model", "provider"];
        for (const col of filterCols) {
          const v = url.searchParams.get(col);
          if (v == null || v === "") continue;
          // "__null__" sentinel = IS NULL filter (used for direct-pipeline leaf drill-down)
          if (v === "__null__") { where.push(`${col} IS NULL`); }
          else                  { where.push(`${col} = ?`); args.push(v); }
        }
        const w = where.length ? `WHERE ${where.join(" AND ")}` : "";
        const sql = `
          WITH attributed AS (
            SELECT cbc.ts, cbc.cost_total, cbc.flow_id, cbc.pipeline_id,
                   cbc.node_id, cbc.model, cbc.provider,
                   cbc.input_tokens, cbc.output_tokens,
                   cbc.cache_read_tokens, cbc.cache_write_tokens,
                   cbc.total_tokens,
                   cbc.cost_input, cbc.cost_output,
                   cbc.cost_cache_read, cbc.cost_cache_write,
                   cbc.session_id, cbc.session_key, cbc.agent_id,
                   cbc.item_key, cbc.msg_id,
                   ${ATTR_ROOT_KIND}  AS root_kind,
                   ${ATTR_ROOT_LABEL} AS root_label
              FROM cost_by_call cbc ${ATTR_JOIN}
          )
          SELECT datetime(ts/1000, 'unixepoch', 'localtime') AS local_ts,
                 provider, model, total_tokens,
                 input_tokens, output_tokens,
                 cache_read_tokens, cache_write_tokens,
                 ROUND(cost_total, 6)        AS cost,
                 ROUND(cost_input, 6)        AS cost_input,
                 ROUND(cost_output, 6)       AS cost_output,
                 ROUND(cost_cache_read, 6)   AS cost_cache_read,
                 ROUND(cost_cache_write, 6)  AS cost_cache_write,
                 root_kind, root_label, pipeline_id, flow_id, node_id,
                 session_id, session_key, item_key, msg_id
          FROM attributed
          ${w}
          ORDER BY cost_total DESC NULLS LAST LIMIT ?
        `;
        const rows = await execAll(store._client, sql, [...args, limit]);
        for (const r of rows) {
          r.display_root_label = displayRootLabel(r.root_kind, r.root_label);
          r.display_root_kind  = displayRootKind(r.root_kind);
        }
        json(res, { limit, rows });
      } catch (err) {
        logger?.warn?.(`lineage[http /top-calls]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /cost/breakdown ───────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/breakdown", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const url = new URL(req.url, "http://x");
        const dim = url.searchParams.get("dim") || "trigger";
        const { where, args } = parseRange(url.searchParams);
        const limit = Math.min(Number(url.searchParams.get("limit") || 100), 500);
        for (const col of ["pipeline_id", "flow_id", "node_id", "root_kind", "root_label", "model", "provider"]) {
          const v = url.searchParams.get(col);
          if (v == null || v === "") continue;
          // "__null__" sentinel = IS NULL, mirroring /cost/top-calls above.
          // The pipeline dim's transform emits this for its (unattributed)
          // row, so a drill-through on that row selects the NULL rows it was
          // computed from instead of matching the synthetic label literally.
          if (v === "__null__") { where.push(`${col} IS NULL`); }
          else                  { where.push(`${col} = ?`); args.push(v); }
        }
        const w = where.length ? `AND ${where.join(" AND ")}` : "";

        const ATTRIBUTED = `(
          SELECT cbc.ts, cbc.cost_total, cbc.flow_id, cbc.pipeline_id,
                 cbc.node_id, cbc.model, cbc.provider,
                 cbc.input_tokens, cbc.output_tokens,
                 cbc.cache_read_tokens, cbc.cache_write_tokens,
                 cbc.session_id, cbc.session_key, cbc.agent_id, cbc.msg_id,
                 ${ATTR_ROOT_KIND}  AS root_kind,
                 ${ATTR_ROOT_LABEL} AS root_label
            FROM cost_by_call cbc ${ATTR_JOIN}
        )`;

        const flowsAttached = await hasFlowsAttached(store._client);
        const dims = {
          trigger: {
            sql: `SELECT root_kind AS raw_label, root_kind AS kind_for_label,
                         COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
                  FROM ${ATTRIBUTED}
                  WHERE root_kind IS NOT NULL ${w}
                  GROUP BY 1 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`,
            transform: (r) => ({
              label: displayRootKind(r.raw_label),
              kind: null,
              calls: r.calls, cost: r.cost,
              filter: { root_kind: r.raw_label },
            }),
          },
          root: {
            sql: `SELECT root_label AS raw_label, root_kind AS kind_for_label,
                         COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
                  FROM ${ATTRIBUTED}
                  WHERE root_label IS NOT NULL ${w}
                  GROUP BY 1, 2 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`,
            transform: (r) => ({
              label: displayRootLabel(r.kind_for_label, r.raw_label),
              kind: displayRootKind(r.kind_for_label),
              calls: r.calls, cost: r.cost,
              filter: { root_label: r.raw_label, root_kind: r.kind_for_label },
            }),
          },
          pipeline: {
            // COALESCE rather than a `pipeline_id IS NOT NULL` filter. A
            // cost_by_call row can legitimately have no pipeline_id: its
            // lineage walk landed on a root that isn't a flow (cron-owned
            // agent turns, operator ad-hoc sessions, spawned subagents,
            // Telegram DMs). Filtering those out made them vanish from this
            // breakdown while still counting toward /cost/home's
            // unconditional SUM(cost_total) — so the top card showed a total
            // that silently exceeded the sum of this list, and the operator
            // had no way to account for the difference. Surfacing them as a
            // labeled bucket keeps the two numbers reconcilable and makes any
            // future unattributed spend self-diagnosing.
            sql: flowsAttached
              ? `SELECT COALESCE(c.pipeline_id, '${UNATTRIBUTED_LABEL}') AS raw_label,
                         COUNT(*) AS calls,
                         ROUND(SUM(c.cost_total), 6) AS cost,
                         COUNT(DISTINCT CASE WHEN f.status = 'succeeded' THEN c.flow_id END) AS succeeded_runs,
                         COUNT(DISTINCT CASE WHEN f.status = 'failed'    THEN c.flow_id END) AS failed_runs,
                         COUNT(DISTINCT CASE WHEN f.status NOT IN ('succeeded','failed') AND f.status IS NOT NULL THEN c.flow_id END) AS other_runs,
                         COUNT(DISTINCT c.flow_id) AS total_runs
                  FROM ${ATTRIBUTED} c
                  LEFT JOIN flows.flow_runs f ON f.flow_id = c.flow_id
                  WHERE 1=1 ${w.replace(/\b(ts|cost_total|root_kind|root_label|model|provider|input_tokens|output_tokens|cache_read_tokens|cache_write_tokens|pipeline_id|flow_id|node_id|item_key|session_id|session_key|agent_id)\b/g, "c.$1")}
                  GROUP BY 1 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`
              : `SELECT COALESCE(pipeline_id, '${UNATTRIBUTED_LABEL}') AS raw_label,
                         COUNT(*) AS calls,
                         ROUND(SUM(cost_total), 6) AS cost,
                         NULL AS succeeded_runs, NULL AS failed_runs,
                         NULL AS other_runs,     NULL AS total_runs
                  FROM ${ATTRIBUTED}
                  WHERE 1=1 ${w}
                  GROUP BY 1 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`,
            transform: (r) => {
              // The COALESCE above synthesizes UNATTRIBUTED_LABEL for NULL
              // pipeline_ids; stripTeamPrefix is a no-op on it (no underscore),
              // so pass it through verbatim rather than deriving a label from
              // a value that was never stored.
              const unattributed = r.raw_label === UNATTRIBUTED_LABEL;
              const out = {
                label: unattributed ? UNATTRIBUTED_LABEL : stripTeamPrefix(r.raw_label),
                kind: "Pipeline",
                calls: r.calls, cost: r.cost,
                // Not `{ pipeline_id: '(unattributed)' }` — no row stores that
                // string, so it would select nothing. The __null__ sentinel is
                // what actually resolves back to these rows, and the filter
                // loop above translates it to `pipeline_id IS NULL`.
                filter: unattributed ? { ...UNATTRIBUTED_FILTER } : { pipeline_id: r.raw_label },
              };
              // Outcome counters are meaningless for the unattributed bucket:
              // its rows carry no flow_id, so total_runs is 0 and the guard
              // already excludes it. Stated explicitly so a future change that
              // gives those rows a flow_id can't start emitting a success_rate
                // a client ROI filter would read as a real pipeline signal.
              if (!unattributed && r.succeeded_runs != null && r.total_runs > 0) {
                out.succeeded_runs = r.succeeded_runs;
                out.failed_runs    = r.failed_runs;
                out.other_runs     = r.other_runs;
                out.total_runs     = r.total_runs;
                if (r.succeeded_runs > 0) {
                  out.cost_per_success = Number((r.cost / r.succeeded_runs).toFixed(6));
                }
                if ((r.succeeded_runs + r.failed_runs) > 0) {
                  out.success_rate = Number(
                    (r.succeeded_runs / (r.succeeded_runs + r.failed_runs)).toFixed(4)
                  );
                }
              }
              return out;
            },
          },
          "cron-job": {
            sql: `SELECT root_label AS raw_label,
                         COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
                  FROM ${ATTRIBUTED}
                  WHERE root_kind = 'cron-owner' AND root_label IS NOT NULL ${w}
                  GROUP BY 1 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`,
            transform: (r) => ({
              // raw_label is the cron job's own free-text name (opaque, no
              // shape contract) — shown as-is, see labels.js's cron-owner case.
              label: r.raw_label,
              kind: "Cron",
              calls: r.calls, cost: r.cost,
              filter: { root_label: r.raw_label, root_kind: "cron-owner" },
            }),
          },
          model: {
            sql: `SELECT model AS raw_label, provider AS extra,
                         COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
                  FROM ${ATTRIBUTED}
                  WHERE model IS NOT NULL ${w}
                  GROUP BY 1, 2 ORDER BY cost DESC NULLS LAST LIMIT ${limit}`,
            transform: (r) => ({
              label: r.raw_label,
              kind: r.extra,
              calls: r.calls, cost: r.cost,
              filter: { model: r.raw_label, ...(r.extra ? { provider: r.extra } : {}) },
            }),
          },
        };
        const def = dims[dim];
        if (!def) return bad(res, `unknown dim: ${dim} (expected: trigger|root|pipeline|cron-job|model)`, 400);
        const raw = await execAll(store._client, def.sql, args);
        let rows = raw.map(def.transform);
        if (dim === "trigger") {
          const byLabel = new Map();
          for (const r of rows) {
            if (byLabel.has(r.label)) {
              const m = byLabel.get(r.label);
              m.calls += r.calls;
              m.cost = Number((m.cost + (r.cost || 0)).toFixed(6));
              delete m.filter;
            } else {
              byLabel.set(r.label, { ...r });
            }
          }
          rows = [...byLabel.values()].sort((a, b) => (b.cost || 0) - (a.cost || 0));
        }
        const total = rows.reduce((a, r) => a + (r.cost || 0), 0);
        json(res, { dimension: dim, total, rows });
      } catch (err) {
        logger?.warn?.(`lineage[http /breakdown]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /cost/home ────────────────────────────────────────────────────────────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/cost/home", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      try {
        const client = store._client;
        const cap = currentCapUSD();
        const dayClause = `date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime')`;
        const yClause   = `date(ts/1000, 'unixepoch', 'localtime') = date('now', 'localtime', '-1 day')`;

        const today = await execGet(client, `
          SELECT COUNT(*) AS calls, COALESCE(ROUND(SUM(cost_total), 6), 0) AS cost
          FROM cost_by_call WHERE ${dayClause}
        `);
        const yest = await execGet(client, `
          SELECT COUNT(*) AS calls, COALESCE(ROUND(SUM(cost_total), 6), 0) AS cost
          FROM cost_by_call WHERE ${yClause}
        `);
        const byRootRaw = await execAll(client, `
          SELECT root_label, root_kind, COUNT(*) AS calls,
                 ROUND(SUM(cost_total), 6) AS cost
          FROM cost_by_call
          WHERE ${dayClause} AND root_label IS NOT NULL
          GROUP BY root_label, root_kind ORDER BY cost DESC LIMIT 5
        `);
        const byRoot = byRootRaw.map((r) => ({
          ...r,
          display_label: displayRootLabel(r.root_kind, r.root_label),
          display_kind:  displayRootKind(r.root_kind),
        }));
        const byDay = await execAll(client, `
          SELECT date(ts/1000, 'unixepoch', 'localtime') AS day,
                 ROUND(SUM(cost_total), 6) AS cost
          FROM cost_by_call
          WHERE ts >= strftime('%s', date('now', 'localtime', '-14 days')) * 1000
          GROUP BY day ORDER BY day ASC
        `);
        const topCall = await execGet(client, `
          SELECT datetime(ts/1000, 'unixepoch', 'localtime') AS local_ts,
                 provider, model, total_tokens,
                 ROUND(cost_total, 6) AS cost,
                 root_kind, root_label, pipeline_id, flow_id, node_id, session_id
          FROM cost_by_call
          WHERE ${dayClause} AND cost_total IS NOT NULL
          ORDER BY cost_total DESC LIMIT 1
        `);
        if (topCall) {
          topCall.display_root_label = displayRootLabel(topCall.root_kind, topCall.root_label);
        }

        const now = new Date();
        const sodMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
        const elapsedMs = Date.now() - sodMs;
        const fullDayMs = 24 * 3600 * 1000;
        const elapsedFrac = elapsedMs / fullDayMs;
        let forecast = null;
        if (elapsedMs > 30 * 60 * 1000 && today.cost > 0) {
          forecast = Number((today.cost * (fullDayMs / elapsedMs)).toFixed(4));
        }

        const comparison = yest.cost > 0
          ? Number((today.cost / yest.cost).toFixed(2))
          : null;

        const yestSameHour = await execGet(client, `
          SELECT COALESCE(ROUND(SUM(cost_total), 6), 0) AS cost
          FROM cost_by_call WHERE ts >= ? AND ts < ?
        `, [sodMs - fullDayMs, Date.now() - fullDayMs]);
        const compareAtSameHour = (yestSameHour?.cost > 0)
          ? Number((today.cost / yestSameHour.cost).toFixed(2))
          : null;

        const avg7Day = await execGet(client, `
          SELECT COUNT(*) AS day_count, AVG(daily_cost) AS avg
          FROM (
            SELECT SUM(cost_total) AS daily_cost
            FROM cost_by_call WHERE ts >= ? AND ts < ?
            GROUP BY date(ts/1000, 'unixepoch', 'localtime')
          )
        `, [sodMs - 7 * fullDayMs, sodMs]);
        const expectedAtThisHour = (avg7Day?.avg != null && avg7Day.avg > 0 && avg7Day.day_count >= 3)
          ? Number((avg7Day.avg * elapsedFrac).toFixed(4))
          : null;

        const currentHour = now.getHours();
        const rawHist = await execAll(client, `
          SELECT date(ts/1000, 'unixepoch', 'localtime') AS day,
                 CAST(strftime('%H', ts/1000, 'unixepoch', 'localtime') AS INTEGER) AS hour,
                 SUM(cost_total) AS hourly_cost
          FROM cost_by_call WHERE ts >= ? AND ts < ?
          GROUP BY day, hour ORDER BY day, hour
        `, [sodMs - 14 * fullDayMs, sodMs]);
        const histByDay = new Map();
        for (const r of rawHist) {
          if (!histByDay.has(r.day)) histByDay.set(r.day, new Map());
          histByDay.get(r.day).set(r.hour, r.hourly_cost);
        }
        const fcBand    = bootstrapForecast(histByDay, currentHour, [{ cost: today.cost }]);
        const midnight  = fcBand?.[fcBand.length - 1] ?? null;

        json(res, {
          today: `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`,
          cap,
          summary: today,
          yesterday: yest,
          byRoot,
          byDay,
          topCall,
          forecastUntilMidnight: forecast,
          forecastP10: midnight ? Number(midnight.p10.toFixed(4)) : null,
          forecastP50: midnight ? Number(midnight.p50.toFixed(4)) : null,
          forecastP90: midnight ? Number(midnight.p90.toFixed(4)) : null,
          compareYesterday: comparison,
          compareAtSameHour,
          expectedAtThisHour,
        });
      } catch (err) {
        logger?.warn?.(`lineage[http /home]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });
}
