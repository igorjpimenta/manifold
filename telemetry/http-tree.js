import { displayRootKind, displayRootLabel, stripTeamPrefix } from "./labels.js";
import {
  peersForKind, ATTR_JOIN, ATTR_ROOT_KIND, ATTR_ROOT_LABEL,
  formatSessionLabel, sessionKindForRoot, formatSessionChildLabel,
  execAll, execGet,
} from "./http-helpers.js";

export function registerTreeRoutes({ api, getStore, logger, json, bad, parseRange }) {
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/tree/expand", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      const store = getStore();
      if (!store) return bad(res, "lineage store not ready");
      const client = store._client;
      try {
        const url = new URL(req.url, "http://x");
        const parent = url.searchParams.get("parent") || "__root__";
        const { where: rangeWhere, args: rangeArgs } = parseRange(url.searchParams);
        const dayClause = rangeWhere.length ? `AND ${rangeWhere.join(" AND ")}` : "";

        const ATTRIBUTED = `(
          SELECT cbc.ts, cbc.cost_total, cbc.flow_id, cbc.pipeline_id,
                 cbc.node_id, cbc.model, cbc.session_id, cbc.session_key,
                 ${ATTR_ROOT_KIND}                                      AS root_kind,
                 ${ATTR_ROOT_LABEL}                                     AS root_label,
                 COALESCE(sl.root_session_key,  cbc.root_session_key)  AS root_session_key
            FROM cost_by_call cbc ${ATTR_JOIN}
        )`;

        let children = [];

        if (parent === "__root__") {
          const rows = await execAll(client, `
            SELECT root_kind, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM ${ATTRIBUTED}
            WHERE root_kind IS NOT NULL ${dayClause}
            GROUP BY root_kind ORDER BY cost DESC NULLS LAST
          `, rangeArgs);
          const triggerNodes = rows.map((r) => ({
            key: `trigger:${r.root_kind}`,
            kind: "trigger",
            label: displayRootKind(r.root_kind),
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: true,
            _multiKind: false,
          }));
          // Merge nodes that translate to the same display label; the
          // highest-cost (first, ORDER BY cost DESC) wins the key. Merged
          // "unknown" becomes non-expandable; named buckets stay expandable.
          const byTriggerLabel = new Map();
          for (const n of triggerNodes) {
            if (byTriggerLabel.has(n.label)) {
              const m = byTriggerLabel.get(n.label);
              m.calls += n.calls;
              m.cost = Number(((m.cost || 0) + (n.cost || 0)).toFixed(6));
              m._multiKind = true;
            } else {
              byTriggerLabel.set(n.label, { ...n });
            }
          }
          children = [...byTriggerLabel.values()]
            .map(({ _multiKind, ...n }) =>
              (_multiKind && n.label === "unknown") ? { ...n, hasChildren: false } : n)
            .sort((a, b) => (b.cost || 0) - (a.cost || 0));

        } else if (parent.startsWith("trigger:")) {
          const kind = parent.slice("trigger:".length);
          const kinds = peersForKind(kind);
          const kindPh = kinds.map(() => "?").join(",");
          const rows = await execAll(client, `
            SELECT root_label, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM ${ATTRIBUTED}
            WHERE root_kind IN (${kindPh}) AND root_label IS NOT NULL ${dayClause}
            GROUP BY root_label ORDER BY cost DESC NULLS LAST
            LIMIT 50
          `, [...kinds, ...rangeArgs]);
          const instanceNodes = rows.map((r) => ({
            key: `instance:${kind}:${r.root_label}`,
            kind: "instance",
            label: displayRootLabel(kind, r.root_label),
            cost: r.cost ?? 0,
            calls: r.calls,
            _multiLabel: false,
          }));
          // Merge instance rows that share a display label (many hand-typed
          // --session-id values all reading "Ad-hoc sessions"). A merged node
          // has no reverse mapping to every raw label it absorbed, so it can't
          // be re-expanded through one instance:<kind>:<rawLabel> key.
          const byInstanceLabel = new Map();
          for (const n of instanceNodes) {
            if (byInstanceLabel.has(n.label)) {
              const m = byInstanceLabel.get(n.label);
              m.calls += n.calls;
              m.cost = Number(((m.cost || 0) + (n.cost || 0)).toFixed(6));
              m._multiLabel = true;
            } else {
              byInstanceLabel.set(n.label, { ...n });
            }
          }
          children = [...byInstanceLabel.values()]
            .map(({ _multiLabel, ...n }) => ({ ...n, hasChildren: !_multiLabel }))
            .sort((a, b) => (b.cost || 0) - (a.cost || 0));

        } else if (parent.startsWith("instance:")) {
          const tail = parent.slice("instance:".length);
          const colonIdx = tail.indexOf(":");
          if (colonIdx < 0) return bad(res, `invalid instance parent: ${parent}`, 400);
          const kind = tail.slice(0, colonIdx);
          const label = tail.slice(colonIdx + 1);
          const kinds = peersForKind(kind);
          const kindPh = kinds.map(() => "?").join(",");
          const rows = await execAll(client, `
            WITH attributed_trigger AS (
              SELECT cbc.ts, cbc.cost_total,
                     COALESCE(sl.root_kind,         cbc.root_kind)   AS root_kind,
                     COALESCE(sl.root_label,        cbc.root_label)  AS root_label,
                     COALESCE(sl.parent_session_key, cbc.session_key) AS trigger_session_key
              FROM cost_by_call cbc
              LEFT JOIN session_lineage sl
                ON cbc.flow_id IS NOT NULL
               AND sl.child_session_key = 'flow:' || cbc.flow_id
               AND sl.source IN ('trigger_hook', 'trigger_backfill')
               AND sl.kind = 'flow-triggered'
            )
            SELECT trigger_session_key AS session_key,
                   COUNT(*) AS calls,
                   ROUND(SUM(cost_total), 6) AS cost,
                   MIN(ts) AS ts_first, MAX(ts) AS ts_last
            FROM attributed_trigger
            WHERE root_kind IN (${kindPh}) AND root_label = ? ${dayClause}
            GROUP BY trigger_session_key
            ORDER BY cost DESC NULLS LAST
            LIMIT 100
          `, [...kinds, label, ...rangeArgs]);
          // hasChildren comes from whether the session actually has expandable
          // children, not a hardcoded true.
          const sessions = rows.map((r) => r.session_key);
          const expandable = new Set();
          if (sessions.length > 0) {
            const ph = sessions.map(() => "?").join(",");
            const triggers = await execAll(client, `
              SELECT DISTINCT parent_session_key FROM session_lineage
              WHERE parent_session_key IN (${ph})
                AND kind = 'flow-triggered'
                AND source IN ('trigger_hook', 'trigger_backfill')
            `, sessions);
            for (const t of triggers) expandable.add(t.parent_session_key);
            const directWhere = ["session_key IN (" + ph + ")", "pipeline_id IS NULL", ...rangeWhere];
            const directs = await execAll(client, `
              SELECT DISTINCT session_key FROM cost_by_call
              WHERE ${directWhere.join(" AND ")}
            `, [...sessions, ...rangeArgs]);
            for (const d of directs) expandable.add(d.session_key);
          }
          children = rows.map((r) => {
            const isFlowKey = r.session_key.startsWith("flow:");
            const flowShort = isFlowKey ? r.session_key.slice("flow:".length, "flow:".length + 8) : null;
            return {
              key: r.session_key,
              kind: isFlowKey ? "flow-triggered" : sessionKindForRoot(kind, r.session_key),
              label: isFlowKey
                ? `flow ${flowShort} · ${formatSessionLabel(null, r.ts_first)}`
                : formatSessionLabel(r.session_key, r.ts_first),
              cost: r.cost ?? 0,
              calls: r.calls,
              hasChildren: expandable.has(r.session_key),
              meta: { ts_first: r.ts_first, ts_last: r.ts_last },
            };
          });

        } else if (parent.startsWith("flow:")) {
          const flowId = parent.slice("flow:".length);
          const rows = await execAll(client, `
            SELECT node_id, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE flow_id = ? AND node_id IS NOT NULL ${dayClause}
            GROUP BY node_id ORDER BY cost DESC NULLS LAST
          `, [flowId, ...rangeArgs]);
          children = rows.map((r) => ({
            key: `node|${flowId}|${r.node_id}`,
            kind: "flow-node",
            label: r.node_id,
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: true,
          }));

        } else if (parent.startsWith("node|")) {
          const parts = parent.split("|");
          if (parts.length !== 3) return bad(res, `invalid node parent: ${parent}`, 400);
          const [, flowId, nodeId] = parts;
          const itemRows = await execAll(client, `
            SELECT item_key, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE flow_id = ? AND node_id = ? AND item_key IS NOT NULL ${dayClause}
            GROUP BY item_key ORDER BY cost DESC NULLS LAST
          `, [flowId, nodeId, ...rangeArgs]);
          children = itemRows.map((r) => ({
            key: `item|${flowId}|${nodeId}|${r.item_key}`,
            kind: "item",
            label: r.item_key,
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: true,
          }));
          const direct = await execGet(client, `
            SELECT COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE flow_id = ? AND node_id = ? AND item_key IS NULL ${dayClause}
          `, [flowId, nodeId, ...rangeArgs]);
          if (direct?.calls > 0) {
            children.push({
              key: `direct-item|${flowId}|${nodeId}`,
              kind: "direct-item",
              label: "Direct (no item)",
              cost: direct.cost ?? 0,
              calls: direct.calls,
              hasChildren: true,
            });
          }

        } else if (parent.startsWith("item|")) {
          const parts = parent.split("|");
          if (parts.length !== 4) return bad(res, `invalid item parent: ${parent}`, 400);
          const [, flowId, nodeId, itemKey] = parts;
          const rows = await execAll(client, `
            SELECT session_key, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE flow_id = ? AND node_id = ? AND item_key = ? ${dayClause}
            GROUP BY session_key ORDER BY cost DESC NULLS LAST
          `, [flowId, nodeId, itemKey, ...rangeArgs]);
          children = rows.map((r) => ({
            key: r.session_key,
            kind: "subagent",
            label: formatSessionChildLabel({ child_key: r.session_key, kind: "flow-node", flow_id: flowId }),
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: false,
          }));

        } else if (parent.startsWith("direct-item|")) {
          const parts = parent.split("|");
          if (parts.length !== 3) return bad(res, `invalid direct-item parent: ${parent}`, 400);
          const [, flowId, nodeId] = parts;
          const rows = await execAll(client, `
            SELECT session_key, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE flow_id = ? AND node_id = ? AND item_key IS NULL ${dayClause}
            GROUP BY session_key ORDER BY cost DESC NULLS LAST
          `, [flowId, nodeId, ...rangeArgs]);
          children = rows.map((r) => ({
            key: r.session_key,
            kind: "subagent",
            label: formatSessionChildLabel({ child_key: r.session_key, kind: "flow-node", flow_id: flowId }),
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: false,
          }));

        } else if (parent.startsWith("direct-pipeline|")) {
          const parts = parent.split("|");
          if (parts.length !== 3) return bad(res, `invalid direct-pipeline parent: ${parent}`, 400);
          const [, kind, label] = parts;
          const rows = await execAll(client, `
            SELECT session_key, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost,
                   MIN(ts) AS ts_first
            FROM ${ATTRIBUTED}
            WHERE root_kind = ? AND root_label = ? AND pipeline_id IS NULL ${dayClause}
            GROUP BY session_key ORDER BY cost DESC NULLS LAST
            LIMIT 100
          `, [kind, label, ...rangeArgs]);
          children = rows.map((r) => ({
            key: r.session_key,
            kind: sessionKindForRoot(kind, r.session_key),
            label: formatSessionLabel(r.session_key, r.ts_first),
            cost: r.cost ?? 0,
            calls: r.calls,
            hasChildren: false,
            meta: { ts_first: r.ts_first },
          }));

        } else if (parent.startsWith("agent:")) {
          const flowEdges = await execAll(client, `
            SELECT flow_id, child_session_key
            FROM session_lineage
            WHERE parent_session_key = ?
              AND source IN ('trigger_hook', 'trigger_backfill')
              AND kind = 'flow-triggered'
              AND flow_id IS NOT NULL
          `, [parent]);
          children = [];
          if (flowEdges.length > 0) {
            const flowIds = flowEdges.map((e) => e.flow_id);
            const ph = flowIds.map(() => "?").join(",");
            const flowRows = await execAll(client, `
              SELECT flow_id, pipeline_id,
                     COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost,
                     MIN(ts) AS ts_first, MAX(ts) AS ts_last
              FROM cost_by_call
              WHERE flow_id IN (${ph}) ${dayClause}
              GROUP BY flow_id
            `, [...flowIds, ...rangeArgs]);
            for (const r of flowRows.sort((a, b) => (b.cost ?? 0) - (a.cost ?? 0))) {
              const pipeName = stripTeamPrefix(r.pipeline_id || "flow").replace(/_/g, "-");
              children.push({
                key: `flow:${r.flow_id}`,
                kind: "flow-triggered",
                label: `${pipeName} · ${r.flow_id.slice(0, 8)} · ${formatSessionLabel(null, r.ts_first)}`,
                cost: r.cost ?? 0,
                calls: r.calls,
                hasChildren: true,
                meta: { flow_id: r.flow_id, ts_first: r.ts_first, ts_last: r.ts_last },
              });
            }
          }
          const direct = await execGet(client, `
            SELECT COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost
            FROM cost_by_call
            WHERE session_key = ? AND pipeline_id IS NULL ${dayClause}
          `, [parent, ...rangeArgs]);
          if (direct?.calls > 0) {
            children.push({
              key: `direct-session|${parent}`,
              kind: "direct-pipeline",
              label: "Direct (this session's own calls)",
              cost: direct.cost ?? 0,
              calls: direct.calls,
              hasChildren: false,
            });
          }

        } else {
          return bad(res, `unknown parent shape: ${parent}`, 400);
        }

        json(res, { parent, children });
      } catch (err) {
        logger?.warn?.(`lineage[http /tree/expand]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });
}
