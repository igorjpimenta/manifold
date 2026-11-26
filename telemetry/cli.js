// Mirrors the http-cost.js endpoints that back the dashboard views — a query
// change here must land there too, or CLI and dashboard diverge.

import { execAll, execGet } from "./http-helpers.js";

export function registerCliCommands({ program, logger, getStore, getResolver, dbPath }) {
  const cmd = program.command("lineage").description("Session lineage + cost attribution");

  cmd.command("lookup <sessionOrId>")
    .description("Walk lineage for a session (or sessionId — looked up via cost_by_call)")
    .action(async (target) => {
      const store = await getStore(); const resolver = await getResolver();
      if (!store || !resolver) { console.error("lineage service not ready"); process.exitCode = 1; return; }
      const key = target.startsWith("agent:") ? target : await sessionIdToSession(store._client, target);
      if (!key) { console.error(`could not resolve ${target} to a session`); process.exitCode = 1; return; }
      const chain = await resolver.walk(key);
      for (let i = 0; i < chain.length; i++) {
        const f = chain[i];
        const indent = "  ".repeat(i);
        console.log(`${indent}[${f.kind}] ${f.key}`);
        const extras = [];
        if (f.pipelineId) extras.push(`pipeline=${f.pipelineId}`);
        if (f.flowId)     extras.push(`flow=${f.flowId.slice(0,8)}`);
        if (f.nodeId)     extras.push(`node=${f.nodeId}`);
        if (f.itemKey)    extras.push(`item=${f.itemKey.slice(0,40)}`);
        if (extras.length) console.log(`${indent}  ${extras.join(" ")}`);
      }
      const root = chain[chain.length - 1];
      if (root) console.log(`\nROOT: ${root.rootLabel ?? root.key} (kind=${root.kind})`);
    });

  cmd.command("tree <sessionOrId>")
    .description("Genealogy tree — descendants of a session with per-node $cost. By default starts at the root of the chain; pass --from-here to root at the given node instead.")
    .option("--from-here", "Root the tree at the given key, not at its chain's root")
    .option("--depth <n>", "Max descendant depth (default 12)", "12")
    .option("--show-zero", "Include zero-cost subtrees (default: collapse them)")
    .option("--with-calls", "Drop individual LLM calls as leaves under each session (model, tokens, $)")
    .requiredOption("--kind <kind>", "Input shape (REQUIRED): session-key | session-id | msg | flow | pipeline | cron | run")
    .action(async (target, opts) => {
      const store = await getStore(); const resolver = await getResolver();
      if (!store || !resolver) { console.error("lineage service not ready"); process.exitCode = 1; return; }
      const client = store._client;
      let key = await resolveTreeTarget(target, client, opts.kind);
      if (!key) {
        console.error(`could not resolve ${target} via --kind ${opts.kind}`);
        process.exitCode = 1; return;
      }
      if (!opts.fromHere) {
        const row = await execGet(client, `SELECT root_session_key FROM session_lineage WHERE child_session_key = ?`, [key]);
        if (row?.root_session_key) key = row.root_session_key;
      }
      const maxDepth = Math.max(1, Number(opts.depth) || 12);
      const showZero = Boolean(opts.showZero);
      const withCalls = Boolean(opts.withCalls);
      // The recursion follows the flow_id side-link as well as
      // parent_session_key: a synthetic `flow:<id>` trigger edge pulls in
      // every session sharing that flow_id. Without it a cron's tree would
      // stop at the edge — subagents parented sideways at the owner session
      // would be missing.
      const rows = await execAll(client, `
        WITH RECURSIVE descendants(child_session_key, depth) AS (
          SELECT child_session_key, 0 FROM session_lineage WHERE child_session_key = ?
          UNION ALL
          SELECT sl.child_session_key, d.depth + 1
            FROM session_lineage sl
            JOIN descendants d
              ON sl.parent_session_key = d.child_session_key
              OR (substr(d.child_session_key, 1, 5) = 'flow:'
                  AND sl.flow_id IS NOT NULL
                  AND sl.flow_id = substr(d.child_session_key, 6)
                  AND sl.child_session_key != d.child_session_key)
           WHERE d.depth < ?
        )
        SELECT sl.child_session_key, sl.parent_session_key, sl.kind, sl.root_label,
               sl.flow_id, sl.first_seen_at,
               COALESCE((SELECT SUM(cost_total) FROM cost_by_call
                          WHERE session_key = sl.child_session_key), 0) AS cost
          FROM session_lineage sl
          JOIN descendants d ON sl.child_session_key = d.child_session_key
         GROUP BY sl.child_session_key
         ORDER BY sl.first_seen_at ASC
      `, [key, maxDepth]);
      if (!rows.length) { console.log(`(no lineage row for ${key})`); return; }
      const childrenOf = new Map();
      const byKey = new Map();
      for (const r of rows) byKey.set(r.child_session_key, r);
      const flowEdgesByFlowId = new Map();
      for (const r of rows) {
        if (r.child_session_key.startsWith("flow:")) flowEdgesByFlowId.set(r.child_session_key.slice(5), r.child_session_key);
      }
      for (const r of rows) {
        if (r.child_session_key === key) continue;
        let pk = r.parent_session_key;
        // Recorded parent outside this subtree: hang the session off its
        // flow's trigger edge instead.
        if ((!pk || !byKey.has(pk)) && r.flow_id && flowEdgesByFlowId.has(r.flow_id)) {
          pk = flowEdgesByFlowId.get(r.flow_id);
        }
        if (!pk) continue;
        if (!childrenOf.has(pk)) childrenOf.set(pk, []);
        childrenOf.get(pk).push(r);
      }
      const subtreeCostMemo = new Map();
      function subtreeCost(k) {
        if (subtreeCostMemo.has(k)) return subtreeCostMemo.get(k);
        const self = byKey.get(k)?.cost ?? 0;
        const kids = childrenOf.get(k) ?? [];
        const total = kids.reduce((a, c) => a + subtreeCost(c.child_session_key), self);
        subtreeCostMemo.set(k, total);
        return total;
      }
      const total = subtreeCost(key);
      function fmt$(v) { return v >= 0.01 ? `$${v.toFixed(2)}` : v > 0 ? `$${v.toFixed(4)}` : "$0"; }
      // Fetched up front so the recursive render stays synchronous.
      const callsBySession = new Map();
      if (withCalls) {
        const ph = rows.map(() => "?").join(",");
        const callRows = await execAll(client, `
          SELECT session_key, ts, model, provider, input_tokens, output_tokens,
                 cache_read_tokens, cache_write_tokens, cost_total
            FROM cost_by_call WHERE session_key IN (${ph}) ORDER BY ts ASC
        `, rows.map((r) => r.child_session_key));
        for (const c of callRows) {
          if (!callsBySession.has(c.session_key)) callsBySession.set(c.session_key, []);
          callsBySession.get(c.session_key).push(c);
        }
      }
      function fmtTok(n) {
        if (n == null) return "?";
        if (n >= 1000) return `${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}k`;
        return String(n);
      }
      function renderCalls(session, prefix) {
        if (!withCalls) return;
        const calls = callsBySession.get(session) ?? [];
        calls.forEach((c, i) => {
          const isLast = i === calls.length - 1;
          const t = new Date(c.ts).toISOString().slice(11, 19);
          const model = (c.model ?? "?").split("/").pop().slice(0, 24);
          const tokParts = [];
          if (c.input_tokens != null)  tokParts.push(`${fmtTok(c.input_tokens)} in`);
          if (c.output_tokens != null) tokParts.push(`${fmtTok(c.output_tokens)} out`);
          if (c.cache_read_tokens) tokParts.push(`${fmtTok(c.cache_read_tokens)} cache-r`);
          const toks = tokParts.length ? ` (${tokParts.join(", ")})` : "";
          console.log(`${prefix}${isLast ? "└── " : "├── "}[call] ${t} ${model.padEnd(24)} ${fmt$(c.cost_total ?? 0)}${toks}`);
        });
      }
      function renderNode(node, prefix, isLast) {
        const tree$ = subtreeCost(node.child_session_key);
        if (!showZero && tree$ === 0 && (childrenOf.get(node.child_session_key)?.length ?? 0) === 0) {
        }
        const self$ = node.cost;
        const costStr =
          self$ > 0 && tree$ > self$ ? `  ${fmt$(self$)} (subtree ${fmt$(tree$)})`
          : self$ > 0                ? `  ${fmt$(self$)}`
          : tree$ > 0                ? `  (subtree ${fmt$(tree$)})`
          : "";
        const tailMaxLen = 80;
        const tail = node.child_session_key.length > tailMaxLen
          ? node.child_session_key.slice(0, tailMaxLen - 1) + "…"
          : node.child_session_key;
        console.log(`${prefix}${isLast ? "└── " : "├── "}[${node.kind}] ${tail}${costStr}`);
        const kids = (childrenOf.get(node.child_session_key) ?? [])
          .filter((c) => showZero || subtreeCost(c.child_session_key) > 0 || (childrenOf.get(c.child_session_key)?.length ?? 0) === 0);
        const newPrefix = prefix + (isLast ? "    " : "│   ");
        kids.forEach((c, i) => renderNode(c, newPrefix, i === kids.length - 1));
        // Calls belong to THIS session, not to a child session.
        if (withCalls && self$ > 0) renderCalls(node.child_session_key, newPrefix);
      }
      const root = byKey.get(key);
      console.log(`[${root.kind}] ${root.root_label ?? root.child_session_key}  ${fmt$(total)} total`);
      const kids = (childrenOf.get(key) ?? [])
        .filter((c) => showZero || subtreeCost(c.child_session_key) > 0 || (childrenOf.get(c.child_session_key)?.length ?? 0) === 0);
      kids.forEach((c, i) => renderNode(c, "", i === kids.length - 1));
      if (withCalls && (root.cost ?? 0) > 0) renderCalls(root.child_session_key, "");
      console.log(`\n${rows.length} session(s) in subtree, ${fmt$(total)} total cost`);
    });

  cmd.command("stats")
    .description("Summary counts from the lineage + cost tables")
    .action(async () => {
      const store = await getStore();
      if (!store) { console.error("lineage service not ready"); process.exitCode = 1; return; }
      const client = store._client;
      const totalSess = await execGet(client, `SELECT COUNT(*) AS n FROM session_lineage`);
      const bySource = await execAll(client, `SELECT source, COUNT(*) AS n FROM session_lineage GROUP BY source ORDER BY n DESC`);
      const byRootKind = await execAll(client, `SELECT root_kind, COUNT(*) AS n FROM session_lineage GROUP BY root_kind ORDER BY n DESC`);
      const costRows = await execGet(client, `SELECT COUNT(*) AS n, COALESCE(SUM(cost_total),0) AS total FROM cost_by_call`);
      const byRootCost = await execAll(client, `SELECT root_kind, COUNT(*) AS calls, ROUND(SUM(cost_total), 4) AS cost FROM cost_by_call GROUP BY root_kind ORDER BY cost DESC LIMIT 10`);
      console.log(`DB: ${dbPath}`);
      console.log(`Sessions in lineage table: ${totalSess.n}`);
      console.log(`By source:`);
      for (const r of bySource) console.log(`  ${r.source.padEnd(22)} ${r.n}`);
      console.log(`By root kind:`);
      for (const r of byRootKind) console.log(`  ${r.root_kind.padEnd(22)} ${r.n}`);
      console.log(`\nCost rows: ${costRows.n}  total: $${costRows.total.toFixed(4)}`);
      console.log(`By root kind:`);
      for (const r of byRootCost) console.log(`  ${r.root_kind.padEnd(22)} calls=${String(r.calls).padStart(6)}  cost=$${(r.cost ?? 0).toFixed(4)}`);
    });

  cmd.command("cost")
    .description("Cost rollup — by-root by default, or pass --by=<dim>")
    .option("--by <dim>", "Dimension: root | pipeline | flow | node | model", "root")
    .option("--since <iso>", "ISO timestamp lower bound")
    .option("--until <iso>", "ISO timestamp upper bound")
    .option("--limit <n>", "Top N", "20")
    .option("--flow-id <id>",       "Filter to a specific flow_id")
    .option("--pipeline-id <id>",   "Filter to a specific pipeline_id")
    .option("--node-id <id>",       "Filter to a specific node_id")
    .option("--root-kind <kind>",   "Filter by root_kind (e.g. cron-owner, pipeline-trigger)")
    .option("--root-label <label>", "Filter by root_label")
    .option("--model <model>",      "Filter by model")
    .option("--provider <provider>","Filter by provider")
    .action(async (opts) => {
      const store = await getStore();
      if (!store) { console.error("lineage service not ready"); process.exitCode = 1; return; }
      const client = store._client;
      const where = []; const args = [];
      if (opts.since) { where.push("ts >= ?"); args.push(Date.parse(opts.since)); }
      if (opts.until) { where.push("ts <= ?"); args.push(Date.parse(opts.until)); }
      // Column filters — mirror the HTTP makeGroupHandler pass-through.
      const colFilters = [
        ["flow_id",     opts.flowId],
        ["pipeline_id", opts.pipelineId],
        ["node_id",     opts.nodeId],
        ["root_kind",   opts.rootKind],
        ["root_label",  opts.rootLabel],
        ["model",       opts.model],
        ["provider",    opts.provider],
      ];
      for (const [col, val] of colFilters) {
        if (val != null && val !== "") { where.push(`${col} = ?`); args.push(val); }
      }
      const w = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const dims = {
        root:     `root_label,        root_kind,                       NULL AS extra`,
        pipeline: `pipeline_id,       'pipeline' AS root_kind,         NULL AS extra`,
        flow:     `flow_id,           'flow' AS root_kind,             pipeline_id AS extra`,
        node:     `node_id,           'node' AS root_kind,             flow_id AS extra`,
        model:    `model,             'model' AS root_kind,            provider AS extra`,
      };
      const proj = dims[opts.by] || dims.root;
      const sql = `SELECT ${proj}, COUNT(*) AS calls, ROUND(SUM(cost_total), 6) AS cost FROM cost_by_call ${w} GROUP BY 1, 2 ORDER BY cost DESC LIMIT ${Number(opts.limit) || 20}`;
      const rows = await execAll(client, sql, args);
      console.log(`By ${opts.by} | rows=${rows.length}`);
      for (const r of rows) {
        const k = r[Object.keys(r)[0]] ?? "(null)";
        const kind = r.root_kind;
        const extra = r.extra ? ` (${r.extra.slice ? r.extra.slice(0, 30) : r.extra})` : "";
        console.log(`  ${String(k).padEnd(40)} ${String(kind).padEnd(12)} calls=${String(r.calls).padStart(6)} cost=$${(r.cost ?? 0).toFixed(4)}${extra}`);
      }
    });
}

async function sessionIdToSession(client, sessionId) {
  const hit = await execGet(client,
    `SELECT session_key FROM cost_by_call WHERE session_id = ? AND session_key IS NOT NULL ORDER BY ts DESC LIMIT 1`,
    [sessionId]);
  return hit?.session_key ?? null;
}

// Explicit --kind rather than shape-sniffing: predictable lookups, no
// ambiguous matches across the table.
async function resolveTreeTarget(target, client, kind) {
  if (!target || typeof target !== "string") return null;
  const t = target.trim();
  const lookups = {
    "session-key": async () => {
      if (!t.startsWith("agent:") && !t.startsWith("flow:")) return null;
      const hit = await execGet(client, `SELECT 1 FROM session_lineage WHERE child_session_key = ? LIMIT 1`, [t]);
      return hit ? t : null;
    },
    "session-id":  () => sessionIdToSession(client, t),
    "msg": async () => {
      const r = await execGet(client, `SELECT session_key FROM cost_by_call WHERE msg_id = ? AND session_key IS NOT NULL LIMIT 1`, [t]);
      return r?.session_key ?? null;
    },
    "flow": async () => {
      const edge = await execGet(client, `SELECT 1 FROM session_lineage WHERE child_session_key = ? LIMIT 1`, [`flow:${t}`]);
      if (edge) return `flow:${t}`;
      const any = await execGet(client, `SELECT child_session_key FROM session_lineage WHERE flow_id = ? LIMIT 1`, [t]);
      return any?.child_session_key ?? null;
    },
    "pipeline": async () => {
      // Most recent flow of this pipeline.
      const any = await execGet(client, `SELECT child_session_key FROM session_lineage WHERE pipeline_id = ? AND child_session_key LIKE 'flow:%' ORDER BY first_seen_at DESC LIMIT 1`, [t]);
      return any?.child_session_key ?? null;
    },
    "cron": async () => {
      // root_label holds the cron name for cron-owner rows.
      const r = await execGet(client, `SELECT child_session_key FROM session_lineage WHERE root_kind = 'cron-owner' AND root_label = ? LIMIT 1`, [t]);
      return r?.child_session_key ?? null;
    },
    "run": async () => {
      // The flow trigger edge that owns this runId.
      const r = await execGet(client, `SELECT child_session_key FROM session_lineage WHERE run_id = ? AND child_session_key LIKE 'flow:%' LIMIT 1`, [t]);
      return r?.child_session_key ?? null;
    },
  };
  const fn = lookups[kind];
  if (!fn) throw new Error(`unknown --kind ${kind}; supported: ${Object.keys(lookups).join(", ")}`);
  return await fn();
}
