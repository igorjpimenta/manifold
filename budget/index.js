// Budget approval queue and gate: gate_state + alerts in ops.sqlite, the
// approval queue in sibling runtime.sqlite; routes under /manifold/budget/*;
// api.runtime.approvalQueue is the async API executor.js calls per-node.

import { mkdirSync } from "node:fs";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "../state/db.js";
import { createEmitter } from "../events/emit.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { deserialize, isGateLocked, resolveNodeDecision, classifyDecisionStatus, todayLocal } from "./lib.js";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER, RUNTIME_MIGRATIONS_FOLDER } from "../state/migrate.js";
import { ensureSessionKeys } from "../state/session-keys.js";
import { ensurePipelineNodes } from "../state/attribution-registries.js";

const DB_PATH = resolveStorePaths().opsDb;

// queue_items lives in runtime.sqlite beside ops.sqlite. Alternate dbPaths
// (tests, one-off stores) get their own sibling so two fixtures never share
// a /tmp/runtime.sqlite.
function runtimeSibling(dbPath) {
  return path.basename(dbPath) === "ops.sqlite"
    ? path.join(path.dirname(dbPath), "runtime.sqlite")
    : `${dbPath}.runtime.sqlite`;
}

const MOCK_ITEMS = [
  {
    label: "SCORE × 12 listings",
    agent: "scorer",
    agentEmoji: "⚖️",
    trigger: "cron",
    pipeline: "workana-proposal",
    node: "score",
    tokenEstimate: 10800,
    costLow: 0.0018,
    costHigh: 0.0054,
    brief: "Score 12 new Workana listings against PROFILE.md rubric (python, typescript, api, automation categories)",
    modelChain: ["zai/glm-4.5-flash", "openrouter/deepseek/deepseek-v4-flash"],
  },
  {
    label: "DRAFT × 3 listings",
    agent: "content-writer",
    agentEmoji: "✍️",
    trigger: "cron",
    pipeline: "workana-proposal",
    node: "propose.draft",
    tokenEstimate: 33000,
    costLow: 0.067,
    costHigh: 0.2,
    brief: "Draft proposals for: sistema-de-automacao-rpa-python, api-integracao-whatsapp-crm, dashboard-analytics-react-typescript",
    modelChain: ["zai/glm-5.1", "openrouter/deepseek/deepseek-v4-pro"],
  },
  {
    label: "SUBMIT × 1 proposal",
    agent: "proposal-sender",
    agentEmoji: "📤",
    trigger: "cron",
    pipeline: "workana-proposal",
    node: "submit",
    tokenEstimate: 3200,
    costLow: 0.0005,
    costHigh: 0.0016,
    brief: "Submit approved draft for: sistema-de-automacao-rpa-python (score 87, budget R$2.400)",
    modelChain: ["zai/glm-4.5-flash", "openrouter/deepseek/deepseek-v4-flash"],
  },
];

// getSharedDb() rather than a fresh openDb(): first call does the real work
// (open + migrate); every call after — including a new registerBudgetRoutes
// closure from a plugin reload — returns the SAME cached connection
// instantly. That cache is what closes the "budget store not attached yet"
// race: a fresh connection per reload opens a genuine "not ready" window by
// construction (see ensureClient below). Snapshots are idempotent no-ops once
// applied, so re-running openStore on a cached connection costs one journal
// SELECT. Exported so tests can exercise this path against a tmp dbPath.
export async function openStore({ dbPath = DB_PATH, runtimeDbPath = runtimeSibling(dbPath) } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER });
  const { client } = await getSharedDb(dbPath);
  await client.execute("PRAGMA foreign_keys = ON");
  // The singleton row is runtime state, not schema: every gate_state reader
  // assumes it exists, so seed it unconditionally on every open.
  await client.execute(`INSERT OR IGNORE INTO gate_state (id, locked) VALUES ('singleton', 0)`);
  // queue_items lives in runtime.sqlite; gate_state and alerts stay in ops.
  // The extra handle rides on the ops client so callers that only inject one
  // fake still work.
  await migrateDatabase({ dbPath: runtimeDbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { client: runtimeClient } = await getSharedDb(runtimeDbPath);
  client.runtime = runtimeClient;
  client.runtimePath = runtimeDbPath;
  return client;
}

async function readBody(req) {
  let raw = "";
  for await (const chunk of req) raw += chunk;
  try { return raw ? JSON.parse(raw) : {}; }
  catch { return null; }
}

const _GW_PORT = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;

async function _loopback(path, body, extraHeaders = {}) {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN ?? "";
  try {
    await fetch(`http://127.0.0.1:${_GW_PORT}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}`, ...extraHeaders },
      body: JSON.stringify(body ?? {}),
      signal: AbortSignal.timeout(8000),
    });
  } catch { /* best-effort fire-and-forget */ }
}

export function registerBudgetRoutes(api, { getBroadcast, openStore: openStoreFn = openStore, onFlowResume } = {}) {
  // onFlowResume({flowId, nodeId, decision}) resumes a flow-bound approval
  // in-process (routes/operator-resume.js, injected by index.js) — no HTTP
  // loopback. Absent (tests), per-node resume is skipped but the decision
  // still commits and broadcasts.
  const { emit } = createEmitter({ getBroadcast, logger: api.logger });
  let runtimeDbPath = runtimeSibling(DB_PATH);
  let client;
  let runtimeClient;
  let clientPromise = null;

  // `client` must NOT be attached only from registerService's start(): under
  // rapid plugin reloads the LAST closure's api.runtime.approvalQueue can be
  // the one installed while an EARLIER generation's start() is the one that
  // ran — or neither. That closure's `client` would stay undefined forever
  // and isGateLocked() would throw on every call, parking the calling node
  // on a wait nothing ever woke. So any call into this closure attaches the
  // client itself on demand; every caller awaits that attachment instead of
  // throwing. Self-healing regardless of whether this generation's start()
  // ever fires, and getSharedDb makes a later generation's attach resolve
  // near-instantly from the cached connection.
  function ensureClient() {
    if (client) return Promise.resolve(client);
    if (!clientPromise) {
      clientPromise = openStoreFn()
        .then((c) => {
          client = c;
          runtimeClient = c.runtime ?? c;
          runtimeDbPath = c.runtimePath ?? runtimeDbPath;
          api.logger.info("[manifold/budget] store ready at " + (c.dbPath ?? DB_PATH));
          return c;
        })
        .catch((err) => {
          api.logger.warn(`[manifold/budget] store attach failed: ${err?.message ?? err}`);
          clientPromise = null; // allow a later call to retry instead of wedging on a permanently-rejected promise
          throw err;
        });
    }
    return clientPromise;
  }

  api.registerService({
    id: "manifold-budget-store",
    start: async () => { await ensureClient(); },
    stop: async () => {
      try { client?.close?.(); } catch { /* ignore */ }
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/pending",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const { rows } = await runtimeClient.execute(
        "SELECT * FROM queue_items WHERE status='pending' ORDER BY created_at ASC"
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: rows.map(deserialize) }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/decide",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const body = await readBody(req);
      if (!body) { res.statusCode = 400; res.end("invalid json"); return true; }
      const { ids, decision } = body;
      if (!Array.isArray(ids) || !ids.length || !["approve", "reject"].includes(decision)) {
        res.statusCode = 400;
        res.end(JSON.stringify({ error: "ids (string[]) and decision (approve|reject) required" }));
        return true;
      }
      const now = Date.now();
      let updated = 0;
      const updatedIds = [];
      for (const id of ids) {
        const r = await withSharedWrite(runtimeDbPath, () => runtimeClient.execute({
          sql: "UPDATE queue_items SET status=?, decided_at=?, decision=? WHERE id=? AND status='pending'",
          args: [classifyDecisionStatus(decision), now, decision, id],
        }));
        if (r.rowsAffected) { updated++; updatedIds.push(id); }
      }
      if (updatedIds.length > 0) {
        const ph = updatedIds.map(() => "?").join(",");
        const { rows: decided } = await runtimeClient.execute({
          sql: `SELECT id, pipeline_id, pipeline_args, flow_id, node, enqueuer_session_key FROM queue_items WHERE id IN (${ph})`,
          args: updatedIds,
        });
        for (const row of decided) {
          if (row.flow_id) {
            // In-process resume (routes/operator-resume.js). The start-gate
            // branch below still loops back: elevation needs gateway scope.
            if (onFlowResume) {
              try { await onFlowResume({ flowId: row.flow_id, nodeId: row.node, decision }); } catch {}
            }
            api.logger.info(`[manifold/budget] ${decision} per-node — resuming flow=${String(row.flow_id).slice(0,8)} node=${row.node}`);
          } else if (decision === "approve" && row.pipeline_id) {
            _loopback(
              "/manifold/_elev/start",
              {
                pipelineId: row.pipeline_id,
                args: row.pipeline_args ? JSON.parse(row.pipeline_args) : {},
                via: "approval",
                ...(row.enqueuer_session_key ? { parentSessionKey: row.enqueuer_session_key } : {}),
              },
              { "x-skip-gate": "1" }
            );
            api.logger.info(`[manifold/budget] approve start-gate — firing pipeline=${row.pipeline_id}`);
          }
        }
      }
      api.logger.info(`[manifold/budget] ${decision}: ${updated}/${ids.length}`);
      if (updated > 0) {
        try { await emit("budget.queue.updated", { decision, count: updated }); } catch {}
      }
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, updated, decision }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/history",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const { rows } = await runtimeClient.execute(
        "SELECT * FROM queue_items WHERE status != 'pending' ORDER BY decided_at DESC LIMIT 50"
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: rows.map(deserialize) }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/mock",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const body = (await readBody(req)) ?? {};
      const count = Math.min(Number(body.count ?? MOCK_ITEMS.length), MOCK_ITEMS.length);
      const now = Date.now();
      const created = [];
      for (let i = 0; i < count; i++) {
        const t = MOCK_ITEMS[i];
        const id = randomUUID();
        await withSharedWrite(runtimeDbPath, () => runtimeClient.execute({
          sql: `INSERT OR REPLACE INTO queue_items
                  (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain, pipeline, node, token_estimate, kind, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'node', 'pending', ?)`,
          args: [id, t.label, t.agent, t.agentEmoji, t.trigger,
                 t.costLow, t.costHigh, t.brief, JSON.stringify(t.modelChain),
                 t.pipeline ?? null, t.node ?? null, t.tokenEstimate ?? null,
                 now],
        }));
        created.push(id);
      }
      res.statusCode = 201;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, created }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/gate",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const { rows } = await client.execute("SELECT locked, day FROM gate_state WHERE id='singleton'");
      const row = rows[0] ?? null;
      const today = todayLocal();
      if (row?.locked && row?.day && row.day < today) {
        await withSharedWrite(DB_PATH, () => client.execute({ sql: "UPDATE gate_state SET locked=0, day=? WHERE id='singleton'", args: [today] }));
        api.logger.info(`[manifold/budget] gate auto-unlocked on stale day rollover (was ${row.day})`);
      }
      const locked = isGateLocked(row, today);
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ locked, day: row?.day ?? null }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/gate/lock",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const today = todayLocal();
      await withSharedWrite(DB_PATH, () => client.execute({ sql: "UPDATE gate_state SET locked=1, day=? WHERE id='singleton'", args: [today] }));
      api.logger.info(`[manifold/budget] gate locked day=${today}`);
      try { await emit("budget.lock.changed", { locked: true, day: today }); } catch {}
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, locked: true, day: today }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/gate/unlock",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const today = todayLocal();
      await withSharedWrite(DB_PATH, () => client.execute({ sql: "UPDATE gate_state SET locked=0, day=? WHERE id='singleton'", args: [today] }));
      api.logger.info(`[manifold/budget] gate unlocked day=${today}`);
      try { await emit("budget.lock.changed", { locked: false, day: today }); } catch {}
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, locked: false, day: today }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/manifold/budget/items",
    auth: "gateway",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const body = await readBody(req);
      if (!body) { res.statusCode = 400; res.end("invalid json"); return true; }
      const { label, agent = "manifold", agentEmoji = "🔁", trigger = "manual",
              pipelineId, pipelineArgs, brief, costLow = 0, costHigh = 0,
              modelChain = [], tokenEstimate = null, pipeline = null, node = null,
              enqueuerSessionKey = null } = body;
      if (!label || !pipelineId) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "label and pipelineId required" }));
        return true;
      }
      const id = randomUUID();
      const now = Date.now();
      await withSharedWrite(runtimeDbPath, async () => {
        await ensureSessionKeys(runtimeClient, [enqueuerSessionKey]);
        await ensurePipelineNodes(runtimeClient, [[pipelineId, node ?? pipeline ?? null]]);
        return runtimeClient.execute({
          sql: `INSERT INTO queue_items
                (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain,
                 pipeline, node, token_estimate, pipeline_id, flow_id, enqueuer_session_key, kind, status, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
          args: [id, label, agent, agentEmoji, trigger, costLow, costHigh, brief ?? null,
                 JSON.stringify(modelChain), pipeline ?? pipelineId, node ?? null,
                 tokenEstimate ?? null, pipelineId, JSON.stringify(pipelineArgs ?? {}),
                 enqueuerSessionKey ?? null, now],
        });
      });
      try {
        await emit("approval.requested", { id });
      } catch {}
      api.logger.info(`[manifold/budget] queued pipeline=${pipelineId} item=${id}`);
      res.statusCode = 201;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, itemId: id }));
      return true;
    },
  });

  // executor.js calls these per-node without an HTTP hop.
  api.runtime = api.runtime ?? {};
  api.runtime.approvalQueue = {
    async isGateLocked() {
      const c = await ensureClient();
      try {
        return await withSharedWrite(DB_PATH, async () => {
          const { rows } = await c.execute("SELECT locked, day FROM gate_state WHERE id='singleton'");
          const row = rows[0] ?? null;
          const today = todayLocal();
          if (row?.locked && row?.day && row.day < today) {
            await c.execute({ sql: "UPDATE gate_state SET locked=0, day=? WHERE id='singleton'", args: [today] });
            return false;
          }
          return isGateLocked(row, today);
        });
      } catch {
        // Fail closed: a gate_state read error must never report the gate unlocked.
        return true;
      }
    },
    async lookupNodeDecision({ flowId, nodeId }) {
      if (!flowId || !nodeId) return null;
      const { rows } = await runtimeClient.execute({
        sql: `SELECT status, decision FROM queue_items WHERE flow_id=? AND node=?
              ORDER BY created_at DESC LIMIT 1`,
        args: [flowId, nodeId],
      });
      return resolveNodeDecision(rows[0] ?? null);
    },
    async enqueueNodeApproval({ flowId, nodeId, pipeline, pipelineId, label, agent, agentEmoji,
                                trigger = "cron", brief, costLow = 0, costHigh = 0,
                                modelChain = [], tokenEstimate = null, enqueuerSessionKey = null, kind = "node" }) {
      const id = randomUUID();
      await withSharedWrite(runtimeDbPath, async () => {
        await ensureSessionKeys(runtimeClient, [enqueuerSessionKey]);
        await ensurePipelineNodes(runtimeClient, [[pipelineId, nodeId]]);
        return runtimeClient.execute({
          sql: `INSERT INTO queue_items
                (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain,
                 pipeline, node, token_estimate, pipeline_id, flow_id, enqueuer_session_key, kind, status, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
          args: [id, label, agent ?? "manifold", agentEmoji ?? "🔁", trigger,
                 costLow, costHigh, brief ?? null, JSON.stringify(modelChain ?? []),
                 pipeline ?? pipelineId ?? null, nodeId, tokenEstimate ?? null,
                 pipelineId ?? null, flowId, enqueuerSessionKey ?? null, kind, Date.now()],
        });
      });
      try {
        await emit("approval.requested", { id, flowId: flowId ?? null });
      } catch {}
      return id;
    },
  };

  api.logger.info("[manifold/budget] routes registered at /manifold/budget/*");
}
