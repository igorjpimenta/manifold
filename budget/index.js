// budget — approval queue module absorbed into manifold.
//
// Owns the SQLite store, now at ~/.openclaw/ops.sqlite (DB_PATH). The old
// ~/.openclaw/approval-queue/store.sqlite location is kept as LEGACY_DB_PATH
// and read exactly once, via migrateFromLegacy(), to carry forward any
// in-flight decisions from before the rename. Registers HTTP routes under
// /manifold/budget/*. Sets api.runtime.approvalQueue with the async API that
// executor.js calls per-node (isGateLocked, lookupNodeDecision, enqueueNodeApproval).

import { mkdirSync } from "node:fs";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { deserialize, isGateLocked, resolveNodeDecision, classifyDecisionStatus, todayLocal } from "./lib.js";
import { createMigrator } from "../state/migrator.js";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER } from "../state/migrate.js";
import { migrateFromLegacy } from "../state/consolidate.js";

const STORE_DIR = path.join(os.homedir(), ".openclaw");
const DB_PATH = path.join(STORE_DIR, "ops.sqlite");
const LEGACY_DB_PATH = path.join(os.homedir(), ".openclaw", "approval-queue", "store.sqlite");

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
  {
    label: "Add agent (product-designer)",
    agent: "agent-forger",
    agentEmoji: "🔨",
    trigger: "manual",
    pipeline: null,
    node: null,
    tokenEstimate: 48000,
    costLow: 0.098,
    costHigh: 0.29,
    brief: "Forge a new product-designer agent — create workspace, write persona files, register in team registry",
    modelChain: ["zai/glm-5.1", "openrouter/deepseek/deepseek-v4-pro"],
  },
  {
    label: "Scout Workana (python, typescript)",
    agent: "marketplace-scout",
    agentEmoji: "🔭",
    trigger: "cron",
    pipeline: null,
    node: null,
    tokenEstimate: 6400,
    costLow: 0.0011,
    costHigh: 0.0033,
    brief: "Fetch and cache new listings from Workana search: python + typescript + api. Last run found 34 new listings.",
    modelChain: ["zai/glm-4.5-flash", "openrouter/deepseek/deepseek-v4-flash"],
  },
];

const BUDGET_MIGRATIONS = [
  // v1: initial schema
  `CREATE TABLE IF NOT EXISTS queue_items (
     id              TEXT PRIMARY KEY,
     label           TEXT NOT NULL,
     agent           TEXT NOT NULL,
     agent_emoji     TEXT NOT NULL DEFAULT '🤖',
     trigger         TEXT NOT NULL DEFAULT 'manual',
     cost_low        REAL NOT NULL DEFAULT 0,
     cost_high       REAL NOT NULL DEFAULT 0,
     brief           TEXT,
     model_chain     TEXT,
     pipeline        TEXT,
     node            TEXT,
     token_estimate  INTEGER,
     status          TEXT NOT NULL DEFAULT 'pending',
     created_at      INTEGER NOT NULL,
     expires_at      INTEGER,
     decided_at      INTEGER,
     decision        TEXT,
     pipeline_id     TEXT,
     pipeline_args   TEXT
   );
   CREATE INDEX IF NOT EXISTS idx_status     ON queue_items(status);
   CREATE INDEX IF NOT EXISTS idx_created_at ON queue_items(created_at DESC);
   CREATE TABLE IF NOT EXISTS gate_state (
     id     TEXT NOT NULL PRIMARY KEY DEFAULT 'singleton',
     locked INTEGER NOT NULL DEFAULT 0,
     day    TEXT
   );
   INSERT OR IGNORE INTO gate_state (id, locked) VALUES ('singleton', 0)`,
  // v2: flow_id + index
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('queue_items')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("flow_id")) {
      await client.execute("ALTER TABLE queue_items ADD COLUMN flow_id TEXT");
      await client.execute("CREATE INDEX IF NOT EXISTS idx_flow_node ON queue_items(flow_id, node)");
    }
  },
  // v3: enqueuer_session_key
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('queue_items')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("enqueuer_session_key")) {
      await client.execute("ALTER TABLE queue_items ADD COLUMN enqueuer_session_key TEXT");
    }
  },
];

// getSharedDb() instead of a fresh openDb() call: on the first call ever for
// this dbPath, this does the real work (open + migrate). On every call
// after — including a brand-new registerBudgetRoutes closure created by a
// gateway plugin reload — it returns the SAME already-open connection
// straight from db.js's process-wide cache, instantly. That shared cached
// connection is what closes the "budget store not attached yet" race: a
// fresh connection per reload opens a genuine "not ready" window each time
// by construction (see ensureClient's own comment below for the hazard).
// createMigrator/migrateFromLegacy are cheap idempotent no-ops once already
// applied, so re-running them against an already-migrated cached connection
// on a later reload costs one quick SELECT, not a meaningful delay.
// Exported so tests can exercise the real getSharedDb-backed path against a
// tmp dbPath (see test/unit/budget/index.test.js's "real openStore()"
// suite) instead of only ever testing through a fully-fake openStore DI
// override that bypasses this function — and by extension getSharedDb —
// entirely.
export async function openStore({ dbPath = DB_PATH, legacyDbPath = LEGACY_DB_PATH } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  const { client } = await getSharedDb(dbPath);
  await client.execute("PRAGMA foreign_keys = ON");
  await createMigrator(client, { name: "budget", migrations: BUDGET_MIGRATIONS });
  // Generated snapshots apply after the legacy chain (see state/migrate.js).
  await migrateDatabase({ dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER });
  if (legacyDbPath) await migrateFromLegacy(client, legacyDbPath, ["queue_items", "gate_state"]);
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

export function registerBudgetRoutes(api, { getBroadcast, openStore: openStoreFn = openStore } = {}) {
  let client;
  let clientPromise = null;

  // `client` must NOT be attached only from registerService's start() — a
  // single external lifecycle hook fired once per registerBudgetRoutes call.
  // When the gateway's plugin set reloads twice in quick succession, the
  // LAST registerBudgetRoutes call's `api.runtime.approvalQueue` closure can
  // end up the one left installed while an EARLIER generation's start() is
  // the one that actually ran — or neither generation's start() runs for the
  // closure left live. That closure's `client` would then stay undefined
  // forever and isGateLocked() would throw on every call in that state,
  // parking the calling node on a wait that nothing ever woke.
  //
  // So: any call into THIS closure's own isGateLocked() attaches the client
  // itself, on demand, the first time it's actually needed, and every caller
  // simply awaits
  // that attachment instead of throwing and needing something else to wake
  // it later. Self-healing regardless of whether this generation's
  // registerService start() ever fires, and getSharedDb (state/db.js) means
  // a LATER generation's ensureClient() resolves near-instantly from the
  // cached connection rather than paying a fresh open+migrate every time.
  function ensureClient() {
    if (client) return Promise.resolve(client);
    if (!clientPromise) {
      clientPromise = openStoreFn()
        .then((c) => {
          client = c;
          api.logger.info("[manifold/budget] store ready at " + DB_PATH);
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

  // GET /manifold/budget/pending
  api.registerHttpRoute({
    path: "/manifold/budget/pending",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const { rows } = await client.execute(
        "SELECT * FROM queue_items WHERE status='pending' ORDER BY created_at ASC"
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: rows.map(deserialize) }));
      return true;
    },
  });

  // POST /manifold/budget/decide  { ids: string[], decision: "approve"|"reject" }
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
        const r = await withSharedWrite(DB_PATH, () => client.execute({
          sql: "UPDATE queue_items SET status=?, decided_at=?, decision=? WHERE id=? AND status='pending'",
          args: [classifyDecisionStatus(decision), now, decision, id],
        }));
        if (r.rowsAffected) { updated++; updatedIds.push(id); }
      }
      if (updatedIds.length > 0) {
        const ph = updatedIds.map(() => "?").join(",");
        const { rows: decided } = await client.execute({
          sql: `SELECT id, pipeline_id, pipeline_args, flow_id, node, enqueuer_session_key FROM queue_items WHERE id IN (${ph})`,
          args: updatedIds,
        });
        for (const row of decided) {
          if (row.flow_id) {
            _loopback("/manifold/resume-from-gate", { flowId: row.flow_id, nodeId: row.node, decision });
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
      if (updated > 0) getBroadcast?.()?.("plugin.budget.queue.changed", { decision, count: updated });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, updated, decision }));
      return true;
    },
  });

  // GET /manifold/budget/history
  api.registerHttpRoute({
    path: "/manifold/budget/history",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const { rows } = await client.execute(
        "SELECT * FROM queue_items WHERE status != 'pending' ORDER BY decided_at DESC LIMIT 50"
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ items: rows.map(deserialize) }));
      return true;
    },
  });

  // POST /manifold/budget/mock  { count?: number }
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
        await withSharedWrite(DB_PATH, () => client.execute({
          sql: `INSERT OR REPLACE INTO queue_items
                  (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain, pipeline, node, token_estimate, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
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

  // GET /manifold/budget/gate
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

  // POST /manifold/budget/gate/lock
  api.registerHttpRoute({
    path: "/manifold/budget/gate/lock",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const today = todayLocal();
      await withSharedWrite(DB_PATH, () => client.execute({ sql: "UPDATE gate_state SET locked=1, day=? WHERE id='singleton'", args: [today] }));
      api.logger.info(`[manifold/budget] gate locked day=${today}`);
      getBroadcast?.()?.("plugin.gate.lock.changed", { locked: true, day: today });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, locked: true, day: today }));
      return true;
    },
  });

  // POST /manifold/budget/gate/unlock
  api.registerHttpRoute({
    path: "/manifold/budget/gate/unlock",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.end("method not allowed"); return true; }
      const today = todayLocal();
      await withSharedWrite(DB_PATH, () => client.execute({ sql: "UPDATE gate_state SET locked=0, day=? WHERE id='singleton'", args: [today] }));
      api.logger.info(`[manifold/budget] gate unlocked day=${today}`);
      getBroadcast?.()?.("plugin.gate.lock.changed", { locked: false, day: today });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, locked: false, day: today }));
      return true;
    },
  });

  // POST /manifold/budget/items — programmatic item creation (called by manifold gate)
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
      await withSharedWrite(DB_PATH, () => client.execute({
        sql: `INSERT INTO queue_items
                (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain,
                 pipeline, node, token_estimate, pipeline_id, pipeline_args, enqueuer_session_key, status, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
        args: [id, label, agent, agentEmoji, trigger, costLow, costHigh, brief ?? null,
               JSON.stringify(modelChain), pipeline ?? pipelineId, node ?? null,
               tokenEstimate ?? null, pipelineId, JSON.stringify(pipelineArgs ?? {}),
               enqueuerSessionKey ?? null, now],
      }));
      api.logger.info(`[manifold/budget] queued pipeline=${pipelineId} item=${id}`);
      res.statusCode = 201;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, itemId: id }));
      return true;
    },
  });

  // ── Runtime API: async libsql reads + writes ──────────────────────────────
  // executor.js calls these per-node (isGateLocked, lookupNodeDecision,
  // enqueueNodeApproval) without an HTTP hop.
  api.runtime = api.runtime ?? {};
  api.runtime.approvalQueue = {
    async isGateLocked() {
      // Awaits attachment in place instead of throwing when the client
      // isn't attached yet — a cold store on the first call after a reload
      // is no longer a special "transient wait" case the caller has to
      // park on and something else has to remember to wake; it's just a
      // normal await, same as run-store.js's Proxy factory already does
      // for every other store in this codebase. getSharedDb (state/db.js)
      // is what makes this cheap: a later generation's attach resolves
      // near-instantly from the cached connection.
      //
      // A genuine attach FAILURE (not "still attaching" — that case never
      // rejects, it just takes longer) is deliberately NOT caught here: it
      // propagates and fails this node closed, same as any other thrown
      // error reaching maybeBudgetGate. Treating "the store is broken, we
      // can't tell if spend is locked" as "not locked" would let arbitrarily
      // expensive nodes dispatch with zero budget control — the one case
      // this function must never fail open on.
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
      } catch { return false; }
    },
    async lookupNodeDecision({ flowId, nodeId }) {
      if (!flowId || !nodeId) return null;
      const { rows } = await client.execute({
        sql: `SELECT status, decision FROM queue_items WHERE flow_id=? AND node=?
              ORDER BY created_at DESC LIMIT 1`,
        args: [flowId, nodeId],
      });
      return resolveNodeDecision(rows[0] ?? null);
    },
    async enqueueNodeApproval({ flowId, nodeId, pipeline, pipelineId, label, agent, agentEmoji,
                                trigger = "cron", brief, costLow = 0, costHigh = 0,
                                modelChain = [], tokenEstimate = null, enqueuerSessionKey = null }) {
      const id = randomUUID();
      await withSharedWrite(DB_PATH, () => client.execute({
        sql: `INSERT INTO queue_items
                (id, label, agent, agent_emoji, trigger, cost_low, cost_high, brief, model_chain,
                 pipeline, node, token_estimate, pipeline_id, flow_id, enqueuer_session_key, status, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
        args: [id, label, agent ?? "manifold", agentEmoji ?? "🔁", trigger,
               costLow, costHigh, brief ?? null, JSON.stringify(modelChain ?? []),
               pipeline ?? pipelineId ?? null, nodeId, tokenEstimate ?? null,
               pipelineId ?? null, flowId, enqueuerSessionKey ?? null, Date.now()],
      }));
      return id;
    },
  };

  api.logger.info("[manifold/budget] routes registered at /manifold/budget/*");
}
