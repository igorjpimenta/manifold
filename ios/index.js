// ios/index.js — Live Activity bridge wired into pipeclaw.
// Absorbed from extensions/live-activity-bridge/; exposes registerIosRoutes(api, deps)
// instead of definePluginEntry so pipeclaw's own plugin entry calls it inline.
//
// Routes: /pipeclaw/ios/{state,intent,test/emit,push}.
// Config read from api.pluginConfig?.ios (nested under pipeclaw's own config).

import os from "node:os";
import path from "node:path";
import { mkdirSync } from "node:fs";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import { createIntentRouter } from "./intents.js";
import { createLiveBridge } from "./live-bridge.js";
import { liveActivityEmitter } from "../events.js";
import { synthAgentApproval } from "./projectors.js";
import { buildRunPatch } from "./gate-projector.js";
import { authorizeBridgeRequest, sendUnauthorized } from "./auth.js";
import { NotificationKinds, pushKind } from "./notifications.js";
import { loadPipelines } from "../engine/loader.js";
import { resolveNodeDiscuss } from "./node-lookup.js";
import { readDeployStatus, readDeployLog } from "./deploy-status.js";

const DEFAULT_PIPELINES_DIR = path.join(os.homedir(), ".openclaw", "workspace", ".pipelines");
const OPS_DB_PATH = path.join(os.homedir(), ".openclaw", "ops.sqlite");

const STATE_PATH      = "/pipeclaw/ios/state";
const INTENT_PATH     = "/pipeclaw/ios/intent";
const TEST_PATH       = "/pipeclaw/ios/test/emit";
const PUSH_PATH       = "/pipeclaw/ios/push";
const LA_TOKEN_PATH   = "/pipeclaw/ios/la-push-token";
const DEPLOY_STATUS_PATH = "/pipeclaw/ios/deploy/status";
const DEPLOY_LOG_PATH    = "/pipeclaw/ios/deploy/log";

export function registerIosRoutes(api, { gateResolver, executor, runStore, nodeExecutionsStore, invokeElevated } = {}) {
  const cfg = api.pluginConfig?.ios ?? {};

  if (!api.runtime?.tasks?.managedFlows) {
    api.logger.info("ios: no managedFlows runtime — skipping registration (CLI context)");
    return;
  }

  const activities = cfg.activities ?? {};
  const enabledTypes = {
    agentApproval:    activities.agentApproval    !== false,
    pipelineProgress: activities.pipelineProgress !== false,
    agentRunning:     activities.agentRunning     !== false, // default true (was opt-in before)
  };

  const agentsById = new Map();
  for (const entry of api.config?.agents?.list ?? []) {
    if (!entry?.id) continue;
    const emoji = entry.identity?.emoji;
    const name = entry.identity?.name ?? entry.name ?? entry.id;
    if (emoji || name) agentsById.set(entry.id, { emoji, name });
  }
  const agentLookup = (agentId) => agentsById.get(agentId);

  const pipelinesById = new Map();
  const pipelinesDir = path.isAbsolute(cfg.pipelinesDir || "") ? cfg.pipelinesDir : DEFAULT_PIPELINES_DIR;

  api.registerService({
    id: "live-activity-bridge-pipeline-registry",
    start: async () => {
      try {
        const loaded = await loadPipelines(pipelinesDir, api.logger);
        for (const [id, pipeline] of loaded.entries()) {
          const identity = pipeline?.identity ?? {};
          pipelinesById.set(id, {
            id,
            emoji: identity.emoji,
            sfSymbol: identity.sfSymbol,
            displayName: identity.displayName ?? identity.name,
            nodes: pipeline?.nodes ?? {},
          });
        }
        api.logger.info(`ios: pipeline registry built (${pipelinesById.size} pipeline(s))`);
      } catch (err) {
        api.logger.warn(`ios: pipeline registry build failed: ${err?.message ?? err}`);
      }
    },
  });

  const pipelineLookup = (pipelineId) => {
    const meta = pipelinesById.get(pipelineId);
    if (!meta) return undefined;
    const out = { id: meta.id };
    if (meta.emoji) out.emoji = meta.emoji;
    if (meta.sfSymbol) out.sfSymbol = meta.sfSymbol;
    if (meta.displayName) out.displayName = meta.displayName;
    return out;
  };

  const nodeAgentLookup = (pipelineId, nodeId) => {
    const meta = pipelinesById.get(pipelineId);
    if (!meta) return null;
    const node = meta.nodes?.[nodeId];
    if (!node) {
      // nodeId may not be a top-level pipeline.nodes key — a human-gate
      // like GAP_GATE can live nested inside a subflow's flow.nodes (e.g.
      // PROPOSE.flow.nodes.GAP_GATE). Search subflow nodes' own flows for a
      // matching sub-node so subflow-internal gates can still report their
      // own deckPresentation opt-in (gate-projector.js's projectGateItem
      // looks this up by the actual awaiting node id, not just its parent
      // subflow's id).
      for (const topNode of Object.values(meta.nodes ?? {})) {
        if (topNode?.kind !== "subflow") continue;
        const subNode = topNode?.flow?.nodes?.[nodeId];
        if (!subNode || typeof subNode !== "object") continue;
        const subKind = subNode.kind;
        const subSkill = typeof subNode.skill === "string" ? subNode.skill : null;
        const subAgent = typeof subNode.agent === "string" ? subNode.agent : null;
        if (subKind === "human-gate") {
          const deckPresentation = subNode.deckPresentation === true;
          if (!subAgent && !deckPresentation) return null;
          return { agent: subAgent, kind: subKind, skill: subSkill, deckPresentation };
        }
        if (!subAgent) return null;
        return { agent: subAgent, kind: subKind, skill: subSkill };
      }
      return null;
    }
    const kind = node.kind;
    const skill = typeof node.skill === "string" ? node.skill : null;
    if (kind === "subflow") {
      const flowNodes = {};
      for (const [subId, subNode] of Object.entries(node?.flow?.nodes ?? {})) {
        if (!subNode || typeof subNode !== "object") continue;
        flowNodes[subId] = {
          kind: subNode.kind,
          agent: typeof subNode.agent === "string" ? subNode.agent : null,
          skill: typeof subNode.skill === "string" ? subNode.skill : null,
        };
      }
      const deckPresentation = node.deckPresentation === true;
      return { agent: null, kind, skill, flowNodes, deckPresentation };
    }
    const agent = typeof node.agent === "string" ? node.agent : null;
    // A plain (non-subflow) human-gate node rarely declares .agent — it's a
    // pendingSelector/approvalRequestBuilder gate, not a skill-invoking
    // step. Root-level multi-item deckPresentation (gate-projector.js's
    // projectGateItem) reads deckPresentation off THIS lookup for
    // such nodes too, so bail to null only when there's neither an agent
    // NOR a deck opt-in to report — an agent-less, non-deck human-gate
    // still correctly returns null (unchanged from before).
    if (kind === "human-gate") {
      const deckPresentation = node.deckPresentation === true;
      if (!agent && !deckPresentation) return null;
      return { agent, kind, skill, deckPresentation };
    }
    if (!agent) return null;
    return { agent, kind, skill };
  };

  const nodeDiscussLookup = (pipelineId, nodeId) => {
    const meta = pipelinesById.get(pipelineId);
    if (!meta) return null;
    return resolveNodeDiscuss(meta, nodeId);
  };

  // ── OpsSummary in-memory mirror ───────────────────────────────────────────
  // Tracks running and recently-terminal flows so we can compute the
  // OpsSummary contentState server-side for APNs LA pushes. Seeded
  // updated incrementally as the live-bridge emits live_activity.start/end events.

  const TERMINAL_WINDOW_MS = 5 * 60_000; // matches iOS EventConsumer.terminalWindowSeconds

  // flowId → { pipelineId, displayName, emoji, startedAtMs, currentNodeLabel }
  const _runningFlows = new Map();
  // flowId → { pipelineId, displayName, emoji, status, endedAtMs }
  const _recentTerminals = new Map();

  function _pruneTerminals() {
    const cutoff = Date.now() - TERMINAL_WINDOW_MS;
    for (const [id, t] of _recentTerminals) {
      if (t.endedAtMs < cutoff) _recentTerminals.delete(id);
    }
  }

  // WS-only relay shim — the SSE /stream endpoint is removed; all
  // live_activity.* / failure_event.start events now reach iOS via WS.
  // Duck-typed to match the relay.broadcast(msg) call sites in live-bridge.js.
  const relay = {
    broadcast: (message) => {
      try { liveActivityEmitter.emit("broadcast", message); } catch {}

      const { type, activityType, activityId } = message ?? {};

      // ── AgentApproval lifecycle → push when pending count changes ──
      if ((type === "live_activity.start" && activityType === "AgentApproval") ||
          (type === "live_activity.end" && typeof activityId === "string" && activityId.startsWith("agent-approval:"))) {
        scheduleOpsSummaryPush();
      }
    },
    get size() { return 0; },
  };

  // ── PipelineProgress lifecycle → update _runningFlows / _recentTerminals ──
  // Used to be driven by live-bridge.js's own live_activity.start/update/end
  // PipelineProgress broadcasts through relay.broadcast above. Those were
  // removed when pipeclaw.run.* absorbed PipelineProgress — index.js's
  // _wsRunEventsListener now fans the same buildRunPatch payload out
  // in-process via liveActivityEmitter (top-level flows only) alongside
  // its WS broadcast, so this tracker stays fed from one source instead of
  // needing its own parallel event.
  liveActivityEmitter.on("pipeclaw.run.patch", (patch) => {
    try {
      const flowId = patch?.flowId;
      if (!flowId) return;
      if (patch.status === "running") {
        const existing = _runningFlows.get(flowId);
        _runningFlows.set(flowId, {
          pipelineId:       patch.pipelineId ?? "unknown",
          displayName:      patch.pipelineDisplayName ?? patch.pipelineId ?? "unknown",
          emoji:            patch.pipelineEmoji ?? null,
          startedAtMs:      patch.createdAtMs ?? existing?.startedAtMs ?? Date.now(),
          currentNodeLabel: patch.currentNodeId ?? existing?.currentNodeLabel ?? null,
        });
        scheduleOpsSummaryPush();
        return;
      }
      const entry = _runningFlows.get(flowId);
      if (!entry) return;
      _runningFlows.delete(flowId);
      api.logger.info(`ios: OpsSummary run.patch end (flow=${flowId.slice(0, 8)}, status=${patch.status ?? "?"}, running=${_runningFlows.size}, terminals=${_recentTerminals.size})`);
      if (patch.status === "succeeded" || patch.status === "failed" || patch.status === "cancelled") {
        _recentTerminals.set(flowId, { ...entry, status: patch.status, endedAtMs: patch.endedAtMs ?? Date.now() });
      }
      scheduleOpsSummaryPush();
    } catch {}
  });

  // Compute the OpsSummary contentState from in-memory state + bridge snapshot.
  // Shape matches OpsSummaryAttributes.ContentState on the iOS side.
  async function computeOpsSummaryContentState() {
    _pruneTerminals();
    const snap = await bridge.snapshot();
    const approvals = snap.filter((m) => m.activityType === "AgentApproval");
    const pendingApprovals = approvals.length;
    const topPendingLabel = approvals[0]?.attributes?.pipelineDisplayName ?? null;

    // pendingByPipeline — omit when only one pipeline has pending items
    const pendingCounts = {};
    for (const m of approvals) {
      const key = m.attributes?.pipelineDisplayName ?? m.attributes?.pipelineId ?? "unknown";
      pendingCounts[key] = (pendingCounts[key] ?? 0) + 1;
    }
    const pendingByPipeline = Object.keys(pendingCounts).length > 1
      ? Object.entries(pendingCounts)
          .map(([displayName, count]) => ({ displayName, count }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 3)
      : [];

    // topRunning — top 2 running pipelines by startedAtMs desc
    const sorted = [..._runningFlows.values()].sort((a, b) => b.startedAtMs - a.startedAtMs);
    const topRunning = sorted.slice(0, 2).map((r) => ({
      pipelineId:       r.pipelineId,
      displayName:      r.displayName,
      emoji:            r.emoji ?? null,
      currentNodeLabel: r.currentNodeLabel ?? null,
      startedAtMs:      r.startedAtMs,
    }));

    // recentTerminal — most recent terminal within the 5-min window
    const terminals = [..._recentTerminals.values()].sort((a, b) => b.endedAtMs - a.endedAtMs);
    const recentTerminal = terminals.length > 0
      ? { pipelineId: terminals[0].pipelineId, displayName: terminals[0].displayName,
          emoji: terminals[0].emoji ?? null, status: terminals[0].status, endedAtMs: terminals[0].endedAtMs }
      : null;

    return {
      pendingApprovals,
      runningPipelines: _runningFlows.size,
      topPendingLabel,
      pendingByPipeline,
      topRunning,
      recentTerminal,
      updatedAtMs: Date.now(),
    };
  }

  // Debounced push — coalesces event bursts into a single APNs call.
  let _opsPushTimer = null;
  function scheduleOpsSummaryPush() {
    if (_opsPushTimer) clearTimeout(_opsPushTimer);
    _opsPushTimer = setTimeout(async () => {
      _opsPushTimer = null;
      const contentState = await computeOpsSummaryContentState();
      const tokenCount = (await listLaTokens()).length;
      api.logger.info(`ios: OpsSummary push scheduled (tokens=${tokenCount}, recentTerminal=${contentState.recentTerminal?.displayName ?? "null"}, pending=${contentState.pendingApprovals})`);
      await sendLaUpdateToAll(contentState).catch((err) => {
        api.logger.warn(`ios: sendLaUpdateToAll threw: ${err?.message ?? err}`);
      });
    }, 400);
  }

  // ── Live Activity push-token store ────────────────────────────────────────
  // Persists APNs push tokens for the OpsSummary Live Activity so the server
  // can push ContentState updates directly to the lock screen without needing
  // the app to be foregrounded. Table lives in ops.sqlite (same DB as budget).

  // Shares the process-wide cached connection (getSharedDb) rather than
  // opening a private one — a private connection can't be serialized
  // against every other ops.sqlite writer (telemetry/store.js's insertCost,
  // telemetry/budget-alert.js, budget/index.js) via withSharedWrite, since
  // that queue is keyed by dbPath against a shared client instance.
  let _laClient = null;
  (async () => {
    try {
      mkdirSync(path.dirname(OPS_DB_PATH), { recursive: true });
      const { client } = await getSharedDb(OPS_DB_PATH);
      await withSharedWrite(OPS_DB_PATH, () => client.execute(`
        CREATE TABLE IF NOT EXISTS la_push_tokens (
          activity_id  TEXT PRIMARY KEY,
          push_token   TEXT NOT NULL,
          environment  TEXT NOT NULL DEFAULT 'production',
          updated_at   INTEGER NOT NULL
        )
      `));
      _laClient = client;
    } catch (err) {
      api.logger.warn(`ios: la_push_tokens table init failed: ${err?.message ?? err}`);
    }
  })();

  async function upsertLaToken(activityId, pushToken, environment = "production") {
    if (!_laClient) return;
    await withSharedWrite(OPS_DB_PATH, () => _laClient.execute({
      sql: `INSERT INTO la_push_tokens (activity_id, push_token, environment, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET push_token=excluded.push_token,
              environment=excluded.environment, updated_at=excluded.updated_at`,
      args: [activityId, pushToken, environment, Date.now()],
    }));
  }

  async function removeLaToken(activityId) {
    if (!_laClient) return;
    await withSharedWrite(OPS_DB_PATH, () => _laClient.execute({ sql: "DELETE FROM la_push_tokens WHERE activity_id = ?", args: [activityId] }));
  }

  async function listLaTokens() {
    if (!_laClient) return [];
    const { rows } = await _laClient.execute("SELECT * FROM la_push_tokens");
    return rows;
  }

  async function sendLaUpdateToAll(contentState) {
    const apns = api.runtime?.apns;
    if (!apns || !apns.isConfigured?.()) return;
    const tokens = await listLaTokens();
    if (tokens.length === 0) return;
    const bundleId = api.pluginConfig?.ios?.bundleId ?? "ai.dashclaw.client.ios";
    const laTopic = `${bundleId}.push-type.liveactivity`;
    const timestamp = Math.floor(Date.now() / 1000);
    const payload = { aps: { timestamp, event: "update", "content-state": contentState } };
    for (const row of tokens) {
      try {
        // iOS sends "development" for debug/simulator builds; the apns plugin
        // understands "sandbox" and "production" — map the former.
        const apnsEnv = row.environment === "development" ? "sandbox" : row.environment;
        const result = await apns.send({
          deviceToken: row.push_token,
          payload,
          pushType: "liveactivity",
          priority: 10,
          topic: laTopic,
          environment: apnsEnv,
        });
        if (result.ok) {
          api.logger.info(`ios: OpsSummary LA push sent (activity=${row.activity_id.slice(0, 8)}, env=${row.environment})`);
        } else {
          api.logger.warn(`ios: LA push rejected (status=${result.status}, reason=${result.reason}, activity=${row.activity_id.slice(0, 8)})`);
          if (result.reason === "BadDeviceToken" || result.reason === "Unregistered") {
            await removeLaToken(row.activity_id);
          }
        }
      } catch (err) {
        api.logger.warn(`ios: LA push threw for ${row.activity_id.slice(0, 8)}: ${err?.message ?? err}`);
      }
    }
  }

  // ── APNs push helpers ──────────────────────────────────────────────────────

  function apnsTokensOrNothing() {
    const apns = api.runtime?.apns;
    if (!apns || !apns.isConfigured?.()) {
      api.logger.warn(`ios: apns push skipped (reason=not_configured)`);
      return null;
    }
    const tokens = apns.listTokens?.() ?? [];
    if (tokens.length === 0) {
      api.logger.warn(`ios: apns push skipped (reason=no_tokens)`);
      return null;
    }
    return { apns, tokens };
  }

  async function apnsSendToAll(payload) {
    const got = apnsTokensOrNothing();
    if (!got) return;
    const { apns, tokens } = got;
    for (const t of tokens) {
      try {
        const result = await apns.send({ deviceToken: t.apns_token, payload, pushType: "alert", priority: 10 });
        if (result.ok) {
          api.logger.info(`ios: apns push sent (device=${(t.device_id || "?").slice(0, 12)}, title="${payload?.aps?.alert?.title ?? ""}")`);
        } else {
          api.logger.warn(`ios: apns push rejected (status=${result.status}, reason=${result.reason}, device=${(t.device_id || "?").slice(0, 12)})`);
          if (result.reason === "BadDeviceToken" || result.reason === "Unregistered") apns.dropToken?.({ deviceId: t.device_id });
        }
      } catch (err) {
        api.logger.warn(`ios: apns push threw for ${(t.device_id || "?").slice(0, 12)}: ${err?.message ?? err}`);
      }
    }
  }

  // `items` (plural, 2+) sends one coalesced summary push instead of one per
  // item — used when a fresh batch opens many gate items at once (e.g. a
  // subflow gate's whole selector settling in one executeSubflowNode pass).
  // `item` (singular) is the normal per-item push. Both call sites (in
  // live-bridge.js) always pass exactly one of the two.
  async function apnsPushApproval({ flow, item, items }) {
    const pipelineId = flow.stateJson?.pipelineId ?? "unknown";
    const declared = pipelineLookup(pipelineId) || {};
    const displayName = declared.displayName || pipelineId.replace(/^([a-z]+)_(.+)$/, "$1: $2").replace(/-/g, " ");
    let ctx;
    if (Array.isArray(items) && items.length > 1) {
      ctx = {
        pipelineDisplayName: displayName,
        body: `${items.length} items need your input`,
        flowId: flow.flowId, itemId: "", activityId: flow.flowId,
      };
    } else {
      ctx = {
        pipelineDisplayName: displayName,
        body: (typeof item.requestPayload?.title === "string" && item.requestPayload.title)
          ? item.requestPayload.title : `Approve ${item.itemId}?`,
        flowId: flow.flowId, itemId: item.itemId, activityId: `${flow.flowId}:${item.itemId}`,
      };
    }
    await pushKind({ kindId: NotificationKinds.APPROVAL, ctx, sendToAll: apnsSendToAll, logger: api.logger });
  }

  async function apnsPushFailure(row) {
    await pushKind({
      kindId: NotificationKinds.FAILURE,
      ctx: {
        failureId: row.id, flowId: row.flow_id, pipelineId: row.pipeline_id,
        branchItemId: row.branch_item_id, failureNode: row.failure_node,
        errorType: row.error_type, failureSummary: row.failure_summary,
      },
      sendToAll: apnsSendToAll,
      logger: api.logger,
    });
  }

  const bridge = createLiveBridge({
    relay, logger: api.logger,
    agentLookup, pipelineLookup, nodeAgentLookup, nodeDiscussLookup,
    enableAgentRunning: enabledTypes.agentRunning,
    apnsPushFailure, apnsPushApproval,
    runStore, nodeExecutionsStore,
  });
  const intentRouter = createIntentRouter({ api, logger: api.logger, gateResolver, invokeElevated });

  // Expose a rescan hook so the APNs plugin can trigger a gate re-scan
  api.runtime = api.runtime ?? {};
  api.runtime.iosBridge = api.runtime.iosBridge ?? {};

  // ── HTTP routes ────────────────────────────────────────────────────────────

  api.registerHttpRoute({
    path: STATE_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.setHeader("Allow", "GET"); res.end("method not allowed"); return true; }
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      try {
        _pruneTerminals();
        const open = await bridge.snapshot();
        const recentTerminals = [..._recentTerminals.entries()].map(([flowId, t]) => ({
          activityId: `pipeline-progress:${flowId}`,
          pipelineId: t.pipelineId,
          displayName: t.displayName,
          emoji: t.emoji ?? "",
          status: t.status,
          endedAtMs: t.endedAtMs,
        }));
        // Separate from `open` on purpose — `open` is decoded client-side
        // as a strict [InboxActivity] (InboxAPI.swift's InboxClient.fetch()
        // requires activityId/activityType/attributes/contentState on every
        // element), so a RunSummaryPatch-shaped entry in there throws the
        // whole array decode. Sourced from _runningFlows (already kept live
        // by the pipeclaw.run.patch listener + boot seed above) rather than
        // a fresh DB query — same data, no extra work.
        const runningPipelines = [..._runningFlows.entries()].map(([flowId, r]) => ({
          type: "pipeclaw.run.updated",
          flowId,
          pipelineId: r.pipelineId,
          status: "running",
          createdAtMs: r.startedAtMs,
          currentNodeId: r.currentNodeLabel,
          pipelineDisplayName: r.displayName,
          pipelineEmoji: r.emoji,
        }));
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.setHeader("Cache-Control", "no-cache");
        res.end(JSON.stringify({ open, recentTerminals, runningPipelines, emittedAtMs: Date.now() }));
      } catch (err) {
        api.logger.warn(`ios state snapshot failed: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: err?.message ?? "snapshot failed" }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: INTENT_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true; }
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      api.logger.info(`ios: intent via ${authz.via}`);
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let payload;
      try { payload = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      try {
        const result = await intentRouter.handle(payload);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify(result));
      } catch (err) {
        api.logger.warn(`ios intent failed: ${err?.message ?? err}`);
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: err?.message ?? "unknown" }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: TEST_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true; }
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let parsed;
      try { parsed = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      let message;
      if (parsed.kind === "synth.agent-approval") {
        message = synthAgentApproval({ seed: parsed.seed, phase: parsed.phase, flowId: parsed.flowId });
      } else if (typeof parsed.type === "string") {
        message = parsed;
      } else {
        res.statusCode = 400; res.end("missing type or kind"); return true;
      }
      relay.broadcast(message);
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, recipients: relay.size, broadcast: message }));
      return true;
    },
  });

  api.registerHttpRoute({
    path: PUSH_PATH, auth: "gateway", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") { res.statusCode = 405; res.setHeader("Allow", "POST"); res.end("method not allowed"); return true; }
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let parsed;
      try { parsed = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      if (typeof parsed.type !== "string" || !parsed.type) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: "missing type" }));
        return true;
      }
      relay.broadcast(parsed);
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, recipients: relay.size }));
      return true;
    },
  });


  // ── Deploys: read-only host state ────────────────────────────────
  // Answers "what is this Mac running, and does it match main?" — which has
  // no other home now that deploys are CI-triggered and nothing polls.
  // auth:"plugin" (device token) like the rest of the app's reads; neither
  // route needs operator scope, so no elevation loopback is involved.
  api.registerHttpRoute({
    path: DEPLOY_STATUS_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.setHeader("Allow", "GET"); res.end("method not allowed"); return true; }
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      try {
        const status = await readDeployStatus({});
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify(status));
      } catch (err) {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: err?.message ?? "unknown" }));
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: DEPLOY_LOG_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") { res.statusCode = 405; res.setHeader("Allow", "GET"); res.end("method not allowed"); return true; }
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const tail = Number(new URL(req.url, "http://localhost").searchParams.get("tail")) || 200;
      const out = readDeployLog({ tail });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(out));
      return true;
    },
  });

  api.registerHttpRoute({
    path: LA_TOKEN_PATH, auth: "plugin", match: "exact",
    handler: async (req, res) => {
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      if (req.method === "POST") {
        let raw = "";
        for await (const chunk of req) raw += chunk;
        let parsed;
        try { parsed = raw ? JSON.parse(raw) : {}; }
        catch { res.statusCode = 400; res.end("invalid json"); return true; }
        const { activityId, pushToken, environment } = parsed;
        if (!activityId || !pushToken) {
          res.statusCode = 400;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: false, error: "activityId and pushToken required" }));
          return true;
        }
        await upsertLaToken(activityId, pushToken, environment ?? "production");
        api.logger.info(`ios: LA push token registered (activity=${activityId.slice(0, 8)}, env=${environment ?? "production"})`);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: true }));
      } else if (req.method === "DELETE") {
        let raw = "";
        for await (const chunk of req) raw += chunk;
        let parsed;
        try { parsed = raw ? JSON.parse(raw) : {}; }
        catch { res.statusCode = 400; res.end("invalid json"); return true; }
        const { activityId } = parsed;
        if (!activityId) {
          res.statusCode = 400;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: false, error: "activityId required" }));
          return true;
        }
        await removeLaToken(activityId);
        api.logger.info(`ios: LA push token removed (activity=${activityId.slice(0, 8)})`);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: true }));
      } else {
        res.statusCode = 405;
        res.setHeader("Allow", "POST, DELETE");
        res.end("method not allowed");
      }
      return true;
    },
  });

  api.registerService({
    id: "live-activity-bridge",
    start: async () => {
      await bridge.start();
      // One-shot boot seed for _runningFlows — mirrors live-bridge.js's own
      // start()/snapshot() seeding (same runStore.listActive() query,
      // same top-level-only filter). Without this, a flow already running
      // when the gateway (re)starts never appears in OpsSummary's
      // topRunning/runningPipelines until its next revision fires.
      try {
        for (const flow of await runStore.listActive()) {
          if (flow.status !== "running" || flow.parentInvocationId) continue;
          const patch = buildRunPatch(flow, Date.now(), { pipelineLookup });
          _runningFlows.set(flow.flowId, {
            pipelineId:       patch.pipelineId ?? "unknown",
            displayName:      patch.pipelineDisplayName ?? patch.pipelineId ?? "unknown",
            emoji:            patch.pipelineEmoji ?? null,
            startedAtMs:      patch.createdAtMs ?? Date.now(),
            currentNodeLabel: patch.currentNodeId ?? null,
          });
        }
      } catch (err) {
        api.logger.warn(`ios: OpsSummary boot seed failed: ${err?.message ?? err}`);
      }
    },
    stop:  async () => bridge.stop(),
  });

  const enabledList = Object.entries(enabledTypes).filter(([, v]) => v).map(([k]) => k).join(", ");
  api.logger.info(
    `ios: state at ${STATE_PATH}; intent at ${INTENT_PATH}. ` +
    `Live bridge (activities=[${enabledList}]).`
  );
}
