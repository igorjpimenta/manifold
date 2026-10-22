// ios-transport bridge — OpenClaw plugin that carries manifold events to
// the iOS app. Manifold core no longer pushes to iOS itself; this module
// owns that lane entirely:
//
//   - Foreground: re-broadcasts the two event names manifold core never
//     emits but the app handles — `failure_event.start` (new failure rows)
//     and `approval.requested` (new pending approvals) — over the gateway
//     WS broadcast, which the app already subscribes to. Everything else
//     the app needs foreground arrives via manifold's canonical WS kinds.
//   - Background: POSTs arrivals to the gateway's /manifold/ios/push
//     endpoint (which fans out to registered devices, APNs included).
//     No token configured → pushes are skipped, WS emission continues.
//
// Reads manifold state directly (fresh short-lived client per cycle, no
// shared handles) and never writes schema — tables are manifold's to
// create. First cycle baselines silently (marks everything seen, emits
// nothing) so a gateway restart never replays history as new arrivals.
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { createClient } from "@libsql/client";

const DEFAULT_RUNTIME_DB = () => path.join(os.homedir(), ".openclaw", "runtime.sqlite");
const DEFAULT_OPS_DB = () => path.join(os.homedir(), ".openclaw", "ops.sqlite");

const MAX_TRACKED_IDS = 500;

function pruneSeen(set) {
  if (set.size <= MAX_TRACKED_IDS) return;
  const drop = set.size - MAX_TRACKED_IDS;
  let i = 0;
  for (const id of set) {
    if (i++ >= drop) break;
    set.delete(id);
  }
}

function postPush({ body, logger, warned }) {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN;
  if (!token) {
    if (!warned.current) {
      warned.current = true;
      logger?.warn?.("ios-transport: OPENCLAW_GATEWAY_TOKEN unset — push delivery skipped (WS emission continues)");
    }
    return Promise.resolve({ ok: false, skipped: true });
  }
  const port = Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789;
  const data = JSON.stringify(body);
  return new Promise((resolve) => {
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      method: "POST",
      path: "/manifold/ios/push",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        "Content-Length": Buffer.byteLength(data),
      },
    }, (res) => {
      res.resume();
      res.on("end", () => resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode ?? 0 }));
    });
    req.on("error", (err) => resolve({ ok: false, error: err?.message ?? String(err) }));
    req.setTimeout(10_000, () => req.destroy(new Error("push timeout")));
    req.write(data);
    req.end();
  });
}

function openDb(dbPath) {
  const client = createClient({ url: `file:${dbPath}` });
  return client;
}

export function createBridge({
  runtimeDbPath = DEFAULT_RUNTIME_DB(),
  opsDbPath = DEFAULT_OPS_DB(),
  pollIntervalMs = 15000,
  getBroadcast = null,
  pushEvent = null,
  logger = null,
} = {}) {
  const seenFailures = new Set();
  const seenPending = new Set();
  let lastGate = null;
  let baselined = false;
  let timer = null;
  const warned = { current: false };

  const push = pushEvent ?? ((body) => postPush({ body, logger, warned }));
  const broadcast = (name, payload) => {
    try {
      const fn = typeof getBroadcast === "function"
        ? getBroadcast()
        : getBroadcast;
      fn?.(`plugin.${name}`, payload);
    } catch (err) {
      logger?.warn?.(`ios-transport: broadcast ${name} failed: ${err?.message ?? err}`);
    }
  };

  async function pollOnce() {
    const freshFailures = [];
    const freshApprovals = [];
    let gate = null;

    try {
      const db = openDb(runtimeDbPath);
      try {
        await db.execute("PRAGMA busy_timeout = 30000");
        const { rows } = await db.execute(`
          SELECT f.id, f.flow_id, f.failed_at, f.failure_summary, f.error_type,
                 r.pipeline_id, n.node_id
            FROM failure_events f
            LEFT JOIN flow_node_invocations n ON n.id = f.invocation_id
            LEFT JOIN flow_runs r ON r.flow_id = f.flow_id
           ORDER BY f.failed_at DESC LIMIT 200
        `);
        for (const r of rows) {
          if (!seenFailures.has(r.id)) freshFailures.push(r);
          seenFailures.add(r.id);
        }
        pruneSeen(seenFailures);
      } finally {
        db.close();
      }
    } catch (err) {
      logger?.warn?.(`ios-transport: failure poll failed: ${err?.message ?? err}`);
      return { failures: [], approvals: [], gateChanged: false };
    }

    try {
      const db = openDb(opsDbPath);
      try {
        await db.execute("PRAGMA busy_timeout = 30000");
        const pending = await db.execute(
          "SELECT id, flow_id, node FROM queue_items WHERE status = 'pending' ORDER BY created_at DESC LIMIT 200",
        );
        for (const r of pending.rows) {
          if (!seenPending.has(r.id)) freshApprovals.push(r);
          seenPending.add(r.id);
        }
        pruneSeen(seenPending);
        try {
          const g = await db.execute("SELECT locked, day FROM gate_state WHERE id = 'singleton' LIMIT 1");
          gate = g.rows[0] ?? null;
        } catch {
          gate = null;
        }
      } finally {
        db.close();
      }
    } catch (err) {
      logger?.warn?.(`ios-transport: ops poll failed: ${err?.message ?? err}`);
      return { failures: [], approvals: [], gateChanged: false };
    }

    if (!baselined) {
      baselined = true;
      lastGate = gate ? { locked: gate.locked, day: gate.day } : null;
      return { failures: [], approvals: [], gateChanged: false, baselined: true };
    }

    for (const r of freshFailures) {
      broadcast("failure_event.start", {
        failureId: r.id,
        flowId: r.flow_id,
        pipelineId: r.pipeline_id ?? null,
        failureNode: r.node_id ?? null,
        errorType: r.error_type ?? null,
        failureSummary: r.failure_summary,
      });
      await push({ kind: "failure", failureId: r.id, flowId: r.flow_id ?? null });
    }
    for (const r of freshApprovals) {
      broadcast("approval.requested", { id: r.id, flowId: r.flow_id ?? null });
      if (r.flow_id) {
        await push({ kind: "approval", flowId: r.flow_id, itemId: r.node ?? null });
      }
    }
    let gateChanged = false;
    const gateNow = gate ? { locked: gate.locked, day: gate.day } : null;
    if (JSON.stringify(gateNow) !== JSON.stringify(lastGate) && lastGate !== null) {
      gateChanged = true;
      await push({ kind: "budget" });
    }
    lastGate = gateNow;
    return { failures: freshFailures.map((r) => r.id), approvals: freshApprovals.map((r) => r.id), gateChanged };
  }

  return {
    pollOnce,
    start() {
      if (timer) return;
      pollOnce().catch(() => {});
      timer = setInterval(() => { pollOnce().catch(() => {}); }, pollIntervalMs);
    },
    stop() {
      if (timer) clearInterval(timer);
      timer = null;
    },
  };
}

export function register(api) {
  const userCfg = api.pluginConfig ?? {};
  const pollIntervalMs = Number.isInteger(userCfg.pollIntervalMs) && userCfg.pollIntervalMs >= 1000
    ? userCfg.pollIntervalMs
    : 15000;
  const getBroadcast = () => api.runtime?.manifold?.getBroadcast?.() ?? null;
  const bridge = createBridge({ pollIntervalMs, getBroadcast, logger: api.logger });
  api.registerService({
    id: "ios-transport-bridge",
    start: async () => {
      api.logger?.info?.("ios-transport: bridge watching manifold state for iOS arrivals");
      bridge.start();
    },
    stop: async () => bridge.stop(),
  });
  return { bridge };
}
