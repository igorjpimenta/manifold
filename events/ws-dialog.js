// events/ws-dialog.js — message transport into an open discuss session,
// over the harness's own WebSocket connection.
//
// The parrot channel's HTTP route and the gateway discuss route both
// implemented this same turn lifecycle (validate → 202+turnId → persist
// the operator message → stamp the session → run the turn → persist the
// reply → emit the transcript). This door is that lifecycle on a socket:
// one connection carries outbound turns and inbound replies, so any
// harness client needs no channel plugin and no HTTP round-trips.
//
// Wire protocol, client → server (JSON text frames):
//   {"type":"message","id":<client-echo?>,flowId,itemId?,text}
// Wire protocol, server → client:
//   {"type":"ack","id"?,turnId}
//   {"type":"reply","id"?,turnId,text,proposedDraft?,proposedMeta?,durationMs?}
//   {"type":"error","id"?,turnId?,code,error?}
// Connect with ?bearer=<MANIFOLD_BEARER> (timing-safe compare, fail-closed
// when the env is unset — same posture as routes/bearer-auth.js, but from
// the query string because WebSocket clients cannot set headers).
// Bearer secrets travel in URLs: prefer wss:// and note proxy access logs.
//
// The turn itself runs through injected runTurn (the adapter closes over
// invokeElevated → /manifold/_elev/discuss, the same elevation the HTTP
// route uses). No pending files: the old discuss-turns markers were
// write-only, and discussStore's turn_status is the live record.
import http from "node:http";
import crypto from "node:crypto";
import { randomUUID } from "node:crypto";
import { attachWsEndpoint, encodeWsFrame } from "./ws-frame.js";
import { buildDiscussPrompt } from "../routes/discuss.js";

const MAX_MESSAGE_BYTES = 1024 * 1024;
// The dialog listener sits at 44302, inside the 44xxx band manifold reserves
// for its own listeners (api 44301, dashboard 44300), so a collision can only
// come from an explicit override.
export const DEFAULT_DIALOG_PORT = 44302;

function tokensEqual(a, b) {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) {
    crypto.timingSafeEqual(ab, ab); // burn the cycles to keep timing flat
    return false;
  }
  return crypto.timingSafeEqual(ab, bb);
}

let warnedNoSecret = false;

function checkBearer(bearer, logger) {
  if (typeof bearer !== "string" || bearer === "") return { ok: false, reason: "no-bearer" };
  const secret = process.env.MANIFOLD_BEARER ?? "";
  if (!secret) {
    if (!warnedNoSecret) {
      warnedNoSecret = true;
      try { logger?.warn?.("manifold: dialog door MANIFOLD_BEARER unset — every connect fails closed"); } catch {}
    }
    return { ok: false, reason: "no-secret" };
  }
  if (!tokensEqual(bearer, secret)) return { ok: false, reason: "no-match" };
  return { ok: true };
}

function send(socket, obj) {
  try {
    if (!socket.destroyed && socket.writable) socket.write(encodeWsFrame(JSON.stringify(obj)));
  } catch { /* client gone; cleanup on close */ }
}

export function createDialogServer({
  port = null,
  host = "127.0.0.1",
  path = "/api/dialog",
  readFlow,
  resolveTarget,
  runTurn,
  discussStore,
  sessionIndex,
  recordLineage = null,
  runStore = null,
  nodeExecutionsStore = null,
  emit,
  logger = null,
} = {}) {
  if (typeof readFlow !== "function") throw new Error("ws-dialog: readFlow(flowId) is required");
  if (typeof resolveTarget !== "function") throw new Error("ws-dialog: resolveTarget(flow, itemId) is required");
  if (typeof runTurn !== "function") throw new Error("ws-dialog: runTurn(turn) is required");
  if (!discussStore) throw new Error("ws-dialog: discussStore is required");
  if (!sessionIndex?.recordThread) throw new Error("ws-dialog: sessionIndex.recordThread is required");
  const listenPort = port ?? DEFAULT_DIALOG_PORT;
  let server = null;
  let door = null;
  let running = false;

  async function handleMessage(socket, raw) {
    let msg = null;
    try {
      if (typeof raw !== "string" || raw.length > MAX_MESSAGE_BYTES) throw new Error("too_large");
      msg = JSON.parse(raw);
    } catch {
      send(socket, { type: "error", code: "invalid_frame", error: "message must be JSON under 1MB" });
      return;
    }
    if (!msg || msg.type !== "message") {
      send(socket, { type: "error", id: msg?.id ?? null, code: "unsupported_type", error: "only {type:\"message\", flowId, text} is accepted" });
      return;
    }
    const flowId = typeof msg.flowId === "string" ? msg.flowId.trim() : "";
    const itemId = typeof msg.itemId === "string" ? msg.itemId : null;
    const text = typeof msg.text === "string" ? msg.text.trim() : "";
    if (!flowId || !text) {
      send(socket, { type: "error", id: msg.id ?? null, code: "invalid_body", error: "flowId and text are required" });
      return;
    }
    let flow = null;
    try {
      flow = await readFlow(flowId);
    } catch (err) {
      logger?.warn?.(`manifold dialog: flow read failed: ${err?.message ?? err}`);
    }
    if (!flow) {
      send(socket, { type: "error", id: msg.id ?? null, code: "flow_not_found", error: `flow not found: ${flowId}` });
      return;
    }
    const target = await resolveTarget(flow, itemId);
    if (!target?.ok) {
      send(socket, { type: "error", id: msg.id ?? null, code: target?.code ?? "no_target", error: target?.error ?? "no discuss target" });
      return;
    }
    const systemPrompt = await buildDiscussPrompt({ flow, target, runStore, nodeExecutionsStore, logger });
    const turnId = randomUUID();
    const flowIdShort = flow.flowId.slice(0, 8);
    send(socket, { type: "ack", id: msg.id ?? null, turnId });

    try {
      await discussStore.appendMessage(flow.flowId, target.itemId, { turnId, role: "operator", text, ts: Date.now() });
      await discussStore.setTurnStatus(flow.flowId, target.itemId, "running", turnId);
    } catch (err) {
      logger?.warn?.(`manifold dialog: discuss-store write (operator msg): ${err?.message ?? err}`);
    }
    if (target.needsStamp) {
      try {
        await sessionIndex.recordThread({
          flowId: flow.flowId, gateNodeId: target.gateNodeId ?? "",
          itemId: target.itemId, agentId: target.agent ?? null, session: target.session,
        });
        await recordLineage?.({
          childSession: target.session, flowId: flow.flowId, nodeId: target.nodeId?.toLowerCase?.() ?? null,
          itemKey: target.itemId, pipelineId: flow.stateJson?.pipelineId ?? null, agentId: target.agent ?? null,
          kind: "flow-gate-discuss", source: "spawn_hook",
        });
      } catch (err) {
        logger?.warn?.(`manifold dialog: stamp failed: ${err?.message ?? err}`);
      }
    }

    const startedMs = Date.now();
    let result;
    try {
      result = await runTurn({
        flowId: flow.flowId, session: target.session, agentId: target.agent,
        gateNodeId: target.gateNodeId,
        modelOverride: target.modelOverride ?? null, itemId: target.itemId, text,
        systemPrompt: typeof systemPrompt === "string" ? systemPrompt : null,
      });
    } catch (err) {
      result = { ok: false, code: "dispatch_failed", error: err?.message ?? String(err) };
    }
    const durationMs = Date.now() - startedMs;
    if (result?.ok) {
      try {
        await discussStore.appendMessage(flow.flowId, target.itemId, {
          turnId, role: "agent", text: result.reply ?? "",
          proposedDraft: result.proposedDraft ?? null, proposedMeta: result.proposedMeta ?? null, ts: Date.now(),
        });
        await discussStore.clearPending(flow.flowId, target.itemId);
        await discussStore.setTurnStatus(flow.flowId, target.itemId, "idle", null);
      } catch (err) {
        logger?.warn?.(`manifold dialog: discuss-store write (agent reply): ${err?.message ?? err}`);
      }
      try {
        await emit?.("session.transcript.completed", {
          scope: "discuss", turnId, flowId: flow.flowId, itemId: target.itemId,
          text: result.reply ?? "", durationMs, emittedAtMs: Date.now(),
        });
      } catch {}
      send(socket, {
        type: "reply", id: msg.id ?? null, turnId, text: result.reply ?? "",
        ...(result.proposedDraft != null ? { proposedDraft: result.proposedDraft } : {}),
        ...(result.proposedMeta != null ? { proposedMeta: result.proposedMeta } : {}),
        durationMs,
      });
    } else {
      const errText = result?.error ?? result?.code ?? "turn failed";
      try {
        await discussStore.stampMessageError(flow.flowId, target.itemId, turnId, errText);
        await discussStore.setTurnStatus(flow.flowId, target.itemId, "error", null);
      } catch {}
      try {
        await emit?.("session.transcript.failed", {
          scope: "discuss", turnId, flowId: flow.flowId, itemId: target.itemId, error: errText,
        });
      } catch {}
      send(socket, { type: "error", id: msg.id ?? null, turnId, code: result?.code ?? "turn_failed", error: errText });
    }
    logger?.info?.(`manifold dialog [${flowIdShort}/${turnId.slice(0, 8)}]: turn ${result?.ok ? "completed" : "failed"} in ${durationMs}ms`);
  }

  return {
    async start() {
      if (running) return;
      server = http.createServer((req, res) => {
        res.writeHead(404, { "content-type": "text/plain" });
        res.end("manifold dialog: websocket only — connect with an Upgrade request\n");
      });
      door = attachWsEndpoint(server, {
        match: (pathname) => pathname === path,
        onConnect: (socket, req) => {
          let bearer = "";
          try {
            bearer = new URL(req.url ?? "/", "http://localhost").searchParams.get("bearer") ?? "";
          } catch { /* keep "" */ }
          const authz = checkBearer(bearer, logger);
          if (!authz.ok) {
            send(socket, { type: "error", code: "unauthorized", error: authz.reason });
            socket.destroy();
            return null;
          }
          return null;
        },
        onMessage: (socket, text) => {
          handleMessage(socket, text).catch((err) => {
            logger?.warn?.(`manifold dialog: turn failed: ${err?.message ?? err}`);
            send(socket, { type: "error", code: "turn_failed", error: err?.message ?? String(err) });
          });
        },
        logger,
      });
      await new Promise((resolve, reject) => {
        server.once("error", (err) => { server = null; reject(err); });
        server.listen(listenPort, host, () => resolve());
      });
      running = true;
      logger?.info?.(`manifold dialog: listening on ws://${host}:${server.address().port}${path} (bearer-guarded)`);
    },
    async stop() {
      running = false;
      try { door?.close(); } catch {}
      door = null;
      if (server) {
        await new Promise((res) => server.close(() => res()));
        server = null;
      }
    },
    // Test seam: drive one turn without a socket.
    _handleMessage: handleMessage,
    clients: () => door?.clientCount?.() ?? 0,
    url() { return `ws://${host}:${server ? server.address().port : listenPort}${path}`; },
  };
}
