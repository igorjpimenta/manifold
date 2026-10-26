// webhook.js — push receiver: wake the agent the moment core attributes
// work to them.
//
// Core POSTs {event, at, flow, session} here when a turn is dispatched
// (agent.dispatched) or a human gate opens (gate.opened) and a push target
// is registered for this harness (see events/push.js for the payload
// contract). This handler verifies the bearer, then opens a FRESH session
// for the receiver via runtime.subagent.run — the new session starts with
// full context, not a pointer. A failed fresh turn falls back to the
// originating sessionKey; if both fail the handler logs loudly (the HTTP
// answer still reports the failure — the run itself is unaffected).
//
// Resilience order per wake: fresh session → originating session →
// report-only. Nothing here ever throws into the HTTP layer.

import { timingSafeEqual } from "node:crypto";

const MAX_BODY_BYTES = 1024 * 1024;

function safeEqual(a, b) {
  const ab = Buffer.from(String(a ?? ""));
  const bb = Buffer.from(String(b ?? ""));
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}

async function readJson(req, res) {
  let size = 0;
  let raw = "";
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      res.statusCode = 413;
      res.end(JSON.stringify({ error: "body too large" }));
      return undefined;
    }
    raw += chunk;
  }
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    res.statusCode = 400;
    res.end(JSON.stringify({ error: "invalid json body" }));
    return undefined;
  }
}

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
}

// Shape-guard push payloads. Returns a string error or null.
// `webhook.test` (from autoregister's test ping) only proves URL + bearer;
// it never wakes anyone.
export function pushEventError(body) {
  if (!body || typeof body !== "object") return "body must be an object";
  if (body.event === "webhook.test") return null;
  if (body.event !== "agent.dispatched" && body.event !== "gate.opened") {
    return `unsupported event ${JSON.stringify(body.event)}`;
  }
  // agent.dispatched always names its receiver — anonymous dispatch is
  // refused upstream, so a missing agent here is a loud shape error.
  // gate.opened may carry no receiver (no agent owns the gate yet); that
  // is a report-only delivery, not an error.
  if (body.event === "agent.dispatched") {
    const s = body.session;
    if (!s || typeof s.agent !== "string" || !s.agent) return "session.agent required";
  }
  return null;
}

// New-session key for a pushed wake. Namespaced so manifold turns are visible.
export function wakeSessionKey(flowId, nodeId) {
  return `manifold-push-${flowId}-${nodeId}`;
}

// Handoff text for the fresh session: full context when core sent it,
// else a composed pointer back to the originating session.
export function wakeMessage(body) {
  if (body.event === "gate.opened") {
    const f = body.flow ?? {};
    return `Manifold gate opened for flow ${f.id ?? "(unknown)"} at node ${f.nodeId ?? "(unknown)"} (pipeline ${f.pipelineId ?? "(unknown)"}). Review it via the manifold operator surface.`;
  }
  const f = body.flow ?? {};
  const s = body.session ?? {};
  return `Manifold dispatched flow ${f.id ?? "(unknown)"} node ${f.nodeId ?? "(unknown)"} to you (originating session ${s.sessionKey ?? "(unknown)"}). Continue the work from the flow state.`;
}

export function registerPushWebhook({ api, logger, getSubagent } = {}) {
  const log = logger ?? api.logger;
  const resolveSubagent = typeof getSubagent === "function" ? getSubagent : () => api.runtime?.subagent;
  api.registerHttpRoute({
    path: "/api/manifold/pushed",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      try {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("Allow", "POST");
          res.end(JSON.stringify({ error: "method not allowed" }));
          return;
        }
        const bearer = api.pluginConfig?.webhookBearer;
        if (typeof bearer !== "string" || bearer === "") {
          // Route registered but disarmed: operator hasn't set the shared
          // secret yet. 503, not 404 — core retries next dispatch, and the
          // operator sees "not configured" instead of a mystery 404.
          json(res, 503, { error: "manifold push webhook not configured (webhookBearer)" });
          return;
        }
        const auth = req.headers?.authorization ?? "";
        if (!auth.startsWith("Bearer ") || !safeEqual(auth.slice(7), bearer)) {
          json(res, 401, { error: "unauthorized" });
          return;
        }
        const body = await readJson(req, res);
        if (body === undefined) return;
        const shapeErr = pushEventError(body);
        if (shapeErr) {
          json(res, 400, { error: shapeErr });
          return;
        }
        if (body.event === "webhook.test") {
          json(res, 200, { ok: true, test: true });
          return;
        }

        if (body.event === "gate.opened" && (typeof body.session?.agent !== "string" || !body.session.agent)) {
          json(res, 200, { ok: true, event: body.event, woken: false, reason: "no-agent" });
          return;
        }
        const agent = body.session.agent;
        const originatingSession = body.session.sessionKey;
        const flowId = body.flow?.id ?? "unknown";
        const nodeId = body.flow?.nodeId ?? "unknown";
        const message = wakeMessage(body);
        const subagent = resolveSubagent();
        let woken = false;
        let wokenSession = null;

        // 1) Fresh session for the receiver.
        if (subagent && typeof subagent.run === "function") {
          try {
            const r = await subagent.run({
              sessionKey: wakeSessionKey(flowId, nodeId),
              message,
              agentId: agent,
            });
            if (r) {
              woken = true;
              wokenSession = wakeSessionKey(flowId, nodeId);
            }
          } catch (err) {
            log?.warn?.(`manifold: fresh-session wake failed, falling back: ${err?.message ?? err}`);
          }
        }
        // 2) Fallback: wake inside the originating session — it definitely
        // exists, since its dispatch just produced this push.
        if (!woken && originatingSession && subagent && typeof subagent.run === "function") {
          try {
            const r = await subagent.run({
              sessionKey: originatingSession,
              message,
              agentId: agent,
            });
            if (r) {
              woken = true;
              wokenSession = originatingSession;
            }
          } catch (err) {
            log?.warn?.(`manifold: fallback wake failed: ${err?.message ?? err}`);
          }
        }
        if (!woken) {
          // Loud: core delivered the event but nobody is awake to hear it.
          // The run itself is unaffected — this is observability, not state.
          log?.error?.(`manifold: push wake ${body.event} ${flowId}/${nodeId} -> agent ${agent} FAILED (no subagent turn opened)`);
        } else {
          log?.info?.(`manifold: push wake ${body.event} ${flowId}/${nodeId} -> agent ${agent} (session=${wokenSession})`);
        }
        json(res, 200, { ok: true, event: body.event, woken, wokenSession });
      } catch (err) {
        try { json(res, 500, { error: "wake failed" }); } catch {}
        try { log?.error?.(`manifold: push webhook failed: ${err?.message ?? err}`); } catch {}
      }
    },
  });
}
