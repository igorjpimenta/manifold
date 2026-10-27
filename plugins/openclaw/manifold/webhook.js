// webhook.js — claim receiver: wake the agent the moment pull work is
// ready for them.
//
// Core POSTs {event, at, flow, session} here when a human gate opens
// (gate.opened), and the harness POSTs {event:"agent.ready", flow, session,
// claim} when a parked claim ticket is ready for pickup (see events/push.js
// for the payload contract). This handler verifies the bearer, then opens
// a FRESH session for the receiver via runtime.subagent.run carrying the
// receiver prompt — for agent.ready, CLAIM instructions (claim the ticket
// via MCP manifold_claim, execute the delivered prompt, report via MCP
// manifold_report); for gate.opened, the manifold_approve resume path —
// and reacts to the turn result. A failed fresh turn falls back to the
// originating sessionKey; if both fail the handler logs loudly (the HTTP
// answer still reports the failure — the run itself is unaffected).
//
// There is deliberately no pending queue / heartbeat drain here: unlike a
// backlog claim (re-readable on the next poll), a ready ticket or an open
// gate needs its receiver now. A double failure pages loudly through the
// error log instead of parking silently.
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
  if (body.event !== "agent.ready" && body.event !== "gate.opened") {
    return `unsupported event ${JSON.stringify(body.event)}`;
  }
  // agent.ready always names its receiver and its ticket — an anonymous or
  // ticketless claim is a loud shape error, not a wake.
  // gate.opened may carry no receiver (no agent owns the gate yet); that
  // is a report-only delivery, not an error.
  if (body.event === "agent.ready") {
    const s = body.session;
    if (!s || typeof s.agent !== "string" || !s.agent) return "session.agent required";
    const c = body.claim;
    if (!c || typeof c.ticket !== "string" || !c.ticket) return "claim.ticket required";
  }
  return null;
}

// New-session key for a pushed wake. Namespaced so manifold turns are visible.
export function wakeSessionKey(flowId, nodeId) {
  return `manifold-push-${flowId}-${nodeId}`;
}

// New-session key for a ready ticket. Namespaced so claim turns are visible.
export function claimSessionKey(ticket) {
  return `manifold-claim-${ticket}`;
}

// CLAIM instructions for the fresh session: the delivered prompt verbatim
// when the sender attached one (full context), else a composed pointer at
// the ticket. Either way the turn claims first — an unclaimed ticket belongs
// to nobody, and executing without claiming risks double work.
export function claimMessage(body) {
  const ticket = body?.claim?.ticket ?? "(unknown)";
  const prompt = body?.claim?.prompt;
  const lines = [
    `Manifold parked claim ticket ${ticket} for you (${body?.session?.agent ?? "(unknown agent)"}).`,
    `First, claim it: call manifold_claim. If the claim comes back empty (someone else took it), stop — do not execute.`,
  ];
  if (typeof prompt === "string" && prompt.trim() !== "") {
    lines.push(`Once claimed, execute this prompt, then report via manifold_report with the ticket and your result:`);
    lines.push(prompt);
  } else {
    lines.push(`Once claimed, execute the node work, then report via manifold_report with the ticket and your result.`);
  }
  return lines.join("\n");
}

// Handoff text for the fresh session. A projection of the push payload
// only (flow/session context) — no session history enters the prompt.
// gate.opened names the manifold_approve resume path.
export function wakeMessage(body) {
  const f = body.flow ?? {};
  const sess = body.session ?? {};
  const context = JSON.stringify({ flow: f, session: sess });
  if (body.event === "gate.opened") {
    return [
      `Manifold gate opened for flow ${f.id ?? "(unknown)"} at node ${f.nodeId ?? "(unknown)"} (pipeline ${f.pipelineId ?? "(unknown)"}).`,
      `Review the blocked item (manifold_status ${f.id ?? "<flowId>"}) then record your decision with: manifold_approve ${f.id ?? "<flowId>"} <itemId> <approve|reject|edit>.`,
      `Context: ${context}`,
    ].join("\n");
  }
  return claimMessage(body);
}

// A subagent turn counts as woken when it returns without signalling
// failure. Reacting to the result (not just its presence) keeps a
// failed-but-returned turn on the fallback chain instead of reporting a
// wake that never started work.
export function turnOk(result) {
  if (!result) return false;
  if (typeof result === "object" && (result.ok === false || result.error != null)) return false;
  return true;
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
          // secret yet. 503, not 404 — the sender retries next wake, and the
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
        // A ready ticket is unclaimed by definition — there is no claiming
        // session to fall back to, only an originating session if the
        // sender attached one.
        const originatingSession = body.session.sessionKey;
        const flowId = body.flow?.id ?? "unknown";
        const nodeId = body.flow?.nodeId ?? "unknown";
        const freshKey = body.event === "agent.ready"
          ? claimSessionKey(body.claim.ticket)
          : wakeSessionKey(flowId, nodeId);
        const message = wakeMessage(body);
        const subagent = resolveSubagent();
        let woken = false;
        let wokenSession = null;

        // 1) Fresh session for the receiver. The result decides: a
        // returned-but-failed turn is not a wake — fall through below.
        if (subagent && typeof subagent.run === "function") {
          try {
            const r = await subagent.run({
              sessionKey: freshKey,
              message,
              agentId: agent,
            });
            if (turnOk(r)) {
              woken = true;
              wokenSession = freshKey;
            } else {
              log?.warn?.(`manifold: fresh-session wake for ${flowId}/${nodeId} returned without starting work, falling back`);
            }
          } catch (err) {
            log?.warn?.(`manifold: fresh-session wake failed, falling back: ${err?.message ?? err}`);
          }
        }
        // 2) Fallback: wake inside the originating session — when the
        // sender attached one. A ready ticket usually has none (it is
        // unclaimed), so this step is routinely skipped, not an error.
        if (!woken && originatingSession && subagent && typeof subagent.run === "function") {
          try {
            const r = await subagent.run({
              sessionKey: originatingSession,
              message,
              agentId: agent,
            });
            if (turnOk(r)) {
              woken = true;
              wokenSession = originatingSession;
            } else {
              log?.warn?.(`manifold: fallback wake for ${flowId}/${nodeId} returned without starting work`);
            }
          } catch (err) {
            log?.warn?.(`manifold: fallback wake failed: ${err?.message ?? err}`);
          }
        }
        if (!woken) {
          // Loud: the sender delivered the event but nobody is awake to hear it.
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
