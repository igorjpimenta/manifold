// companion/runner.js — the harness half of agent dispatch.
//
// Subscribes to the sidecar's dispatch channel. Each dispatch.request is
// acked (pickup receipt — without it the sidecar fails the op fast), then
// executed against the gateway's subagent runtime, then fulfilled by POST:
//
//   POST /manifold/internal/dispatch/ack    { id }
//   POST /manifold/internal/dispatch/result { id, ok, result?, error? }
//
// Op names mirror engine/gateway-subagent.js's GATED_METHODS; the wire
// field names mirror sidecar/dispatch.js. Keep them in sync.
import http from "node:http";
import https from "node:https";
import { subscribeSse } from "./sse.js";

const DISPATCH_OPS = ["run", "waitForRun", "getSessionMessages"];

function postJson({ sidecarUrl, serviceToken, path, body, logger }) {
  return new Promise((resolve) => {
    const base = new URL(sidecarUrl);
    const transport = base.protocol === "https:" ? https : http;
    const raw = JSON.stringify(body ?? {});
    const req = transport.request(
      {
        hostname: base.hostname,
        port: base.port,
        path,
        method: "POST",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(raw), authorization: `Bearer ${serviceToken}` },
      },
      (res) => {
        res.resume();
        res.on("end", () => resolve(res.statusCode));
      },
    );
    req.on("error", (err) => {
      logger?.warn?.(`companion runner: POST ${path} failed: ${err?.message ?? err}`);
      resolve(0);
    });
    req.end(raw);
  });
}

export async function executeDispatchOp({ subagent, op, args }) {
  if (!DISPATCH_OPS.includes(op)) throw new Error(`companion runner: unknown op ${op}`);
  if (!subagent || typeof subagent[op] !== "function") {
    throw new Error(`companion runner: subagent ${op} unavailable on this harness`);
  }
  return subagent[op](args ?? {});
}

export function startDispatchRunner({ api, sidecarUrl, serviceToken, logger = null }) {
  return subscribeSse({
    url: `${sidecarUrl}/manifold/internal/dispatch`,
    token: serviceToken,
    name: "dispatch",
    logger,
    onEvent: (event) => {
      if (event?.type !== "dispatch.request" || !event.id) return;
      void handleRequest(event);
    },
  });

  async function handleRequest({ id, op, args }) {
    const post = (path, body) => postJson({ sidecarUrl, serviceToken, path, body, logger });
    await post("/manifold/internal/dispatch/ack", { id });
    try {
      const result = await executeDispatchOp({ subagent: api.runtime?.subagent, op, args });
      await post("/manifold/internal/dispatch/result", { id, ok: true, result: result ?? null });
    } catch (err) {
      await post("/manifold/internal/dispatch/result", { id, ok: false, error: err?.message ?? String(err) });
    }
  }
}
