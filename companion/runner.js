// companion/runner.js — the harness half of agent dispatch.
//
// Subscribes to the sidecar's dispatch door over WebSocket, the way winch's
// harness client subscribes to the daemon's event pipe. Each dispatch.request
// is acked (pickup receipt — without it the sidecar fails the op fast), then
// executed against the gateway's subagent runtime, then answered on the same
// socket:
//
//   dispatch.ack    { id }
//   dispatch.result { id, ok, result?, error? }
//
// Op names mirror engine/gateway-subagent.js's GATED_METHODS; the wire
// field names mirror sidecar/dispatch.js. Keep them in sync.
import { startDispatchClient } from "./ws-client.js";

const DISPATCH_OPS = ["run", "waitForRun", "getSessionMessages"];
const DISPATCH_WS_PATH = "/manifold/internal/dispatch";

export async function executeDispatchOp({ subagent, op, args }) {
  if (!DISPATCH_OPS.includes(op)) throw new Error(`companion runner: unknown op ${op}`);
  if (!subagent || typeof subagent[op] !== "function") {
    throw new Error(`companion runner: subagent ${op} unavailable on this harness`);
  }
  return subagent[op](args ?? {});
}

export function startDispatchRunner({ api, sidecarUrl, serviceToken, logger = null }) {
  const url = `${String(sidecarUrl).replace(/^http/, "ws")}${DISPATCH_WS_PATH}`;
  const client = startDispatchClient({
    url,
    token: serviceToken,
    logger,
    onFrame: (frame) => {
      if (frame?.type !== "dispatch.request" || !frame.id) return;
      void handleRequest(frame);
    },
  });

  async function handleRequest({ id, op, args }) {
    client.send({ type: "dispatch.ack", id });
    try {
      const result = await executeDispatchOp({ subagent: api.runtime?.subagent, op, args });
      client.send({ type: "dispatch.result", id, ok: true, result: result ?? null });
    } catch (err) {
      client.send({ type: "dispatch.result", id, ok: false, error: err?.message ?? String(err) });
    }
  }

  return {
    stop() {
      client.close();
    },
  };
}
