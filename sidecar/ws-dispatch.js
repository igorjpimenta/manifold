// sidecar/ws-dispatch.js — the dispatch door over WebSocket, winch-shaped.
//
// One socket carries the whole conversation: the sidecar writes
// dispatch.request frames down, the linked companion writes dispatch.ack and
// dispatch.result back up. Winch's shape: a daemon-side ws door and a
// harness-side client that subscribes and reacts — the daemon never calls
// into the harness, and the harness never polls the daemon.
//
// The bearer check lands after the 101 because events/ws-frame.js owns the
// handshake: an unauthenticated socket is destroyed before it is registered,
// so it never receives a dispatch frame and never appears in the count.
import { attachWsEndpoint, encodeWsFrame } from "../events/ws-frame.js";

export const DISPATCH_WS_PATH = "/manifold/internal/dispatch";

export function createWsDispatchDoor({ serviceToken = "", logger = null, path = DISPATCH_WS_PATH } = {}) {
  const sockets = new Set();
  let hub = null;

  function authorized(req) {
    if (!serviceToken) return false;
    const auth = req?.headers?.authorization ?? "";
    return auth === `Bearer ${serviceToken}`;
  }

  function publish(frame) {
    let delivered = 0;
    for (const socket of [...sockets]) {
      if (socket.destroyed || !socket.writable) {
        sockets.delete(socket);
        continue;
      }
      try {
        socket.write(encodeWsFrame(JSON.stringify(frame)));
        delivered++;
      } catch (err) {
        logger?.warn?.(`sidecar dispatch: frame write failed: ${err?.message ?? err}`);
        sockets.delete(socket);
      }
    }
    return delivered;
  }

  function attach(server, dispatchHub) {
    hub = dispatchHub;
    const endpoint = attachWsEndpoint(server, {
      match: (pathname) => pathname === path,
      onConnect: (socket, req) => {
        if (!authorized(req)) {
          logger?.warn?.("sidecar dispatch: unauthorized dispatch socket refused");
          socket.destroy();
          return undefined;
        }
        sockets.add(socket);
        socket.on("close", () => sockets.delete(socket));
        // Requests the previous socket never answered are replayed on connect:
        // a companion restart must not strand an in-flight op (winch replays
        // history on connect for the same reason).
        for (const frame of hub?.pending?.() ?? []) publishTo(socket, frame);
        return undefined;
      },
      onMessage: (socket, text) => {
        let frame;
        try {
          frame = JSON.parse(text);
        } catch {
          return;
        }
        if (frame?.type === "dispatch.ack") hub?.ack?.(frame.id);
        else if (frame?.type === "dispatch.result") hub?.fulfill?.(frame.id, frame);
      },
      logger,
    });
    return endpoint;
  }

  function publishTo(socket, frame) {
    try {
      socket.write(encodeWsFrame(JSON.stringify(frame)));
      return true;
    } catch (err) {
      logger?.warn?.(`sidecar dispatch: frame write failed: ${err?.message ?? err}`);
      sockets.delete(socket);
      return false;
    }
  }

  return {
    attach,
    publish,
    clientCount: () => sockets.size,
  };
}
