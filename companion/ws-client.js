// companion/ws-client.js — the harness-side dispatch socket.
//
// Winch's shape: the daemon serves a ws door, the harness connects and
// reacts. One socket, both directions: the sidecar writes dispatch.request
// frames down, the companion writes dispatch.ack and dispatch.result back
// up. Reconnects forever — a dropped socket is a dispatch outage until it
// comes back, and the sidecar fails pending ops fast rather than hanging.
//
// The bearer rides the upgrade request headers (the service token, not the
// operator token): the socket can run agent turns, so it is gated like the
// sidecar's other internal endpoints.
export function startDispatchClient({ url, token = null, onFrame = null, logger = null, reconnectMs = 2000 } = {}) {
  let socket = null;
  let stopped = false;
  let retry = null;

  function scheduleReconnect() {
    if (stopped) return;
    clearTimeout(retry);
    retry = setTimeout(connect, reconnectMs);
    retry.unref?.();
  }

  function connect() {
    if (stopped) return;
    try {
      socket = token
        ? new WebSocket(url, { headers: { authorization: `Bearer ${token}` } })
        : new WebSocket(url);
    } catch (err) {
      logger?.warn?.(`companion dispatch: connect failed (${err?.message ?? err}); retrying`);
      scheduleReconnect();
      return;
    }
    socket.addEventListener("open", () => logger?.info?.(`companion dispatch: connected ${url}`));
    socket.addEventListener("message", (event) => {
      let frame;
      try {
        frame = JSON.parse(event.data);
      } catch {
        return;
      }
      onFrame?.(frame);
    });
    socket.addEventListener("close", () => {
      if (!stopped) scheduleReconnect();
    });
    socket.addEventListener("error", () => {});
  }

  connect();

  return {
    send(frame) {
      if (!socket || socket.readyState !== 1) return false;
      try {
        socket.send(JSON.stringify(frame));
        return true;
      } catch (err) {
        logger?.warn?.(`companion dispatch: send failed: ${err?.message ?? err}`);
        return false;
      }
    },
    close() {
      stopped = true;
      clearTimeout(retry);
      try {
        socket?.close();
      } catch {}
    },
  };
}
