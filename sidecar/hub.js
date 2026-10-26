// sidecar/hub.js — the sidecar's outbound fan-out. Two SSE channels over
// one hub:
//
//   events   — canonical manifold kinds (the 19 wire kinds). Harnesses
//              subscribe and react; delivery faults never throw.
//   dispatch — transport ops, NOT event kinds: dispatch.request down to the
//              linked harness runner, fulfilled by POST back. Never logged
//              as history, never replayed.
//
// Harnesses hold the subscription; a sidecar with no subscriber keeps
// serving state and queues nothing (dispatch.request fails fast at accept
// timeout — see dispatch.js). A dead harness therefore degrades to
// "dispatch unavailable", never to a wedged engine.
export function createHub({ logger = null } = {}) {
  const channels = new Map(); // channel -> Set<res>
  let seq = 0;

  function write(res, obj) {
    try {
      res.write(`data: ${JSON.stringify(obj)}\n\n`);
      return true;
    } catch (err) {
      logger?.warn?.(`sidecar hub: subscriber write failed: ${err?.message ?? err}`);
      return false;
    }
  }

  return {
    subscribe(channel, res) {
      let set = channels.get(channel);
      if (!set) {
        set = new Set();
        channels.set(channel, set);
      }
      set.add(res);
      try {
        res.write(`: connected ${channel}\n\n`);
      } catch {}
      return () => {
        set.delete(res);
      };
    },
    publish(channel, event) {
      const set = channels.get(channel);
      if (!set || set.size === 0) return 0;
      const obj = { seq: ++seq, channel, ...event };
      let delivered = 0;
      for (const res of [...set]) {
        if (write(res, obj)) delivered++;
        else set.delete(res);
      }
      return delivered;
    },
    subscriberCount(channel) {
      return channels.get(channel)?.size ?? 0;
    },
    // Emit-compatible broadcast sink: (name, envelope) -> events channel.
    // Keeps the `plugin.` prefix producers already carry.
    broadcast(name, envelope) {
      return this.publish("events", { kind: name, ...(envelope ?? {}) });
    },
  };
}
