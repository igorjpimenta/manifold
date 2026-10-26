// companion/relay.js — canonical events out of the sidecar, into gateway
// broadcast. The sidecar emits what happened (kind + fields, `plugin.`
// prefix intact); delivery stays the harness's business, same as before.
import { subscribeSse } from "./sse.js";

export function startEventRelay({ sidecarUrl, serviceToken, getBroadcast, logger = null }) {
  return subscribeSse({
    url: `${sidecarUrl}/manifold/events`,
    token: serviceToken,
    name: "events",
    logger,
    onEvent: (event) => {
      const broadcast = typeof getBroadcast === "function" ? getBroadcast() : null;
      if (!broadcast || !event?.kind) return;
      const { channel: _ch, seq: _seq, type: _t, kind, ...fields } = event;
      try {
        broadcast(kind, { kind, ...fields });
      } catch (err) {
        logger?.warn?.(`companion relay: broadcast failed (${kind}): ${err?.message ?? err}`);
      }
    },
  });
}
