// events/emit.js — single fan-out for canonical kinds.
//
// emit(kind, payload) validates, stamps the envelope, and delivers to the
// gateway WS broadcast. Transport faults are reported in the delivery
// receipt, never thrown (a dead gateway must not fail the dispatch that
// produced the event); validation faults reject (programmer errors).
// The `plugin.` prefix and legacy wire names are applied here and only
// here — producers never name the wire.
import {
  assertPayload,
  gatewayWireName,
} from "./kinds.js";

export function createEmitter({ getBroadcast, logger } = {}) {
  async function emit(kind, payload = {}) {
    // Throws (rejects) on unknown/reserved kinds and missing fields.
    assertPayload(kind, payload);
    // The incoming `type`, if any, is a legacy wire string — it stays at
    // the boundary. Inside core the kind is the only name.
    const { type: _legacyType, ...fields } = payload;
    const envelope = { ...fields, kind };
    if (envelope.emittedAtMs == null) envelope.emittedAtMs = Date.now();

    // Gateway WS boundary — the only delivery path.
    try {
      const broadcast = typeof getBroadcast === "function" ? getBroadcast() : null;
      const wire = gatewayWireName(kind);
      if (broadcast && wire) {
        try {
          broadcast(`plugin.${wire}`, envelope);
          return { ok: true, kind, deliveries: { ws: "sent" } };
        } catch (err) {
          try { logger?.warn?.(`events/emit: ws broadcast failed (${kind}): ${err?.message ?? err}`); } catch {}
          return { ok: false, kind, deliveries: { ws: "error" } };
        }
      }
    } catch {
      return { ok: false, kind, deliveries: { ws: "error" } };
    }

    return { ok: true, kind, deliveries: { ws: "skipped" } };
  };

  return { emit };
}
