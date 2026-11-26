// Single fan-out for canonical kinds: validate, stamp the envelope, deliver
// to the gateway WS broadcast. Transport faults are reported in the delivery
// receipt, never thrown (a dead gateway must not fail the dispatch that
// produced the event); validation faults reject (programmer errors). The
// `plugin.` prefix is applied here and only here — producers name the kind,
// never the wire.
import {
  assertPayload,
  isDashboardOnly,
} from "./kinds.js";

export function createEmitter({ getBroadcast, logger } = {}) {
  async function emit(kind, payload = {}) {
    assertPayload(kind, payload);
    // A `type` field here is a producer bug — the kind is the only name.
    const { type: _strayType, ...fields } = payload;
    const envelope = { ...fields, kind };
    if (envelope.emittedAtMs == null) envelope.emittedAtMs = Date.now();

    // Gateway WS boundary — the only delivery path. Dashboard-only kinds
    // skip it.
    try {
      const broadcast = typeof getBroadcast === "function" ? getBroadcast() : null;
      if (broadcast && !isDashboardOnly(kind)) {
        try {
          broadcast(`plugin.${kind}`, envelope);
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
