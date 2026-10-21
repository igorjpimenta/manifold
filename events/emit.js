// events/emit.js — single fan-out for canonical kinds.
//
// emit(kind, payload) validates, stamps the envelope, and delivers to
// every configured sink. Transport faults are reported in the delivery
// receipt, never thrown (a dead gateway must not fail the dispatch that
// produced the event); validation faults reject (programmer errors).
// The `plugin.` prefix is applied here and only here — producers name
// the kind, never the wire.
import {
  assertPayload,
  isDashboardOnly,
  isSseKind,
} from "./kinds.js";

export function createEmitter({ getBroadcast, emitSseEvent, logger } = {}) {
  async function emit(kind, payload = {}) {
    // Throws (rejects) on unknown/reserved kinds and missing fields.
    assertPayload(kind, payload);
    // A `type` field here is a producer bug — the kind is the only name.
    const { type: _strayType, ...fields } = payload;
    const envelope = { ...fields, kind };
    if (envelope.emittedAtMs == null) envelope.emittedAtMs = Date.now();

    const deliveries = { ws: "skipped", sse: "skipped" };

    // Gateway WS boundary. Dashboard-only kinds skip it.
    try {
      const broadcast = typeof getBroadcast === "function" ? getBroadcast() : null;
      if (broadcast && !isDashboardOnly(kind)) {
        try {
          broadcast(`plugin.${kind}`, envelope);
          deliveries.ws = "sent";
        } catch (err) {
          deliveries.ws = "error";
          try { logger?.warn?.(`events/emit: ws broadcast failed (${kind}): ${err?.message ?? err}`); } catch {}
        }
      }
    } catch {
      deliveries.ws = "error";
    }

    // SSE/iOS-push boundary — session + discuss families only.
    if (typeof emitSseEvent === "function" && isSseKind(kind)) {
      try {
        const res = await emitSseEvent(envelope);
        deliveries.sse = res?.ok ? "sent" : "error";
      } catch {
        deliveries.sse = "error";
      }
    }

    return {
      ok: deliveries.ws !== "error" && deliveries.sse !== "error",
      kind,
      deliveries,
    };
  }

  return { emit };
}
