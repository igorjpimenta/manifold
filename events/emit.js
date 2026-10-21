// events/emit.js — single fan-out for canonical kinds.
//
// emit(kind, payload) validates, stamps the envelope, and delivers to
// every configured sink. Transport faults are reported in the delivery
// receipt, never thrown (a dead gateway must not fail the dispatch that
// produced the event); validation faults reject (programmer errors).
// The `plugin.` prefix and legacy wire names are applied here and only
// here — producers never name the wire.
import {
  assertPayload,
  gatewayWireName,
  isSseKind,
} from "./kinds.js";

export function createEmitter({ getBroadcast, emitSseEvent, logger } = {}) {
  async function emit(kind, payload = {}) {
    // Throws (rejects) on unknown/reserved kinds and missing fields.
    assertPayload(kind, payload);
    // The incoming `type`, if any, is a legacy wire string — it stays at
    // the boundary. Inside core the kind is the only name.
    const { type: _legacyType, ...fields } = payload;
    const envelope = { ...fields, kind };
    if (envelope.emittedAtMs == null) envelope.emittedAtMs = Date.now();

    const deliveries = { ws: "skipped", sse: "skipped" };

    // Gateway WS boundary.
    try {
      const broadcast = typeof getBroadcast === "function" ? getBroadcast() : null;
      const wire = gatewayWireName(kind);
      if (broadcast && wire) {
        try {
          broadcast(`plugin.${wire}`, envelope);
          deliveries.ws = "sent";
        } catch (err) {
          deliveries.ws = "error";
          try { logger?.warn?.(`events/emit: ws broadcast failed (${kind}): ${err?.message ?? err}`); } catch {}
        }
      }
    } catch {
      deliveries.ws = "error";
    }

    // SSE/iOS-push boundary — session + discuss families only. The wire
    // event carries the legacy `type` so emitSseEvent's own prefix rule
    // keeps producing the exact strings current clients match on.
    if (typeof emitSseEvent === "function" && isSseKind(kind)) {
      try {
        const res = await emitSseEvent({ ...envelope, type: gatewayWireName(kind) ?? kind });
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
