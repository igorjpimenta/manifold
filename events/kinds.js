// events/kinds.js — canonical Phase-1 event-kind registry.
//
// Core emits bare kinds (dots only, no `plugin.` prefix); transport
// boundaries re-apply the prefix plus the exact legacy wire strings in
// KIND_TO_LEGACY_WIRE, so renaming core never breaks iOS/dashboard
// mid-migration. Every payload contract here is the minimum a consumer
// can rely on — producers may attach more fields, never fewer.

// Every kind with a Phase-1 producer. Frozen: adding a kind is a
// deliberate registry change, never an inline string at a call site.
// Evolution rule (CloudEvents primer §versioning): additive payload
// changes keep the kind; a breaking change mints a NEW kind and dual-
// produces through KIND_TO_LEGACY_WIRE until consumers cut over. There
// is deliberately no integer version field — an unread version versions
// nothing; the kind name is the version.
export const WIRE_KINDS = Object.freeze([
  "run.created",
  "run.started",
  "run.updated",
  "run.ended",
  "gate.opened",
  "gate.resolved",
  "session.transcript.completed",
  "session.transcript.failed",
  "session.transcript.updated",
  "discuss.component.proposed",
  "discuss.pending.updated",
  "pipeline.catalog.updated",
  "pipeline.stats.updated",
  "budget.queue.updated",
  "budget.lock.changed",
  "budget.cap.changed",
  "cost.record.added",
]);

// Named but producer-less: emitting one is a typo or a Phase-2 leak.
export const RESERVED_KINDS = Object.freeze([
  "gate.dissolved",
]);

// Minimum required payload fields per kind. Session kinds additionally
// require a valid `scope` (checked in assertPayload, not expressible here).
const REQUIRED_FIELDS = {
  "run.created": ["flowId"],
  "run.started": ["flowId"],
  "run.updated": ["flowId"],
  "run.ended": ["flowId", "status"],
  "gate.opened": ["flowId"],
  "gate.resolved": ["flowId", "itemId", "decision"],
  "session.transcript.completed": ["scope", "text"],
  "session.transcript.failed": ["scope", "error"],
  "session.transcript.updated": ["scope"],
  "discuss.component.proposed": ["flowId", "itemId", "component"],
  "discuss.pending.updated": ["flowId", "itemId"],
  "pipeline.catalog.updated": ["count", "ids"],
  "pipeline.stats.updated": ["id"],
  "budget.queue.updated": ["decision", "count"],
  "budget.lock.changed": ["locked", "day"],
  "budget.cap.changed": [],
  "cost.record.added": ["costTotal"],
};

export const SESSION_SCOPES = Object.freeze(["node", "discuss"]);

// Gateway egress names. `null` = no gateway wire string (dashboard-only
// kind; the WS sink skips it). Everything else keeps the exact legacy
// string current subscribers match on.
const GATEWAY_WIRE = {
  "run.created": null,
  "run.started": "pipeclaw.run.started",
  "run.updated": "pipeclaw.run.updated",
  "run.ended": "pipeclaw.run.ended",
  "gate.opened": "gate.opened",
  "gate.resolved": "gate.resolved",
  "session.transcript.completed": "discuss.turn",
  "session.transcript.failed": "discuss.turn",
  "session.transcript.updated": "discuss.transcript",
  "discuss.component.proposed": "discuss.component",
  "discuss.pending.updated": "discuss.pending",
  "pipeline.catalog.updated": "pipeclaw.pipeline.catalog.changed",
  "pipeline.stats.updated": "pipeclaw.pipeline.stats.updated",
  "budget.queue.updated": "budget.queue.changed",
  "budget.lock.changed": "gate.lock.changed",
  "budget.cap.changed": "budget.cap.changed",
  "cost.record.added": "cost.record.added",
};

// Dashboard-bus `type` values for the kinds the bus carries.
// Only kinds the bus carries are listed; the bus migration (slice 4)
// reads this, nothing else should.
const DASHBOARD_TYPE = {
  "run.created": "flow_created",
  "run.updated": "flow_changed",
  "run.ended": "flow_terminated",
  "pipeline.catalog.updated": "pipelines_changed",
};

export function isWireKind(kind) {
  return WIRE_KINDS.includes(kind);
}

export function isReservedKind(kind) {
  return RESERVED_KINDS.includes(kind);
}

// Throws on unknown/reserved kinds and missing fields. Returns the
// payload untouched — validation only, no normalization.
export function assertPayload(kind, payload) {
  if (isReservedKind(kind)) {
    throw new Error(`event kind reserved (no Phase-1 producer): ${kind}`);
  }
  const required = REQUIRED_FIELDS[kind];
  if (!required) {
    throw new Error(`unknown event kind: ${kind}`);
  }
  if (payload === null || typeof payload !== "object") {
    throw new Error(`event payload must be an object: ${kind}`);
  }
  for (const field of required) {
    if (payload[field] === undefined || payload[field] === null) {
      throw new Error(`event ${kind} missing required field: ${field}`);
    }
  }
  if (kind.startsWith("session.transcript.") && !SESSION_SCOPES.includes(payload.scope)) {
    throw new Error(`event ${kind} requires scope ${SESSION_SCOPES.join("|")}`);
  }
  if (kind === "session.transcript.completed" && payload.scope === "node" && payload.nodeId == null) {
    throw new Error(`event ${kind} scope=node requires nodeId`);
  }
  if (kind === "session.transcript.completed" && payload.scope === "discuss" && payload.turnId == null) {
    throw new Error(`event ${kind} scope=discuss requires turnId`);
  }
  return payload;
}

// Gateway wire name for a kind, or null when the kind has no gateway
// wire string. Unknown kinds throw — never emit an unmapped name.
export function gatewayWireName(kind) {
  if (!isWireKind(kind)) throw new Error(`unknown event kind: ${kind}`);
  // `in` check, not `??`: an explicit null means "no gateway wire
  // string" and must survive instead of falling through to the kind.
  return kind in GATEWAY_WIRE ? GATEWAY_WIRE[kind] : kind;
}

export function dashboardLegacyType(kind) {
  if (!isWireKind(kind)) throw new Error(`unknown event kind: ${kind}`);
  return DASHBOARD_TYPE[kind] ?? null;
}
