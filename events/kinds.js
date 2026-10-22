// events/kinds.js — canonical Phase-1 event-kind registry.
//
// Core emits bare kinds (dots only, no `plugin.` prefix); the gateway
// boundary adds the prefix. There are no legacy names: a breaking change
// mints a new kind. Every payload contract here is the minimum a consumer
// can rely on — producers may attach more fields, never fewer.

// Every kind with a Phase-1 producer. Frozen: adding a kind is a
// deliberate registry change, never an inline string at a call site.
// Evolution rule (CloudEvents primer §versioning): additive payload
// changes keep the kind; a breaking change mints a NEW kind and dual-
// produces old and new until consumers cut over. There is deliberately no
// integer version field — an unread version versions nothing; the kind
// name is the version.
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
  "failure.recorded",
  "approval.requested",
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
  "failure.recorded": ["failureId", "flowId"],
  "approval.requested": ["id"],
};

export const SESSION_SCOPES = Object.freeze(["node", "discuss"]);

// Gateway WS carries every wire kind except run.created (dashboard-only;
// the WS sink skips it).

// Dashboard-only kinds (served over the dashboard bus/SSE, never gateway
// WS) — read by the catalog publisher, nothing else should.
const DASHBOARD_ONLY = Object.freeze(["run.created"]);

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

// True for kinds served over the dashboard bus/SSE only — the gateway WS
// sink skips them. Unknown kinds throw — never route an unmapped name.
export function isDashboardOnly(kind) {
  if (!isWireKind(kind)) throw new Error(`unknown event kind: ${kind}`);
  return DASHBOARD_ONLY.includes(kind);
}
