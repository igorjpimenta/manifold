import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

export const flowRuns = sqliteTable('flow_runs', {
  flowId:             text('flow_id').primaryKey(),
  ownerKey:           text('owner_key').notNull().default(''),
  revision:           integer('revision').notNull().default(0),
  status:             text('status').notNull(),
  lastStep:           text('last_step'),
  statusReason:       text('status_reason'),
  stateJson:          text('state_json').notNull().default('{}'),
  waitJson:           text('wait_json'),
  cancelRequestedAt:  integer('cancel_requested_at'),
  createdAt:          integer('created_at').notNull(),
  updatedAt:          integer('updated_at').notNull(),
  endedAt:            integer('ended_at'),
  parentInvocationId: text('parent_invocation_id'),
  pipelineId:         text('pipeline_id'),
});

export const flowNodeInvocations = sqliteTable('flow_node_invocations', {
  id:         text('id').primaryKey(),
  flowId:     text('flow_id').notNull(),
  pipelineId: text('pipeline_id'),
  nodeId:     text('node_id').notNull(),
  subflowKey: text('subflow_key'),
  visitSeq:   integer('visit_seq').notNull().default(1),
  invokedAt:  integer('invoked_at').notNull(),
  // Option B, Phase B0 — this
  // table's first payload column. resultWrittenAt is the "is this real"
  // signal a bare nullable resultJson can't provide on its own (not
  // dispatched vs. dispatched-not-complete vs. complete-with-null-result).
  // resultTooLargeBytes carried the real size when a result was capped
  // instead of stored — removed 2026-08-19 (see writeResult() in
  // state/node-executions.js). Only non-null on rows written before that;
  // every write since always sets it null.
  resultJson:          text('result_json'),
  resultWrittenAt:     integer('result_written_at'),
  resultTooLargeBytes: integer('result_too_large_bytes'),
});

// error/error_type intentionally NOT here — Phase 1.5 ("Eradicate soft
// errors") moved error content entirely
// into failure_events, which now gets a permanent row on every single
// throw regardless of what a cascade edge decides to do next. This table
// is purely an execution-window/identity ledger: which node, which
// attempt, when it ran, why kind of dispatch (fresh/retry/revival/
// recheck) — never what went wrong.
export const flowNodeExecutions = sqliteTable('flow_node_executions', {
  id:           text('id').primaryKey(),
  invocationId: text('invocation_id').notNull(),
  enteredAt:    integer('entered_at').notNull(),
  exitedAt:     integer('exited_at').notNull(),
  kind:         text('kind').notNull().default('legacy'),
});

// flow_branches (old-model branch storage, from the pre-childFlows
// branchService era) is still a real table in runtime.sqlite — read
// directly via raw SQL by state/node-executions.js, never through Drizzle
// — but its `flowBranches` schema export here had zero importers anywhere
// in the codebase. Deleted 2026-08-12 (dead-code cleanup).

// Deliberately thin — pipeline_id/branch_item_id/failure_node/recovery_hint/
// resolved status all used to be stored here and are now reconstructed at
// read time via invocation_id/execution_id joining to flow_node_invocations/
// flow_node_executions (see failure-store.js's deriveFailureContext). This
// table went from 21 columns to 6.
export const failureEvents = sqliteTable('failure_events', {
  id:             text('id').primaryKey(),
  flowId:         text('flow_id').notNull(),
  failedAt:       integer('failed_at').notNull(),
  failureSummary: text('failure_summary').notNull(),
  invocationId:   text('invocation_id'),
  executionId:    text('execution_id'),
  // The one field that moved here from flow_node_executions in Phase 1.5 —
  // no longer derivable via a join once that table stopped storing error
  // content. Same NodeFailure vocabulary as before: never_attempted|
  // confirmed_absent|inconclusive|infra_unavailable|blocked — plus
  // "subflow-mode-not-satisfied" for a subflow node whose exit mode wasn't
  // met (not a thrown NodeFailure at all, a policy verdict — see
  // failure-store.js's classifyRecoveryHint) — or null for an ordinary
  // engine-caught throw with no further classification.
  errorType:      text('error_type'),
});

export const discussState = sqliteTable('discuss_state', {
  flowId:         text('flow_id').notNull(),
  itemId:         text('item_id').notNull(),
  messagesJson:   text('messages_json').notNull().default('[]'),
  turnStatus:     text('turn_status').notNull().default('idle'),
  activeTurnId:   text('active_turn_id'),
  pendingJson:    text('pending_json'),
  updatedAt:      integer('updated_at').notNull(),
  componentsJson: text('components_json').notNull().default('[]'),
});

export const flowSessions = sqliteTable('flow_sessions', {
  id:           text('id').primaryKey(),
  flowId:       text('flow_id').notNull(),
  sessionKey:   text('session_key').notNull(),
  nodeId:       text('node_id'),
  branchItemId: text('branch_item_id'),
  spawnedAt:    integer('spawned_at').notNull(),
});

export const nodeHealth = sqliteTable('node_health', {
  id:              text('id').primaryKey(),
  pipelineId:      text('pipeline_id').notNull(),
  nodeId:          text('node_id').notNull(),
  status:          text('status').notNull(),
  reason:          text('reason').notNull(),
  blockedByFlowId: text('blocked_by_flow_id'),
  resolvedVia:     text('resolved_via'),
  createdAt:       integer('created_at').notNull(),
  updatedAt:       integer('updated_at').notNull(),
  endedAt:         integer('ended_at'),
});
// Model-routing circuit-breaker state. Separate from node_health because the
// two tiers disagree on expiry: a quarantined model carries its own deadline
// in until_ms, while a blocked node has no deadline at all and waits for an
// operator. See model-circuit-breaker/store.js.
export const modelHealth = sqliteTable('model_health', {
  id:             text('id').primaryKey(),
  ref:            text('ref').notNull(),
  status:         text('status').notNull(),
  reason:         text('reason').notNull(),
  downSince:      integer('down_since').notNull(),
  untilMs:        integer('until_ms'),
  probing:        integer('probing').notNull().default(0),
  strikeLevel:    integer('strike_level').notNull().default(0),
  strikeEndedAt:  integer('strike_ended_at'),
  createdAt:      integer('created_at').notNull(),
  updatedAt:      integer('updated_at').notNull(),
});

