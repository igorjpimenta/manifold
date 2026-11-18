import { sqliteTable, text, integer, real, index, uniqueIndex, primaryKey, foreignKey } from 'drizzle-orm/sqlite-core';
import { sql, desc } from 'drizzle-orm';

export const harnesses = sqliteTable('harnesses', {
  id:        text('id').primaryKey(),
  createdAt: integer('created_at').notNull(),
  updatedAt: integer('updated_at').notNull(),
});

export const pipelines = sqliteTable('pipelines', {
  id:        text('id').primaryKey(),
  name:      text('name').notNull(),
  harness:   text('harness').references(() => harnesses.id, { onDelete: 'set null' }),
  createdAt: integer('created_at').notNull(),
  updatedAt: integer('updated_at').notNull(),
  deletedAt: integer('deleted_at'),
}, (table) => [
  index('idx_pipelines_harness_name').on(table.harness, table.name),
]);

// Session keys are assigned by the harness, outside this database, so a
// key can be referenced (parent/root lineage, cost attribution, dispatch
// session) before any table of ours has seen it. This registry is the
// target that makes those pointers FKs; writers insert OR IGNORE the keys
// they are about to reference, and state/session-keys.js backfills the
// union on migration. Rows are never pruned — a key is an identity, not a
// live record.
export const sessions = sqliteTable('sessions', {
  sessionKey:  text('session_key').primaryKey(),
  firstSeenAt: integer('first_seen_at').notNull(),
}, (table) => [
  index('idx_sessions_first_seen').on(table.firstSeenAt),
]);

// Attribution keys (item, node name, harness turn run) are written by the
// engine/hook frames, not owned by a row of ours, so these three registries
// are their materialized key set. Every writer registers what it is about
// to attribute (state/attribution-registries.js), and migration backfills
// the union, so a key is never dangling. Rows are append-only: a pruned
// flow must not erase the attribution that names it.
export const flowItems = sqliteTable('flow_items', {
  flowId:      text('flow_id').notNull(),
  itemKey:     text('item_key').notNull(),
  firstSeenAt: integer('first_seen_at').notNull(),
}, (table) => [
  primaryKey({ columns: [table.flowId, table.itemKey] }),
  index('idx_flow_items_key').on(table.itemKey),
]);

export const turnRuns = sqliteTable('turn_runs', {
  runId:       text('run_id').primaryKey(),
  firstSeenAt: integer('first_seen_at').notNull(),
});

export const pipelineNodes = sqliteTable('pipeline_nodes', {
  pipelineId:  text('pipeline_id').notNull().references(() => pipelines.id, { onDelete: 'cascade' }),
  nodeName:    text('node_name').notNull(),
  firstSeenAt: integer('first_seen_at').notNull(),
}, (table) => [
  primaryKey({ columns: [table.pipelineId, table.nodeName] }),
  index('idx_pipeline_nodes_name').on(table.nodeName),
]);

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
  parentInvocationId: text('parent_invocation_id').references(() => flowNodeInvocations.id, { onDelete: 'set null' }),
  pipelineId:         text('pipeline_id').references(() => pipelines.id, { onDelete: 'set null' }),
}, (table) => [
  index('idx_pfr_status').on(table.status).where(sql`${table.endedAt} IS NULL`),
  index('idx_pfr_updated').on(desc(table.updatedAt)),
  index('idx_pfr_created').on(desc(table.createdAt)),
  uniqueIndex('idx_pfr_parent_invocation').on(table.parentInvocationId).where(sql`${table.parentInvocationId} IS NOT NULL`),
  index('idx_flow_runs_pipeline_id').on(table.pipelineId),
]);

export const flowNodeInvocations = sqliteTable('flow_node_invocations', {
  id:         text('id').primaryKey(),
  flowId:     text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  pipelineId: text('pipeline_id'),
  nodeId:     text('node_id').notNull(),
  subflowKey: text('subflow_key'),
  visitSeq:   integer('visit_seq').notNull().default(1),
  invokedAt:  integer('invoked_at').notNull(),
  // Option B, Phase B0 — this
  // table's first payload column. resultWrittenAt is the "is this real"
  // signal a bare nullable resultJson can't provide on its own (not
  // dispatched vs. dispatched-not-complete vs. complete-with-null-result).
  // resultTooLargeBytes carries the real size of a result that was capped
  // instead of stored (see writeResult() in
  // state/node-executions.js). Every write today sets it null; a non-null
  // value marks a row whose result was never stored.
  resultJson:          text('result_json'),
  resultWrittenAt:     integer('result_written_at'),
  resultTooLargeBytes: integer('result_too_large_bytes'),
}, (table) => [
  // NOTE: idx_invocations_identity (UNIQUE on flow_id, node_id,
  // COALESCE(subflow_key, ''), visit_seq) is intentionally absent here —
  // drizzle-kit cannot serialize sql fragments inside an index ON clause,
  // so it lives verbatim at the end of .migrations/runtime/0000 (same
  // anchor-edit category as the IF NOT EXISTS pass above it).
  index('idx_invocations_flow').on(table.flowId),
  index('idx_invocations_node_subflow').on(table.nodeId, table.subflowKey),
  index('idx_invocations_pipe_node').on(table.pipelineId, table.nodeId),
]);

// error/error_type intentionally NOT here — error content lives entirely
// into failure_events, which gets a permanent row on every single
// throw regardless of what a cascade edge decides to do next. This table
// is purely an execution-window/identity ledger: which node, which
// attempt, when it ran, why kind of dispatch (fresh/retry/revival/
// recheck) — never what went wrong.
export const flowNodeExecutions = sqliteTable('flow_node_executions', {
  id:           text('id').primaryKey(),
  invocationId: text('invocation_id').notNull().references(() => flowNodeInvocations.id, { onDelete: 'cascade' }),
  enteredAt:    integer('entered_at').notNull(),
  exitedAt:     integer('exited_at').notNull(),
  kind:         text('kind').notNull().default('legacy'),
}, (table) => [
  index('idx_fne_invocation').on(table.invocationId),
  index('idx_fne_exited').on(table.exitedAt),
]);

// flow_branches (branch storage) is still a real table in runtime.sqlite —
// read directly via raw SQL by state/node-executions.js, never through
// Drizzle — and deliberately has no schema export here: nothing imports
// one.

// Deliberately thin — pipeline_id/branch_item_id/failure_node/recovery_hint/
// resolved status are reconstructed at read time via invocation_id/
// execution_id joining to flow_node_invocations/flow_node_executions
// (see failure-store.js's deriveFailureContext), not stored here.
export const failureEvents = sqliteTable('failure_events', {
  id:             text('id').primaryKey(),
  flowId:         text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  failedAt:       integer('failed_at').notNull(),
  failureSummary: text('failure_summary').notNull(),
  invocationId:   text('invocation_id').references(() => flowNodeInvocations.id, { onDelete: 'set null' }),
  executionId:    text('execution_id').references(() => flowNodeExecutions.id, { onDelete: 'set null' }),
  // Error content lives here, not in flow_node_executions, so it is not
  // derivable via a join. NodeFailure vocabulary: never_attempted|
  // confirmed_absent|inconclusive|infra_unavailable|blocked — plus
  // "subflow-mode-not-satisfied" for a subflow node whose exit mode wasn't
  // met (not a thrown NodeFailure at all, a policy verdict — see
  // failure-store.js's classifyRecoveryHint) — or null for an ordinary
  // engine-caught throw with no further classification.
  errorType:      text('error_type'),
}, (table) => [
  index('idx_failures_flow').on(table.flowId),
  index('idx_failures_failed_at').on(desc(table.failedAt)),
  index('idx_failures_invocation').on(table.invocationId),
  uniqueIndex('idx_failures_execution').on(table.executionId).where(sql`${table.executionId} IS NOT NULL`),
]);

export const discussState = sqliteTable('discuss_state', {
  flowId:         text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  itemId:         text('item_id').notNull(),
  messagesJson:   text('messages_json').notNull().default('[]'),
  turnStatus:     text('turn_status').notNull().default('idle'),
  activeTurnId:   text('active_turn_id'),
  pendingJson:    text('pending_json'),
  updatedAt:      integer('updated_at').notNull(),
  componentsJson: text('components_json').notNull().default('[]'),
}, (table) => [
  index('idx_discuss_updated').on(table.updatedAt),
  // One row per discuss thread. Without this constraint two concurrent
  // appendMessage calls each insert their own row and the thread's
  // messages split across them.
  uniqueIndex('idx_discuss_state_unique').on(table.flowId, table.itemId),
]);

export const flowSessions = sqliteTable('flow_sessions', {
  id:           text('id').primaryKey(),
  flowId:     text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  session:     text('session_key').notNull().references(() => sessions.sessionKey),
  nodeId:       text('node_id'),
  branchItemId: text('branch_item_id'),
  spawnedAt:    integer('spawned_at').notNull(),
}, (table) => [
  foreignKey({ columns: [table.flowId, table.branchItemId], foreignColumns: [flowItems.flowId, flowItems.itemKey] }),
  index('idx_flow_sessions_flow_id').on(table.flowId),
  uniqueIndex('idx_flow_sessions_uniq').on(table.flowId, table.session),
]);

// Gate discuss thread identity: one row per (flow, gate, item), keyed by
// the coordinates — never by parsing the session id. Replaces the
// per-thread JSON sidecars: the roster is the map, the harness owns the id.
export const discussThreads = sqliteTable('discuss_threads', {
  flowId:      text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  gateNodeId:  text('gate_node_id').notNull(),
  itemId:      text('item_id').notNull(),
  agentId:     text('agent_id'),
  session:     text('session_key').notNull().references(() => sessions.sessionKey),
  updatedAt:   integer('updated_at').notNull(),
}, (table) => [
  uniqueIndex('idx_discuss_threads_unique').on(table.flowId, table.gateNodeId, table.itemId),
]);

export const nodeHealth = sqliteTable('node_health', {
  id:              text('id').primaryKey(),
  pipelineId:      text('pipeline_id').notNull().references(() => pipelines.id, { onDelete: 'restrict' }),
  nodeId:          text('node_id').notNull(),
  status:          text('status').notNull(),
  reason:          text('reason').notNull(),
  blockedByFlowId: text('blocked_by_flow_id').references(() => flowRuns.flowId, { onDelete: 'set null' }),
  resolvedVia:     text('resolved_via'),
  createdAt:       integer('created_at').notNull(),
  updatedAt:       integer('updated_at').notNull(),
  endedAt:         integer('ended_at'),
}, (table) => [
  foreignKey({ columns: [table.pipelineId, table.nodeId], foreignColumns: [pipelineNodes.pipelineId, pipelineNodes.nodeName] }),
  uniqueIndex('idx_node_health_one_active').on(table.pipelineId, table.nodeId).where(sql`${table.status} = 'blocked'`),
  index('idx_node_health_pipeline').on(table.pipelineId, table.status),
]);

// Point lookup of one gate node's own record — keyed (flow_id, node_id) so
// sibling gates never share a key that can go stale. Written/read via raw
// SQL (like flow_branches below); the def exists so snapshots cover it.
export const gateRecords = sqliteTable('gate_records', {
  flowId:     text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  nodeId:     text('node_id').notNull(),
  recordJson: text('record_json').notNull(),
  updatedAt:  integer('updated_at').notNull(),
}, (table) => [
  primaryKey({ columns: [table.flowId, table.nodeId] }),
]);

export const pipelineVersions = sqliteTable('pipeline_versions', {
  id:               text('id').primaryKey(),
  pipelineId:       text('pipeline_id').notNull().references(() => pipelines.id, { onDelete: 'restrict' }),
  kind:             text('kind'),
  allowConcurrent:  text('allow_concurrent'),
  isSynthetic:      integer('is_synthetic').notNull(),
  parentPipelineId: text('parent_pipeline_id').references(() => pipelines.id, { onDelete: 'set null' }),
  sourcePath:       text('source_path'),
  contentHash:      text('content_hash').notNull(),
  registeredAt:     integer('registered_at').notNull(),
  retiredAt:        integer('retired_at'),
}, (table) => [
  index('idx_pv_lookup').on(table.pipelineId, table.registeredAt),
  uniqueIndex('idx_pv_active').on(table.pipelineId).where(sql`${table.retiredAt} IS NULL`),
]);

export const pipelineNodeVersions = sqliteTable('pipeline_node_versions', {
  id:                text('id').primaryKey(),
  pipelineVersionId: text('pipeline_version_id').notNull().references(() => pipelineVersions.id, { onDelete: 'cascade' }),
  pipelineId:        text('pipeline_id').notNull().references(() => pipelines.id, { onDelete: 'restrict' }),
  nodeName:          text('node_name').notNull(),
  nodeId:            text('node_id'),
  kind:              text('kind').notNull(),
  agent:             text('agent'),
  mode:              text('mode'),
  contentHash:       text('content_hash').notNull(),
  registeredAt:      integer('registered_at').notNull(),
  retiredAt:         integer('retired_at'),
}, (table) => [
  index('idx_pnv_lookup').on(table.pipelineId, table.nodeName, table.registeredAt),
  uniqueIndex('idx_pnv_active').on(table.pipelineId, table.nodeName).where(sql`${table.retiredAt} IS NULL`),
]);

export const flowAdmissionQueue = sqliteTable('flow_admission_queue', {
  id:         text('id').primaryKey(),
  kind:       text('kind').notNull(),
  flowId:     text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  nodeId:     text('node_id'),
  status:     text('status').notNull(),
  createdAt:  integer('created_at').notNull(),
  admittedAt: integer('admitted_at'),
}, (table) => [
  index('idx_admission_status').on(table.status),
  index('idx_admission_flow').on(table.flowId),
  index('idx_admission_status_created').on(table.status, table.createdAt),
]);

export const flowEvents = sqliteTable('flow_events', {
  id:         text('id').primaryKey(),
  flowId:     text('flow_id').notNull().references(() => flowRuns.flowId, { onDelete: 'cascade' }),
  eventType:  text('event_type').notNull(),
  prevStep:   text('prev_step'),
  currStep:   text('curr_step'),
  prevStatus: text('prev_status'),
  currStatus: text('curr_status'),
  waitKind:   text('wait_kind'),
  occurredAt: integer('occurred_at').notNull(),
}, (table) => [
  index('idx_flow_events_flow_id').on(table.flowId),
  index('idx_flow_events_occurred_at').on(table.occurredAt),
]);
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


// Per-harness pipeline directory overrides, one row per harness. Written by
// deliberate admin acts (`manifold pipelines register`, operator tooling);
// read once at boot to resolve the pipelines dir. No expiry: removing an
// override is a delete, not a flag.
export const pipelineDirs = sqliteTable('pipeline_dirs', {
  harness:   text('harness').primaryKey().references(() => harnesses.id, { onDelete: 'cascade' }),
  path:      text('path').notNull(),
  createdAt: integer('created_at').notNull(),
});

// Structured tool results delivered by agents over MCP
// (manifold_deliver_result). Keyed by the harness-stamped session the
// agent and the waiting dispatch both know — no invocationId resolution.
// Latest write wins; the dispatch consumes the row for its own turn.
export const toolResults = sqliteTable('tool_results', {
  session:  text('session_key').primaryKey().references(() => sessions.sessionKey),
  runId:       text('run_id').references(() => flowRuns.flowId, { onDelete: 'set null' }),
  agentId:     text('agent_id'),
  kind:        text('kind'),
  payloadJson: text('payload_json').notNull(),
  updatedAt:   integer('updated_at').notNull(),
});

// Lineage and cost telemetry, moved out of ops.sqlite: flow_id and
// pipeline_id carry FKs to runtime parents, and SQLite cannot constrain a
// column against a table in another file. The ops-side definitions stay in
// schema.ops.js as the frozen shape of the old file.
export const queueItems = sqliteTable('queue_items', {
  id:               text('id').primaryKey(),
  label:            text('label').notNull(),
  agent:            text('agent').notNull(),
  agentEmoji:       text('agent_emoji').notNull().default('🤖'),
  trigger:          text('trigger').notNull().default('manual'),
  costLow:          real('cost_low').notNull().default(0),
  costHigh:         real('cost_high').notNull().default(0),
  brief:            text('brief'),
  modelChain:       text('model_chain'),
  pipeline:         text('pipeline'),
  node:             text('node'),
  tokenEstimate:    integer('token_estimate'),
  status:           text('status').notNull().default('pending'),
  createdAt:        integer('created_at').notNull(),
  expiresAt:        integer('expires_at'),
  decidedAt:        integer('decided_at'),
  decision:         text('decision'),
  pipelineId:       text('pipeline_id').references(() => pipelines.id, { onDelete: 'set null' }),
  pipelineArgs:     text('pipeline_args'),
  flowId:           text('flow_id').references(() => flowRuns.flowId, { onDelete: 'set null' }),
  enqueuerSession:  text('enqueuer_session_key').references(() => sessions.sessionKey, { onDelete: 'set null' }),
  kind:             text('kind'),
}, (table) => [
  foreignKey({ columns: [table.pipelineId, table.node], foreignColumns: [pipelineNodes.pipelineId, pipelineNodes.nodeName] }),
  index('idx_status').on(table.status),
  index('idx_created_at').on(desc(table.createdAt)),
  index('idx_flow_node').on(table.flowId, table.node),
]);

export const sessionLineage = sqliteTable('session_lineage', {
  childSession:     text('child_session_key').primaryKey().references(() => sessions.sessionKey),
  parentSessionKey: text('parent_session_key').references(() => sessions.sessionKey, { onDelete: 'set null' }),
  rootSessionKey:   text('root_session_key').notNull().references(() => sessions.sessionKey),
  kind:             text('kind').notNull(),
  rootKind:         text('root_kind').notNull(),
  rootLabel:        text('root_label'),
  agentId:          text('agent_id'),
  pipelineId:       text('pipeline_id').references(() => pipelines.id, { onDelete: 'set null' }),
  flowId:           text('flow_id').references(() => flowRuns.flowId, { onDelete: 'set null' }),
  nodeId:           text('node_id'),
  itemKey:          text('item_key'),
  firstSeenAt:      integer('first_seen_at').notNull(),
  source:           text('source').notNull(),
  runId:            text('run_id').references(() => turnRuns.runId, { onDelete: 'set null' }),
}, (table) => [
  foreignKey({ columns: [table.pipelineId, table.nodeId], foreignColumns: [pipelineNodes.pipelineId, pipelineNodes.nodeName] }),
  foreignKey({ columns: [table.flowId, table.itemKey], foreignColumns: [flowItems.flowId, flowItems.itemKey] }),
  index('idx_lineage_root').on(table.rootSessionKey),
  index('idx_lineage_parent').on(table.parentSessionKey),
  index('idx_lineage_pipeline').on(table.pipelineId),
  index('idx_lineage_flow').on(table.flowId),
  index('idx_lineage_root_kind').on(table.rootKind),
  index('idx_lineage_run').on(table.runId),
]);

export const costByCall = sqliteTable('cost_by_call', {
  sessionId:        text('session_id').notNull(),
  msgId:            text('msg_id').notNull(),
  ts:               integer('ts').notNull(),
  provider:         text('provider'),
  model:            text('model'),
  inputTokens:      integer('input_tokens'),
  outputTokens:     integer('output_tokens'),
  cacheReadTokens:  integer('cache_read_tokens'),
  cacheWriteTokens: integer('cache_write_tokens'),
  totalTokens:      integer('total_tokens'),
  costInput:        real('cost_input'),
  costOutput:       real('cost_output'),
  costCacheRead:    real('cost_cache_read'),
  costCacheWrite:   real('cost_cache_write'),
  costTotal:        real('cost_total').notNull(),
  session:          text('session_key').references(() => sessions.sessionKey, { onDelete: 'set null' }),
  agentId:          text('agent_id'),
  rootSessionKey:   text('root_session_key').references(() => sessions.sessionKey, { onDelete: 'set null' }),
  rootKind:         text('root_kind'),
  rootLabel:        text('root_label'),
  pipelineId:       text('pipeline_id').references(() => pipelines.id, { onDelete: 'set null' }),
  flowId:           text('flow_id').references(() => flowRuns.flowId, { onDelete: 'set null' }),
  nodeId:           text('node_id'),
  itemKey:          text('item_key'),
}, (table) => [
  primaryKey({ columns: [table.sessionId, table.msgId] }),
  foreignKey({ columns: [table.pipelineId, table.nodeId], foreignColumns: [pipelineNodes.pipelineId, pipelineNodes.nodeName] }),
  foreignKey({ columns: [table.flowId, table.itemKey], foreignColumns: [flowItems.flowId, flowItems.itemKey] }),
  index('idx_cost_ts').on(table.ts),
  index('idx_cost_root_key').on(table.rootSessionKey),
  index('idx_cost_root_kind_ts').on(table.rootKind, table.ts),
  index('idx_cost_pipeline_ts').on(table.pipelineId, table.ts),
  index('idx_cost_flow_node').on(table.flowId, table.nodeId),
  index('idx_cost_model_ts').on(table.model, table.ts),
  index('idx_cost_session').on(table.sessionId),
]);

