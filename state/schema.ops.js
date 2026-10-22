// state/schema.ops.js — ops.sqlite table definitions (drizzle source of truth).
//
// Companion to state/schema.js (runtime.sqlite). Same contract: drizzle-kit
// generates .migrations/ops snapshots from this file; state/migrate.js
// applies them at open. Raw-SQL readers stay as they are — defs exist so
// snapshots cover every table, not to force ORM queries.
import { sqliteTable, text, integer, real, index, uniqueIndex, primaryKey } from 'drizzle-orm/sqlite-core';
import { desc, sql } from 'drizzle-orm';

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
  pipelineId:       text('pipeline_id'),
  pipelineArgs:     text('pipeline_args'),
  flowId:           text('flow_id'),
  enqueuerSessionKey: text('enqueuer_session_key'),
}, (table) => [
  index('idx_status').on(table.status),
  index('idx_created_at').on(desc(table.createdAt)),
  index('idx_flow_node').on(table.flowId, table.node),
]);

export const gateState = sqliteTable('gate_state', {
  id:     text('id').primaryKey().default('singleton'),
  locked: integer('locked').notNull().default(0),
  day:    text('day'),
});

export const sessionLineage = sqliteTable('session_lineage', {
  childSessionKey:  text('child_session_key').primaryKey(),
  parentSessionKey: text('parent_session_key'),
  rootSessionKey:   text('root_session_key').notNull(),
  kind:             text('kind').notNull(),
  rootKind:         text('root_kind').notNull(),
  rootLabel:        text('root_label'),
  agentId:          text('agent_id'),
  pipelineId:       text('pipeline_id'),
  flowId:           text('flow_id'),
  nodeId:           text('node_id'),
  itemKey:          text('item_key'),
  firstSeenAt:      integer('first_seen_at').notNull(),
  source:           text('source').notNull(),
  runId:            text('run_id'),
}, (table) => [
  index('idx_lineage_root').on(table.rootSessionKey),
  index('idx_lineage_parent').on(table.parentSessionKey),
  index('idx_lineage_pipeline').on(table.pipelineId),
  index('idx_lineage_flow').on(table.flowId),
  index('idx_lineage_root_kind').on(table.rootKind),
  index('idx_lineage_run').on(table.runId),
]);

export const costByCall = sqliteTable('cost_by_call', {
  sessionId:       text('session_id').notNull(),
  msgId:           text('msg_id').notNull(),
  ts:              integer('ts').notNull(),
  provider:        text('provider'),
  model:           text('model'),
  inputTokens:     integer('input_tokens'),
  outputTokens:    integer('output_tokens'),
  cacheReadTokens: integer('cache_read_tokens'),
  cacheWriteTokens: integer('cache_write_tokens'),
  totalTokens:     integer('total_tokens'),
  costInput:       real('cost_input'),
  costOutput:      real('cost_output'),
  costCacheRead:   real('cost_cache_read'),
  costCacheWrite:  real('cost_cache_write'),
  costTotal:       real('cost_total').notNull(),
  sessionKey:      text('session_key'),
  agentId:         text('agent_id'),
  rootSessionKey:  text('root_session_key'),
  rootKind:        text('root_kind'),
  rootLabel:       text('root_label'),
  pipelineId:      text('pipeline_id'),
  flowId:          text('flow_id'),
  nodeId:          text('node_id'),
  itemKey:         text('item_key'),
}, (table) => [
  primaryKey({ columns: [table.sessionId, table.msgId] }),
  index('idx_cost_ts').on(table.ts),
  index('idx_cost_root_key').on(table.rootSessionKey),
  index('idx_cost_root_kind_ts').on(table.rootKind, table.ts),
  index('idx_cost_pipeline_ts').on(table.pipelineId, table.ts),
  index('idx_cost_flow_node').on(table.flowId, table.nodeId),
  index('idx_cost_model_ts').on(table.model, table.ts),
  index('idx_cost_session').on(table.sessionId),
]);

export const capVersions = sqliteTable('cap_versions', {
  id:            text('id').primaryKey(),
  capUsd:        real('cap_usd').notNull(),
  lockThreshold: real('lock_threshold').notNull(),
  registeredAt:  integer('registered_at').notNull(),
  retiredAt:     integer('retired_at'),
}, (table) => [
  index('idx_cv_lookup').on(table.registeredAt),
  uniqueIndex('idx_cv_active').on(sql`(1)`).where(sql`${table.retiredAt} IS NULL`),
]);

// Durable log of threshold crossings — the .alerted-thresholds.json debounce
// file only ever answers "did we already fire threshold X today" and is
// overwritten every local day, so it can't answer "when did we last
// cross 80%". This table is the queryable history that backs that question.
export const budgetAlertEvents = sqliteTable('budget_alert_events', {
  id:        text('id').primaryKey(),
  threshold: real('threshold').notNull(),
  totalUsd:  real('total_usd').notNull(),
  capUsd:    real('cap_usd').notNull(),
  firedAt:   integer('fired_at').notNull(),
}, (table) => [
  index('idx_bae_fired_at').on(table.firedAt),
]);
