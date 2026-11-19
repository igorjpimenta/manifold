// state/schema.ops.js — ops.sqlite table definitions (drizzle source of truth).
//
// Companion to state/schema.js (runtime.sqlite). Same contract: drizzle-kit
// generates .migrations/ops snapshots from this file; state/migrate.js
// applies them at open. Raw-SQL readers stay as they are — defs exist so
// snapshots cover every table, not to force ORM queries.
// queue_items / session_lineage / cost_by_call are not defined here: they
// live in state/schema.js (runtime.sqlite) so their flow_id and pipeline_id
// can carry FKs. Older ops files still contain the copies they were moved
// from; state/drop-moved-ops.js removes them once the runtime side holds
// every row (see that file for the ordering and the row-count guard).
import { sqliteTable, text, integer, real, index, uniqueIndex, primaryKey } from 'drizzle-orm/sqlite-core';
import { desc, sql } from 'drizzle-orm';

export const gateState = sqliteTable('gate_state', {
  id:     text('id').primaryKey().default('singleton'),
  locked: integer('locked').notNull().default(0),
  day:    text('day'),
});

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
