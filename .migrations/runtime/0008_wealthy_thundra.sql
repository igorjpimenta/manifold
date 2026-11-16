-- FK retrofit + pipelines registry.
--
-- SQLite has no ALTER TABLE ADD CONSTRAINT: every FK on an existing table is
-- applied by the 12-step rebuild (create __new, copy, DROP, RENAME). The
-- foreign_keys is off for the whole run, set by state/migrate.js before
-- drizzle begins its snapshot transaction (a PRAGMA inside that transaction
-- is a no-op). Existing rows already violate two of these edges (22
-- invocations without a run, 38 failure rows without an execution); they copy
-- as-is and stay violations until cleaned. pipelines is backfilled from the
-- union of every pipeline_id column before any rebuilt table references it.

CREATE TABLE IF NOT EXISTS `pipelines` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`harness` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`deleted_at` integer
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pipelines_harness_name` ON `pipelines` (`harness`,`name`);--> statement-breakpoint
INSERT OR IGNORE INTO `pipelines` (`id`, `name`, `harness`, `created_at`, `updated_at`)
SELECT `id`, `id`, NULL, CAST(strftime('%s','now') AS INTEGER) * 1000, CAST(strftime('%s','now') AS INTEGER) * 1000
FROM (
	SELECT `pipeline_id` AS `id` FROM `flow_runs` WHERE `pipeline_id` IS NOT NULL
	UNION SELECT `pipeline_id` FROM `pipeline_versions`
	UNION SELECT `pipeline_id` FROM `pipeline_node_versions`
	UNION SELECT `pipeline_id` FROM `node_health`
	UNION SELECT `parent_pipeline_id` FROM `pipeline_versions` WHERE `parent_pipeline_id` IS NOT NULL
);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_pipeline_node_versions` (
	`id` text PRIMARY KEY NOT NULL,
	`pipeline_version_id` text NOT NULL,
	`pipeline_id` text NOT NULL,
	`node_name` text NOT NULL,
	`node_id` text,
	`kind` text NOT NULL,
	`agent` text,
	`mode` text,
	`content_hash` text NOT NULL,
	`registered_at` integer NOT NULL,
	`retired_at` integer,
	FOREIGN KEY (`pipeline_version_id`) REFERENCES `pipeline_versions`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE restrict
);
--> statement-breakpoint
INSERT INTO `__new_pipeline_node_versions`("id", "pipeline_version_id", "pipeline_id", "node_name", "node_id", "kind", "agent", "mode", "content_hash", "registered_at", "retired_at") SELECT "id", "pipeline_version_id", "pipeline_id", "node_name", "node_id", "kind", "agent", "mode", "content_hash", "registered_at", "retired_at" FROM `pipeline_node_versions`;--> statement-breakpoint
DROP TABLE `pipeline_node_versions`;--> statement-breakpoint
ALTER TABLE `__new_pipeline_node_versions` RENAME TO `pipeline_node_versions`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pnv_lookup` ON `pipeline_node_versions` (`pipeline_id`,`node_name`,`registered_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pnv_active` ON `pipeline_node_versions` (`pipeline_id`,`node_name`) WHERE "pipeline_node_versions"."retired_at" IS NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_discuss_state` (
	`flow_id` text NOT NULL,
	`item_id` text NOT NULL,
	`messages_json` text DEFAULT '[]' NOT NULL,
	`turn_status` text DEFAULT 'idle' NOT NULL,
	`active_turn_id` text,
	`pending_json` text,
	`updated_at` integer NOT NULL,
	`components_json` text DEFAULT '[]' NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_discuss_state`("flow_id", "item_id", "messages_json", "turn_status", "active_turn_id", "pending_json", "updated_at", "components_json") SELECT "flow_id", "item_id", "messages_json", "turn_status", "active_turn_id", "pending_json", "updated_at", "components_json" FROM `discuss_state`;--> statement-breakpoint
DROP TABLE `discuss_state`;--> statement-breakpoint
ALTER TABLE `__new_discuss_state` RENAME TO `discuss_state`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_discuss_updated` ON `discuss_state` (`updated_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_state_unique` ON `discuss_state` (`flow_id`,`item_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_discuss_threads` (
	`flow_id` text NOT NULL,
	`gate_node_id` text NOT NULL,
	`item_id` text NOT NULL,
	`agent_id` text,
	`session_key` text NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_discuss_threads`("flow_id", "gate_node_id", "item_id", "agent_id", "session_key", "updated_at") SELECT "flow_id", "gate_node_id", "item_id", "agent_id", "session_key", "updated_at" FROM `discuss_threads`;--> statement-breakpoint
DROP TABLE `discuss_threads`;--> statement-breakpoint
ALTER TABLE `__new_discuss_threads` RENAME TO `discuss_threads`;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_threads_unique` ON `discuss_threads` (`flow_id`,`gate_node_id`,`item_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_failure_events` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`failed_at` integer NOT NULL,
	`failure_summary` text NOT NULL,
	`invocation_id` text,
	`execution_id` text,
	`error_type` text,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`invocation_id`) REFERENCES `flow_node_invocations`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`execution_id`) REFERENCES `flow_node_executions`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_failure_events`("id", "flow_id", "failed_at", "failure_summary", "invocation_id", "execution_id", "error_type") SELECT "id", "flow_id", "failed_at", "failure_summary", "invocation_id", "execution_id", "error_type" FROM `failure_events`;--> statement-breakpoint
DROP TABLE `failure_events`;--> statement-breakpoint
ALTER TABLE `__new_failure_events` RENAME TO `failure_events`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_failures_flow` ON `failure_events` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_failures_failed_at` ON `failure_events` (`failed_at` DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_failures_invocation` ON `failure_events` (`invocation_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_failures_execution` ON `failure_events` (`execution_id`) WHERE "failure_events"."execution_id" IS NOT NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_admission_queue` (
	`id` text PRIMARY KEY NOT NULL,
	`kind` text NOT NULL,
	`flow_id` text NOT NULL,
	`node_id` text,
	`status` text NOT NULL,
	`created_at` integer NOT NULL,
	`admitted_at` integer,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_flow_admission_queue`("id", "kind", "flow_id", "node_id", "status", "created_at", "admitted_at") SELECT "id", "kind", "flow_id", "node_id", "status", "created_at", "admitted_at" FROM `flow_admission_queue`;--> statement-breakpoint
DROP TABLE `flow_admission_queue`;--> statement-breakpoint
ALTER TABLE `__new_flow_admission_queue` RENAME TO `flow_admission_queue`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status` ON `flow_admission_queue` (`status`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_flow` ON `flow_admission_queue` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status_created` ON `flow_admission_queue` (`status`,`created_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_events` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`event_type` text NOT NULL,
	`prev_step` text,
	`curr_step` text,
	`prev_status` text,
	`curr_status` text,
	`wait_kind` text,
	`occurred_at` integer NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_flow_events`("id", "flow_id", "event_type", "prev_step", "curr_step", "prev_status", "curr_status", "wait_kind", "occurred_at") SELECT "id", "flow_id", "event_type", "prev_step", "curr_step", "prev_status", "curr_status", "wait_kind", "occurred_at" FROM `flow_events`;--> statement-breakpoint
DROP TABLE `flow_events`;--> statement-breakpoint
ALTER TABLE `__new_flow_events` RENAME TO `flow_events`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_events_flow_id` ON `flow_events` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_events_occurred_at` ON `flow_events` (`occurred_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_node_executions` (
	`id` text PRIMARY KEY NOT NULL,
	`invocation_id` text NOT NULL,
	`entered_at` integer NOT NULL,
	`exited_at` integer NOT NULL,
	`kind` text DEFAULT 'legacy' NOT NULL,
	FOREIGN KEY (`invocation_id`) REFERENCES `flow_node_invocations`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_flow_node_executions`("id", "invocation_id", "entered_at", "exited_at", "kind") SELECT "id", "invocation_id", "entered_at", "exited_at", "kind" FROM `flow_node_executions`;--> statement-breakpoint
DROP TABLE `flow_node_executions`;--> statement-breakpoint
ALTER TABLE `__new_flow_node_executions` RENAME TO `flow_node_executions`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_fne_invocation` ON `flow_node_executions` (`invocation_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_fne_exited` ON `flow_node_executions` (`exited_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_node_invocations` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`pipeline_id` text,
	`node_id` text NOT NULL,
	`subflow_key` text,
	`visit_seq` integer DEFAULT 1 NOT NULL,
	`invoked_at` integer NOT NULL,
	`result_json` text,
	`result_written_at` integer,
	`result_too_large_bytes` integer,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_flow_node_invocations`("id", "flow_id", "pipeline_id", "node_id", "subflow_key", "visit_seq", "invoked_at", "result_json", "result_written_at", "result_too_large_bytes") SELECT "id", "flow_id", "pipeline_id", "node_id", "subflow_key", "visit_seq", "invoked_at", "result_json", "result_written_at", "result_too_large_bytes" FROM `flow_node_invocations`;--> statement-breakpoint
DROP TABLE `flow_node_invocations`;--> statement-breakpoint
ALTER TABLE `__new_flow_node_invocations` RENAME TO `flow_node_invocations`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_flow` ON `flow_node_invocations` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_node_subflow` ON `flow_node_invocations` (`node_id`,`subflow_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_pipe_node` ON `flow_node_invocations` (`pipeline_id`,`node_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_invocations_identity` ON `flow_node_invocations` (`flow_id`,`node_id`,COALESCE(`subflow_key`, ''),`visit_seq`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_runs` (
	`flow_id` text PRIMARY KEY NOT NULL,
	`owner_key` text DEFAULT '' NOT NULL,
	`revision` integer DEFAULT 0 NOT NULL,
	`status` text NOT NULL,
	`last_step` text,
	`status_reason` text,
	`state_json` text DEFAULT '{}' NOT NULL,
	`wait_json` text,
	`cancel_requested_at` integer,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`ended_at` integer,
	`parent_invocation_id` text,
	`pipeline_id` text,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_flow_runs`("flow_id", "owner_key", "revision", "status", "last_step", "status_reason", "state_json", "wait_json", "cancel_requested_at", "created_at", "updated_at", "ended_at", "parent_invocation_id", "pipeline_id") SELECT "flow_id", "owner_key", "revision", "status", "last_step", "status_reason", "state_json", "wait_json", "cancel_requested_at", "created_at", "updated_at", "ended_at", "parent_invocation_id", "pipeline_id" FROM `flow_runs`;--> statement-breakpoint
DROP TABLE `flow_runs`;--> statement-breakpoint
ALTER TABLE `__new_flow_runs` RENAME TO `flow_runs`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_status` ON `flow_runs` (`status`) WHERE "flow_runs"."ended_at" IS NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_updated` ON `flow_runs` (`updated_at` DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_created` ON `flow_runs` (`created_at` DESC);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pfr_parent_invocation` ON `flow_runs` (`parent_invocation_id`) WHERE "flow_runs"."parent_invocation_id" IS NOT NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_runs_pipeline_id` ON `flow_runs` (`pipeline_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_sessions` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`session_key` text NOT NULL,
	`node_id` text,
	`branch_item_id` text,
	`spawned_at` integer NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_flow_sessions`("id", "flow_id", "session_key", "node_id", "branch_item_id", "spawned_at") SELECT "id", "flow_id", "session_key", "node_id", "branch_item_id", "spawned_at" FROM `flow_sessions`;--> statement-breakpoint
DROP TABLE `flow_sessions`;--> statement-breakpoint
ALTER TABLE `__new_flow_sessions` RENAME TO `flow_sessions`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_sessions_flow_id` ON `flow_sessions` (`flow_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_flow_sessions_uniq` ON `flow_sessions` (`flow_id`,`session_key`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_gate_records` (
	`flow_id` text NOT NULL,
	`node_id` text NOT NULL,
	`record_json` text NOT NULL,
	`updated_at` integer NOT NULL,
	PRIMARY KEY(`flow_id`, `node_id`),
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_gate_records`("flow_id", "node_id", "record_json", "updated_at") SELECT "flow_id", "node_id", "record_json", "updated_at" FROM `gate_records`;--> statement-breakpoint
DROP TABLE `gate_records`;--> statement-breakpoint
ALTER TABLE `__new_gate_records` RENAME TO `gate_records`;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_node_health` (
	`id` text PRIMARY KEY NOT NULL,
	`pipeline_id` text NOT NULL,
	`node_id` text NOT NULL,
	`status` text NOT NULL,
	`reason` text NOT NULL,
	`blocked_by_flow_id` text,
	`resolved_via` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`ended_at` integer,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE restrict,
	FOREIGN KEY (`blocked_by_flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_node_health`("id", "pipeline_id", "node_id", "status", "reason", "blocked_by_flow_id", "resolved_via", "created_at", "updated_at", "ended_at") SELECT "id", "pipeline_id", "node_id", "status", "reason", "blocked_by_flow_id", "resolved_via", "created_at", "updated_at", "ended_at" FROM `node_health`;--> statement-breakpoint
DROP TABLE `node_health`;--> statement-breakpoint
ALTER TABLE `__new_node_health` RENAME TO `node_health`;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_node_health_one_active` ON `node_health` (`pipeline_id`,`node_id`) WHERE "node_health"."status" = 'blocked';--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_node_health_pipeline` ON `node_health` (`pipeline_id`,`status`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_pipeline_versions` (
	`id` text PRIMARY KEY NOT NULL,
	`pipeline_id` text NOT NULL,
	`kind` text,
	`allow_concurrent` text,
	`is_synthetic` integer NOT NULL,
	`parent_pipeline_id` text,
	`source_path` text,
	`content_hash` text NOT NULL,
	`registered_at` integer NOT NULL,
	`retired_at` integer,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE restrict,
	FOREIGN KEY (`parent_pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_pipeline_versions`("id", "pipeline_id", "kind", "allow_concurrent", "is_synthetic", "parent_pipeline_id", "source_path", "content_hash", "registered_at", "retired_at") SELECT "id", "pipeline_id", "kind", "allow_concurrent", "is_synthetic", "parent_pipeline_id", "source_path", "content_hash", "registered_at", "retired_at" FROM `pipeline_versions`;--> statement-breakpoint
DROP TABLE `pipeline_versions`;--> statement-breakpoint
ALTER TABLE `__new_pipeline_versions` RENAME TO `pipeline_versions`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pv_lookup` ON `pipeline_versions` (`pipeline_id`,`registered_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pv_active` ON `pipeline_versions` (`pipeline_id`) WHERE "pipeline_versions"."retired_at" IS NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_tool_results` (
	`session_key` text PRIMARY KEY NOT NULL,
	`run_id` text,
	`agent_id` text,
	`kind` text,
	`payload_json` text NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
INSERT INTO `__new_tool_results`("session_key", "run_id", "agent_id", "kind", "payload_json", "updated_at") SELECT "session_key", "run_id", "agent_id", "kind", "payload_json", "updated_at" FROM `tool_results`;--> statement-breakpoint
DROP TABLE `tool_results`;--> statement-breakpoint
ALTER TABLE `__new_tool_results` RENAME TO `tool_results`;