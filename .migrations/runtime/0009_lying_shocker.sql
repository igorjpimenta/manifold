-- Move the lineage/cost tables into runtime so their flow_id/pipeline_id
-- columns can carry FKs (SQLite cannot constrain across database files).
-- Data is copied once by state/ops-merge.js after this snapshot runs; the
-- CREATEs stay IF NOT EXISTS so a retried migration is safe.
CREATE TABLE IF NOT EXISTS `cost_by_call` (
	`session_id` text NOT NULL,
	`msg_id` text NOT NULL,
	`ts` integer NOT NULL,
	`provider` text,
	`model` text,
	`input_tokens` integer,
	`output_tokens` integer,
	`cache_read_tokens` integer,
	`cache_write_tokens` integer,
	`total_tokens` integer,
	`cost_input` real,
	`cost_output` real,
	`cost_cache_read` real,
	`cost_cache_write` real,
	`cost_total` real NOT NULL,
	`session_key` text,
	`agent_id` text,
	`root_session_key` text,
	`root_kind` text,
	`root_label` text,
	`pipeline_id` text,
	`flow_id` text,
	`node_id` text,
	`item_key` text,
	PRIMARY KEY(`session_id`, `msg_id`),
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_ts` ON `cost_by_call` (`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_key` ON `cost_by_call` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_kind_ts` ON `cost_by_call` (`root_kind`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_pipeline_ts` ON `cost_by_call` (`pipeline_id`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_flow_node` ON `cost_by_call` (`flow_id`,`node_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_model_ts` ON `cost_by_call` (`model`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_session` ON `cost_by_call` (`session_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `queue_items` (
	`id` text PRIMARY KEY NOT NULL,
	`label` text NOT NULL,
	`agent` text NOT NULL,
	`agent_emoji` text DEFAULT '🤖' NOT NULL,
	`trigger` text DEFAULT 'manual' NOT NULL,
	`cost_low` real DEFAULT 0 NOT NULL,
	`cost_high` real DEFAULT 0 NOT NULL,
	`brief` text,
	`model_chain` text,
	`pipeline` text,
	`node` text,
	`token_estimate` integer,
	`status` text DEFAULT 'pending' NOT NULL,
	`created_at` integer NOT NULL,
	`expires_at` integer,
	`decided_at` integer,
	`decision` text,
	`pipeline_id` text,
	`pipeline_args` text,
	`flow_id` text,
	`enqueuer_session_key` text,
	`kind` text,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_status` ON `queue_items` (`status`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_created_at` ON `queue_items` ("created_at" desc);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_node` ON `queue_items` (`flow_id`,`node`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `session_lineage` (
	`child_session_key` text PRIMARY KEY NOT NULL,
	`parent_session_key` text,
	`root_session_key` text NOT NULL,
	`kind` text NOT NULL,
	`root_kind` text NOT NULL,
	`root_label` text,
	`agent_id` text,
	`pipeline_id` text,
	`flow_id` text,
	`node_id` text,
	`item_key` text,
	`first_seen_at` integer NOT NULL,
	`source` text NOT NULL,
	`run_id` text,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root` ON `session_lineage` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_parent` ON `session_lineage` (`parent_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_pipeline` ON `session_lineage` (`pipeline_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_flow` ON `session_lineage` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root_kind` ON `session_lineage` (`root_kind`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_run` ON `session_lineage` (`run_id`);