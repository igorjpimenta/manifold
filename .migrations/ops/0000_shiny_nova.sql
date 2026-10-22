CREATE TABLE IF NOT EXISTS `budget_alert_events` (
	`id` text PRIMARY KEY NOT NULL,
	`threshold` real NOT NULL,
	`total_usd` real NOT NULL,
	`cap_usd` real NOT NULL,
	`fired_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_bae_fired_at` ON `budget_alert_events` (`fired_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `cap_versions` (
	`id` text PRIMARY KEY NOT NULL,
	`cap_usd` real NOT NULL,
	`lock_threshold` real NOT NULL,
	`registered_at` integer NOT NULL,
	`retired_at` integer
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cv_lookup` ON `cap_versions` (`registered_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_cv_active` ON `cap_versions` ((1)) WHERE "cap_versions"."retired_at" IS NULL;--> statement-breakpoint
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
	PRIMARY KEY(`session_id`, `msg_id`)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_ts` ON `cost_by_call` (`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_key` ON `cost_by_call` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_kind_ts` ON `cost_by_call` (`root_kind`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_pipeline_ts` ON `cost_by_call` (`pipeline_id`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_flow_node` ON `cost_by_call` (`flow_id`,`node_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_model_ts` ON `cost_by_call` (`model`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_session` ON `cost_by_call` (`session_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `gate_state` (
	`id` text PRIMARY KEY DEFAULT 'singleton' NOT NULL,
	`locked` integer DEFAULT 0 NOT NULL,
	`day` text
);
--> statement-breakpoint
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
	`enqueuer_session_key` text
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
	`run_id` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root` ON `session_lineage` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_parent` ON `session_lineage` (`parent_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_pipeline` ON `session_lineage` (`pipeline_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_flow` ON `session_lineage` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root_kind` ON `session_lineage` (`root_kind`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_run` ON `session_lineage` (`run_id`);