CREATE TABLE IF NOT EXISTS `sessions` (
	`session_key` text PRIMARY KEY NOT NULL,
	`first_seen_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_sessions_first_seen` ON `sessions` (`first_seen_at`);--> statement-breakpoint
PRAGMA foreign_keys=OFF;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_cost_by_call` (
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
	FOREIGN KEY (`session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`root_session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_cost_by_call`("session_id", "msg_id", "ts", "provider", "model", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "total_tokens", "cost_input", "cost_output", "cost_cache_read", "cost_cache_write", "cost_total", "session_key", "agent_id", "root_session_key", "root_kind", "root_label", "pipeline_id", "flow_id", "node_id", "item_key") SELECT "session_id", "msg_id", "ts", "provider", "model", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "total_tokens", "cost_input", "cost_output", "cost_cache_read", "cost_cache_write", "cost_total", "session_key", "agent_id", "root_session_key", "root_kind", "root_label", "pipeline_id", "flow_id", "node_id", "item_key" FROM `cost_by_call`;--> statement-breakpoint
DROP TABLE `cost_by_call`;--> statement-breakpoint
ALTER TABLE `__new_cost_by_call` RENAME TO `cost_by_call`;--> statement-breakpoint
PRAGMA foreign_keys=ON;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_ts` ON `cost_by_call` (`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_key` ON `cost_by_call` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_root_kind_ts` ON `cost_by_call` (`root_kind`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_pipeline_ts` ON `cost_by_call` (`pipeline_id`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_flow_node` ON `cost_by_call` (`flow_id`,`node_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_model_ts` ON `cost_by_call` (`model`,`ts`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cost_session` ON `cost_by_call` (`session_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_discuss_threads` (
	`flow_id` text NOT NULL,
	`gate_node_id` text NOT NULL,
	`item_id` text NOT NULL,
	`agent_id` text,
	`session_key` text NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
INSERT INTO `__new_discuss_threads`("flow_id", "gate_node_id", "item_id", "agent_id", "session_key", "updated_at") SELECT "flow_id", "gate_node_id", "item_id", "agent_id", "session_key", "updated_at" FROM `discuss_threads`;--> statement-breakpoint
DROP TABLE `discuss_threads`;--> statement-breakpoint
ALTER TABLE `__new_discuss_threads` RENAME TO `discuss_threads`;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_threads_unique` ON `discuss_threads` (`flow_id`,`gate_node_id`,`item_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_flow_sessions` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`session_key` text NOT NULL,
	`node_id` text,
	`branch_item_id` text,
	`spawned_at` integer NOT NULL,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
INSERT INTO `__new_flow_sessions`("id", "flow_id", "session_key", "node_id", "branch_item_id", "spawned_at") SELECT "id", "flow_id", "session_key", "node_id", "branch_item_id", "spawned_at" FROM `flow_sessions`;--> statement-breakpoint
DROP TABLE `flow_sessions`;--> statement-breakpoint
ALTER TABLE `__new_flow_sessions` RENAME TO `flow_sessions`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_sessions_flow_id` ON `flow_sessions` (`flow_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_flow_sessions_uniq` ON `flow_sessions` (`flow_id`,`session_key`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_queue_items` (
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
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`enqueuer_session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_queue_items`("id", "label", "agent", "agent_emoji", "trigger", "cost_low", "cost_high", "brief", "model_chain", "pipeline", "node", "token_estimate", "status", "created_at", "expires_at", "decided_at", "decision", "pipeline_id", "pipeline_args", "flow_id", "enqueuer_session_key", "kind") SELECT "id", "label", "agent", "agent_emoji", "trigger", "cost_low", "cost_high", "brief", "model_chain", "pipeline", "node", "token_estimate", "status", "created_at", "expires_at", "decided_at", "decision", "pipeline_id", "pipeline_args", "flow_id", "enqueuer_session_key", "kind" FROM `queue_items`;--> statement-breakpoint
DROP TABLE `queue_items`;--> statement-breakpoint
ALTER TABLE `__new_queue_items` RENAME TO `queue_items`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_status` ON `queue_items` (`status`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_created_at` ON `queue_items` ("created_at" desc);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_node` ON `queue_items` (`flow_id`,`node`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_session_lineage` (
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
	FOREIGN KEY (`child_session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`parent_session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`root_session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`flow_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_session_lineage`("child_session_key", "parent_session_key", "root_session_key", "kind", "root_kind", "root_label", "agent_id", "pipeline_id", "flow_id", "node_id", "item_key", "first_seen_at", "source", "run_id") SELECT "child_session_key", "parent_session_key", "root_session_key", "kind", "root_kind", "root_label", "agent_id", "pipeline_id", "flow_id", "node_id", "item_key", "first_seen_at", "source", "run_id" FROM `session_lineage`;--> statement-breakpoint
DROP TABLE `session_lineage`;--> statement-breakpoint
ALTER TABLE `__new_session_lineage` RENAME TO `session_lineage`;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root` ON `session_lineage` (`root_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_parent` ON `session_lineage` (`parent_session_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_pipeline` ON `session_lineage` (`pipeline_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_flow` ON `session_lineage` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_root_kind` ON `session_lineage` (`root_kind`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_lineage_run` ON `session_lineage` (`run_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `__new_tool_results` (
	`session_key` text PRIMARY KEY NOT NULL,
	`run_id` text,
	`agent_id` text,
	`kind` text,
	`payload_json` text NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`session_key`) REFERENCES `sessions`(`session_key`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`run_id`) REFERENCES `flow_runs`(`flow_id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_tool_results`("session_key", "run_id", "agent_id", "kind", "payload_json", "updated_at") SELECT "session_key", "run_id", "agent_id", "kind", "payload_json", "updated_at" FROM `tool_results`;--> statement-breakpoint
DROP TABLE `tool_results`;--> statement-breakpoint
ALTER TABLE `__new_tool_results` RENAME TO `tool_results`;