CREATE TABLE IF NOT EXISTS `discuss_state` (
	`flow_id` text NOT NULL,
	`item_id` text NOT NULL,
	`messages_json` text DEFAULT '[]' NOT NULL,
	`turn_status` text DEFAULT 'idle' NOT NULL,
	`active_turn_id` text,
	`pending_json` text,
	`updated_at` integer NOT NULL,
	`components_json` text DEFAULT '[]' NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_discuss_updated` ON `discuss_state` (`updated_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `failure_events` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`failed_at` integer NOT NULL,
	`failure_summary` text NOT NULL,
	`invocation_id` text,
	`execution_id` text,
	`error_type` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_failures_flow` ON `failure_events` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_failures_invocation` ON `failure_events` (`invocation_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_failures_execution` ON `failure_events` (`execution_id`) WHERE "failure_events"."execution_id" IS NOT NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_admission_queue` (
	`id` text PRIMARY KEY NOT NULL,
	`kind` text NOT NULL,
	`flow_id` text NOT NULL,
	`node_id` text,
	`status` text NOT NULL,
	`created_at` integer NOT NULL,
	`admitted_at` integer
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status` ON `flow_admission_queue` (`status`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_flow` ON `flow_admission_queue` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status_created` ON `flow_admission_queue` (`status`,`created_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_events` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`event_type` text NOT NULL,
	`prev_step` text,
	`curr_step` text,
	`prev_status` text,
	`curr_status` text,
	`wait_kind` text,
	`occurred_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_events_flow_id` ON `flow_events` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_events_occurred_at` ON `flow_events` (`occurred_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_node_executions` (
	`id` text PRIMARY KEY NOT NULL,
	`invocation_id` text NOT NULL,
	`entered_at` integer NOT NULL,
	`exited_at` integer NOT NULL,
	`kind` text DEFAULT 'legacy' NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_fne_invocation` ON `flow_node_executions` (`invocation_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_fne_exited` ON `flow_node_executions` (`exited_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_node_invocations` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`pipeline_id` text,
	`node_id` text NOT NULL,
	`subflow_key` text,
	`visit_seq` integer DEFAULT 1 NOT NULL,
	`invoked_at` integer NOT NULL,
	`result_json` text,
	`result_written_at` integer,
	`result_too_large_bytes` integer
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_flow` ON `flow_node_invocations` (`flow_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_node_subflow` ON `flow_node_invocations` (`node_id`,`subflow_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_pipe_node` ON `flow_node_invocations` (`pipeline_id`,`node_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_runs` (
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
	`pipeline_id` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_status` ON `flow_runs` (`status`) WHERE "flow_runs"."ended_at" IS NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_updated` ON `flow_runs` ("updated_at" desc);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pfr_parent_invocation` ON `flow_runs` (`parent_invocation_id`) WHERE "flow_runs"."parent_invocation_id" IS NOT NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_runs_pipeline_id` ON `flow_runs` (`pipeline_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `flow_sessions` (
	`id` text PRIMARY KEY NOT NULL,
	`flow_id` text NOT NULL,
	`session_key` text NOT NULL,
	`node_id` text,
	`branch_item_id` text,
	`spawned_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_sessions_flow_id` ON `flow_sessions` (`flow_id`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_flow_sessions_uniq` ON `flow_sessions` (`flow_id`,`session_key`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `gate_records` (
	`flow_id` text NOT NULL,
	`node_id` text NOT NULL,
	`record_json` text NOT NULL,
	`updated_at` integer NOT NULL,
	PRIMARY KEY(`flow_id`, `node_id`)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `model_health` (
	`id` text PRIMARY KEY NOT NULL,
	`ref` text NOT NULL,
	`status` text NOT NULL,
	`reason` text NOT NULL,
	`down_since` integer NOT NULL,
	`until_ms` integer,
	`probing` integer DEFAULT 0 NOT NULL,
	`strike_level` integer DEFAULT 0 NOT NULL,
	`strike_ended_at` integer,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `node_health` (
	`id` text PRIMARY KEY NOT NULL,
	`pipeline_id` text NOT NULL,
	`node_id` text NOT NULL,
	`status` text NOT NULL,
	`reason` text NOT NULL,
	`blocked_by_flow_id` text,
	`resolved_via` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`ended_at` integer
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_node_health_one_active` ON `node_health` (`pipeline_id`,`node_id`) WHERE "node_health"."status" = 'blocked';--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_node_health_pipeline` ON `node_health` (`pipeline_id`,`status`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `pipeline_node_versions` (
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
	FOREIGN KEY (`pipeline_version_id`) REFERENCES `pipeline_versions`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pnv_lookup` ON `pipeline_node_versions` (`pipeline_id`,`node_name`,`registered_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pnv_active` ON `pipeline_node_versions` (`pipeline_id`,`node_name`) WHERE "pipeline_node_versions"."retired_at" IS NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `pipeline_versions` (
	`id` text PRIMARY KEY NOT NULL,
	`pipeline_id` text NOT NULL,
	`kind` text,
	`allow_concurrent` text,
	`is_synthetic` integer NOT NULL,
	`parent_pipeline_id` text,
	`source_path` text,
	`content_hash` text NOT NULL,
	`registered_at` integer NOT NULL,
	`retired_at` integer
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pv_lookup` ON `pipeline_versions` (`pipeline_id`,`registered_at`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pv_active` ON `pipeline_versions` (`pipeline_id`) WHERE "pipeline_versions"."retired_at" IS NULL;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_invocations_identity` ON `flow_node_invocations` (`flow_id`,`node_id`,COALESCE(`subflow_key`, ''),`visit_seq`);
