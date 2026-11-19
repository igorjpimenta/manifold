-- Restore the harness / parent-invocation constraints on databases that
-- applied an earlier revision of 0008/0009 before those FKs were added to the
-- files. drizzle records an applied migration by timestamp, so editing an
-- already-applied file never re-runs it: the table keeps its old shape while
-- the snapshot says it is current.
--
-- Adds:
--   pipelines.harness          -> harnesses(id)          ON DELETE set null
--   pipeline_dirs.harness      -> harnesses(id)          ON DELETE cascade
--   flow_runs.parent_invocation_id -> flow_node_invocations(id) ON DELETE set null
--   flow_admission_queue.flow_id   -> flow_runs(flow_id) ON DELETE cascade
--
-- The 12-step rebuild is not guardable: CREATE/RENAME fail if the target
-- exists, so this migration runs exactly once per database. Column lists are
-- explicit and ordered as the source tables define them.
CREATE TABLE `__new_pipelines` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`harness` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`deleted_at` integer,
	FOREIGN KEY (`harness`) REFERENCES `harnesses`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_pipelines`(`id`, `name`, `harness`, `created_at`, `updated_at`, `deleted_at`) SELECT `id`, `name`, `harness`, `created_at`, `updated_at`, `deleted_at` FROM `pipelines`;
--> statement-breakpoint
DROP TABLE `pipelines`;
--> statement-breakpoint
ALTER TABLE `__new_pipelines` RENAME TO `pipelines`;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pipelines_harness_name` ON `pipelines` (`harness`,`name`);
--> statement-breakpoint
CREATE TABLE `__new_pipeline_dirs` (
	`harness` text PRIMARY KEY NOT NULL,
	`path` text NOT NULL,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`harness`) REFERENCES `harnesses`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
INSERT INTO `__new_pipeline_dirs`(`harness`, `path`, `created_at`) SELECT `harness`, `path`, `created_at` FROM `pipeline_dirs`;
--> statement-breakpoint
DROP TABLE `pipeline_dirs`;
--> statement-breakpoint
ALTER TABLE `__new_pipeline_dirs` RENAME TO `pipeline_dirs`;
--> statement-breakpoint
CREATE TABLE `__new_flow_admission_queue` (
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
INSERT INTO `__new_flow_admission_queue`(`id`, `kind`, `flow_id`, `node_id`, `status`, `created_at`, `admitted_at`) SELECT `id`, `kind`, `flow_id`, `node_id`, `status`, `created_at`, `admitted_at` FROM `flow_admission_queue`;
--> statement-breakpoint
DROP TABLE `flow_admission_queue`;
--> statement-breakpoint
ALTER TABLE `__new_flow_admission_queue` RENAME TO `flow_admission_queue`;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status` ON `flow_admission_queue` (`status`);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_flow` ON `flow_admission_queue` (`flow_id`);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_admission_status_created` ON `flow_admission_queue` (`status`,`created_at`);
--> statement-breakpoint
CREATE TABLE `__new_flow_runs` (
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
	FOREIGN KEY (`parent_invocation_id`) REFERENCES `flow_node_invocations`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`pipeline_id`) REFERENCES `pipelines`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
INSERT INTO `__new_flow_runs`(`flow_id`, `owner_key`, `revision`, `status`, `last_step`, `status_reason`, `state_json`, `wait_json`, `cancel_requested_at`, `created_at`, `updated_at`, `ended_at`, `parent_invocation_id`, `pipeline_id`) SELECT `flow_id`, `owner_key`, `revision`, `status`, `last_step`, `status_reason`, `state_json`, `wait_json`, `cancel_requested_at`, `created_at`, `updated_at`, `ended_at`, `parent_invocation_id`, `pipeline_id` FROM `flow_runs`;
--> statement-breakpoint
DROP TABLE `flow_runs`;
--> statement-breakpoint
ALTER TABLE `__new_flow_runs` RENAME TO `flow_runs`;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_status` ON `flow_runs` (`status`) WHERE "flow_runs"."ended_at" IS NULL;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_updated` ON `flow_runs` (`updated_at` DESC);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_pfr_created` ON `flow_runs` (`created_at` DESC);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_pfr_parent_invocation` ON `flow_runs` (`parent_invocation_id`) WHERE "flow_runs"."parent_invocation_id" IS NOT NULL;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_flow_runs_pipeline_id` ON `flow_runs` (`pipeline_id`);
