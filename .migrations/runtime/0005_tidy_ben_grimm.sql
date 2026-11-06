CREATE TABLE IF NOT EXISTS `discuss_threads` (
	`flow_id` text NOT NULL,
	`gate_node_id` text NOT NULL,
	`item_id` text NOT NULL,
	`agent_id` text,
	`session_key` text NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_threads_unique` ON `discuss_threads` (`flow_id`,`gate_node_id`,`item_id`);