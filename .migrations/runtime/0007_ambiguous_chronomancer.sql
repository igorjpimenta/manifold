CREATE TABLE IF NOT EXISTS `tool_results` (
	`session_key` text PRIMARY KEY NOT NULL,
	`run_id` text,
	`agent_id` text,
	`kind` text,
	`payload_json` text NOT NULL,
	`updated_at` integer NOT NULL
);
