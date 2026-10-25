CREATE INDEX `idx_failures_failed_at` ON `failure_events` ("failed_at" desc);--> statement-breakpoint
CREATE INDEX `idx_pfr_created` ON `flow_runs` ("created_at" desc);