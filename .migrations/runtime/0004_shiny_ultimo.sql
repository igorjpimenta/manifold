ALTER TABLE `flow_node_invocations` ADD `claim_state` text;--> statement-breakpoint
ALTER TABLE `flow_node_invocations` ADD `claimed_harness` text;--> statement-breakpoint
ALTER TABLE `flow_node_invocations` ADD `claimed_agent` text;--> statement-breakpoint
ALTER TABLE `flow_node_invocations` ADD `claimed_session` text;--> statement-breakpoint
ALTER TABLE `flow_node_invocations` ADD `claimed_at` integer;--> statement-breakpoint
ALTER TABLE `flow_node_invocations` ADD `lease_expires_at` integer;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_invocations_claim` ON `flow_node_invocations` (`claim_state`);