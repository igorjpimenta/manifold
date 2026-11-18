-- queue_items / session_lineage / cost_by_call moved to runtime.sqlite so
-- flow_id and pipeline_id can carry FKs. The copy runs when the runtime
-- sibling is migrated, and state/migrate.js migrates that sibling before
-- this file is applied, so the drop below never precedes the copy.
DROP TABLE IF EXISTS `cost_by_call`;--> statement-breakpoint
DROP TABLE IF EXISTS `queue_items`;--> statement-breakpoint
DROP TABLE IF EXISTS `session_lineage`;
