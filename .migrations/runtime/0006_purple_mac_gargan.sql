CREATE TABLE IF NOT EXISTS `pending_proposals` (
	`flow_id` text NOT NULL,
	`item_id` text NOT NULL,
	`text` text,
	`fields_json` text,
	`draft_id` text,
	`updated_at` integer NOT NULL,
	PRIMARY KEY(`flow_id`, `item_id`)
);
