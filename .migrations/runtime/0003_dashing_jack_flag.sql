CREATE TABLE IF NOT EXISTS `pipeline_dirs` (
	`harness` text PRIMARY KEY NOT NULL,
	`path` text NOT NULL,
	`created_at` integer NOT NULL
);
