-- Make discuss_state one row per (flow_id, item_id).
--
-- discuss_state shipped without a uniqueness constraint, so two concurrent
-- appendMessage calls could each insert their own row for the same thread.
-- Every write updates all rows of the group, so the rows converge on the
-- same values; the survivor is the one carrying the most messages, and the
-- group's newest non-null pending wins so a proposal is never dropped.
WITH `grouped` AS MATERIALIZED (
	SELECT
		`rowid` AS `rid`,
		`flow_id` AS `flow_id`,
		`item_id` AS `item_id`,
		`messages_json` AS `messages_json`,
		`turn_status` AS `turn_status`,
		`active_turn_id` AS `active_turn_id`,
		`components_json` AS `components_json`,
		COUNT(*) OVER (PARTITION BY `flow_id`, `item_id`) AS `group_size`,
		ROW_NUMBER() OVER (
			PARTITION BY `flow_id`, `item_id`
			ORDER BY (CASE WHEN json_valid(`messages_json`) THEN json_array_length(`messages_json`) ELSE 0 END) DESC,
				`updated_at` DESC,
				`rowid` DESC
		) AS `survivor_rn`,
		FIRST_VALUE(`pending_json`) OVER (
			PARTITION BY `flow_id`, `item_id`
			ORDER BY (`pending_json` IS NULL) ASC,
				`updated_at` DESC,
				`rowid` DESC
		) AS `group_pending_json`
	FROM `discuss_state`
)
UPDATE `discuss_state`
SET
	`messages_json` = (SELECT g.`messages_json` FROM `grouped` g WHERE g.`rid` = `discuss_state`.`rowid`),
	`turn_status` = (SELECT g.`turn_status` FROM `grouped` g WHERE g.`rid` = `discuss_state`.`rowid`),
	`active_turn_id` = (SELECT g.`active_turn_id` FROM `grouped` g WHERE g.`rid` = `discuss_state`.`rowid`),
	`components_json` = (SELECT g.`components_json` FROM `grouped` g WHERE g.`rid` = `discuss_state`.`rowid`),
	`pending_json` = (SELECT g.`group_pending_json` FROM `grouped` g WHERE g.`rid` = `discuss_state`.`rowid`)
WHERE `rowid` IN (
	SELECT g.`rid` FROM `grouped` g WHERE g.`survivor_rn` = 1 AND g.`group_size` > 1
);
--> statement-breakpoint
WITH `ranked` AS MATERIALIZED (
	SELECT
		`rowid` AS `rid`,
		ROW_NUMBER() OVER (
			PARTITION BY `flow_id`, `item_id`
			ORDER BY (CASE WHEN json_valid(`messages_json`) THEN json_array_length(`messages_json`) ELSE 0 END) DESC,
				`updated_at` DESC,
				`rowid` DESC
		) AS `rn`
	FROM `discuss_state`
)
DELETE FROM `discuss_state`
WHERE `rowid` IN (SELECT `rid` FROM `ranked` WHERE `rn` > 1);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_state_unique` ON `discuss_state` (`flow_id`,`item_id`);
