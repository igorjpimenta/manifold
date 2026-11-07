-- Collapse pending_proposals into discuss_state.pending_json, and make
-- discuss_state one row per (flow_id, item_id).
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
INSERT INTO `discuss_state` (
	`flow_id`,
	`item_id`,
	`messages_json`,
	`turn_status`,
	`active_turn_id`,
	`pending_json`,
	`updated_at`,
	`components_json`
)
SELECT
	p.`flow_id`,
	p.`item_id`,
	'[]',
	'idle',
	NULL,
	json_object(
		'text', p.`text`,
		'fields', json(CASE WHEN p.`fields_json` IS NOT NULL AND json_valid(p.`fields_json`) THEN p.`fields_json` END),
		'draftId', p.`draft_id`,
		'updatedAt', p.`updated_at`
	),
	p.`updated_at`,
	'[]'
FROM `pending_proposals` p
WHERE NOT EXISTS (
	SELECT 1 FROM `discuss_state` d
	WHERE d.`flow_id` = p.`flow_id` AND d.`item_id` = p.`item_id`
);
--> statement-breakpoint
WITH `folded` AS MATERIALIZED (
	SELECT
		d.`rowid` AS `d_rowid`,
		p.`text` AS `p_text`,
		p.`draft_id` AS `p_draft_id`,
		p.`updated_at` AS `p_updated_at`,
		d.`pending_json` AS `existing_pending`,
		COALESCE(p.`text`, json_extract(d.`pending_json`, '$.text')) AS `merged_text`,
		CASE
			WHEN json_valid(COALESCE(p.`fields_json`, json_extract(d.`pending_json`, '$.fields')))
			THEN COALESCE(p.`fields_json`, json_extract(d.`pending_json`, '$.fields'))
		END AS `merged_fields_json`,
		COALESCE(p.`draft_id`, json_extract(d.`pending_json`, '$.draftId')) AS `merged_draft_id`,
		MAX(p.`updated_at`, COALESCE(CAST(json_extract(d.`pending_json`, '$.updatedAt') AS INTEGER), 0)) AS `merged_updated_at`,
		COALESCE(CAST(json_extract(d.`pending_json`, '$.updatedAt') AS INTEGER), 0) AS `existing_updated_at`
	FROM `pending_proposals` p
	JOIN `discuss_state` d
		ON d.`flow_id` = p.`flow_id` AND d.`item_id` = p.`item_id`
)
UPDATE `discuss_state`
SET `pending_json` = (
	SELECT CASE
		WHEN f.`existing_updated_at` > f.`p_updated_at` THEN f.`existing_pending`
		ELSE json_object(
			'text', f.`merged_text`,
			'fields', json(f.`merged_fields_json`),
			'draftId', f.`merged_draft_id`,
			'updatedAt', f.`merged_updated_at`
		)
	END
	FROM `folded` f
	WHERE f.`d_rowid` = `discuss_state`.`rowid`
)
WHERE `rowid` IN (SELECT f.`d_rowid` FROM `folded` f);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `idx_discuss_state_unique` ON `discuss_state` (`flow_id`,`item_id`);
--> statement-breakpoint
DROP TABLE IF EXISTS `pending_proposals`;
