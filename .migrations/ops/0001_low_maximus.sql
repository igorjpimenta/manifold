-- The moved tables (queue_items, session_lineage, cost_by_call) are not
-- dropped here: SQL cannot verify that runtime.sqlite already holds every
-- row, and a DROP that runs first is unrecoverable. state/drop-moved-ops.js
-- performs the drop under a canonical-name and row-count guard.
SELECT 1;
