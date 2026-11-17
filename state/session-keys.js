// The sessions registry is the FK target for every session-key pointer.
// Keys are assigned by the harness, so a row has to exist before the first
// pointer to it is written; writers insert OR IGNORE the key they are about
// to reference (telemetry/store.js, state/session-index.js,
// state/tool-handoff.js, budget/index.js). This module backfills the keys
// already referenced by rows that predate the registry — it is the only
// place that reads across the child tables, and it runs once per file open
// after migrations (and after the ops→runtime copy, which brings its own
// queue/lineage/cost keys).

export const SESSION_KEYS_FROM_ROWS_SQL = `
  INSERT OR IGNORE INTO sessions (session_key, first_seen_at)
  SELECT k, ? FROM (
    SELECT child_session_key AS k FROM session_lineage
    UNION SELECT parent_session_key FROM session_lineage WHERE parent_session_key IS NOT NULL
    UNION SELECT root_session_key FROM session_lineage
    UNION SELECT session_key FROM flow_sessions
    UNION SELECT session_key FROM discuss_threads
    UNION SELECT session_key FROM tool_results
    UNION SELECT enqueuer_session_key FROM queue_items WHERE enqueuer_session_key IS NOT NULL
    UNION SELECT session_key FROM cost_by_call WHERE session_key IS NOT NULL
    UNION SELECT root_session_key FROM cost_by_call WHERE root_session_key IS NOT NULL
  )
`;

export async function backfillSessionKeys(client, now = Date.now()) {
  await client.execute({ sql: SESSION_KEYS_FROM_ROWS_SQL, args: [now] });
}

export const ENSURE_SESSION_SQL = "INSERT OR IGNORE INTO sessions (session_key, first_seen_at) VALUES (?, ?)";

// Writer-side guard: the rows an insert is about to reference must exist
// before the insert, or the FK turns a best-effort write into a dropped
// one. Batch, so N keys cost one round-trip.
export async function ensureSessionKeys(client, keys, now = Date.now()) {
  const unique = [...new Set(keys.filter((k) => typeof k === "string" && k.length > 0))];
  if (unique.length === 0) return;
  await client.batch(unique.map((k) => ({ sql: ENSURE_SESSION_SQL, args: [k, now] })), "write");
}
