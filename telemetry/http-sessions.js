// /pipeclaw/telemetry/sessions/parts — session transcript from lcm.db.
import os from "node:os";
import path from "node:path";
import { openDb } from "../state/db.js";

const LCM_DB_PATH = path.join(os.homedir(), ".openclaw", "lcm.db");

// Opened once and shared across all requests. readOnly handles are safe for
// concurrent reads. Lazy so a missing lcm.db at startup doesn't crash the route.
let _lcmDb = null;
function sharedLcmDb() {
  if (!_lcmDb) {
    _lcmDb = openDb(LCM_DB_PATH, { readonly: true }).sqlite;
  }
  return _lcmDb;
}

export function registerSessionRoutes({ api, logger, json, bad }) {
  api.registerHttpRoute({
    method: "GET",
    path: "/pipeclaw/telemetry/sessions/parts",
    auth: "plugin",
    handler: async (req, res) => {
      const u = new URL(req.url, "http://localhost");
      const sessionKey = u.searchParams.get("session_key");
      const limit = Math.min(parseInt(u.searchParams.get("limit") ?? "80", 10), 2000);
      if (!sessionKey) return bad(res, "session_key required");
      let lcm;
      try {
        lcm = sharedLcmDb();
      } catch (err) {
        logger?.warn?.(`lineage[sessions/parts] lcm.db open failed: ${err?.message}`);
        _lcmDb = null;  // reset so next request retries the open
        return json(res, { error: "lcm.db not available" }, 503);
      }
      try {
        // Cursor params:
        //   before_seq — newest `limit` rows with seq < before_seq (backward)
        //   after_seq  — oldest `limit` rows with seq > after_seq  (forward)
        // Default (no cursor): newest `limit` rows in the session.
        const beforeSeq = u.searchParams.has("before_seq")
          ? parseInt(u.searchParams.get("before_seq"), 10)
          : null;
        const afterSeq = u.searchParams.has("after_seq")
          ? parseInt(u.searchParams.get("after_seq"), 10)
          : null;

        let seqClause, seqArgs, order;
        if (afterSeq != null) {
          seqClause = "AND m.seq > ?";
          seqArgs   = [afterSeq];
          order     = "ASC";
        } else if (beforeSeq != null) {
          seqClause = "AND m.seq < ?";
          seqArgs   = [beforeSeq];
          order     = "DESC";  // newest-first so LIMIT grabs the right page; re-sort after
        } else {
          seqClause = "";
          seqArgs   = [];
          order     = "DESC";
        }

        const rawRows = lcm.prepare(`
          SELECT mp.part_id, mp.part_type, mp.ordinal,
                 m.seq, m.role,
                 datetime(m.created_at, 'localtime') AS msg_ts,
                 mp.text_content, mp.is_ignored, mp.is_synthetic,
                 mp.tool_call_id, mp.tool_name, mp.tool_status,
                 mp.tool_input, mp.tool_output, mp.tool_error, mp.tool_title,
                 mp.patch_files,
                 mp.step_cost, mp.step_tokens_in, mp.step_tokens_out,
                 mp.compaction_auto, mp.metadata
          FROM message_parts mp
          JOIN messages m ON m.message_id = mp.message_id
          JOIN conversations c ON c.conversation_id = m.conversation_id
          WHERE c.session_key = ?
            ${seqClause}
            AND (mp.is_ignored IS NULL OR mp.is_ignored = 0)
            AND mp.part_type IN ('text','reasoning','tool','patch','compaction')
          ORDER BY m.seq ${order}, mp.ordinal ${order}
          LIMIT ?
        `).all(sessionKey, ...seqArgs, limit);

        // DESC queries come back newest-first — re-sort ascending for the client.
        const rows = order === "DESC"
          ? rawRows.slice().sort((a, b) => a.seq - b.seq || a.ordinal - b.ordinal)
          : rawRows;

        const firstSeq = rows.length ? rows[0].seq : null;
        const lastSeq  = rows.length ? rows[rows.length - 1].seq : null;

        let hasMore = false;
        if (firstSeq != null) {
          hasMore = lcm.prepare(`
            SELECT 1 FROM messages m
            JOIN conversations c ON c.conversation_id = m.conversation_id
            WHERE c.session_key = ? AND m.seq < ? LIMIT 1
          `).get(sessionKey, firstSeq) != null;
        }

        let hasMoreNewer = false;
        if (lastSeq != null) {
          hasMoreNewer = lcm.prepare(`
            SELECT 1 FROM messages m
            JOIN conversations c ON c.conversation_id = m.conversation_id
            WHERE c.session_key = ? AND m.seq > ? LIMIT 1
          `).get(sessionKey, lastSeq) != null;
        }

        return json(res, { sessionKey, parts: rows, hasMore, hasMoreNewer, firstSeq, lastSeq });
      } catch (err) {
        logger?.warn?.(`lineage[sessions/parts]: ${err?.message ?? err}`);
        return json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });
}
