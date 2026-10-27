// telemetry-sources.js — harness-side gateway reads behind telemetry.
//
// Core owns the query layer (telemetry/store.js queries, grammar, resolver,
// labels, pricer, HTTP route shapes). Every read of a gateway-owned file
// lives here: trajectory/session walks for backfill, sessionId lookup for
// the CLI, and the lcm.db transcript query for the sessions route. Core
// receives these through an injected `sources` object (this module in
// production, a stub in tests) — core never imports this file, so the
// core→plugins boundary stays one-directional.

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { getSharedDb } from "../../../state/db.js";

// Evaluated per call, not at module load, so tests pass explicit dirs
// without mocking os.homedir.
export function defaultAgentsDir() {
  return path.join(os.homedir(), ".openclaw", "agents");
}

export function defaultLcmDbPath() {
  return path.join(os.homedir(), ".openclaw", "lcm.db");
}

// Every trajectory/session pair under the agents dir. Throws when the dir
// itself is unreadable — the caller (backfill) logs and returns empty stats.
export function listTrajectoryPairs({ agentsDir } = {}) {
  const dir = agentsDir ?? defaultAgentsDir();
  const pairs = [];
  const agents = fs.readdirSync(dir, { withFileTypes: true });
  for (const ent of agents) {
    if (!ent.isDirectory()) continue;
    const sd = path.join(dir, ent.name, "sessions");
    let files;
    try { files = fs.readdirSync(sd); } catch { continue; }
    for (const fn of files) {
      if (!fn.endsWith(".trajectory.jsonl")) continue;
      pairs.push({
        trajPath: path.join(sd, fn),
        sessionFile: path.join(sd, fn.replace(/\.trajectory\.jsonl$/, ".jsonl")),
      });
    }
  }
  return pairs;
}

// First line of a trajectory file, without loading the whole file.
export function readTrajectoryHead(trajPath) {
  const fd = fs.openSync(trajPath, "r");
  try {
    const buf = Buffer.alloc(4096);
    const n = fs.readSync(fd, buf, 0, buf.length, 0);
    const s = buf.slice(0, n).toString("utf8");
    const nl = s.indexOf("\n");
    return nl === -1 ? s : s.slice(0, nl);
  } finally { fs.closeSync(fd); }
}

// Full session jsonl text, or null when the file is absent.
export function readSessionText(sessionFile) {
  try {
    return fs.readFileSync(sessionFile, "utf8");
  } catch {
    return null;
  }
}

// SessionId → sessionKey via the trajectory headline. Best-effort, null
// when no agent dir carries that session.
export async function sessionKeyForSessionId(sessionId, { agentsDir } = {}) {
  const dir = agentsDir ?? defaultAgentsDir();
  let agents;
  try {
    agents = await fs.promises.readdir(dir, { withFileTypes: true });
  } catch { return null; }
  for (const ent of agents) {
    if (!ent.isDirectory()) continue;
    const trajPath = path.join(dir, ent.name, "sessions", `${sessionId}.trajectory.jsonl`);
    try {
      const first = (await fs.promises.readFile(trajPath, "utf8")).split("\n", 1)[0];
      const d = JSON.parse(first);
      if (d?.sessionKey) return d.sessionKey;
    } catch { /* not in this agent's dir */ }
  }
  return null;
}

// Transcript parts for one session out of lcm.db. Cursor params mirror the
// route: before_seq pages backward, after_seq pages forward, neither means
// the newest `limit` rows. Throws code LCM_DB_MISSING when the file is
// absent so the route can answer 503 instead of 500.
export async function querySessionParts({ lcmDbPath, sessionKey, limit = 80, beforeSeq = null, afterSeq = null } = {}) {
  const dbPath = lcmDbPath ?? defaultLcmDbPath();
  if (!sessionKey) throw new Error("telemetry-sources: sessionKey required");
  if (!fs.existsSync(dbPath)) {
    const err = new Error("lcm.db not found");
    err.code = "LCM_DB_MISSING";
    throw err;
  }
  const { client } = await getSharedDb(dbPath, { readonly: true });

  let seqClause, seqArgs, order;
  if (afterSeq != null) {
    seqClause = "AND m.seq > ?";
    seqArgs   = [afterSeq];
    order     = "ASC";
  } else if (beforeSeq != null) {
    seqClause = "AND m.seq < ?";
    seqArgs   = [beforeSeq];
    order     = "DESC";
  } else {
    seqClause = "";
    seqArgs   = [];
    order     = "DESC";
  }

  const rawRows = Array.from((await client.execute({
    sql: `
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
  `,
    args: [sessionKey, ...seqArgs, limit],
  })).rows);

  // DESC queries come back newest-first — re-sort ascending for the client.
  const parts = order === "DESC"
    ? rawRows.slice().sort((a, b) => a.seq - b.seq || a.ordinal - b.ordinal)
    : rawRows;

  const firstSeq = parts.length ? parts[0].seq : null;
  const lastSeq  = parts.length ? parts[parts.length - 1].seq : null;

  let hasMore = false;
  if (firstSeq != null) {
    hasMore = (await client.execute({
      sql: `
      SELECT 1 FROM messages m
      JOIN conversations c ON c.conversation_id = m.conversation_id
      WHERE c.session_key = ? AND m.seq < ? LIMIT 1
    `,
      args: [sessionKey, firstSeq],
    })).rows.length > 0;
  }

  let hasMoreNewer = false;
  if (lastSeq != null) {
    hasMoreNewer = (await client.execute({
      sql: `
      SELECT 1 FROM messages m
      JOIN conversations c ON c.conversation_id = m.conversation_id
      WHERE c.session_key = ? AND m.seq > ? LIMIT 1
    `,
      args: [sessionKey, lastSeq],
    })).rows.length > 0;
  }

  return { parts, hasMore, hasMoreNewer, firstSeq, lastSeq };
}
