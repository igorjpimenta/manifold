// Adapter gateway reads against real tmp files — no gateway, no mocks.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import {
  listTrajectoryPairs,
  readTrajectoryHead,
  readSessionText,
  sessionKeyForSessionId,
  querySessionParts,
} from "../../../plugins/openclaw/manifold/telemetry-sources.js";

let root;
before(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "gf-tsrc-"));
  const sessions = path.join(root, "agents", "cyrano", "sessions");
  fs.mkdirSync(sessions, { recursive: true });
  const key = "agent:cyrano:explicit:demo";
  fs.writeFileSync(
    path.join(sessions, "sid-1.trajectory.jsonl"),
    `${JSON.stringify({ sessionKey: key, sessionId: "sid-1", ts: new Date().toISOString() })}\n`,
  );
  fs.writeFileSync(
    path.join(sessions, "sid-1.jsonl"),
    `${JSON.stringify({ type: "assistant", id: "m1" })}\n`,
  );
});
after(() => fs.rmSync(root, { recursive: true, force: true }));

const agentsDir = () => path.join(root, "agents");

describe("trajectory helpers", () => {
  it("lists trajectory/session pairs", () => {
    const pairs = listTrajectoryPairs({ agentsDir: agentsDir() });
    assert.equal(pairs.length, 1);
    assert.ok(pairs[0].trajPath.endsWith(".trajectory.jsonl"));
    assert.ok(pairs[0].sessionFile.endsWith("sid-1.jsonl"));
  });

  it("reads the trajectory headline", () => {
    const [{ trajPath }] = listTrajectoryPairs({ agentsDir: agentsDir() });
    const d = JSON.parse(readTrajectoryHead(trajPath));
    assert.equal(d.sessionKey, "agent:cyrano:explicit:demo");
  });

  it("reads session text, null when missing", () => {
    const [{ sessionFile }] = listTrajectoryPairs({ agentsDir: agentsDir() });
    assert.ok(readSessionText(sessionFile).includes('"m1"'));
    assert.equal(readSessionText(path.join(root, "missing.jsonl")), null);
  });

  it("resolves a sessionId to its sessionKey", async () => {
    assert.equal(
      await sessionKeyForSessionId("sid-1", { agentsDir: agentsDir() }),
      "agent:cyrano:explicit:demo",
    );
    assert.equal(await sessionKeyForSessionId("nope", { agentsDir: agentsDir() }), null);
  });
});

describe("querySessionParts", () => {
  let lcmDb;
  before(async () => {
    lcmDb = path.join(root, "lcm.db");
    const client = createClient({ url: `file:${lcmDb}` });
    await client.execute(`CREATE TABLE conversations (conversation_id TEXT PRIMARY KEY, session_key TEXT)`);
    await client.execute(`CREATE TABLE messages (message_id TEXT PRIMARY KEY, conversation_id TEXT, seq INTEGER, role TEXT, created_at TEXT)`);
    await client.execute(`CREATE TABLE message_parts (part_id TEXT PRIMARY KEY, message_id TEXT, part_type TEXT, ordinal INTEGER, text_content TEXT, is_ignored INTEGER, is_synthetic INTEGER, tool_call_id TEXT, tool_name TEXT, tool_status TEXT, tool_input TEXT, tool_output TEXT, tool_error TEXT, tool_title TEXT, patch_files TEXT, step_cost REAL, step_tokens_in INTEGER, step_tokens_out INTEGER, compaction_auto INTEGER, metadata TEXT)`);
    await client.execute({ sql: `INSERT INTO conversations VALUES (?, ?)`, args: ["c1", "agent:cyrano:explicit:demo"] });
    for (let i = 1; i <= 3; i++) {
      await client.execute({ sql: `INSERT INTO messages VALUES (?, ?, ?, ?, ?)`, args: [`m${i}`, "c1", i, i % 2 ? "user" : "assistant", "2026-01-01 00:00:00"] });
      await client.execute({ sql: `INSERT INTO message_parts (part_id, message_id, part_type, ordinal, text_content) VALUES (?, ?, 'text', 0, ?)`, args: [`p${i}`, `m${i}`, `hello ${i}`] });
    }
    client.close();
  });

  it("returns parts newest-first re-sorted ascending", async () => {
    const r = await querySessionParts({ lcmDbPath: lcmDb, sessionKey: "agent:cyrano:explicit:demo" });
    assert.equal(r.parts.length, 3);
    assert.deepEqual(r.parts.map((p) => p.seq), [1, 2, 3]);
    assert.equal(r.firstSeq, 1);
    assert.equal(r.lastSeq, 3);
  });

  it("pages with before_seq / after_seq", async () => {
    const back = await querySessionParts({ lcmDbPath: lcmDb, sessionKey: "agent:cyrano:explicit:demo", beforeSeq: 3, limit: 1 });
    assert.deepEqual(back.parts.map((p) => p.seq), [2]);
    assert.equal(back.hasMore, true);
    const fwd = await querySessionParts({ lcmDbPath: lcmDb, sessionKey: "agent:cyrano:explicit:demo", afterSeq: 1, limit: 1 });
    assert.deepEqual(fwd.parts.map((p) => p.seq), [2]);
    assert.equal(fwd.hasMoreNewer, true);
  });

  it("throws LCM_DB_MISSING for an absent file", async () => {
    await assert.rejects(
      querySessionParts({ lcmDbPath: path.join(root, "absent.db"), sessionKey: "k" }),
      (err) => err.code === "LCM_DB_MISSING",
    );
  });
});
