import { describe, it } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { initCapStore, getAlertHistory } from "../../../telemetry/budget-alert.js";

// Each test gets its own fresh SQLite file — never touches the real
// ~/.openclaw/ops.sqlite. initCapStore runs the module's snapshot migrate,
// which is what actually creates the budget_alert_events table.
async function freshDb() {
  const dbPath = path.join(os.tmpdir(), `manifold-alert-history-test-${randomUUID()}.sqlite`);
  await initCapStore({ dbPath });
  return dbPath;
}

// checkBudget's own writer (_recordAlertEvent) is private, and checkBudget
// itself reads/writes the real, non-injectable ~/.openclaw/lineage/
// .alerted-thresholds.json debounce file — not safe to exercise from a test
// (it could suppress or duplicate a real operator's alerts on this machine).
// Insert directly against the same physical file instead, isolating
// getAlertHistory's read path from checkBudget's process-wide debounce state.
async function insertEvent(dbPath, { threshold, totalUSD, capUSD, firedAt }) {
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute({
    sql: `INSERT INTO budget_alert_events (id, threshold, total_usd, cap_usd, fired_at) VALUES (?, ?, ?, ?, ?)`,
    args: [randomUUID(), threshold, totalUSD, capUSD, firedAt],
  });
}

describe("budget-alert budget_alert_events / getAlertHistory", () => {
  it("returns an empty array when nothing has fired", async () => {
    await freshDb();
    const events = await getAlertHistory();
    assert.deepEqual(events, []);
  });

  it("returns recorded events newest first", async () => {
    const dbPath = await freshDb();
    await insertEvent(dbPath, { threshold: 0.5, totalUSD: 0.5, capUSD: 1.0, firedAt: 1000 });
    await insertEvent(dbPath, { threshold: 0.8, totalUSD: 0.85, capUSD: 1.0, firedAt: 2000 });
    await insertEvent(dbPath, { threshold: 1.0, totalUSD: 1.1, capUSD: 1.0, firedAt: 3000 });

    const events = await getAlertHistory();
    assert.equal(events.length, 3);
    assert.deepEqual(events.map((e) => e.firedAt), [3000, 2000, 1000], "newest first");
    assert.equal(events[0].threshold, 1.0);
    assert.equal(events[0].totalUSD, 1.1);
    assert.equal(events[0].capUSD, 1.0);
  });

  it("respects the limit option", async () => {
    const dbPath = await freshDb();
    for (let i = 0; i < 5; i++) {
      await insertEvent(dbPath, { threshold: 0.5, totalUSD: 0.5, capUSD: 1.0, firedAt: 1000 + i });
    }
    const events = await getAlertHistory({ limit: 2 });
    assert.equal(events.length, 2);
  });
});
