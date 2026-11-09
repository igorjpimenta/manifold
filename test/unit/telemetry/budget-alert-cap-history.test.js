import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import {
  initCapStore, currentCapUSD, currentLockThreshold,
  setCapAndPersist, setLockThresholdAndPersist, getCapHistory,
} from "../../../telemetry/budget-alert.js";

// Each test gets its own fresh SQLite file so cap_versions history doesn't
// leak between cases — never touches the real ~/.openclaw/ops.sqlite. The
// file is an isolated tmp file, so seeding is deterministic
// (DEFAULT_CAP_USD/DEFAULT_LOCK_THRESHOLD).
async function freshStore() {
  const dbPath = path.join(os.tmpdir(), `manifold-cap-history-test-${randomUUID()}.sqlite`);
  await initCapStore({ dbPath });
  return dbPath;
}

describe("budget-alert cap_versions (SCD2)", () => {
  beforeEach(async () => { await freshStore(); });

  it("first init on a bare table seeds a default active row", async () => {
    assert.equal(currentCapUSD(), 3.0);
    assert.equal(currentLockThreshold(), 0.8);
    const history = await getCapHistory();
    assert.equal(history.length, 1);
    assert.equal(history[0].retiredAt, null);
  });

  it("setCapAndPersist retires the old row and inserts a new active one", async () => {
    await setCapAndPersist(1.0);
    assert.equal(currentCapUSD(), 1.0);

    const history = await getCapHistory();
    assert.equal(history.length, 2);
    assert.notEqual(history[0].retiredAt, null, "first (seed) row is retired");
    assert.equal(history[0].capUSD, 3.0);
    assert.equal(history[1].retiredAt, null, "second row is active");
    assert.equal(history[1].capUSD, 1.0);
    // lockThreshold carries forward unchanged onto the new row
    assert.equal(history[1].lockThreshold, 0.8);
  });

  it("setLockThresholdAndPersist versions independently of capUSD", async () => {
    await setCapAndPersist(2.0);
    await setLockThresholdAndPersist(0.5);
    assert.equal(currentCapUSD(), 2.0);
    assert.equal(currentLockThreshold(), 0.5);

    const history = await getCapHistory();
    assert.equal(history.length, 3);
    const active = history.at(-1);
    assert.equal(active.retiredAt, null);
    assert.equal(active.capUSD, 2.0);
    assert.equal(active.lockThreshold, 0.5);
    // exactly one active row at any time
    assert.equal(history.filter((h) => h.retiredAt === null).length, 1);
  });

  it("setting the same value is a no-op (no new row)", async () => {
    await setCapAndPersist(3.0); // same as the seeded default
    const history = await getCapHistory();
    assert.equal(history.length, 1);
  });

  it("history stays ordered oldest-first", async () => {
    await setCapAndPersist(1.5);
    await setCapAndPersist(2.5);
    await setCapAndPersist(4.0);
    const history = await getCapHistory();
    const values = history.map((h) => h.capUSD);
    assert.deepEqual(values, [3.0, 1.5, 2.5, 4.0]);
    for (let i = 1; i < history.length; i++) {
      assert.ok(history[i].registeredAt >= history[i - 1].registeredAt);
    }
  });

  it("rejects invalid cap/threshold values without writing a row", async () => {
    await setCapAndPersist(-5);
    await setCapAndPersist(NaN);
    await setLockThresholdAndPersist(1.5);
    await setLockThresholdAndPersist(0);
    assert.equal(currentCapUSD(), 3.0);
    assert.equal(currentLockThreshold(), 0.8);
    const history = await getCapHistory();
    assert.equal(history.length, 1);
  });

  it("re-initCapStore against the same path loads the already-active row, not the legacy default", async () => {
    const dbPath = path.join(os.tmpdir(), `manifold-cap-history-test-${randomUUID()}.sqlite`);
    await initCapStore({ dbPath });
    await setCapAndPersist(1.23);

    // Simulate a gateway restart: re-run initCapStore against the same file.
    await initCapStore({ dbPath });
    assert.equal(currentCapUSD(), 1.23);
    const history = await getCapHistory();
    assert.equal(history.length, 2, "restart must not re-seed a duplicate default row");
  });
});
