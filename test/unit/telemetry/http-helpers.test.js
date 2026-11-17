import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";
import { randomUUID } from "node:crypto";
import {
  peersForKind,
  ATTR_JOIN, ATTR_ROOT_KIND, ATTR_ROOT_LABEL,
  formatSessionLabel,
  sessionKindForRoot,
  formatSessionChildLabel,
  bootstrapForecast,
  hasFlowRuns,
} from "../../../telemetry/http-helpers.js";

// ── ATTR_* constants ──────────────────────────────────────────────────────────

describe("ATTR constants", () => {
  it("ATTR_JOIN is a non-empty SQL fragment", () => {
    assert.equal(typeof ATTR_JOIN, "string");
    assert.ok(ATTR_JOIN.includes("session_lineage"));
  });
  it("ATTR_ROOT_KIND uses COALESCE", () => {
    assert.ok(ATTR_ROOT_KIND.startsWith("COALESCE"));
  });
  it("ATTR_ROOT_LABEL uses COALESCE", () => {
    assert.ok(ATTR_ROOT_LABEL.startsWith("COALESCE"));
  });
});

// ── peersForKind ──────────────────────────────────────────────────────────────

describe("peersForKind", () => {
  it("returns [kind] for unknown kind", () => {
    const result = peersForKind("something-unknown-xyz");
    assert.deepEqual(result, ["something-unknown-xyz"]);
  });

  it("cron-owner and cron-tick map to the same display bucket", () => {
    const owner = peersForKind("cron-owner");
    const tick  = peersForKind("cron-tick");
    assert.ok(owner.includes("cron-owner"));
    assert.ok(owner.includes("cron-tick"));
    assert.deepEqual(owner, tick);
  });

  it("explicit maps to its own bucket and includes itself", () => {
    const peers = peersForKind("explicit");
    assert.ok(peers.includes("explicit"));
  });

  it("telegram-dm maps to its own bucket", () => {
    const peers = peersForKind("telegram-dm");
    assert.ok(peers.includes("telegram-dm"));
  });
});

// ── formatSessionLabel ────────────────────────────────────────────────────────

describe("formatSessionLabel", () => {
  it("formats a known timestamp as 'Mon DD HH:MM'", () => {
    const d = new Date(2025, 5, 15, 9, 5); // June 15, 09:05
    const label = formatSessionLabel("any-key", d.getTime());
    assert.ok(label.includes("Jun"));
    assert.ok(label.includes("15"));
    assert.ok(label.includes("09:05"));
  });

  it("falls back to key tail when no timestamp", () => {
    const label = formatSessionLabel("agent:main:cron:my-job", null);
    assert.equal(label, "my-job");
  });

  it("truncates long tails to 24 chars", () => {
    const label = formatSessionLabel("x:".repeat(20) + "a".repeat(40), null);
    assert.ok(label.length <= 24);
  });

  it("returns empty string for null key and no timestamp", () => {
    const label = formatSessionLabel(null, null);
    assert.equal(typeof label, "string");
  });
});

// ── sessionKindForRoot ────────────────────────────────────────────────────────

describe("sessionKindForRoot", () => {
  it("legacy operator prefix falls through to the generic operator-trigger", () => {
    assert.equal(sessionKindForRoot("explicit", "agent:operator:ios:abc"), "operator-trigger");
  });

  it("cli prefix → cli-trigger", () => {
    assert.equal(sessionKindForRoot("explicit", "agent:operator:cli:abc"), "cli-trigger");
  });

  it("operator prefix (non-cli) → operator-trigger", () => {
    assert.equal(sessionKindForRoot("explicit", "agent:operator:dashboard:abc"), "operator-trigger");
  });

  it("cron-owner with :run: in key → cron-tick", () => {
    assert.equal(sessionKindForRoot("cron-owner", "agent:main:cron:job:run:sess"), "cron-tick");
  });

  it("cron-owner without :run: → cron-job-session", () => {
    assert.equal(sessionKindForRoot("cron-owner", "agent:main:cron:job"), "cron-job-session");
  });

  it("explicit → operator-run", () => {
    assert.equal(sessionKindForRoot("explicit", "agent:main:explicit:abc"), "operator-run");
  });

  it("telegram-dm → conversation", () => {
    assert.equal(sessionKindForRoot("telegram-dm", "agent:main:telegram:abc"), "conversation");
  });

  it("unknown kind → session", () => {
    assert.equal(sessionKindForRoot("some-unknown-kind", "agent:x:y:z"), "session");
  });
});

// ── formatSessionChildLabel ───────────────────────────────────────────────────

describe("formatSessionChildLabel", () => {
  it("flow: prefix → 'flow <short-id>'", () => {
    const label = formatSessionChildLabel({ child_key: "flow:abcd1234efgh5678", kind: "flow-triggered", flow_id: "abcd1234efgh5678" });
    assert.ok(label.startsWith("flow "));
    assert.ok(label.includes("abcd1234"));
  });

  it("cron-tick kind shows run id tail", () => {
    const label = formatSessionChildLabel({ child_key: "agent:main:cron:job:run:runsess1", kind: "cron-tick", flow_id: null });
    assert.ok(label.includes("run "));
  });

  it("flow-node kind → 'agent · tail'", () => {
    const label = formatSessionChildLabel({ child_key: "agent:worker:subagent:some-node-key", kind: "flow-node", flow_id: "f1" });
    assert.ok(label.includes("worker"));
    assert.ok(label.includes("·"));
  });

  it("fallback → 'agent · tail'", () => {
    const label = formatSessionChildLabel({ child_key: "agent:argus:explicit:session-id", kind: "session", flow_id: null });
    assert.ok(label.includes("argus"));
    assert.ok(label.includes("·"));
  });
});

// ── bootstrapForecast ─────────────────────────────────────────────────────────

describe("bootstrapForecast", () => {
  it("returns null when no historical days", () => {
    const result = bootstrapForecast(new Map(), 10, [{ cost: 0.5 }]);
    assert.equal(result, null);
  });

  it("returns null when currentHour is 23", () => {
    const byDay = new Map([["2025-06-01", new Map([[10, 0.1]])]]);
    const result = bootstrapForecast(byDay, 23, [{ cost: 0.5 }]);
    assert.equal(result, null);
  });

  it("returns one entry per remaining hour", () => {
    const byDay = new Map([
      ["2025-06-01", new Map([[11, 0.1], [12, 0.2], [13, 0.05]])],
      ["2025-06-02", new Map([[11, 0.15], [12, 0.18], [13, 0.08]])],
    ]);
    const result = bootstrapForecast(byDay, 10, [{ cost: 0.3 }], 50);
    assert.ok(Array.isArray(result));
    assert.equal(result.length, 13);  // hours 11–23
    assert.equal(result[0].hour, 11);
    assert.equal(result[result.length - 1].hour, 23);
  });

  it("p10 <= p50 <= p90 for each entry", () => {
    const byDay = new Map([
      ["2025-06-01", new Map([[15, 0.2], [16, 0.3]])],
      ["2025-06-02", new Map([[15, 0.1], [16, 0.4]])],
      ["2025-06-03", new Map([[15, 0.3], [16, 0.2]])],
    ]);
    const result = bootstrapForecast(byDay, 14, [{ cost: 1.0 }], 100);
    for (const r of result) {
      assert.ok(r.p10 <= r.p50, `p10 <= p50 for hour ${r.hour}`);
      assert.ok(r.p50 <= r.p90, `p50 <= p90 for hour ${r.hour}`);
    }
  });
});

// ── hasFlowRuns ──────────────────────────────────────────────────────────

describe("hasFlowRuns", () => {
  it("returns false when the db has no flow_runs table", async () => {
    const dbPath = join(tmpdir(), `http-helpers-test-${randomUUID()}.sqlite`);
    const client = createClient({ url: `file:${dbPath}` });
    await client.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY)");
    assert.equal(await hasFlowRuns(client), false);
    await client.execute("CREATE TABLE flow_runs (flow_id TEXT PRIMARY KEY)");
    assert.equal(await hasFlowRuns(client), true);
    client.close();
  });

  it("returns false when client.execute throws", async () => {
    const fakeClient = { execute: async () => { throw new Error("no db"); } };
    assert.equal(await hasFlowRuns(fakeClient), false);
  });
});
