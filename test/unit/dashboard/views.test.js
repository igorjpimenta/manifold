
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { computeViews } from "../../../dashboard/views.js";

const DAY = 86_400_000;

let home;
let dir;

function doc(name, fields) {
  const body = Object.entries(fields).map(([k, v]) => `${k}: ${v}`).join("\n");
  writeFileSync(path.join(dir, `${name}.md`), `---\n${body}\n---\nbody text\n`, "utf8");
}

before(() => {
  home = mkdtempSync(path.join(os.tmpdir(), "views-test-"));
  dir = path.join(home, "sent");
  mkdirSync(dir, { recursive: true });
  const now = new Date().toISOString();
  const old = new Date(Date.now() - 40 * DAY).toISOString();
  doc("a", { "sent-at": now, outcome: "won", amount: "100" });
  doc("b", { "sent-at": now, outcome: "replied", amount: "50" });
  doc("c", { "sent-at": now, outcome: "qualified", amount: "10" });
  doc("d", { "sent-at": now, outcome: "ignored", amount: "x" });
  doc("old", { "sent-at": old, outcome: "won", amount: "999" });
  writeFileSync(path.join(dir, "notes.txt"), "not markdown", "utf8");
  mkdirSync(path.join(dir, "nested.md"));
});

after(() => {
  rmSync(home, { recursive: true, force: true });
});

const source = () => ({ kind: "frontmatter-dir", path: dir });

describe("computeViews — dispatch", () => {
  test("no views (or not an array) yields an empty list", async () => {
    assert.deepEqual(await computeViews({}), []);
    assert.deepEqual(await computeViews({ views: [] }), []);
    assert.deepEqual(await computeViews({ views: "nope" }), []);
  });

  test("an unknown kind returns the base fields plus an error, not a throw", async () => {
    const [view] = await computeViews({ views: [{ id: "v", kind: "nope", label: "L" }] });
    assert.equal(view.id, "v");
    assert.equal(view.label, "L");
    assert.match(view.error, /unknown view kind: nope/);
  });

  test("one failing view does not take down its siblings", async () => {
    const out = await computeViews({
      views: [
        { id: "bad", kind: "nope" },
        { id: "good", kind: "metric", source: source(), compute: "count" },
      ],
    });
    assert.ok(out[0].error);
    assert.equal(out[1].value, 5);
  });
});

describe("computeViews — funnel", () => {
  const view = () => ({
    id: "outcomes", kind: "funnel", label: "outcomes (last 30d)", source: source(),
    stages: [
      { id: "sent", label: "sent" },
      { id: "engaged", label: "engaged", outcomeIn: ["replied", "qualified", "won"] },
      { id: "won", label: "won", outcomeEq: "won" },
    ],
  });

  test("counts each stage against the windowed cohort and computes conversions", async () => {
    const [out] = await computeViews({ views: [view()] }, { sinceMs: Date.now() - 30 * DAY });
    assert.equal(out.total, 4, "the 40-day-old record is outside the 30-day window");
    assert.deepEqual(out.funnel.map((s) => [s.id, s.count]), [["sent", 4], ["engaged", 3], ["won", 1]]);
    assert.equal(out.funnel[0].conversionFromPrev, null);
    assert.equal(out.funnel[1].conversionFromPrev, 3 / 4);
    assert.equal(out.funnel[2].conversionFromPrev, 1 / 3);
    assert.equal(out.funnel[2].conversionFromStart, 1 / 4);
  });

  test("byOutcome reports the raw outcome distribution of the cohort", async () => {
    const [out] = await computeViews({ views: [view()] }, { sinceMs: Date.now() - 30 * DAY });
    assert.deepEqual(out.byOutcome, { won: 1, replied: 1, qualified: 1, ignored: 1 });
  });

  test("sinceDays on the view overrides the caller's window", async () => {
    const [out] = await computeViews({ views: [{ ...view(), sinceDays: 60 }] });
    assert.equal(out.total, 5, "the 40-day-old record is inside a 60-day window");
    assert.equal(out.funnel[2].count, 2);
  });

  test("a stage whose predecessor is empty reports a null conversion, not NaN", async () => {
    const [out] = await computeViews({
      views: [{
        id: "v", kind: "funnel", source: source(),
        stages: [{ id: "none", outcomeEq: "does-not-exist" }, { id: "won", outcomeEq: "won" }],
      }],
    });
    assert.equal(out.funnel[0].count, 0);
    assert.equal(out.funnel[1].conversionFromPrev, null);
  });

  test("a single-stage funnel omits conversionFromStart", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "funnel", source: source(), stages: [{ id: "all" }] }] });
    assert.equal(out.funnel.length, 1);
    assert.equal("conversionFromStart" in out.funnel[0], false);
  });

  test("a missing source directory yields an empty funnel", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "funnel", source: { kind: "frontmatter-dir", path: path.join(home, "nope") }, stages: [{ id: "all" }] }] });
    assert.equal(out.total, 0);
    assert.deepEqual(out.funnel, []);
  });
});

describe("computeViews — metric", () => {
  test("count is the cohort size", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "count" }] });
    assert.equal(out.value, 5);
  });

  test("sum and avg ignore non-numeric values", async () => {
    const [sum] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "sum:amount" }] });
    assert.equal(sum.value, 1159);
    const [avg] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "avg:amount" }] });
    assert.equal(avg.value, 1159 / 4);
  });

  test("a metric with no numeric values reports null", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "sum:outcome" }] });
    assert.equal(out.value, null);
  });

  test("an unknown op or a malformed compute reports an error", async () => {
    const [badOp] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "max:amount" }] });
    assert.match(badOp.error, /unknown metric op "max"/);
    const [malformed] = await computeViews({ views: [{ id: "v", kind: "metric", source: source(), compute: "sum" }] });
    assert.match(malformed.error, /must be "count" or "op:field"/);
  });

  test("a missing source yields value null", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "metric", compute: "count" }] });
    assert.equal(out.value, null);
  });
});

describe("computeViews — table", () => {
  test("defaults to the non-internal frontmatter keys and sorts by timeField desc", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "table", source: source() }] });
    assert.equal(out.total, 5);
    assert.ok(out.columns.includes("outcome"));
    assert.equal(out.columns.some((c) => c.startsWith("__")), false);
    assert.ok(out.rows.length <= 50);
  });

  test("honours explicit columns, limit and sortBy", async () => {
    const [out] = await computeViews({
      views: [{
        id: "v", kind: "table", source: source(),
        columns: ["outcome", "amount"], limit: 2, sortBy: "amount",
      }],
    });
    assert.deepEqual(out.columns, ["outcome", "amount"]);
    assert.equal(out.rows.length, 2);
    const amounts = out.rows.map((r) => Number(r.amount));
    assert.deepEqual(amounts, [...amounts].sort((a, b) => b - a));
  });

  test("a missing column value is reported as null rather than dropped", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "table", source: source(), columns: ["outcome", "nope"] }] });
    assert.ok(out.rows.every((r) => r.nope === null));
  });

  test("a missing source yields an empty table with the declared columns", async () => {
    const [out] = await computeViews({ views: [{ id: "v", kind: "table", columns: ["a"], source: { kind: "frontmatter-dir", path: path.join(home, "nope") } }] });
    assert.deepEqual(out.rows, []);
    assert.equal(out.total, 0);
  });
});

describe("computeViews — frontmatter parsing", () => {
  test("quoted values are unquoted; files without frontmatter are skipped", async () => {
    writeFileSync(path.join(dir, "quoted.md"), '---\nsent-at: "2026-01-01T00:00:00Z"\noutcome: \'won\'\n---\n', "utf8");
    writeFileSync(path.join(dir, "bare.md"), "no frontmatter here", "utf8");
    const [out] = await computeViews({ views: [{ id: "v", kind: "table", source: source(), columns: ["outcome"], sortBy: "sent-at" }] });
    assert.equal(out.total, 6, "the bare file contributes nothing");
    const quoted = out.rows.find((r) => r.outcome === "won");
    assert.ok(quoted, "quoted outcome parsed to the bare value");
  });

  test("a custom timeField gates the window", async () => {
    const customDir = path.join(home, "custom");
    mkdirSync(customDir, { recursive: true });
    writeFileSync(path.join(customDir, "x.md"), "---\ncreated: 2020-01-01T00:00:00Z\n---\n", "utf8");
    const [out] = await computeViews({
      views: [{ id: "v", kind: "metric", source: { kind: "frontmatter-dir", path: customDir, timeField: "created" }, compute: "count" }],
    }, { sinceMs: Date.now() - DAY });
    assert.equal(out.value, 0, "a 2020 record is outside a 1-day window");
  });
});
