// open-flows.test.js — open-flows sync over the event queue: harness
// tagging, per-subscriber filtering, the connect-time open-flows snapshot,
// and the flow-change producer. Pure unit tests with stub stores.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  entryHarness,
  visibleTo,
  filterEntries,
  collectOpenFlows,
  presenceEvent,
  attachPresenceProducer,
} from "../../../events/open-flows.js";

const tagged = (harness) => ({ event: "agent.dispatched", session: { harness } });
const untagged = () => ({ event: "flow.changed", session: { harness: null } });

// ── entryHarness ─────────────────────────────────────────────────────────────

test("entryHarness reads session.harness, null when absent or blank", () => {
  assert.equal(entryHarness(tagged("h")), "h");
  assert.equal(entryHarness(untagged()), null);
  assert.equal(entryHarness({ event: "x" }), null);
  assert.equal(entryHarness({ session: { harness: "  " } }), null);
  assert.equal(entryHarness(null), null);
});

// ── visibleTo / filterEntries ────────────────────────────────────────────────

test("unscoped subscriber sees everything (legacy verbatim feed)", () => {
  assert.equal(visibleTo(tagged("h"), null), true);
  assert.equal(visibleTo(untagged(), null), true);
  assert.deepEqual(filterEntries([tagged("h"), untagged()], null).length, 2);
});

test("harness-scoped subscriber sees own + untagged, never another harness", () => {
  assert.equal(visibleTo(tagged("h"), "h"), true);
  assert.equal(visibleTo(untagged(), "h"), true);
  assert.equal(visibleTo(tagged("other"), "h"), false);
  assert.deepEqual(
    filterEntries([tagged("h"), tagged("other"), untagged()], "h").map((e) => e.session.harness),
    ["h", null],
  );
});

// ── collectOpenFlows ─────────────────────────────────────────────────────────

function stubStore(rows, { active = false } = {}) {
  return active
    ? { async listActive() { return rows; } }
    : { async list() { return rows; } };
}

const openRow = { flowId: "F1", pipelineId: "p", status: "running", lastStep: "n1", waitJson: null };
const blockedRow = { flowId: "F2", pipelineId: "p", status: "blocked", lastStep: "n2", waitJson: { nodeId: "gate" } };
const doneRow = { flowId: "F3", pipelineId: "p", status: "succeeded", lastStep: "n3", waitJson: null };

test("collectOpenFlows returns non-terminal rows labelled with the subscriber harness", async () => {
  const flows = await collectOpenFlows({ runStore: stubStore([openRow, blockedRow, doneRow]), harness: "h" });
  assert.deepEqual(flows, [
    { id: "F1", pipelineId: "p", nodeId: "n1", status: "running", harness: "h" },
    { id: "F2", pipelineId: "p", nodeId: "gate", status: "blocked", harness: "h" },
  ]);
});

test("collectOpenFlows prefers listActive when the store has it", async () => {
  const seen = [];
  const store = {
    async listActive() { seen.push("active"); return [openRow]; },
    async list() { seen.push("list"); return [openRow]; },
  };
  const flows = await collectOpenFlows({ runStore: store, harness: null });
  assert.deepEqual(seen, ["active"]);
  assert.equal(flows.length, 1);
});

test("collectOpenFlows tolerates missing stores and failures with []", async () => {
  assert.deepEqual(await collectOpenFlows({}), []);
  assert.deepEqual(await collectOpenFlows({ runStore: { async list() { throw new Error("db down"); } } }), []);
});

// ── presenceEvent ────────────────────────────────────────────────────────────

test("presenceEvent projects a run row into a queue entry", () => {
  const entry = presenceEvent(blockedRow, "h");
  assert.equal(entry.event, "flow.changed");
  assert.deepEqual(entry.flow, { id: "F2", pipelineId: "p", nodeId: "gate", status: "blocked" });
  assert.deepEqual(entry.session, { harness: "h" });
});

test("presenceEvent returns null for a missing row", () => {
  assert.equal(presenceEvent(null), null);
});

// ── attachPresenceProducer ───────────────────────────────────────────────────

test("attachPresenceProducer appends one entry per flow change and unsubscribes", async () => {
  const appended = [];
  const stream = { append: (e) => appended.push(e) };
  const runStore = { async get(id) { return { flowId: id, pipelineId: "p", status: "running", lastStep: "n", waitJson: null }; } };
  let listener = null;
  const unsub = attachPresenceProducer({
    stream,
    runStore,
    harness: null,
    subscribe: (fn) => { listener = fn; return () => { listener = null; }; },
  });
  listener("F9");
  await new Promise((r) => setImmediate(r));
  assert.equal(appended.length, 1);
  assert.equal(appended[0].flow.id, "F9");
  unsub();
  assert.equal(listener, null);
});

test("attachPresenceProducer never throws into the producer on lookup failure", async () => {
  const appended = [];
  const stream = { append: (e) => appended.push(e) };
  const runStore = { async get() { throw new Error("db down"); } };
  let listener = null;
  attachPresenceProducer({
    stream,
    runStore,
    subscribe: (fn) => { listener = fn; return () => {}; },
    logger: { warn: () => {} },
  });
  listener("F9");
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(appended, []);
});

test("attachPresenceProducer requires stream, runStore, and subscribe", () => {
  const noop = () => () => {};
  assert.throws(() => attachPresenceProducer({ runStore: {}, subscribe: noop }), /requires a stream/);
  assert.throws(() => attachPresenceProducer({ stream: { append() {} }, subscribe: noop }), /requires a runStore/);
  assert.throws(() => attachPresenceProducer({ stream: { append() {} }, runStore: { async get() {} } }), /requires a subscribe/);
});
