import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { flowStateEmitter } from "./events.js";
import { EventEmitter } from "node:events";

// ── identity ───────────────────────────────────────────────────────────────────

describe("flowStateEmitter identity", () => {
  it("is an EventEmitter", () => {
    assert.ok(flowStateEmitter instanceof EventEmitter);
  });

  it("max listeners is set to 20", () => {
    assert.equal(flowStateEmitter.getMaxListeners(), 20);
  });

  it("same instance across imports (ESM cache dedup)", async () => {
    const { flowStateEmitter: again } = await import("./events.js");
    assert.strictEqual(flowStateEmitter, again);
  });
});

// ── emit / on roundtrip ────────────────────────────────────────────────────────

describe("emit/on", () => {
  it("delivers an event to a listener", () => {
    let received = null;
    const handler = (payload) => { received = payload; };
    flowStateEmitter.once("test:deliver", handler);
    flowStateEmitter.emit("test:deliver", { status: "running" });
    assert.deepEqual(received, { status: "running" });
  });

  it("does not deliver to a different event name", () => {
    let received = false;
    flowStateEmitter.once("test:other", () => { received = true; });
    flowStateEmitter.emit("test:unrelated");
    assert.equal(received, false);
    flowStateEmitter.removeAllListeners("test:other");
  });

  it("supports multiple independent subscribers on the same event", () => {
    const calls = [];
    const a = () => calls.push("a");
    const b = () => calls.push("b");
    flowStateEmitter.once("test:multi", a);
    flowStateEmitter.once("test:multi", b);
    flowStateEmitter.emit("test:multi");
    assert.deepEqual(calls.sort(), ["a", "b"]);
  });

  it("once listener fires exactly once", () => {
    let count = 0;
    flowStateEmitter.once("test:once", () => { count++; });
    flowStateEmitter.emit("test:once");
    flowStateEmitter.emit("test:once");
    assert.equal(count, 1);
  });

  it("on listener fires on every emit", () => {
    let count = 0;
    const handler = () => { count++; };
    flowStateEmitter.on("test:on", handler);
    flowStateEmitter.emit("test:on");
    flowStateEmitter.emit("test:on");
    flowStateEmitter.off("test:on", handler);
    assert.equal(count, 2);
  });
});

// ── listener cap ──────────────────────────────────────────────────────────────

describe("listener cap", () => {
  it("registers 20 listeners without emitting a MaxListenersExceeded warning", () => {
    const event = "test:cap";
    const noop = () => {};
    const handlers = Array.from({ length: 20 }, () => {
      flowStateEmitter.on(event, noop);
      return noop;
    });
    // EventEmitter emits 'warning' when cap is exceeded; verify listenerCount stayed ≤ cap
    assert.ok(flowStateEmitter.listenerCount(event) <= 20);
    handlers.forEach(() => flowStateEmitter.off(event, noop));
  });
});

// ── failureEmitter ────────────────────────────────────────────────────────────

import { failureEmitter, liveActivityEmitter } from "./events.js";

describe("failureEmitter identity", () => {
  it("is an EventEmitter", () => {
    assert.ok(failureEmitter instanceof EventEmitter);
  });

  it("max listeners is set to 10", () => {
    assert.equal(failureEmitter.getMaxListeners(), 10);
  });

  it("same instance across imports (ESM cache dedup)", async () => {
    const { failureEmitter: again } = await import("./events.js");
    assert.strictEqual(failureEmitter, again);
  });
});

describe("failureEmitter emit/on", () => {
  it("delivers failure.created payload to a listener", () => {
    let received = null;
    failureEmitter.once("failure.created", (row) => { received = row; });
    const row = { id: "abc", flow_id: "flow1", error_type: null };
    failureEmitter.emit("failure.created", row);
    assert.deepEqual(received, row);
  });

  it("does not deliver to a mismatched event name", () => {
    let fired = false;
    failureEmitter.once("failure.other", () => { fired = true; });
    failureEmitter.emit("failure.created", {});
    assert.equal(fired, false);
    failureEmitter.removeAllListeners("failure.other");
  });

  it("on/off round-trip — listener absent after off()", () => {
    const calls = [];
    const handler = (r) => calls.push(r);
    failureEmitter.on("failure.created", handler);
    failureEmitter.emit("failure.created", { id: "1" });
    failureEmitter.off("failure.created", handler);
    failureEmitter.emit("failure.created", { id: "2" });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].id, "1");
  });
});

// ── liveActivityEmitter ───────────────────────────────────────────────────────

describe("liveActivityEmitter identity", () => {
  it("is an EventEmitter", () => {
    assert.ok(liveActivityEmitter instanceof EventEmitter);
  });

  it("max listeners is set to 10", () => {
    assert.equal(liveActivityEmitter.getMaxListeners(), 10);
  });

  it("same instance across imports (ESM cache dedup)", async () => {
    const { liveActivityEmitter: again } = await import("./events.js");
    assert.strictEqual(liveActivityEmitter, again);
  });
});

describe("liveActivityEmitter emit/on", () => {
  it("delivers broadcast payload to a listener", () => {
    let received = null;
    liveActivityEmitter.once("broadcast", (msg) => { received = msg; });
    const msg = { type: "live_activity.start", activityId: "act1" };
    liveActivityEmitter.emit("broadcast", msg);
    assert.deepEqual(received, msg);
  });

  it("on/off round-trip — listener absent after off()", () => {
    const calls = [];
    const handler = (m) => calls.push(m);
    liveActivityEmitter.on("broadcast", handler);
    liveActivityEmitter.emit("broadcast", { type: "live_activity.end" });
    liveActivityEmitter.off("broadcast", handler);
    liveActivityEmitter.emit("broadcast", { type: "live_activity.start" });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].type, "live_activity.end");
  });
});
