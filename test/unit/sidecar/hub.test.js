// hub.test.js — SSE fan-out: subscribe, publish, prune, broadcast shape.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHub } from "../../../sidecar/hub.js";

function fakeRes() {
  return { frames: [], write(s) { this.frames.push(s); return true; } };
}

describe("createHub", () => {
  it("delivers to channel subscribers only", () => {
    const hub = createHub({});
    const a = fakeRes();
    const b = fakeRes();
    const unsubA = hub.subscribe("events", a);
    hub.subscribe("dispatch", b);
    assert.equal(hub.subscriberCount("events"), 1);
    assert.equal(hub.publish("events", { kind: "plugin.x", n: 1 }), 1);
    assert.equal(a.frames.length, 2); // connected comment + frame
    assert.equal(b.frames.length, 1); // connected comment only
    const frame = JSON.parse(a.frames[1].replace(/^data: /, ""));
    assert.equal(frame.kind, "plugin.x");
    assert.equal(frame.channel, "events");
    unsubA();
    assert.equal(hub.subscriberCount("events"), 0);
  });
  it("prunes dead subscribers", () => {
    const hub = createHub({});
    hub.subscribe("events", { write() { throw new Error("gone"); } });
    assert.equal(hub.publish("events", { kind: "plugin.x" }), 0);
    assert.equal(hub.subscriberCount("events"), 0);
  });
  it("broadcast() targets the events channel with kind intact", () => {
    const hub = createHub({});
    const a = fakeRes();
    hub.subscribe("events", a);
    hub.broadcast("plugin.manifold.gate.opened", { flowId: "f" });
    const frame = JSON.parse(a.frames[1].replace(/^data: /, ""));
    assert.equal(frame.kind, "plugin.manifold.gate.opened");
    assert.equal(frame.flowId, "f");
  });
});
