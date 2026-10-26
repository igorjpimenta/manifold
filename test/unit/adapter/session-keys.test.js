// session-keys.test.js — adapter edge mapping: opaque core handles ↔
// gateway keys, single-wrap identity, run/session translation, lifecycle.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  toGatewayKey,
  toOpaqueKey,
  toGatewaySessionKey,
  toCoreSessionKey,
  coreRunIdForGateway,
  wrapSubagentKeys,
} from "../../../plugins/openclaw/manifold/session-keys.js";

function makeRaw(log = []) {
  return {
    run: async (args) => { log.push(["run", args.sessionKey]); return { runId: "gw-run-1" }; },
    waitForRun: async (args) => { log.push(["wait", args.runId]); return { status: "ok" }; },
    getSessionMessages: async (args) => {
      log.push(["msgs", args.sessionKey]);
      return { messages: [{ role: "assistant", content: "done" }] };
    },
  };
}

describe("toGatewayKey / toOpaqueKey", () => {
  it("round-trips an opaque handle", () => {
    const gw = toGatewayKey({ opaque: "mf-12345678-propose", agent: "cyrano" });
    assert.equal(gw, "agent:cyrano:subagent:mf-12345678-propose");
    assert.equal(toOpaqueKey(gw), "mf-12345678-propose");
  });

  it("returns null, never throws, on foreign shapes", () => {
    assert.equal(toOpaqueKey("manifold-push-f-n"), null);
    assert.equal(toOpaqueKey("agent:main:cron:x"), null);
    assert.equal(toOpaqueKey(null), null);
    assert.equal(toOpaqueKey(42), null);
  });

  it("throws on empty input", () => {
    assert.throws(() => toGatewayKey({ opaque: "", agent: "c" }));
    assert.throws(() => toGatewayKey({ opaque: "mf-x", agent: "" }));
  });
});

describe("passthrough rules", () => {
  it("leaves gateway and foreign keys verbatim outbound", () => {
    assert.equal(toGatewaySessionKey("agent:c:subagent:mf-aa-bb", "c"), "agent:c:subagent:mf-aa-bb");
    assert.equal(toGatewaySessionKey("manifold-push-f-n", "c"), "manifold-push-f-n");
  });

  it("leaves foreign keys verbatim inbound", () => {
    assert.equal(toCoreSessionKey("manifold-push-f-n"), "manifold-push-f-n");
    assert.equal(toCoreSessionKey("agent:c:subagent:mf-aa-bb"), "mf-aa-bb");
  });
});

describe("wrapSubagentKeys", () => {
  it("wraps once per raw runtime", () => {
    const raw = makeRaw();
    assert.equal(wrapSubagentKeys(raw), wrapSubagentKeys(raw));
    assert.equal(wrapSubagentKeys(null), null);
  });

  it("maps opaque→gateway on run and back on waitForRun, then forgets", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    const out = await w.run({ sessionKey: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    assert.ok(out.runId.startsWith("mf-run-"), out.runId);
    assert.equal(log[0][1], "agent:cyrano:subagent:mf-12345678-propose");
    assert.equal(coreRunIdForGateway("gw-run-1"), out.runId);
    await w.waitForRun({ runId: out.runId });
    assert.equal(log[1][1], "gw-run-1");
    assert.equal(coreRunIdForGateway("gw-run-1"), null);
  });

  it("resolves opaque handles on getSessionMessages via the run registration", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    await w.run({ sessionKey: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    await w.getSessionMessages({ sessionKey: "mf-12345678-propose" });
    assert.equal(log[1][1], "agent:cyrano:subagent:mf-12345678-propose");
  });

  it("passes foreign keys through on every method", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    await w.run({ sessionKey: "manifold-push-f-n", agentId: "c", message: "x" });
    await w.getSessionMessages({ sessionKey: "manifold-push-f-n" });
    assert.equal(log[0][1], "manifold-push-f-n");
    assert.equal(log[1][1], "manifold-push-f-n");
  });
});
