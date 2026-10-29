// session-keys.test.js — plain-text session identity plus the runId
// registry: session keys pass through verbatim in both directions;
// legacy gateway-shaped keys still map back; runIds still map both ways.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  toOpaqueKey,
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

describe("toOpaqueKey (legacy gateway map-back)", () => {
  it("maps pre-cutover gateway keys to their opaque handle", () => {
    assert.equal(toOpaqueKey("agent:cyrano:subagent:mf-12345678-propose"), "mf-12345678-propose");
  });

  it("returns null, never throws, on anything else", () => {
    assert.equal(toOpaqueKey("mf-12345678-propose"), null);
    assert.equal(toOpaqueKey("agent:main:cron:x"), null);
    assert.equal(toOpaqueKey(null), null);
    assert.equal(toOpaqueKey(42), null);
  });
});

describe("passthrough rules", () => {
  it("leaves every key verbatim inbound", () => {
    assert.equal(toCoreSessionKey("mf-12345678-propose"), "mf-12345678-propose");
    assert.equal(toCoreSessionKey("gf-discuss-12345678-gate-i1"), "gf-discuss-12345678-gate-i1");
    assert.equal(toCoreSessionKey("agent:c:subagent:mf-aa-bb"), "mf-aa-bb");
  });
});

describe("wrapSubagentKeys", () => {
  it("wraps once per raw runtime", () => {
    const raw = makeRaw();
    assert.equal(wrapSubagentKeys(raw), wrapSubagentKeys(raw));
    assert.equal(wrapSubagentKeys(null), null);
  });

  it("passes session keys through verbatim and maps runIds both ways, then forgets", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    const out = await w.run({ sessionKey: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    assert.ok(out.runId.startsWith("mf-run-"), out.runId);
    assert.equal(log[0][1], "mf-12345678-propose");
    assert.equal(coreRunIdForGateway("gw-run-1"), out.runId);
    await w.waitForRun({ runId: out.runId });
    assert.equal(log[1][1], "gw-run-1");
    assert.equal(coreRunIdForGateway("gw-run-1"), null);
  });

  it("passes every key through on getSessionMessages", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    await w.run({ sessionKey: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    await w.getSessionMessages({ sessionKey: "mf-12345678-propose" });
    assert.equal(log[1][1], "mf-12345678-propose");
  });
});
