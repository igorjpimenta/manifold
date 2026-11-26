import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  coreRunIdForGateway,
  wrapSubagentKeys,
} from "../../../../../plugins/openclaw/manifold/session-keys.js";

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

describe("wrapSubagentKeys", () => {
  it("wraps once per raw runtime", () => {
    const raw = makeRaw();
    assert.equal(wrapSubagentKeys(raw), wrapSubagentKeys(raw));
    assert.equal(wrapSubagentKeys(null), null);
  });

  it("maps core session to the harness sessionKey field and runIds both ways, then forgets", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    const out = await w.run({ session: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    assert.ok(out.runId.startsWith("mf-run-"), out.runId);
    assert.equal(log[0][1], "mf-12345678-propose");
    assert.equal(coreRunIdForGateway("gw-run-1"), out.runId);
    await w.waitForRun({ runId: out.runId });
    assert.equal(log[1][1], "gw-run-1");
    assert.equal(coreRunIdForGateway("gw-run-1"), null);
  });

  it("maps core session to the harness field on getSessionMessages", async () => {
    const log = [];
    const w = wrapSubagentKeys(makeRaw(log));
    await w.run({ session: "mf-12345678-propose", agentId: "cyrano", message: "hi" });
    await w.getSessionMessages({ session: "mf-12345678-propose" });
    assert.equal(log[1][1], "mf-12345678-propose");
    await w.getSessionMessages({ session: "gf-discuss-12345678-gate-i1" });
    assert.equal(log[2][1], "gf-discuss-12345678-gate-i1");
  });

  it("maps a harness sessionKey echo back to core session", async () => {
    const raw = { getSessionMessages: async () => ({ messages: [{ sessionKey: "agent:c:1", content: "x" }] }) };
    const w = wrapSubagentKeys(raw);
    const res = await w.getSessionMessages({ session: "agent:c:1" });
    assert.equal(res.messages[0].session, "agent:c:1");
  });

  it("maps gateway runIds back to core runIds inside message payloads", async () => {
    const raw = {
      run: async () => ({ runId: "gw-run-9" }),
      getSessionMessages: async () => ({
        messages: [{ runId: "gw-run-9", content: "x" }, { run_id: "gw-run-9", content: "y" }],
      }),
    };
    const w = wrapSubagentKeys(raw);
    const out = await w.run({ session: "mf-1", agentId: "c", message: "hi" });
    const res = await w.getSessionMessages({ session: "mf-1" });
    assert.equal(res.messages[0].runId, out.runId);
    assert.equal(res.messages[1].run_id, out.runId);
  });
});
