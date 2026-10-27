// E2E: boot/flow-revival.js against a real executor + real stores
// (tmp-sqlite), pull-first: a flow interrupted before its agent node parks
// is revived through runFlow and re-parks its ticket; a flow that was only
// ever queued re-enters the same way. No subagent runtime exists anywhere
// in this path — reaching `blocked` on `agent-claim` proves the pull
// branch ran, and claim → report → resume completes the run.
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  makeClaimHarness, startAndPark, claimAndReport, waitForStatus, TRIPLE,
} from "../../helpers/pull-claim-driver.js";

function fixturePipeline() {
  return {
    id: "test_agent_fixture",
    state: {},
    nodes: {
      AGENT_STEP: { kind: "agent", agent: "scout" },
      DONE: { kind: "code", run: () => ({ DONE: { ok: true } }) },
    },
    edges: {
      __start__: "AGENT_STEP",
      AGENT_STEP: (state) => (state.AGENT_STEP?.reply ? "DONE" : null),
      DONE: () => "__end__",
    },
  };
}

describe("flow-revival — agent-node orphans re-park through runFlow (fixture pipeline)", () => {
  let harness;
  afterEach(() => { harness?.close(); });

  test("a flow interrupted mid-agent-node re-parks its ticket on revival", async () => {
    harness = await makeClaimHarness({ test_agent_fixture: fixturePipeline() });
    const { executor, runStore, nodeExecutionsStore } = harness;
    const { reviveOrphanFlows } = await import("../../../boot/flow-revival.js");

    // Crash model: the flow started and entered AGENT_STEP (invocation row
    // exists) but the process died before the node parked.
    const record = await executor.start("test_agent_fixture", {}, { kind: "test" });
    await nodeExecutionsStore.enter({
      flowId: record.flowId, pipelineId: "test_agent_fixture",
      nodeId: "AGENT_STEP", subflowKey: null, kind: "fresh",
    });

    const flowRow = await runStore.get(record.flowId);
    assert.equal(flowRow.status, "running");
    const result = await reviveOrphanFlows({
      all: [flowRow],
      pipelines: new Map([["test_agent_fixture", fixturePipeline()]]),
      store: harness.store,
      logger: { info: () => {}, warn: () => {} },
      executor,
      runStore,
      nodeExecutionsStore,
    });

    assert.equal(result.revived, 1);
    assert.equal(result.revivedAgent, 1);
    // Revival re-enters fire-and-forget (production shape) — poll for park.
    const parked = await waitForStatus(runStore, record.flowId, "blocked");
    assert.equal(parked.waitJson?.kind, "agent-claim");
    assert.equal(parked.waitJson?.nodeId, "AGENT_STEP");

    // And the re-parked ticket completes the run through the real path.
    await claimAndReport(harness, parked.waitJson.ticket, { AGENT_STEP: { reply: "ok" } }, TRIPLE);
    const terminal = await waitForStatus(runStore, record.flowId, "succeeded");
    assert.equal(terminal.status, "succeeded");
  });

  test("a flow only ever queued re-enters and parks like any other orphan", async () => {
    harness = await makeClaimHarness({ test_agent_fixture: fixturePipeline() });
    const { store, runStore, executor, nodeExecutionsStore } = harness;
    const { reviveOrphanFlows } = await import("../../../boot/flow-revival.js");

    const flow = await store.create({ pipelineId: "test_agent_fixture", initialState: {} });
    await store.setQueued({
      flowId: flow.flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
      stateJson: flow.stateJson,
      waitJson: { kind: "resource-lock", resource: "browser:freelance", nodeId: "AGENT_STEP" },
      statusReason: "queued: waiting on a resource (test fixture)",
    });

    const flowRow = await runStore.get(flow.flowId);
    const result = await reviveOrphanFlows({
      all: [flowRow],
      pipelines: new Map([["test_agent_fixture", fixturePipeline()]]),
      store,
      logger: { info: () => {}, warn: () => {} },
      executor,
      runStore,
      nodeExecutionsStore,
    });

    assert.equal(result.revived, 1);
    const parked = await waitForStatus(runStore, flow.flowId, "blocked");
    assert.equal(parked.waitJson?.kind, "agent-claim");

    await claimAndReport(harness, parked.waitJson.ticket, { AGENT_STEP: { reply: "ok" } }, TRIPLE);
    const terminal = await waitForStatus(runStore, flow.flowId, "succeeded");
    assert.equal(terminal.status, "succeeded");
  });
});
