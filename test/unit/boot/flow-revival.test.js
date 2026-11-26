import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { reviveOrphanFlows, reconcileStuckAdmissions } from "../../../boot/flow-revival.js";
import { _resetForTests as resetGlobalConcurrency } from "../../../engine/global-concurrency.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createRunStore } from "../../../state/run-store.js";
import { drainDetachedResumes } from "../../../engine/admission-promotion-resume.js";

async function waitUntil(check, { timeout = 2000, interval = 10 } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const result = await check();
    if (result) return result;
    if (Date.now() > deadline) throw new Error("waitUntil: timed out");
    await new Promise((r) => setTimeout(r, interval));
  }
}

afterEach(() => { resetGlobalConcurrency(); });


function makeFlow({ flowId, status = "running", lastStep = "SOME_CODE_NODE", pipelineId = "p1", revision = 10, waitJson = null } = {}) {
  return {
    flowId: flowId ?? randomUUID(),
    status,
    lastStep,
    revision,
    stateJson: { pipelineId },
    waitJson,
  };
}

function makeNodeExecutionsStoreFromFlows(flows) {
  const byId = new Map(flows.map((f) => [f.flowId, f.lastStep]));
  return {
    findCurrentStep: async (flowId) => byId.get(flowId) ?? null,
    getInvocation: () => null,
    listInvocationIdsForNode: () => [],
  };
}

function makeAdmission(dbPath, limit) {
  const getLimit = typeof limit === "function" ? limit : () => limit;
  const store = createAdmissionQueueStore({ dbPath, getLimit });
  const runStore = createRunStore({ dbPath });
  const admit = store.admit.bind(store);
  return new Proxy(store, {
    get(target, key) {
      if (key === "admit") {
        return async (args) => {
          const runs = await runStore;
          await runs.insert({ flowId: args.flowId, status: "running" });
          return admit(args);
        };
      }
      const value = target[key];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function makePipelines(defs = {}) {
  return new Map(Object.entries(defs));
}

function makeStore() {
  const failed = [];
  const preloaded = [];
  const queued = [];
  return {
    failed,
    preloaded,
    queued,
    fail(args) { failed.push(args); return { applied: true }; },
    setRunning(args) { preloaded.push(args); return { applied: true }; },
    setQueued(args) { queued.push(args); return { applied: true }; },
  };
}

function makeExecutor() {
  const revived = [];
  const revivedAgent = [];
  const resumed = [];
  return {
    revived,
    revivedAgent,
    resumed,
    runFlow(flowId) { revived.push(flowId); return Promise.resolve(); },
    reviveAgentKindOrphan(flow) { revivedAgent.push(flow.flowId); return Promise.resolve(); },
    resume(flowId) { resumed.push(flowId); return Promise.resolve(); },
  };
}

function makeLogger() {
  return { info: () => {}, warn: () => {} };
}

function makeSessionIndex(hasSession = true) {
  return { hasSessionForNode: () => hasSession };
}


function makeRunStore(childRows = [], parentRows = []) {
  const allRows = [...parentRows, ...childRows];
  return {
    get(flowId) { return allRows.find((r) => r.flowId === flowId) ?? null; },
    listForParentInvocations(invocationIds) {
      return childRows.filter((r) => invocationIds.includes(r.parentInvocationId));
    },
    casSetRunning(flowId, { lastStep, stateJson }) {
      const row = allRows.find((r) => r.flowId === flowId);
      if (!row) return { applied: false, code: "not_found" };
      row.status = "running";
      row.revision = (row.revision ?? 0) + 1;
      row.lastStep = lastStep;
      row.stateJson = stateJson;
      row.waitJson = null;
      return { applied: true };
    },
    casSetQueued(flowId, { lastStep, stateJson, waitJson, statusReason }) {
      const row = allRows.find((r) => r.flowId === flowId);
      if (!row) return { applied: false, code: "not_found" };
      row.status = "queued";
      row.revision = (row.revision ?? 0) + 1;
      row.lastStep = lastStep;
      row.stateJson = stateJson;
      row.waitJson = waitJson;
      row.statusReason = statusReason;
      return { applied: true };
    },
  };
}

function makeNodeExecutionsStore(invocations = []) {
  return {
    getInvocation(id) { return invocations.find((i) => i.id === id) ?? null; },
    listInvocationIdsForNode(flowId, nodeId) {
      return invocations.filter((i) => i.flowId === flowId && i.nodeId === nodeId).map((i) => i.id);
    },
  };
}


describe("reviveOrphanFlows", () => {
  describe("ignores terminal and gate-blocked flows", () => {
    it("skips succeeded, failed, cancelled, waiting, and blocked flows", async () => {
      const statuses = ["succeeded", "failed", "cancelled", "waiting", "blocked"];
      const all = statuses.map((status) => makeFlow({ status }));
      const store = makeStore();
      const executor = makeExecutor();

      await reviveOrphanFlows({ all, pipelines: makePipelines(), store, logger: makeLogger(), executor });

      assert.equal(store.failed.length, 0);
      assert.equal(executor.revived.length, 0);
    });

  });

  describe("agent-kind orphans → revived via continuation", () => {
    it("revives a flow whose lastStep.kind is 'agent' via reviveAgentKindOrphan, not runFlow", async () => {
      const flow = makeFlow({ lastStep: "AGENT_NODE" });
      const pipelines = makePipelines({
        p1: { nodes: { AGENT_NODE: { kind: "agent" } } },
      });
      const store = makeStore();
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([flow]),
      });

      assert.equal(result.revived, 1);
      assert.equal(result.revivedAgent, 1);
      assert.equal(executor.revivedAgent[0], flow.flowId);
      assert.equal(executor.revived.length, 0, "agent-kind orphans go through reviveAgentKindOrphan, not runFlow");
      assert.equal(store.failed.length, 0, "no longer auto-failed");
    });

    it("revives a 'running' agent-kind orphan via reviveAgentKindOrphan even with no session record yet", async () => {
      const flow = makeFlow({ lastStep: "AGENT_NODE" });
      const pipelines = makePipelines({
        p1: { nodes: { AGENT_NODE: { kind: "agent" } } },
      });
      const store = makeStore();
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([flow]),
      });

      assert.equal(result.revived, 1);
      assert.equal(result.revivedAgent, 1);
      assert.equal(executor.revivedAgent[0], flow.flowId);
      assert.equal(executor.revived.length, 0);
    });

    it("treats a missing sessionIndex the same as no session found, for a target resolved via resolveEdge (defensive default)", async () => {
      const flow = makeFlow({ lastStep: "AGENT_NODE" });
      const pipelines = makePipelines({
        p1: { nodes: { AGENT_NODE: { kind: "agent" } } },
      });
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store: makeStore(), logger: makeLogger(), executor,
      });

      assert.equal(result.revivedAgent, 0);
      assert.equal(executor.revived[0], flow.flowId);
    });

    it("revives all agent-kind orphans when the shared limit covers both", async () => {
      resetGlobalConcurrency(2);
      const flows = [
        makeFlow({ lastStep: "NODE_A", pipelineId: "p1" }),
        makeFlow({ lastStep: "NODE_B", pipelineId: "p1" }),
      ];
      const pipelines = makePipelines({
        p1: { nodes: { NODE_A: { kind: "agent" }, NODE_B: { kind: "agent" } } },
      });
      const store = makeStore();
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: flows, pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows(flows),
      });

      assert.equal(result.revivedAgent, 2);
      assert.equal(result.revived, 2);
      assert.equal(store.failed.length, 0);
    });

    it("revives an in-flight agent node inside the run's own slot — no admission taken, released, or promoted", async () => {
      const flow = makeFlow({ lastStep: "AGENT_NODE" });
      const pipelines = makePipelines({ p1: { nodes: { AGENT_NODE: { kind: "agent" } } } });
      const executor = makeExecutor();
      const dbPath = path.join(os.tmpdir(), `flow-revival-agent-slot-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const runSlot = await admissionQueueStore.admit({ kind: "flow-start", flowId: flow.flowId });
      const waiting = await admissionQueueStore.admit({ kind: "flow-start", flowId: "other-flow" });
      assert.equal(runSlot.status, "admitted");
      assert.equal(waiting.status, "queued");

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store: makeStore(), logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([flow]),
        admissionQueueStore,
      });
      await drainDetachedResumes();

      assert.equal(result.revivedAgent, 1);
      assert.deepEqual(executor.revivedAgent, [flow.flowId]);
      assert.equal((await admissionQueueStore.get(runSlot.id)).status, "admitted", "the run keeps its own slot while its agent node is revived");
      assert.equal((await admissionQueueStore.get(waiting.id)).status, "queued", "nothing was released, so nothing was promoted");
      assert.deepEqual(executor.resumed, []);
    });
  });

  describe("code-kind orphans → revived", () => {
    it("revives a single code-kind orphan", async () => {
      const flow = makeFlow({ lastStep: "CAP_CHECK" });
      const pipelines = makePipelines({
        p1: { nodes: { CAP_CHECK: { kind: "code" } } },
      });
      const store = makeStore();
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({ all: [flow], pipelines, store, logger: makeLogger(), executor });

      assert.equal(result.revived, 1);
      assert.equal(result.revivedAgent, 0);
      assert.equal(result.queuedForAdmission, 0);
      assert.equal(executor.revived[0], flow.flowId);
      assert.equal(store.failed.length, 0);
    });

    it("revives all code-kind orphans regardless of count — the admission-pool skip step no longer caps revival by count (that's the shared admission pool's job, live and durable across the crash, not a boot-time re-derivation)", async () => {
      const flows = [1, 2, 3].map((n) => makeFlow({ lastStep: "STEP", revision: n }));
      const pipelines = makePipelines({
        p1: { nodes: { STEP: { kind: "code" } } },
      });
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({ all: flows, pipelines, store: makeStore(), logger: makeLogger(), executor });

      assert.equal(result.revived, 3);
      assert.equal(result.queuedForAdmission, 0);
    });
  });


  describe("children-pending reconciliation", () => {
    it("resumes a blocked parent whose children are all terminal", async () => {
      const parent = makeFlow({ status: "blocked" });
      parent.waitJson = { kind: "children-pending", nodeId: "LOOP" };
      const invocations = [
        { id: "inv-1", flowId: parent.flowId, nodeId: "LOOP" },
        { id: "inv-2", flowId: parent.flowId, nodeId: "LOOP" },
      ];
      const children = [
        { flowId: "child-1", parentInvocationId: "inv-1", status: "succeeded" },
        { flowId: "child-2", parentInvocationId: "inv-2", status: "failed" },
      ];
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [parent],
        pipelines: makePipelines(),
        store: makeStore(),
        logger: makeLogger(),
        executor,
        runStore: makeRunStore(children, [parent]),
        nodeExecutionsStore: makeNodeExecutionsStore(invocations),
      });

      assert.equal(result.childrenReconciled, 1);
      assert.deepEqual(executor.resumed, [parent.flowId]);
    });

    it("does not resume when a sibling is still non-terminal", async () => {
      const parent = makeFlow({ status: "blocked" });
      parent.waitJson = { kind: "children-pending", nodeId: "LOOP" };
      const invocations = [
        { id: "inv-1", flowId: parent.flowId, nodeId: "LOOP" },
        { id: "inv-2", flowId: parent.flowId, nodeId: "LOOP" },
      ];
      const children = [
        { flowId: "child-1", parentInvocationId: "inv-1", status: "succeeded" },
        { flowId: "child-2", parentInvocationId: "inv-2", status: "running" },
      ];
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [parent],
        pipelines: makePipelines(),
        store: makeStore(),
        logger: makeLogger(),
        executor,
        runStore: makeRunStore(children),
        nodeExecutionsStore: makeNodeExecutionsStore(invocations),
      });

      assert.equal(result.childrenReconciled, 0);
      assert.equal(executor.resumed.length, 0);
    });

    it("does not resume when not every sibling invocation has spawned a child yet", async () => {
      const parent = makeFlow({ status: "blocked" });
      parent.waitJson = { kind: "children-pending", nodeId: "LOOP" };
      const invocations = [
        { id: "inv-1", flowId: parent.flowId, nodeId: "LOOP" },
        { id: "inv-2", flowId: parent.flowId, nodeId: "LOOP" },
      ];
      const children = [
        { flowId: "child-1", parentInvocationId: "inv-1", status: "succeeded" },
      ];
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [parent],
        pipelines: makePipelines(),
        store: makeStore(),
        logger: makeLogger(),
        executor,
        runStore: makeRunStore(children),
        nodeExecutionsStore: makeNodeExecutionsStore(invocations),
      });

      assert.equal(result.childrenReconciled, 0);
      assert.equal(executor.resumed.length, 0);
    });

    it("ignores blocked flows that aren't waiting on children (ordinary human-gate)", async () => {
      const parent = makeFlow({ status: "blocked" });
      parent.waitJson = { kind: "human-gate-approval" };
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [parent],
        pipelines: makePipelines(),
        store: makeStore(),
        logger: makeLogger(),
        executor,
        runStore: makeRunStore([]),
        nodeExecutionsStore: makeNodeExecutionsStore([]),
      });

      assert.equal(result.childrenReconciled, 0);
      assert.equal(executor.resumed.length, 0);
    });

    it("no-ops (does not throw) when runStore/nodeExecutionsStore aren't passed", async () => {
      const parent = makeFlow({ status: "blocked" });
      parent.waitJson = { kind: "children-pending", nodeId: "LOOP" };
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [parent], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor,
      });

      assert.equal(result.childrenReconciled, 0);
      assert.equal(executor.resumed.length, 0);
    });
  });

  describe("stuck-gate-decision reconciliation", () => {
    function makeMaybeResolveFlow() {
      const calls = [];
      const fn = (flowId) => { calls.push(flowId); return Promise.resolve(); };
      fn.calls = calls;
      return fn;
    }

    it("reconciles a blocked human-gate flow whose approvalPending decision was never applied", async () => {
      const flow = makeFlow({ status: "blocked" });
      flow.waitJson = {
        kind: "human-gate", nodeId: "REPLY",
        items: [{ itemId: "job-1" }],
      };
      flow.stateJson = {
        ...flow.stateJson,
        approvalPending: { "job-1": { decision: "reject", source: "ws", decidedAt: 123 } },
      };
      const maybeResolveFlow = makeMaybeResolveFlow();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
        maybeResolveFlow,
      });

      assert.equal(result.gatesReconciled, 1);
      assert.deepEqual(maybeResolveFlow.calls, [flow.flowId]);
    });

    it("reconciles a sendFailed marker the same way as a real decision", async () => {
      const flow = makeFlow({ status: "blocked" });
      flow.waitJson = { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "p-1" }] };
      flow.stateJson = {
        ...flow.stateJson,
        approvalPending: { "p-1": { sendFailed: true, sendError: "telegram delivery failed" } },
      };
      const maybeResolveFlow = makeMaybeResolveFlow();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
        maybeResolveFlow,
      });

      assert.equal(result.gatesReconciled, 1);
    });

    it("does not reconcile when the pending item has no decision yet (still genuinely awaiting)", async () => {
      const flow = makeFlow({ status: "blocked" });
      flow.waitJson = { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "p-1" }] };
      flow.stateJson = { ...flow.stateJson, approvalPending: {} };
      const maybeResolveFlow = makeMaybeResolveFlow();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
        maybeResolveFlow,
      });

      assert.equal(result.gatesReconciled, 0);
      assert.equal(maybeResolveFlow.calls.length, 0);
    });

    it("does not reconcile a running or terminal flow", async () => {
      const running = makeFlow({ status: "running" });
      running.waitJson = { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "p-1" }] };
      running.stateJson = { ...running.stateJson, approvalPending: { "p-1": { decision: "approve" } } };
      const maybeResolveFlow = makeMaybeResolveFlow();

      const result = await reviveOrphanFlows({
        all: [running], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
        maybeResolveFlow,
      });

      assert.equal(result.gatesReconciled, 0);
      assert.equal(maybeResolveFlow.calls.length, 0);
    });

    it("ignores a blocked flow that isn't a gate wait (e.g. children-pending)", async () => {
      const flow = makeFlow({ status: "blocked" });
      flow.waitJson = { kind: "children-pending", nodeId: "LOOP" };
      const maybeResolveFlow = makeMaybeResolveFlow();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
        maybeResolveFlow,
      });

      assert.equal(result.gatesReconciled, 0);
      assert.equal(maybeResolveFlow.calls.length, 0);
    });

    it("no-ops (does not throw) when maybeResolveFlow isn't passed", async () => {
      const flow = makeFlow({ status: "blocked" });
      flow.waitJson = { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "p-1" }] };
      flow.stateJson = { ...flow.stateJson, approvalPending: { "p-1": { decision: "approve" } } };

      const result = await reviveOrphanFlows({
        all: [flow], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor: makeExecutor(),
      });

      assert.equal(result.gatesReconciled, 0);
    });
  });

  describe("admission-pool leak reconciliation", () => {
    it("releases an admitted admission row whose owning flow is already terminal (in `all`), and promotes the next queued entry", async () => {
      const terminalFlow = makeFlow({ status: "cancelled" });
      const pipelines = makePipelines();
      const store = makeStore();
      const executor = makeExecutor();
      const dbPath = path.join(os.tmpdir(), `flow-revival-admission-leak-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const leaked = await admissionQueueStore.admit({ kind: "flow-start", flowId: terminalFlow.flowId });
      assert.equal(leaked.status, "admitted", "pre-existing leak: the terminal flow's own admission still holds the pool's one slot");
      const waiting = await admissionQueueStore.admit({ kind: "flow-start", flowId: "other-flow" });
      assert.equal(waiting.status, "queued", "a real, still-non-terminal flow is queued behind the leaked slot");

      const result = await reviveOrphanFlows({
        all: [terminalFlow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([terminalFlow]),
        admissionQueueStore,
      });

      assert.equal(result.admissionLeaksReleased, 1);
      const stats = await admissionQueueStore._stats();
      assert.deepEqual(stats, { admitted: 1, queued: 0 }, "the leaked row was released and the other flow's queued request promoted into the freed slot");
      const promotedRow = await admissionQueueStore.get(waiting.id);
      assert.equal(promotedRow.status, "admitted", "the real flow's request was promoted once the leaked row was released");
    });

    it("resumes the flow it promotes as a side effect of releasing a leaked admission", async () => {
      const terminalFlow = makeFlow({ status: "cancelled" });
      const promotedFlow = makeFlow({ flowId: "promoted-flow", status: "queued", lastStep: null, revision: 3 });
      const pipelines = makePipelines();
      const store = makeStore();
      const executor = makeExecutor();
      const runStore = makeRunStore([], [promotedFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-admission-leak-resume-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const leaked = await admissionQueueStore.admit({ kind: "flow-start", flowId: terminalFlow.flowId });
      assert.equal(leaked.status, "admitted");
      const waiting = await admissionQueueStore.admit({ kind: "flow-start", flowId: promotedFlow.flowId });
      assert.equal(waiting.status, "queued");

      const result = await reviveOrphanFlows({
        all: [terminalFlow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([terminalFlow]),
        runStore, admissionQueueStore,
      });

      assert.equal(result.admissionLeaksReleased, 1);
      const promotedRow = await admissionQueueStore.get(waiting.id);
      assert.equal(promotedRow.status, "admitted", "still promoted at the admission-queue level");
      await drainDetachedResumes();
      assert.deepEqual(executor.resumed, ["promoted-flow"], "the promoted flow must actually be resumed, not just have its admission flipped");
      assert.equal(promotedFlow.status, "running", "runStore's own row must be flipped to running, not left at 'queued' forever");
    });

    it("releases a queued (never-admitted) admission row whose owning flow is already terminal", async () => {
      const terminalFlow = makeFlow({ status: "failed" });
      const busyFlow = makeFlow({ status: "running" });
      const pipelines = makePipelines();
      const store = makeStore();
      const executor = makeExecutor();
      const dbPath = path.join(os.tmpdir(), `flow-revival-admission-leak-queued-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "flow-start", flowId: busyFlow.flowId });
      const leakedQueued = await admissionQueueStore.admit({ kind: "flow-start", flowId: terminalFlow.flowId });
      assert.equal(leakedQueued.status, "queued", "the terminal flow's own request never got admitted before it went terminal");

      const result = await reviveOrphanFlows({
        all: [terminalFlow, busyFlow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([terminalFlow, busyFlow]),
        admissionQueueStore,
      });

      assert.equal(result.admissionLeaksReleased, 1);
      const stats = await admissionQueueStore._stats();
      assert.deepEqual(stats, { admitted: 1, queued: 0 }, "the leaked queued row is gone; the genuinely active flow's admitted slot is untouched");
    });

    it("does not release an admission row whose owning flow is still non-terminal", async () => {
      const runningFlow = makeFlow({ status: "running", lastStep: "CODE_NODE" });
      const pipelines = makePipelines({ p1: { nodes: { CODE_NODE: { kind: "code" } } } });
      const store = makeStore();
      const executor = makeExecutor();
      const dbPath = path.join(os.tmpdir(), `flow-revival-admission-no-leak-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "flow-start", flowId: runningFlow.flowId });

      const result = await reviveOrphanFlows({
        all: [runningFlow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([runningFlow]),
        admissionQueueStore,
      });

      assert.equal(result.admissionLeaksReleased, 0);
      const stats = await admissionQueueStore._stats();
      assert.deepEqual(stats, { admitted: 1, queued: 0 }, "a genuinely non-terminal flow's admission is left alone");
    });

    it("releases an admission row for a flow this same revival pass just finished cancelling (cancelledByRevival)", async () => {
      const interruptedFlow = makeFlow({ status: "running" });
      interruptedFlow.cancelRequestedAt = Date.now() - 1000;
      const pipelines = makePipelines();
      const store = { ...makeStore(), cancel: async () => ({ cancelled: true }) };
      const executor = makeExecutor();
      const dbPath = path.join(os.tmpdir(), `flow-revival-admission-leak-midcancel-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const leaked = await admissionQueueStore.admit({ kind: "flow-start", flowId: interruptedFlow.flowId });
      assert.equal(leaked.status, "admitted");

      const result = await reviveOrphanFlows({
        all: [interruptedFlow], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([interruptedFlow]),
        admissionQueueStore,
      });

      assert.equal(result.recancelled, 1, "sanity check: this flow went through the interrupted-cancellation path");
      assert.equal(result.admissionLeaksReleased, 1);
      const stats = await admissionQueueStore._stats();
      assert.deepEqual(stats, { admitted: 0, queued: 0 });
    });
  });

  describe("agent-dispatch wait → re-requests its own run slot", () => {
    it("pool has room: admitted on its own run kind, flipped to running, resumed — not re-entered via runFlow", async () => {
      const flow = makeFlow({ status: "queued", lastStep: "CACHE_CHECK", waitJson: { kind: "agent-dispatch", nodeId: "SCORE_LLM" } });
      const pipelines = makePipelines({ p1: { nodes: { SCORE_LLM: { kind: "agent" } } } });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [flow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-readmit-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const result = await reviveOrphanFlows({
        all: [{ ...flow }], pipelines, store: makeStore(), logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([flow]),
        runStore, admissionQueueStore,
      });
      await drainDetachedResumes();

      assert.equal(result.readmitted, 1);
      assert.equal(result.revived, 0, "not routed through the direct-dispatch path");
      assert.deepEqual(executor.revived, []);
      assert.deepEqual(executor.resumed, [flow.flowId]);
      assert.equal(flow.status, "running");
      const [row] = await admissionQueueStore.listByStatus("admitted");
      assert.equal(row.kind, "flow-start");
      assert.equal(row.flowId, flow.flowId);
    });

    it("pool full: left queued on its own run kind for the normal promotion cycle, not resumed", async () => {
      const flow = makeFlow({ status: "queued", lastStep: "CACHE_CHECK", waitJson: { kind: "agent-dispatch", nodeId: "SCORE_LLM" } });
      flow.parentInvocationId = "inv-1";
      const pipelines = makePipelines({ p1: { nodes: { SCORE_LLM: { kind: "agent" } } } });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [flow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-readmit-full-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();
      await admissionQueueStore.admit({ kind: "flow-start", flowId: "occupant" });

      const result = await reviveOrphanFlows({
        all: [{ ...flow }], pipelines, store: makeStore(), logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([flow]),
        runStore, admissionQueueStore,
      });
      await drainDetachedResumes();

      assert.equal(result.readmitted, 1);
      assert.deepEqual(executor.resumed, []);
      assert.deepEqual(executor.revived, []);
      assert.equal(flow.status, "queued");
      assert.deepEqual(flow.waitJson, { kind: "child-dispatch" }, "a subflow child re-queues on child-dispatch, its own run kind");
      const [queued] = await admissionQueueStore.listByStatus("queued");
      assert.equal(queued.kind, "child-dispatch");
      assert.equal(queued.flowId, flow.flowId);
    });
  });

  describe("free-slot fill", () => {
    it("promotes queued runs into capacity freed without a release, and resumes them", async () => {
      const queuedFlow = makeFlow({ flowId: "queued-flow", status: "queued", lastStep: "__start__", waitJson: { kind: "flow-start" } });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [queuedFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-fill-${randomUUID()}.sqlite`);
      let limit = 0;
      const admissionQueueStore = makeAdmission(dbPath, () => limit);
      await admissionQueueStore.ready();
      const row = await admissionQueueStore.admit({ kind: "flow-start", flowId: queuedFlow.flowId });
      assert.equal(row.status, "queued");
      limit = 2;

      const result = await reviveOrphanFlows({
        all: [{ ...queuedFlow }], pipelines: makePipelines(), store: makeStore(), logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([queuedFlow]),
        runStore, admissionQueueStore,
      });
      await drainDetachedResumes();

      assert.equal(result.queuedForAdmission, 1, "classified as queued for admission first");
      assert.equal(result.freeSlotsFilled, 1);
      assert.equal((await admissionQueueStore.get(row.id)).status, "admitted");
      assert.equal(queuedFlow.status, "running");
      assert.deepEqual(executor.resumed, ["queued-flow"]);
    });
  });

  describe("stuck-promotion reconciliation", () => {
    it("force-resumes a flow-start admission stuck 'admitted' while its flow is still 'queued'", async () => {
      const stuckFlow = makeFlow({
        status: "queued", lastStep: "PRIME", revision: 4,
        waitJson: { kind: "flow-start" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [stuckFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-stuck-admission-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      const admission = await admissionQueueStore.admit({ kind: "flow-start", flowId: stuckFlow.flowId });
      assert.equal(admission.status, "admitted");

      const result = await reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });

      assert.equal(result.stuckAdmissionsResumed, 1);
      assert.deepEqual(executor.resumed, [stuckFlow.flowId]);
      assert.equal(stuckFlow.status, "running", "runStore's own row must be flipped, not left at 'queued' forever");
    });

    it("leaves a just-admitted row alone within the minAgeMs grace window (a genuine in-flight resume, not a leak)", async () => {
      const freshFlow = makeFlow({
        status: "queued", lastStep: "PRIME", revision: 4,
        waitJson: { kind: "flow-start" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [freshFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-stuck-admission-fresh-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "flow-start", flowId: freshFlow.flowId });

      const result = await reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, logger: makeLogger() });

      assert.equal(result.stuckAdmissionsResumed, 0);
      assert.deepEqual(executor.resumed, [], "must not force-resume something that may still be resuming normally");
      assert.equal(freshFlow.status, "queued");
    });

    it("leaves a running flow's admission alone — that's the healthy state, not a leak", async () => {
      const runningFlow = makeFlow({ status: "running" });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [runningFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-stuck-admission-healthy-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "flow-start", flowId: runningFlow.flowId });

      const result = await reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });

      assert.equal(result.stuckAdmissionsResumed, 0);
      assert.deepEqual(executor.resumed, []);
    });

    it("reaps an unmanaged-kind admitted row without driving its flow", async () => {
      const stuckFlow = makeFlow({
        status: "queued", lastStep: "AGENT_NODE", revision: 4,
        waitJson: { kind: "agent-dispatch", nodeId: "AGENT_NODE" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [stuckFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-stuck-admission-agent-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "agent-dispatch", flowId: stuckFlow.flowId, nodeId: "AGENT_NODE" });

      const result = await reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });

      assert.equal(result.retiredKindReaped, 1);
      assert.equal(result.stuckAdmissionsResumed, 0);
      assert.deepEqual(executor.resumed, [], "a non-run kind must never be routed through flipToRunningAndResume");
      assert.equal(stuckFlow.status, "queued", "the flow is not this pass's job — only the slot is");
      assert.deepEqual(await admissionQueueStore.listByStatus("admitted"), [], "the zombie slot must be reclaimed, not skipped");
    });

    it("reaps a retired-kind admitted slot and promotes the flow's own canonical queued row in the same pass", async () => {
      const stranded = makeFlow({
        status: "queued", lastStep: "SCORE", revision: 9,
        waitJson: { kind: "child-dispatch" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [stranded]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-retired-reap-promote-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "agent-dispatch", flowId: stranded.flowId, nodeId: "SCORE_LLM" });
      const canonical = await admissionQueueStore.admit({ kind: "child-dispatch", flowId: stranded.flowId });
      assert.equal(canonical.status, "queued", "precondition: the pool is wedged shut by the zombie row");

      const result = await reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });
      await drainDetachedResumes();

      assert.equal(result.retiredKindReaped, 1);
      assert.equal(result.stuckAdmissionsResumed, 0, "the flow was driven via the promotion wake, not the stuck-resume path");
      assert.deepEqual(executor.resumed, [stranded.flowId]);
      assert.equal(stranded.status, "running", "the freed slot must flow straight into the stranded flow's own admission");
      const rows = await admissionQueueStore.listByStatus("admitted");
      assert.equal(rows.length, 1);
      assert.equal(rows[0].kind, "child-dispatch", "the survivor is the canonical row, not the zombie");
    });

    it("leaves a fresh retired-kind row alone within the grace window — an older-code process may still own it mid-deploy", async () => {
      const freshFlow = makeFlow({
        status: "queued", lastStep: "AGENT_NODE", revision: 4,
        waitJson: { kind: "agent-dispatch", nodeId: "AGENT_NODE" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [freshFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-retired-fresh-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "agent-dispatch", flowId: freshFlow.flowId, nodeId: "AGENT_NODE" });

      const result = await reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, logger: makeLogger() });

      assert.equal(result.retiredKindReaped, 0, "default minAgeMs grace — the admitting process may still be alive");
      assert.equal((await admissionQueueStore.listByStatus("admitted")).length, 1);
      assert.equal(freshFlow.status, "queued");
    });

    it("leaves a child-dispatch admission alone when the flow is queued for a DIFFERENT, unrelated resource", async () => {
      const busyChild = makeFlow({
        status: "queued", lastStep: "SUBMIT", revision: 7,
        waitJson: { kind: "agent-dispatch", nodeId: "SUBMIT" },
      });
      const executor = makeExecutor();
      const runStore = makeRunStore([], [busyChild]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-stuck-admission-unrelated-wait-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "child-dispatch", flowId: busyChild.flowId });

      const result = await reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });

      assert.equal(result.stuckAdmissionsResumed, 0);
      assert.deepEqual(executor.resumed, [], "must not force-resume a flow that's healthily queued for an unrelated resource");
      assert.equal(busyChild.status, "queued", "left exactly as found — this is the normal, expected state, not a leak");
    });

    it("reviveOrphanFlows itself runs this reconciliation and reports the count", async () => {
      const stuckFlow = makeFlow({
        status: "queued", lastStep: "PRIME", revision: 4,
        waitJson: { kind: "flow-start" },
      });
      const pipelines = makePipelines();
      const store = makeStore();
      const executor = makeExecutor();
      const runStore = makeRunStore([], [stuckFlow]);
      const dbPath = path.join(os.tmpdir(), `flow-revival-boot-stuck-admission-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 1);
      await admissionQueueStore.ready();

      await admissionQueueStore.admit({ kind: "flow-start", flowId: stuckFlow.flowId });

      const result = await reviveOrphanFlows({
        all: [], pipelines, store, logger: makeLogger(), executor,
        nodeExecutionsStore: makeNodeExecutionsStoreFromFlows([]),
        runStore, admissionQueueStore,
        stuckAdmissionMinAgeMs: 0,
      });

      assert.equal(result.stuckAdmissionsResumed, 1);
      assert.equal(result.stuckAdmissionsFailed, 0);
      assert.equal(stuckFlow.status, "running");
    });

    it("drives multiple stuck candidates concurrently — a slow resume() does not block a faster one", async () => {
      const slowFlow = makeFlow({ status: "queued", lastStep: "PRIME", revision: 4, waitJson: { kind: "flow-start" } });
      const fastFlow = makeFlow({ status: "queued", lastStep: "PRIME", revision: 4, waitJson: { kind: "flow-start" } });
      const runStore = makeRunStore([], [slowFlow, fastFlow]);

      let releaseSlow;
      const slowGate = new Promise((resolve) => { releaseSlow = resolve; });
      const resumed = [];
      const executor = {
        resume: async (flowId) => {
          if (flowId === slowFlow.flowId) await slowGate;
          resumed.push(flowId);
        },
      };

      const dbPath = path.join(os.tmpdir(), `flow-revival-concurrent-reconcile-${randomUUID()}.sqlite`);
      const admissionQueueStore = makeAdmission(dbPath, 2);
      await admissionQueueStore.ready();
      await admissionQueueStore.admit({ kind: "flow-start", flowId: slowFlow.flowId });
      await admissionQueueStore.admit({ kind: "flow-start", flowId: fastFlow.flowId });

      const reconcilePromise = reconcileStuckAdmissions({
        admissionQueueStore, runStore, executor, logger: makeLogger(), minAgeMs: 0,
      });

      await new Promise((resolve) => setTimeout(resolve, 20));
      assert.deepEqual(resumed, [fastFlow.flowId], "the fast candidate resumed without waiting on the slow one");

      releaseSlow();
      const result = await reconcilePromise;
      assert.equal(result.stuckAdmissionsResumed, 2);
      assert.deepEqual(new Set(resumed), new Set([slowFlow.flowId, fastFlow.flowId]));
    });
  });

  describe("queued orphans (any wait kind) → revived like running ones", () => {
    it("with no admission pool wired in, an agent-dispatch-queued orphan is re-entered via plain runFlow, not the continuation path", async () => {
      const flow = makeFlow({
        status: "queued", lastStep: "AGENT_NODE",
        waitJson: { kind: "agent-dispatch", nodeId: "AGENT_NODE" },
      });
      const pipelines = makePipelines({ p1: { nodes: { AGENT_NODE: { kind: "agent" } } } });
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store: makeStore(), logger: makeLogger(), executor,
        sessionIndex: makeSessionIndex(false),
      });

      assert.equal(result.revived, 1);
      assert.equal(result.revivedAgent, 0);
      assert.deepEqual(executor.revived, [flow.flowId]);
      assert.equal(executor.revivedAgent.length, 0);
    });

    it("re-submits a queued orphan with no waitJson at all", async () => {
      const flow = makeFlow({ status: "queued", waitJson: null });
      const pipelines = makePipelines({ p1: { nodes: { SOME_CODE_NODE: { kind: "code" } } } });
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({
        all: [flow], pipelines, store: makeStore(), logger: makeLogger(), executor,
      });

      assert.equal(result.revived, 1);
      assert.deepEqual(executor.revived, [flow.flowId]);
    });

    it("a child-dispatch-queued orphan is likewise left queued, not dispatched", async () => {
      const flow = makeFlow({ status: "queued", waitJson: { kind: "child-dispatch" } });
      const pipelines = makePipelines({ p1: { nodes: { SOME_CODE_NODE: { kind: "code" } } } });
      const store = makeStore();
      const executor = makeExecutor();

      const result = await reviveOrphanFlows({ all: [flow], pipelines, store, logger: makeLogger(), executor });

      assert.equal(result.revived, 0);
      assert.equal(result.queuedForAdmission, 1);
      assert.equal(executor.revived.length, 0);
    });
  });
});
