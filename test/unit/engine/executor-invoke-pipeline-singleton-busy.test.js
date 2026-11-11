/**
 * Regression coverage for nodeCtx.invokePipeline's singleton-busy
 * detection. Originally, start() threw synchronously when a
 * pipeline was at its concurrency limit, and invokePipeline caught that
 * throw, pattern-matching its message, to return {invoked:false,
 * reason:"singleton-busy"} instead of propagating the error.
 *
 * The admission-queue rearchitecture changed that: start() no
 * longer throws when the shared admission pool is full — it queues the
 * new flow instead (state/admission-queue-store.js). invokePipeline was
 * updated to check record.status === "queued" instead of catching a
 * throw, cancelling the just-queued record and returning the same
 * invoked:false/reason:"singleton-busy" shape callers already depend on —
 * this test now drives that path via a REAL createExecutor()/start()/
 * admissionQueueStore, with the shared concurrency limit set to 1.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { assembleState } from "../../../engine/executor-helpers.js";
import { getConcurrencyLimit, _resetForTests as resetConcurrency } from "../../../engine/concurrency.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("invoke-singleton-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("invoke-singleton-execs"));
  const admissionQueueStore = createAdmissionQueueStore({ dbPath: tmpPath("invoke-singleton-admission"), getLimit: getConcurrencyLimit });
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, admissionQueueStore, logger });

  // This fixture deliberately omits allowConcurrent. The per-pipeline
  // singleton gate in executor.js's start() fires on `allowConcurrent ===
  // false` only, so omitting the field leaves this pipeline unconstrained by
  // that gate and competing for the shared global limit alone (set by the
  // test below). Setting `allowConcurrent: false` here would change what the
  // test exercises: whenever the gate fired it would leave start() with
  // status "skipped" rather than "queued", and it is the queued→singleton-busy
  // translation above that this test asserts on.
  const singletonTarget = {
    id: "singleton_target",
    state: {},
    nodes: { NOOP: { kind: "code", run: () => ({ NOOP: { ran: true } }) } },
    edges: { __start__: "NOOP", NOOP: "__end__" },
  };
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: {
      CALL_TWICE: {
        kind: "code",
        run: async (state, ctx) => {
          const first = await ctx.invokePipeline("singleton_target");
          const second = await ctx.invokePipeline("singleton_target");
          return { CALL_TWICE: { first, second } };
        },
      },
    },
    edges: { __start__: "CALL_TWICE", CALL_TWICE: "__end__" },
  };
  const pipelinesRef = {
    value: new Map([
      ["test_parent", parentPipeline],
      ["singleton_target", singletonTarget],
    ]),
  };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    runTurn: null,
    failureStore: null,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, store, executor };
}

describe("nodeCtx.invokePipeline — singleton-busy detection against the real start() error text", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    resetConcurrency();
  });

  test("second call to an already-active singleton target returns invoked:false, reason:singleton-busy — does not throw", async () => {
    // limit=2, not 1 — under the shared GLOBAL pool, the PARENT
    // flow (test_parent) itself now holds one of the slots for its own
    // entire run (kind:"flow-start", same pool every kind draws from), so
    // a limit of 1 would make even the FIRST invokePipeline call busy.
    // limit=2 leaves exactly one free slot for a target flow — enough for
    // the first call to succeed and the second (a genuinely concurrent 2nd
    // instance of the SAME target pipeline) to find the pool full.
    resetConcurrency(2);
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await executor.runFlow(record.flowId);

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded", "the parent node must not have thrown out of CALL_TWICE.run");

    const flowState = await assembleState(flow.flowId, { runStore, nodeExecutionsStore, baseState: flow.stateJson });
    const { first, second } = flowState.CALL_TWICE;
    assert.equal(first.invoked, true, "first call against an idle singleton target must succeed");
    assert.equal(second.invoked, false);
    assert.equal(second.reason, "singleton-busy");
    assert.match(second.message, /is at its concurrency limit/i);
  });
});
