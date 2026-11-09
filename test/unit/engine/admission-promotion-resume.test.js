/**
 * Unit tests for engine/admission-promotion-resume.js — isEngineResumableKind
 * and wakePromotedRelease, the single shared "release → check promoted →
 * decide routing → resume-or-skip → log" helper that replaced 7 independent
 * hand-rolled copies of the same glue across state-store.js, child-resume-
 * trigger.js (x2), executor-nodes.js, boot/flow-revival.js (x2), and
 * bin/cli.js (x2). Each of those call sites still has its own thin wiring
 * test (or indirect coverage via state-store.test.js's promotion-routing
 * block) — this file is the one place that tests wakePromotedRelease's own
 * contract directly, independent of any one call site.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { isEngineResumableKind, wakePromotedRelease, scheduleDetachedResume, drainDetachedResumes } from "../../../engine/admission-promotion-resume.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

function makeWarningLogger() {
  const warnings = [];
  return { warnings, logger: { warn: (msg) => warnings.push(msg) } };
}

describe("isEngineResumableKind", () => {
  test("child-dispatch and flow-start are resumable via this engine path", () => {
    assert.equal(isEngineResumableKind("child-dispatch"), true);
    assert.equal(isEngineResumableKind("flow-start"), true);
  });

  test("an unrecognized kind is treated as not resumable, not thrown on", () => {
    assert.equal(isEngineResumableKind("agent-dispatch"), false);
    assert.equal(isEngineResumableKind("something-new"), false);
    assert.equal(isEngineResumableKind(undefined), false);
  });
});

describe("scheduleDetachedResume / drainDetachedResumes", () => {
  test("drain waits for resumes scheduled by other resumes — a woken run can release a slot and wake the next flow", async () => {
    const ran = [];
    const resume = async (flowId) => {
      ran.push(flowId);
      if (flowId === "first") scheduleDetachedResume(resume, "second", {});
    };
    scheduleDetachedResume(resume, "first", {});
    assert.deepEqual(ran, [], "nothing runs synchronously");
    await drainDetachedResumes();
    assert.deepEqual(ran, ["first", "second"]);
  });

  test("drain resolves immediately when nothing is pending", async () => {
    await drainDetachedResumes();
  });
});

describe("wakePromotedRelease", () => {
  let harness;
  afterEach(() => harness?.runStore?.close());

  test("no promotion at all: no-op, no log, reason nothing_to_wake", async () => {
    const { warnings, logger } = makeWarningLogger();
    const result = await wakePromotedRelease({ released: true, promoted: null }, {
      runStore: null, resume: async () => {}, logger, api: {}, context: "test",
    });
    assert.deepEqual(result, { resumed: false, reason: "nothing_to_wake" });
    assert.equal(warnings.length, 0);
  });

  test("a promotion of a kind the engine doesn't admit is a silent no-op", async () => {
    const { warnings, logger } = makeWarningLogger();
    let resumeCalled = false;
    const result = await wakePromotedRelease(
      { released: true, promoted: { id: randomUUID(), kind: "agent-dispatch", flowId: randomUUID(), nodeId: "SCORE_LLM" } },
      { runStore: null, resume: async () => { resumeCalled = true; }, logger, api: {}, context: "test" },
    );
    assert.deepEqual(result, { resumed: false, reason: "nothing_to_wake" });
    assert.equal(resumeCalled, false);
    assert.equal(warnings.length, 0);
  });

  test("accepts an ARRAY of release results and picks the first non-null promotion — the dual flow-start/child-dispatch release shape", async () => {
    const runStore = await createRunStore({ dbPath: tmpPath("wake-array-runs") });
    harness = { runStore };
    const resumed = [];
    const target = randomUUID();
    await runStore.insert({ flowId: target, ownerKey: "test", pipelineId: "p", revision: 0, status: "queued", lastStep: "__start__", stateJson: {} });

    const result = await wakePromotedRelease(
      [
        { released: false, promoted: null },
        { released: true, promoted: { id: randomUUID(), kind: "child-dispatch", flowId: target, nodeId: "LOOP" } },
      ],
      { runStore, resume: async (flowId) => resumed.push(flowId), logger: { warn: () => {} }, api: {}, context: "test" },
    );

    assert.equal(result.resumed, true);
    await drainDetachedResumes();
    assert.deepEqual(resumed, [target]);
  });

  // The releaser (a flow inside its own state-store write) must not wait for
  // the promoted flow's run. Here that run never finishes — if
  // wakePromotedRelease awaited it, this test would hang.
  test("returns once the promoted flow is flipped to running, without waiting for its run to finish", async () => {
    const runStore = await createRunStore({ dbPath: tmpPath("wake-detach-runs") });
    harness = { runStore };
    const target = randomUUID();
    await runStore.insert({ flowId: target, ownerKey: "test", pipelineId: "p", revision: 0, status: "queued", lastStep: "__start__", stateJson: {} });
    let releaseRun;
    const runStarted = [];
    const neverEndingRun = async (flowId) => {
      runStarted.push(flowId);
      await new Promise((resolve) => { releaseRun = resolve; });
    };

    const result = await wakePromotedRelease(
      { released: true, promoted: { id: randomUUID(), kind: "child-dispatch", flowId: target, nodeId: "LOOP" } },
      { runStore, resume: neverEndingRun, logger: { warn: () => {} }, api: {}, context: "test" },
    );

    assert.equal(result.resumed, true);
    assert.equal((await runStore.get(target)).status, "running", "the durable flip still completes before returning");
    assert.deepEqual(runStarted, [], "the promoted run starts on a later tick, not inside the releaser's call");
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(runStarted, [target]);
    releaseRun();
    await drainDetachedResumes();
  });

  test("a resumable kind that genuinely fails to resume (target flow missing) logs a warning tagged with the caller's context", async () => {
    const { warnings, logger } = makeWarningLogger();
    const missingFlowId = randomUUID();
    const runStore = await createRunStore({ dbPath: tmpPath("wake-fail-runs") });
    harness = { runStore };

    const result = await wakePromotedRelease(
      { released: true, promoted: { id: randomUUID(), kind: "flow-start", flowId: missingFlowId, nodeId: null } },
      { runStore, resume: async () => {}, logger, api: {}, context: "my-caller" },
    );

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "parent_not_found");
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /^my-caller: promoted \[.+\] did not resume: parent_not_found$/);
  });

  test("the promoted flow's run throwing is caught and logged by the detached task, tagged with the caller's context", async () => {
    const { warnings, logger } = makeWarningLogger();
    const runStore = await createRunStore({ dbPath: tmpPath("wake-throw-runs") });
    harness = { runStore };
    const target = randomUUID();
    await runStore.insert({ flowId: target, ownerKey: "test", pipelineId: "p", revision: 0, status: "queued", lastStep: "__start__", stateJson: {} });

    const result = await wakePromotedRelease(
      { released: true, promoted: { id: randomUUID(), kind: "child-dispatch", flowId: target, nodeId: "LOOP" } },
      { runStore, resume: async () => { throw new Error("boom"); }, logger, api: {}, context: "throwing-caller" },
    );

    assert.equal(result.resumed, true, "the releaser only learns the flip succeeded and the run was scheduled");
    await drainDetachedResumes();
    assert.ok(
      warnings.some((w) => /^throwing-caller: detached resume \[.+\] failed: boom$/.test(w)),
      `expected a throwing-caller-tagged warning among: ${JSON.stringify(warnings)}`,
    );
  });

  // A throw reaching wakePromotedRelease's own try/catch needs something
  // in the awaited part — e.g. runStore.get() inside flipToRunningAndResume.
  test("a throw from upstream of resume() (e.g. runStore.get failing) is caught here, logged, and returned as reason:'threw'", async () => {
    const { warnings, logger } = makeWarningLogger();
    const throwingRunStore = { async get() { throw new Error("db exploded"); } };

    const result = await wakePromotedRelease(
      { released: true, promoted: { id: randomUUID(), kind: "flow-start", flowId: randomUUID(), nodeId: null } },
      { runStore: throwingRunStore, resume: async () => {}, logger, api: {}, context: "throwing-caller" },
    );

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "threw");
    assert.equal(result.error, "db exploded");
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /^throwing-caller: resume after promotion \[.+\] failed: db exploded$/);
  });
});
