import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createNodeRunner } from "../../../engine/executor-nodes.js";
import { NodeBlocked } from "../../../engine/executor-errors.js";
import { currentLockOwner } from "../../../locks/lock.js";
import { resolveInvocationId, clearTicket } from "../../../state/tool-handoff.js";

// ── helpers ───────────────────────────────────────────────────────────────────

const logger = { info: () => {}, warn: () => {}, error: () => {} };
const api = { runtime: {} };

// Minimal nodeExecutionsStore covering what assembleState()/makeNeedsCtx()
// touch via optional chaining (so a missing method there is a harmless
// no-op overlay) PLUS the two gate_records methods the human-gate branch
// calls directly, unconditionally — a real store (or the production no-op
// fallback) always has these, so the branch doesn't optional-chain them;
// this mock has to provide them for the same reason.
function makeNodeExecutionsStore(overrides = {}) {
  const gateRecords = new Map();
  return {
    getGateRecord: async (flowId, nodeId) => gateRecords.get(`${flowId}:${nodeId}`) ?? null,
    writeGateRecord: async (flowId, nodeId, record) => { gateRecords.set(`${flowId}:${nodeId}`, record); return { ok: true }; },
    ...overrides,
  };
}

function makeRunner(opts = {}) {
  return createNodeRunner({
    executeSubflowNode: opts.executeSubflowNode ?? (async () => ({ stateUpdate: {} })),
    withCrossProcessLockMaybe: (_lock, fn) => fn(),
    getNodeCtx: () => ({ invokePipeline: () => ({ invoked: false }) }),
    api,
    logger,
    emit: opts.emit,
    runStore: opts.runStore,
    nodeExecutionsStore: opts.nodeExecutionsStore ?? makeNodeExecutionsStore(),
  });
}

// ── code kind ─────────────────────────────────────────────────────────────────

describe("executeNode — code kind", () => {
  it("calls node.run and returns stateUpdate", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "code", run: (state) => ({ count: (state.count ?? 0) + 1 }) };
    const result = await executeNode({ node, nodeId: "COUNTER", state: { count: 5 }, flowId: "flow-00000001" });
    assert.deepEqual(result, { stateUpdate: { count: 6 } });
  });

  it("detects _blocked sentinel from code node", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "code", run: () => ({ _blocked: { reason: "no url", needed: [], hint: "" } }) };
    const result = await executeNode({ node, nodeId: "SCOUT", state: {}, flowId: "flow-00000002" });
    assert.ok(result.blockedInfo, "should have blockedInfo");
    assert.equal(result.blockedInfo.reason, "no url");
  });

  it("detects _skipped sentinel from code node", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "code", run: () => ({ _skipped: { reason: "already done" } }) };
    const result = await executeNode({ node, nodeId: "DEDUP", state: {}, flowId: "flow-00000003" });
    assert.ok(result.skippedInfo, "should have skippedInfo");
    assert.equal(result.skippedInfo.reason, "already done");
  });

  it("ctx.blocked throws NodeBlocked which propagates", async () => {
    const { executeNode } = makeRunner();
    const node = {
      kind: "code",
      run: (_state, ctx) => { ctx.blocked("need input", "provide it"); },
    };
    await assert.rejects(
      () => executeNode({ node, nodeId: "NODE", state: {}, flowId: "flow-00000004" }),
      (err) => err instanceof NodeBlocked
    );
  });
});

// ── agent kind (pull-first: every agent node parks) ────────────────────────────

describe("executeNode — agent kind", () => {
  it("parks as a claim ticket and returns a reentrant agent-claim wait", async () => {
    const parked = [];
    const nodeExecutionsStore = makeNodeExecutionsStore({
      markPullReady: async (invocationId) => { parked.push(invocationId); return { ok: true }; },
    });
    const emitted = [];
    const { executeNode } = makeRunner({
      nodeExecutionsStore,
      emit: async (kind, payload) => { emitted.push({ kind, payload }); },
    });
    const node = { kind: "agent", agent: "cyrano" };
    const result = await executeNode({ node, nodeId: "DRAFT", state: { pipelineId: "p" }, flowId: "flow-00000005", invocationId: "inv-park-1" });
    assert.deepEqual(parked, ["inv-park-1"]);
    assert.equal(result.waitJson?.kind, "agent-claim");
    assert.equal(result.waitJson?.ticket, "inv-park-1");
    assert.equal(result.waitJson?.nodeId, "DRAFT");
    assert.equal(result.reentrant, true);
    assert.deepEqual(result.stateUpdate, {});
    assert.equal(resolveInvocationId("inv-park-1"), "inv-park-1", "park registers the ticket for claiming-agent tools");
    clearTicket("inv-park-1");
    assert.deepEqual(emitted.map((e) => e.kind), ["agent.claim.parked"]);
  });

  it("a reported ticket without apply() consumes the stored result as-is (nodes with no author logic keep raw trust)", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getClaim: async () => ({ claimState: "reported" }),
      getResultForNode: async () => ({ resultWrittenAt: 1, resultJson: JSON.stringify({ draft: "done" }) }),
      markApplied: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const node = { kind: "agent", agent: "cyrano" };
    const result = await executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f", invocationId: "inv-rep-1" });
    assert.deepEqual(result, { stateUpdate: { draft: "done" } });
  });

  it("resume runs node.apply with the reported output, once, in the push-dispatch ctx shape", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getClaim: async () => ({ claimState: "reported" }),
      getResultForNode: async () => ({ resultWrittenAt: 1, resultJson: JSON.stringify({ text: "raw" }) }),
      markApplied: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const seen = [];
    const node = {
      kind: "agent", agent: "cyrano",
      apply: (state, output, item, meta, needs) => {
        seen.push([state, output, item, meta, typeof needs]);
        return { shaped: output.text };
      },
    };
    const result = await executeNode({ node, nodeId: "DRAFT", state: { base: 1 }, flowId: "flow-abc", invocationId: "inv-apply-1" });
    assert.equal(seen.length, 1, "apply runs exactly once");
    assert.deepEqual(seen[0][0], { base: 1 }, "1st arg is the raw state (applyCtx)");
    assert.deepEqual(seen[0][1], { text: "raw" }, "2nd arg is the reported output");
    assert.equal(seen[0][2], undefined, "no subflow item on a top-level node");
    assert.deepEqual(seen[0][3], { flowId: "flow-abc", nodeId: "DRAFT", invocationId: "inv-apply-1" }, "4th arg is the apply meta");
    assert.equal(seen[0][4], "function", "5th arg is the lazy needs ctx");
    assert.deepEqual(result, { stateUpdate: { shaped: "raw" } }, "the apply return becomes the state update, not the raw report");
  });

  it("resume honors the single-arg named apply convention", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getClaim: async () => ({ claimState: "reported" }),
      getResultForNode: async () => ({ resultWrittenAt: 1, resultJson: JSON.stringify({ n: 2 }) }),
      markApplied: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const seen = [];
    const node = {
      kind: "agent", agent: "cyrano",
      apply: ({ state, output, item, meta, needs }) => {
        seen.push({ state, output, item, meta, hasNeeds: typeof needs === "function" });
        return { doubled: output.n * 2 };
      },
    };
    const result = await executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f", invocationId: "inv-apply-2" });
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0].output, { n: 2 });
    assert.equal(seen[0].item, undefined);
    assert.equal(seen[0].meta.nodeId, "DRAFT");
    assert.equal(seen[0].hasNeeds, true);
    assert.deepEqual(result, { stateUpdate: { doubled: 4 } });
  });

  it("a throwing apply fails the node instead of succeeding silently", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getClaim: async () => ({ claimState: "reported" }),
      getResultForNode: async () => ({ resultWrittenAt: 1, resultJson: JSON.stringify({}) }),
      markApplied: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const node = {
      kind: "agent", agent: "cyrano",
      apply: () => { throw new Error("apply boom"); },
    };
    await assert.rejects(
      () => executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f", invocationId: "inv-apply-3" }),
      /apply boom/,
    );
  });

  it("park builds the prompt through promptBuilder and persists it with the ticket", async () => {
    let storedPrompt;
    const nodeExecutionsStore = makeNodeExecutionsStore({
      markPullReady: async (invocationId, { prompt } = {}) => { storedPrompt = prompt; return { ok: true }; },
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const node = {
      kind: "agent", agent: "cyrano",
      promptBuilder: (state, { needs }) => `summarize ${state.topic} (needs:${typeof needs})`,
    };
    const result = await executeNode({ node, nodeId: "DRAFT", state: { topic: "q3" }, flowId: "flow-00000005", invocationId: "inv-park-p" });
    assert.equal(storedPrompt, "summarize q3 (needs:function)", "the exact promptBuilder output is what the ticket stores");
    assert.equal(result.waitJson?.kind, "agent-claim");
    assert.ok(typeof result.waitJson?.sessionKey === "string" && result.waitJson.sessionKey.startsWith("mf-"), "waitJson carries the minted core handle");
  });

  it("park honors explicit sessionKey and sessionKeyBuilder overrides verbatim", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      markPullReady: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const explicit = await executeNode({
      node: { kind: "agent", agent: "cyrano", sessionKey: "custom-key" },
      nodeId: "DRAFT", state: {}, flowId: "flow-00000005", invocationId: "inv-p1",
    });
    assert.equal(explicit.waitJson.sessionKey, "custom-key");
    const built = await executeNode({
      node: { kind: "agent", agent: "cyrano", sessionKeyBuilder: (state, flowId) => `built:${flowId}` },
      nodeId: "DRAFT", state: {}, flowId: "flow-00000005", invocationId: "inv-p2",
    });
    assert.equal(built.waitJson.sessionKey, "built:flow-00000005");
  });

  it("park refuses an agent node with no entry.agent", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      markPullReady: async () => ({ ok: true }),
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    await assert.rejects(
      () => executeNode({ node: { kind: "agent" }, nodeId: "DRAFT", state: {}, flowId: "f", invocationId: "inv-anon" }),
      /refusing anonymous dispatch/,
    );
  });

  it("a reported ticket with no stored result fails loudly", async () => {
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getClaim: async () => ({ claimState: "reported" }),
      getResultForNode: async () => null,
    });
    const { executeNode } = makeRunner({ nodeExecutionsStore });
    const node = { kind: "agent", agent: "cyrano" };
    await assert.rejects(
      () => executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f", invocationId: "inv-rep-2" }),
      /has no stored result/,
    );
  });
});

describe("executeNode — loop kind", () => {
  it("delegates to executeSubflowNode", async () => {
    let called = false;
    const { executeNode } = makeRunner({
      executeSubflowNode: async () => { called = true; return { stateUpdate: { ran: true } }; },
    });
    const node = { kind: "subflow", selector: () => [], apply: () => ({}) };
    const result = await executeNode({ node, nodeId: "LOOP", state: {}, flowId: "f" });
    assert.equal(called, true);
    assert.deepEqual(result.stateUpdate, { ran: true });
  });
});

// ── human-gate kind ───────────────────────────────────────────────────────────

describe("executeNode — human-gate kind", () => {
  it("returns empty stateUpdate when pending is empty", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "human-gate", pendingSelector: () => [], approvalRequestBuilder: () => ({}) };
    const result = await executeNode({ node, nodeId: "GATE", state: {}, flowId: "f" });
    assert.deepEqual(result, { stateUpdate: {} });
  });

  it("returns waitJson with items when pending is non-empty", async () => {
    const { executeNode } = makeRunner();
    const node = {
      kind: "human-gate",
      pendingSelector: () => ["item-1", "item-2"],
      approvalRequestBuilder: (_state, itemId) => ({ title: `Approve ${itemId}?` }),
    };
    const result = await executeNode({ node, nodeId: "GATE", state: {}, flowId: "f" });
    assert.ok(result.waitJson, "waitJson should be present");
    assert.equal(result.waitJson.kind, "human-gate");
    assert.equal(result.waitJson.items.length, 2);
    assert.equal(result.waitJson.items[0].itemId, "item-1");
  });

  it("sets reentrant:true when pending is non-empty (regression: engine/executor.js's waitJson-persist branch stamps lastStep to THIS node's own id unless reentrant is set, so a resume after a partial decision would skip re-checking pendingSelector and jump straight to this node's outgoing edge — see the loop path's identical reentrant:true for the same reason)", async () => {
    const { executeNode } = makeRunner();
    const node = {
      kind: "human-gate",
      pendingSelector: () => ["item-1", "item-2"],
      approvalRequestBuilder: (_state, itemId) => ({ title: `Approve ${itemId}?` }),
    };
    const result = await executeNode({ node, nodeId: "GATE", state: {}, flowId: "f" });
    assert.equal(result.reentrant, true);
  });

  it("does NOT set reentrant:true when pending is empty (gate genuinely satisfied — lastStep should advance to this node's own id so its outgoing edge is evaluated next)", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "human-gate", pendingSelector: () => [], approvalRequestBuilder: () => ({}) };
    const result = await executeNode({ node, nodeId: "GATE", state: {}, flowId: "f" });
    assert.notEqual(result.reentrant, true);
  });

  it("re-invokes pendingSelector with updated state on each call — simulates partial-decision resume staying open for remaining items", async () => {
    const { executeNode } = makeRunner();
    const decided = new Set();
    const allItems = ["spelling", "formatting", "links"];
    const node = {
      kind: "human-gate",
      pendingSelector: (state) => allItems.filter((id) => !(state.approvedIds ?? []).includes(id)),
      approvalRequestBuilder: (_state, itemId) => ({ title: `${itemId}?` }),
    };
    // First entry: nothing decided yet — all 3 pending, reentrant true.
    const first = await executeNode({ node, nodeId: "CHECKLIST", state: { approvedIds: [] }, flowId: "f" });
    assert.equal(first.waitJson.items.length, 3);
    assert.equal(first.reentrant, true);
    // Resume after ONE decision — pendingSelector re-run with updated
    // state must still report the other two as pending, still reentrant.
    const second = await executeNode({ node, nodeId: "CHECKLIST", state: { approvedIds: ["spelling"] }, flowId: "f" });
    assert.equal(second.waitJson.items.length, 2);
    assert.deepEqual(second.waitJson.items.map((i) => i.itemId), ["formatting", "links"]);
    assert.equal(second.reentrant, true);
    // All three decided — gate satisfied, no waitJson, not reentrant.
    const third = await executeNode({ node, nodeId: "CHECKLIST", state: { approvedIds: allItems }, flowId: "f" });
    assert.equal(third.waitJson, undefined);
    assert.notEqual(third.reentrant, true);
  });
});

// ── unknown kind ──────────────────────────────────────────────────────────────

describe("executeNode — unknown kind", () => {
  it("throws for unrecognized node kind", async () => {
    const { executeNode } = makeRunner();
    const node = { kind: "magic" };
    await assert.rejects(
      () => executeNode({ node, nodeId: "X", state: {}, flowId: "f" }),
      (err) => err.message.includes("unknown node kind: magic")
    );
  });
});

// ── lock wiring ───────────────────────────────────────────────────────────────

describe("executeNode — lock wiring", () => {
  it("calls withCrossProcessLockMaybe with the node's declared crossProcessLock", async () => {
    let lockNameSeen;
    const runner = createNodeRunner({
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (lockName, fn) => { lockNameSeen = lockName; return fn(); },
      getNodeCtx: () => ({}),
      api,
      logger,
    });
    const node = {
      kind: "code",
      crossProcessLock: "browser:freelance",
      run: () => ({ x: 1 }),
    };
    await runner.executeNode({ node, nodeId: "N", state: {}, flowId: "f" });
    assert.equal(lockNameSeen, "browser:freelance");
  });

  it("passes {flowId, nodeId} as the 3rd arg — the crossProcessLock queued-status hook needs both to persist a blocked-acquire transition", async () => {
    let contextSeen;
    const runner = createNodeRunner({
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn, ctx) => { contextSeen = ctx; return fn(); },
      getNodeCtx: () => ({}),
      api,
      logger,
    });
    const node = {
      kind: "code",
      crossProcessLock: "browser:linkedin",
      run: () => ({ x: 1 }),
    };
    await runner.executeNode({ node, nodeId: "SCRAPE_PROFILE", state: {}, flowId: "flow-00000099" });
    assert.deepEqual(contextSeen, {
      flowId: "flow-00000099",
      nodeId: "SCRAPE_PROFILE",
      // Always forwarded alongside flowId/nodeId (Workana
      // Turnstile incident) — undefined here because this node doesn't
      // declare crossProcessLockCooldownMs/crossProcessLockCooldownJitterMs.
      cooldownMs: undefined,
      cooldownJitterMs: undefined,
    });
  });

  it("forwards the node's crossProcessLockCooldownMs/crossProcessLockCooldownJitterMs as cooldownMs/cooldownJitterMs on the 3rd arg", async () => {
    let contextSeen;
    const runner = createNodeRunner({
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn, ctx) => { contextSeen = ctx; return fn(); },
      getNodeCtx: () => ({}),
      api,
      logger,
    });
    const node = {
      kind: "code",
      crossProcessLock: "browser:freelance",
      crossProcessLockCooldownMs: 25_000,
      crossProcessLockCooldownJitterMs: 20_000,
      run: () => ({ x: 1 }),
    };
    await runner.executeNode({ node, nodeId: "SUBMIT_CODE", state: {}, flowId: "flow-00000100" });
    assert.deepEqual(contextSeen, {
      flowId: "flow-00000100",
      nodeId: "SUBMIT_CODE",
      cooldownMs: 25_000,
      cooldownJitterMs: 20_000,
    });
  });
});

// ── invocation lock owner ─────────────────────────────────────────────────────

describe("executeNode — invocation lock owner", () => {
  it("runs the node (and its crossProcessLock wrapper) under the invocation's lock owner", async () => {
    let ownerInWrapper;
    let ownerInRun;
    const runner = createNodeRunner({
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn) => { ownerInWrapper = currentLockOwner(); return fn(); },
      getNodeCtx: () => ({}),
      api,
      logger,
      nodeExecutionsStore: makeNodeExecutionsStore(),
    });
    const node = { kind: "code", crossProcessLock: "browser:freelance", run: () => { ownerInRun = currentLockOwner(); return {}; } };
    await runner.executeNode({ node, nodeId: "SCAN", state: { pipelineId: "p1" }, flowId: "flow-1", invocationId: "inv-1" });
    const expected = { kind: "invocation", id: "inv-1", flowId: "flow-1", nodeId: "SCAN", pipelineId: "p1" };
    assert.deepEqual(ownerInWrapper, expected);
    assert.deepEqual(ownerInRun, expected);
    assert.equal(currentLockOwner(), null, "the owner context must not leak past the node");
  });

  it("no invocationId → no lock owner (the lock stays unowned, as before)", async () => {
    let ownerInWrapper = "unset";
    const runner = createNodeRunner({
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn) => { ownerInWrapper = currentLockOwner(); return fn(); },
      getNodeCtx: () => ({}),
      api,
      logger,
      nodeExecutionsStore: makeNodeExecutionsStore(),
    });
    await runner.executeNode({ node: { kind: "code", run: () => ({}) }, nodeId: "N", state: {}, flowId: "f" });
    assert.equal(ownerInWrapper, null);
  });

;
});
