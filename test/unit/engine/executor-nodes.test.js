import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createNodeRunner } from "../../../engine/executor-nodes.js";
import { NodeBlocked, GatewayHandoffNeeded } from "../../../engine/executor-errors.js";
import { currentLockOwner, sessionOwner, withLock, registerSessionOwner, clearSessionOwner, runWithLockOwner } from "../../../plugins/openclaw/manifold/lock.js";

// Lock service the harness plugin injects into the engine in production
// (plugins/openclaw/manifold/index.js) — the real implementation, so
// owner-context assertions below exercise the actual AsyncLocalStorage.
const testLocks = { withLock, registerSessionOwner, clearSessionOwner, runWithLockOwner };

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
    maybeBudgetGate: opts.maybeBudgetGate ?? (() => ({ proceed: true })),
    dispatchAgentWithRetry: opts.dispatchAgentWithRetry ?? (async ({ node, applyCtx }) => node.apply(applyCtx, { text: "mock" })),
    executeSubflowNode: opts.executeSubflowNode ?? (async () => ({ stateUpdate: {} })),
    withCrossProcessLockMaybe: (_lock, fn) => fn(),
    locks: testLocks,
    getNodeCtx: () => ({ invokePipeline: () => ({ invoked: false }) }),
    api,
    logger,
    nodeHealthStore: opts.nodeHealthStore,
    runStore: opts.runStore,
    nodeExecutionsStore: opts.nodeExecutionsStore ?? makeNodeExecutionsStore(),
  });
}

// Records isBlocked/markUnhealthy calls so tests can assert exactly what
// dispatchAgentEntry threaded through, without a real sqlite-backed store.
// Methods return Promises (not plain values) — the real store is async, and
// a sync mock here would let a missing `await` at the call site pass green
// (an un-awaited Promise from a sync mock is falsy-truthy-wrong in ways an
// un-awaited Promise from a real async call is not caught by).
function makeFakeNodeHealthStore(overrides = {}) {
  const marked = [];
  const isBlockedImpl = overrides.isBlocked ?? (() => false);
  return {
    isBlocked: async (...args) => isBlockedImpl(...args),
    markUnhealthy: async (pipelineId, nodeId, info) => {
      marked.push({ pipelineId, nodeId, ...info });
      return { marked: true };
    },
    marked,
  };
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

// ── agent kind ───────────────────────────────────────────────────────────────

describe("executeNode — agent kind", () => {
  it("returns stateUpdate from apply", async () => {
    const { executeNode } = makeRunner({
      dispatchAgentWithRetry: async ({ node, applyCtx }) => node.apply(applyCtx, { text: "output" }),
    });
    const node = {
      kind: "agent",
      agent: "cyrano",
      sessionKey: "agent:cyrano:draft",
      promptBuilder: () => "write a proposal",
      apply: (_state, output) => ({ draft: output.text }),
    };
    const result = await executeNode({ node, nodeId: "DRAFT", state: { pipelineId: "p" }, flowId: "flow-00000005" });
    assert.deepEqual(result.stateUpdate, { draft: "output" });
  });

  it("skips when budget gate returns skip", async () => {
    const { executeNode } = makeRunner({ maybeBudgetGate: () => ({ skip: true }) });
    const node = { kind: "agent", agent: "a", promptBuilder: () => "x", apply: () => ({ x: 1 }) };
    const result = await executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f" });
    assert.deepEqual(result, { stateUpdate: {} });
  });

  it("returns blockedInfo when budget gate returns pause", async () => {
    const { executeNode } = makeRunner({
      maybeBudgetGate: () => ({ pause: true, blockedInfo: { _kind: "budget-gate-approval", reason: "cap reached", hint: "" } }),
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", promptBuilder: () => "x", apply: () => ({}) };
    const result = await executeNode({ node, nodeId: "DRAFT", state: {}, flowId: "f" });
    assert.ok(result.blockedInfo);
    assert.equal(result.blockedInfo._kind, "budget-gate-approval");
  });
});

// ── agent kind — circuit breaker ──
// node.circuitBreaker: true is a generalist opt-in on any "agent" node,
// independent of which kind of flow it's running inside — separate from
// (and additive to) the OLD kind:"fallback" node's own circuit-breaker
// check in executor-fallback.js's runFallbackList, which never sets this
// flag on its own entries, so the two never double-apply for one dispatch.

describe("executeNode — agent kind: circuit breaker", () => {
  it("fails fast without dispatching when already blocked", async () => {
    let dispatched = false;
    const nodeHealthStore = makeFakeNodeHealthStore({ isBlocked: () => true });
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async () => { dispatched = true; return {}; },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    await assert.rejects(
      () => executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" }),
      /unhealthy agent dispatch/,
    );
    assert.equal(dispatched, false, "a blocked entry must never reach dispatchAgentWithRetry");
  });

  it("marks unhealthy on a genuine dispatch failure, error still propagates", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async () => { throw new Error("provider timeout"); },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    await assert.rejects(
      () => executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" }),
      /provider timeout/,
    );
    assert.equal(nodeHealthStore.marked.length, 1);
    assert.equal(nodeHealthStore.marked[0].pipelineId, "p");
    assert.equal(nodeHealthStore.marked[0].nodeId, "SCAN_AGENT");
    assert.equal(nodeHealthStore.marked[0].reason, "provider timeout");
  });

  it("does not mark unhealthy on success", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async ({ node, applyCtx }) => node.apply(applyCtx, { text: "ok" }),
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: (_s, o) => ({ text: o.text }) };
    const result = await executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" });
    assert.deepEqual(result.stateUpdate, { text: "ok" });
    assert.equal(nodeHealthStore.marked.length, 0);
  });

  it("ignores isBlocked/markUnhealthy entirely when circuitBreaker is not declared", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore({ isBlocked: () => true });
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async ({ node, applyCtx }) => node.apply(applyCtx, { text: "ok" }),
    });
    // No circuitBreaker flag — isBlocked() returning true must not matter.
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", promptBuilder: () => "x", apply: (_s, o) => ({ text: o.text }) };
    const result = await executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" });
    assert.deepEqual(result.stateUpdate, { text: "ok" });
    assert.equal(nodeHealthStore.marked.length, 0);
  });

  it("a budget-gate pause (NodeBlocked) does not mark unhealthy", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      maybeBudgetGate: () => ({ pause: true, blockedInfo: { _kind: "budget-gate-approval", reason: "cap reached", hint: "" } }),
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    const result = await executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" });
    assert.ok(result.blockedInfo, "a budget-gate pause is a wait, not a failure");
    assert.equal(nodeHealthStore.marked.length, 0);
  });

  it("a missing tool scope does NOT mark unhealthy — an operator grants that, not a retry", async () => {
    // A block has no TTL, so a configuration fault would park the node
    // forever behind something only a human can fix.
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async () => { throw new Error("missing scope: operator.write"); },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    await assert.rejects(
      () => executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" }),
      /missing scope/,
    );
    assert.equal(nodeHealthStore.marked.length, 0, "the error still propagates, but must not park the node");
  });

  it("a gateway-context mismatch does NOT mark unhealthy — structural, not a symptom", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async () => { throw new Error("subagent call requires gateway context: missing api"); },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    await assert.rejects(
      () => executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" }),
      /requires gateway context/,
    );
    assert.equal(nodeHealthStore.marked.length, 0);
  });

  it("a GatewayHandoffNeeded (subagent runtime unavailable in this context) does not mark unhealthy", async () => {
    const nodeHealthStore = makeFakeNodeHealthStore();
    const { executeNode } = makeRunner({
      nodeHealthStore,
      dispatchAgentWithRetry: async () => { throw new GatewayHandoffNeeded("subagent runtime unavailable on this api instance"); },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:test", circuitBreaker: true, promptBuilder: () => "x", apply: () => ({}) };
    await assert.rejects(
      () => executeNode({ node, nodeId: "SCAN_AGENT", state: { pipelineId: "p" }, flowId: "f" }),
      (err) => err instanceof GatewayHandoffNeeded,
    );
    assert.equal(nodeHealthStore.marked.length, 0, "a context limitation isn't evidence the agent itself is unhealthy");
  });
});

// ── loop kind ─────────────────────────────────────────────────────────────────

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
      maybeBudgetGate: () => ({ proceed: true }),
      dispatchAgentWithRetry: async ({ node }) => node.apply({}, { text: "x" }),
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (lockName, fn) => { lockNameSeen = lockName; return fn(); },
      locks: testLocks,
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
      maybeBudgetGate: () => ({ proceed: true }),
      dispatchAgentWithRetry: async ({ node }) => node.apply({}, { text: "x" }),
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn, ctx) => { contextSeen = ctx; return fn(); },
      locks: testLocks,
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
      maybeBudgetGate: () => ({ proceed: true }),
      dispatchAgentWithRetry: async ({ node }) => node.apply({}, { text: "x" }),
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn, ctx) => { contextSeen = ctx; return fn(); },
      locks: testLocks,
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
      maybeBudgetGate: () => ({ proceed: true }),
      dispatchAgentWithRetry: async () => ({}),
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn) => { ownerInWrapper = currentLockOwner(); return fn(); },
      locks: testLocks,
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
      maybeBudgetGate: () => ({ proceed: true }),
      dispatchAgentWithRetry: async () => ({}),
      executeSubflowNode: async () => ({ stateUpdate: {} }),
      withCrossProcessLockMaybe: (_lockName, fn) => { ownerInWrapper = currentLockOwner(); return fn(); },
      locks: testLocks,
      getNodeCtx: () => ({}),
      api,
      logger,
      nodeExecutionsStore: makeNodeExecutionsStore(),
    });
    await runner.executeNode({ node: { kind: "code", run: () => ({}) }, nodeId: "N", state: {}, flowId: "f" });
    assert.equal(ownerInWrapper, null);
  });

  it("an agent dispatch registers the invocation (with the node's lease cooldown) as its session's lock owner, cleared afterwards", async () => {
    let ownerDuringDispatch;
    const { executeNode } = makeRunner({
      dispatchAgentWithRetry: async ({ sessionKey }) => { ownerDuringDispatch = sessionOwner(sessionKey); return {}; },
    });
    const node = {
      kind: "agent", agent: "proposal-sender", sessionKey: "agent:proposal-sender:test-owner",
      browserLeaseCooldownMs: 60_000, browserLeaseCooldownJitterMs: 5_000,
      promptBuilder: () => "go", apply: () => ({}),
    };
    await executeNode({ node, nodeId: "SUBMIT", state: { pipelineId: "p2" }, flowId: "flow-2", invocationId: "inv-2" });
    assert.deepEqual(ownerDuringDispatch, {
      kind: "invocation", id: "inv-2", flowId: "flow-2", nodeId: "SUBMIT", pipelineId: "p2",
      cooldownMs: 60_000, cooldownJitterMs: 5_000,
    });
    assert.equal(sessionOwner("agent:proposal-sender:test-owner"), null);
  });

  it("the session owner is cleared even when the dispatch throws", async () => {
    const { executeNode } = makeRunner({
      dispatchAgentWithRetry: async () => { throw new Error("boom"); },
    });
    const node = { kind: "agent", agent: "a", sessionKey: "agent:a:throws", promptBuilder: () => "go", apply: () => ({}) };
    await assert.rejects(() => executeNode({ node, nodeId: "X", state: {}, flowId: "flow-3", invocationId: "inv-3" }));
    assert.equal(sessionOwner("agent:a:throws"), null);
  });
});
