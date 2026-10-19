import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { mkdirSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import { queryRunningOrphans, cancelOrphan, reviveSession, reviveViaFlowRevival, scanAndCleanup, isPipeclawOwnedSession, REVIVAL_MSG } from "../../../boot/taskflow-revive.js";
import { createGatewaySubagentGate } from "../../../engine/gateway-subagent.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeTempDb(rows = []) {
  const dbPath = join(tmpdir(), `taskflow-test-${randomUUID()}.sqlite`);
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE task_runs (
      task_id TEXT PRIMARY KEY,
      agent_id TEXT,
      child_session_key TEXT,
      runtime TEXT,
      last_event_at INTEGER,
      status TEXT
    )
  `);
  const insert = db.prepare(
    "INSERT INTO task_runs (task_id, agent_id, child_session_key, runtime, last_event_at, status) VALUES (?,?,?,?,?,?)"
  );
  for (const r of rows) {
    insert.run(r.task_id, r.agent_id ?? null, r.child_session_key ?? null, r.runtime ?? null, r.last_event_at ?? null, r.status);
  }
  db.close();
  return dbPath;
}

function makeApi({ cfg = {}, cancelResult = { cancelled: true }, runResult = { runId: "run-1" }, cancelThrows = false, runThrows = false } = {}) {
  return {
    config: cfg,
    runtime: {
      config: { current: () => cfg },
      tasks: {
        runs: {
          bindSession: ({ sessionKey }) => ({
            cancel: async ({ taskId }) => {
              if (cancelThrows) throw new Error("cancel failed");
              return cancelResult;
            },
          }),
        },
      },
      subagent: {
        run: async ({ sessionKey, message }) => {
          if (runThrows) throw new Error("subagent failed");
          return runResult;
        },
      },
    },
  };
}

function makeAgentsRoot(agentId, sessions) {
  const root = join(tmpdir(), `taskflow-agents-${randomUUID()}`);
  const sessionsDir = join(root, agentId, "sessions");
  mkdirSync(sessionsDir, { recursive: true });
  writeFileSync(join(sessionsDir, "sessions.json"), JSON.stringify(sessions), "utf8");
  return root;
}

function makeLogger() {
  const lines = { info: [], warn: [] };
  return {
    info: (msg) => lines.info.push(msg),
    warn: (msg) => lines.warn.push(msg),
    _lines: lines,
  };
}

// ── REVIVAL_MSG ───────────────────────────────────────────────────────────────

describe("REVIVAL_MSG", () => {
  it("is a non-empty string", () => {
    assert.equal(typeof REVIVAL_MSG, "string");
    assert.ok(REVIVAL_MSG.length > 0);
  });
});

// ── queryRunningOrphans ───────────────────────────────────────────────────────

describe("queryRunningOrphans", () => {
  it("returns [] when db file does not exist", async () => {
    const rows = await queryRunningOrphans("/tmp/does-not-exist-taskflow-test.sqlite");
    assert.deepEqual(rows, []);
  });

  it("returns [] when no running rows exist", async () => {
    const dbPath = makeTempDb([
      { task_id: "t1", status: "completed" },
      { task_id: "t2", status: "cancelled" },
    ]);
    assert.deepEqual(await queryRunningOrphans(dbPath), []);
  });

  it("returns only running rows", async () => {
    const dbPath = makeTempDb([
      { task_id: "t1", status: "running", child_session_key: "agent:main:cron:x", last_event_at: 1000 },
      { task_id: "t2", status: "completed", child_session_key: "agent:main:cron:y" },
      { task_id: "t3", status: "running", child_session_key: "agent:scorer:sub:z", last_event_at: 2000 },
    ]);
    const rows = await queryRunningOrphans(dbPath);
    assert.equal(rows.length, 2);
    const ids = rows.map((r) => r.task_id);
    assert.ok(ids.includes("t1"));
    assert.ok(ids.includes("t3"));
  });

  it("orders by last_event_at DESC", async () => {
    const dbPath = makeTempDb([
      { task_id: "old", status: "running", last_event_at: 100 },
      { task_id: "new", status: "running", last_event_at: 999 },
    ]);
    const rows = await queryRunningOrphans(dbPath);
    assert.equal(rows[0].task_id, "new");
    assert.equal(rows[1].task_id, "old");
  });
});

// ── cancelOrphan ──────────────────────────────────────────────────────────────

describe("cancelOrphan", () => {
  it("returns ok:false when cfg unavailable", async () => {
    const api = { config: null, runtime: { config: { current: () => null } } };
    const r = await cancelOrphan(api, "sk", "tid");
    assert.equal(r.ok, false);
    assert.ok(r.error.includes("no cfg"));
  });

  it("returns ok:false when runs.bindSession unavailable", async () => {
    const api = { config: {}, runtime: { config: { current: () => ({}) }, tasks: {} } };
    const r = await cancelOrphan(api, "sk", "tid");
    assert.equal(r.ok, false);
    assert.ok(r.error.includes("bindSession unavailable"));
  });

  it("returns ok:true on successful cancel", async () => {
    const api = makeApi({ cancelResult: { cancelled: true } });
    const r = await cancelOrphan(api, "sk", "task-1");
    assert.equal(r.ok, true);
  });

  it("returns ok:false when cancelled=false", async () => {
    const api = makeApi({ cancelResult: { cancelled: false, reason: "not found" } });
    const r = await cancelOrphan(api, "sk", "task-1");
    assert.equal(r.ok, false);
    assert.equal(r.reason, "not found");
  });

  it("returns ok:false on thrown error", async () => {
    const api = makeApi({ cancelThrows: true });
    const r = await cancelOrphan(api, "sk", "task-1");
    assert.equal(r.ok, false);
    assert.ok(r.error.includes("cancel failed"));
  });
});

// ── reviveSession ─────────────────────────────────────────────────────────────

describe("reviveSession", () => {
  it("returns ok:false when subagent unavailable", async () => {
    const r = await reviveSession(undefined, "sk");
    assert.equal(r.ok, false);
    assert.ok(r.error.includes("subagent.run unavailable"));
  });

  it("returns ok:true with runId on success", async () => {
    const api = makeApi({ runResult: { runId: "run-42" } });
    const r = await reviveSession(api.runtime.subagent, "agent:main:cron:x");
    assert.equal(r.ok, true);
    assert.equal(r.runId, "run-42");
  });

  it("passes REVIVAL_MSG as message to subagent.run", async () => {
    let capturedMsg = null;
    const subagent = { run: async ({ message }) => { capturedMsg = message; return {}; } };
    await reviveSession(subagent, "sk");
    assert.equal(capturedMsg, REVIVAL_MSG);
  });

  it("returns ok:false on thrown error", async () => {
    const api = makeApi({ runThrows: true });
    const r = await reviveSession(api.runtime.subagent, "sk");
    assert.equal(r.ok, false);
    assert.ok(r.error.includes("subagent failed"));
  });
});

// ── isPipeclawOwnedSession ──────────────────────────────────────────────────────

describe("isPipeclawOwnedSession", () => {
  it("returns true when the session store's pluginOwnerId is 'pipeclaw'", () => {
    const sessionKey = "agent:scorer:subagent:custom-key-not-gf-shaped";
    const agentsRoot = makeAgentsRoot("scorer", { [sessionKey]: { pluginOwnerId: "pipeclaw" } });
    assert.equal(isPipeclawOwnedSession({ agentId: "scorer", sessionKey }, { agentsRoot }), true);
  });

  it("returns true for the legacy 'graph-flow' pluginOwnerId", () => {
    const sessionKey = "agent:scorer:subagent:another-custom-key";
    const agentsRoot = makeAgentsRoot("scorer", { [sessionKey]: { pluginOwnerId: "graph-flow" } });
    assert.equal(isPipeclawOwnedSession({ agentId: "scorer", sessionKey }, { agentsRoot }), true);
  });

  it("returns false when the store says a different plugin owns it, even if the key looks pipeclaw-shaped", () => {
    // The authoritative store record wins over the naming-convention heuristic.
    const sessionKey = "agent:scorer:subagent:gf-99d6948f-score_llm";
    const agentsRoot = makeAgentsRoot("scorer", { [sessionKey]: { pluginOwnerId: "some-other-plugin" } });
    assert.equal(isPipeclawOwnedSession({ agentId: "scorer", sessionKey }, { agentsRoot }), false);
  });

  it("falls back to the sessionKey heuristic when the agent has no session store on disk", () => {
    const agentsRoot = join(tmpdir(), `taskflow-agents-missing-${randomUUID()}`);
    assert.equal(
      isPipeclawOwnedSession({ agentId: "nonexistent-agent", sessionKey: "agent:scorer:subagent:gf-99d6948f-score_llm" }, { agentsRoot }),
      true,
    );
    assert.equal(
      isPipeclawOwnedSession({ agentId: "nonexistent-agent", sessionKey: "agent:main:cron:some-job" }, { agentsRoot }),
      false,
    );
  });

  it("falls back to the sessionKey heuristic when the session isn't in the store", () => {
    const agentsRoot = makeAgentsRoot("scorer", { "agent:scorer:subagent:unrelated": { pluginOwnerId: "pipeclaw" } });
    assert.equal(
      isPipeclawOwnedSession({ agentId: "scorer", sessionKey: "agent:scorer:subagent:gf-99d6948f-score_llm" }, { agentsRoot }),
      true,
    );
  });

  it("reuses a shared cache across calls for the same agentId", () => {
    const sessionKeyA = "agent:scorer:subagent:key-a";
    const sessionKeyB = "agent:scorer:subagent:key-b";
    const agentsRoot = makeAgentsRoot("scorer", {
      [sessionKeyA]: { pluginOwnerId: "pipeclaw" },
      [sessionKeyB]: { pluginOwnerId: "not-pipeclaw" },
    });
    const cache = new Map();
    assert.equal(isPipeclawOwnedSession({ agentId: "scorer", sessionKey: sessionKeyA }, { agentsRoot, cache }), true);
    assert.equal(isPipeclawOwnedSession({ agentId: "scorer", sessionKey: sessionKeyB }, { agentsRoot, cache }), false);
    assert.equal(cache.size, 1, "one parsed store cached per agentId, reused for both lookups");
  });
});

// ── scanAndCleanup ────────────────────────────────────────────────────────────

describe("scanAndCleanup", () => {
  it("logs no-orphans when db is empty", async () => {
    const dbPath = makeTempDb([]);
    const api = makeApi();
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.info.some((m) => m.includes("no pre-boot orphans")));
  });

  it("logs no-orphans when db file does not exist", async () => {
    const api = makeApi();
    const logger = makeLogger();
    await scanAndCleanup(api, "/tmp/nonexistent-taskflow.sqlite", 0, logger);
    assert.ok(logger._lines.info.some((m) => m.includes("no pre-boot orphans")));
  });

  it("cancels and revives a running orphan", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-uuid-0001", status: "running", child_session_key: "agent:main:x", last_event_at: 1000 },
    ]);
    const api = makeApi();
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.info.some((m) => m.includes("cancelled")));
    assert.ok(logger._lines.info.some((m) => m.includes("revived")));
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=1 revived=1")));
  });

  it("skips revival when child_session_key is null", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-no-sk-001", status: "running", child_session_key: null, last_event_at: 1000 },
    ]);
    const api = makeApi();
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.warn.some((m) => m.includes("missing child_session_key")));
    assert.ok(logger._lines.info.some((m) => m.includes("revived=0")));
  });

  it("warns on cancel failure but continues to next row", async () => {
    const dbPath = makeTempDb([
      { task_id: "fail-cancel-001", status: "running", child_session_key: "sk-1", last_event_at: 1000 },
      { task_id: "ok-cancel-00002", status: "running", child_session_key: "sk-2", last_event_at: 500 },
    ]);
    let callCount = 0;
    const api = {
      config: {},
      runtime: {
        config: { current: () => ({}) },
        tasks: {
          runs: {
            bindSession: () => ({
              cancel: async () => {
                callCount += 1;
                if (callCount === 1) return { cancelled: false, reason: "already gone" };
                return { cancelled: true };
              },
            }),
          },
        },
        subagent: { run: async () => ({ runId: "r" }) },
      },
    };
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.warn.some((m) => m.includes("cancel skipped")));
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=2")));
  });

  it("warns on revival failure but continues to next row", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-rev-fail-01", status: "running", child_session_key: "sk-x", last_event_at: 1000 },
    ]);
    const api = makeApi({ runThrows: true });
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.warn.some((m) => m.includes("revival failed")));
    assert.ok(logger._lines.info.some((m) => m.includes("revived=0")));
  });

  // Both pipeclaw-flow-revival and pipeclaw-taskflow-revive start at boot
  // with no ordering between them (startupDelayMs defaults to 0). A cancel
  // issued here by session key can tear down a fresh reviveAgentKindOrphan
  // dispatch already in flight under that exact deterministic key — the
  // dispatch then hangs with nothing left to observe or log it. A
  // pipeclaw-owned session must not be touched at all, not even to cancel it.
  it("skips a pipeclaw-owned session entirely — no cancel call, no revival dispatch", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-pipeclaw-01", status: "running", child_session_key: "agent:scorer:subagent:gf-99d6948f-score_llm", last_event_at: 1000 },
    ]);
    let cancelCalled = false;
    let runCalled = false;
    const api = {
      config: {},
      runtime: {
        config: { current: () => ({}) },
        tasks: { runs: { bindSession: () => ({ cancel: async () => { cancelCalled = true; return { cancelled: true }; } }) } },
        subagent: { run: async () => { runCalled = true; return { runId: "should-not-run" }; } },
      },
    };
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.equal(cancelCalled, false, "must not cancel a pipeclaw-owned session's task_run — it may be bound to a fresh dispatch already in flight");
    assert.equal(runCalled, false, "must not dispatch a competing turn on a pipeclaw-owned session");
    assert.ok(logger._lines.info.some((m) => m.includes("skipping") && m.includes("entirely") && m.includes("owned by pipeclaw")));
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=0 revived=0")));
  });

  // The actual point of the delegation: when sessionIndex/runStore/executor
  // are wired in, a pipeclaw-owned orphan is routed to the flow's own
  // revival function — the one that actually knows how to resolve the
  // pipeline/node and feed a reply back into flow state — instead of
  // either the generic cancel+reviveSession path or a silent skip.
  it("delegates a pipeclaw-owned orphan to the flow's own revival function", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-pipeclaw-02", status: "running", child_session_key: "agent:scorer:subagent:gf-99d6948f-score_llm", last_event_at: 1000 },
    ]);
    let cancelCalled = false;
    let genericRunCalled = false;
    const api = {
      config: {},
      runtime: {
        config: { current: () => ({}) },
        tasks: { runs: { bindSession: () => ({ cancel: async () => { cancelCalled = true; return { cancelled: true }; } }) } },
        subagent: { run: async () => { genericRunCalled = true; return { runId: "should-not-run" }; } },
      },
    };
    const flow = { flowId: "flow-99d6948f", status: "queued", waitJson: { kind: "agent-dispatch", nodeId: "SCORE_LLM" } };
    const sessionIndex = { flowIdForSessionKey: async (key) => (key === "agent:scorer:subagent:gf-99d6948f-score_llm" ? flow.flowId : null) };
    const runStore = { get: async (id) => (id === flow.flowId ? flow : null) };
    let delegatedFlow = null;
    let delegatedNodeId = null;
    const executor = { reviveAgentKindOrphan: async (f, nodeId) => { delegatedFlow = f; delegatedNodeId = nodeId; } };
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger, { sessionIndex, runStore, executor });
    assert.equal(cancelCalled, false, "delegation handles this session — the generic cancel path must not also run");
    assert.equal(genericRunCalled, false, "must not use the generic flow-unaware reviveSession");
    assert.equal(delegatedFlow, flow, "the resolved flow row is passed straight to reviveAgentKindOrphan");
    assert.equal(delegatedNodeId, "SCORE_LLM", "the in-flight node comes from the flow's own waitJson");
    assert.ok(logger._lines.info.some((m) => m.includes("delegated") && m.includes("flow revival")));
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=0 revived=0 delegated=1")));
  });

  // A failed delegation (flow already terminal, not found, or deps not
  // wired) must still skip the row entirely — never fall back to the
  // generic cancel+reviveSession path, which is exactly the race this
  // delegation exists to avoid for a pipeclaw-owned session.
  it("skips (does not fall back to generic cancel) when the delegated flow is already terminal", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-pipeclaw-03", status: "running", child_session_key: "agent:scorer:subagent:gf-deadbeef-score_llm", last_event_at: 1000 },
    ]);
    let cancelCalled = false;
    const api = {
      config: {},
      runtime: {
        config: { current: () => ({}) },
        tasks: { runs: { bindSession: () => ({ cancel: async () => { cancelCalled = true; return { cancelled: true }; } }) } },
        subagent: { run: async () => ({ runId: "should-not-run" }) },
      },
    };
    const flow = { flowId: "flow-terminal", status: "succeeded" };
    const sessionIndex = { flowIdForSessionKey: async () => flow.flowId };
    const runStore = { get: async () => flow };
    const executor = { reviveAgentKindOrphan: async () => { throw new Error("must not be called for a terminal flow"); } };
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger, { sessionIndex, runStore, executor });
    assert.equal(cancelCalled, false);
    assert.ok(logger._lines.info.some((m) => m.includes("skipping") && m.includes("flow_terminal")));
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=0 revived=0 delegated=0")));
  });
});

// ── reviveViaFlowRevival ─────────────────────────────────────────────────────

describe("reviveViaFlowRevival", () => {
  it("returns ok:false when sessionIndex/runStore/executor aren't wired", async () => {
    const result = await reviveViaFlowRevival({ child_session_key: "sk-x" });
    assert.equal(result.ok, false);
    assert.equal(result.reason, "flow_revival_deps_unavailable");
  });

  it("returns ok:false when the session key doesn't resolve to a flow", async () => {
    const result = await reviveViaFlowRevival(
      { child_session_key: "sk-x" },
      { sessionIndex: { flowIdForSessionKey: async () => null }, runStore: { get: async () => null }, executor: { reviveAgentKindOrphan: async () => {} } },
    );
    assert.equal(result.ok, false);
    assert.equal(result.reason, "flow_id_not_found");
  });

  it("returns ok:false when the resolved flow no longer exists", async () => {
    const result = await reviveViaFlowRevival(
      { child_session_key: "sk-x" },
      { sessionIndex: { flowIdForSessionKey: async () => "flow-gone" }, runStore: { get: async () => null }, executor: { reviveAgentKindOrphan: async () => {} } },
    );
    assert.equal(result.ok, false);
    assert.equal(result.reason, "flow_not_found");
  });

  it("returns ok:true and calls reviveAgentKindOrphan with the flow and its waitJson node id", async () => {
    const flow = { flowId: "flow-1", status: "queued", waitJson: { kind: "agent-dispatch", nodeId: "SCORE_LLM" } };
    let called = null;
    const result = await reviveViaFlowRevival(
      { child_session_key: "sk-x" },
      {
        sessionIndex: { flowIdForSessionKey: async () => flow.flowId },
        runStore: { get: async () => flow },
        executor: { reviveAgentKindOrphan: async (f, nodeId) => { called = { f, nodeId }; } },
      },
    );
    assert.equal(result.ok, true);
    assert.equal(result.flowId, flow.flowId);
    assert.deepEqual(called, { f: flow, nodeId: "SCORE_LLM" });
  });
});

// ── scanAndCleanup — dispatch through the executor's gated subagent ─────────

describe("scanAndCleanup — revival dispatch goes through the executor's subagent", () => {
  it("cancels and classifies while the gate holds the dispatch; dispatches once the gateway is ready", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-gated-01", status: "running", child_session_key: "agent:main:x", last_event_at: 1000 },
    ]);
    let runCalled = false;
    const api = {
      config: {},
      runtime: {
        config: { current: () => ({}) },
        tasks: { runs: { bindSession: () => ({ cancel: async () => ({ cancelled: true }) }) } },
      },
    };
    let resolveReady;
    const ready = new Promise((r) => { resolveReady = r; });
    const gate = createGatewaySubagentGate({ whenGatewayReady: () => ready });
    const raw = { run: async () => { runCalled = true; return { runId: "r" }; } };
    const executor = { getSubagent: () => gate(raw) };
    const logger = makeLogger();
    const donePromise = scanAndCleanup(api, dbPath, 0, logger, { executor });
    let doneSettled = false;
    donePromise.then(() => { doneSettled = true; });

    await new Promise((r) => setTimeout(r, 20));

    assert.equal(doneSettled, false, "the dispatch is held at the gate");
    assert.ok(logger._lines.info.some((m) => m.includes("cancelled task-gat")), "the cancel already happened");
    assert.equal(runCalled, false);

    resolveReady();
    await donePromise;
    assert.equal(runCalled, true);
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=1 revived=1")));
  });

  it("prefers the executor's subagent over api.runtime.subagent", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-exec-01", status: "running", child_session_key: "agent:main:x", last_event_at: 1000 },
    ]);
    const api = makeApi();
    let viaExecutor = 0;
    const executor = { getSubagent: () => ({ run: async () => { viaExecutor += 1; return { runId: "r" }; } }) };
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger, { executor });
    assert.equal(viaExecutor, 1);
  });

  it("falls back to api.runtime.subagent without an executor", async () => {
    const dbPath = makeTempDb([
      { task_id: "task-default-01", status: "running", child_session_key: "agent:main:x", last_event_at: 1000 },
    ]);
    const api = makeApi();
    const logger = makeLogger();
    await scanAndCleanup(api, dbPath, 0, logger);
    assert.ok(logger._lines.info.some((m) => m.includes("scanned=1 cancelled=1 revived=1")));
  });
});
