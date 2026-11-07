// ws-dialog.test.js — WS message transport into open discuss sessions:
// connect-time bearer gate, the message/ack/reply/error protocol, and the
// turn lifecycle (persist, stamp, run, reply, emit) against stub deps.
// Real server + real sockets on an ephemeral port.
import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { createDialogServer } from "../../../events/ws-dialog.js";

const BEARER = "test-bearer-dialog";
let savedBearer;
let srv;
let sockets;

const FLOW = {
  flowId: "flow-dialog-1",
  stateJson: { pipelineId: "p1" },
  waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "i1" }] },
};

const TARGET = {
  ok: true,
  session: "agent:op:subagent:gf-99d6948f-propose",
  itemId: "i1",
  agent: "op",
  needsStamp: true,
  gateNodeId: "GATE",
  modelOverride: null,
};

function makeDeps(overrides = {}) {
  const calls = { append: [], status: [], stamp: [], lineage: [], emit: [], run: [] };
  return {
    calls,
    readFlow: async (id) => (id === FLOW.flowId ? { ...FLOW } : null),
    resolveTarget: (flow, itemId) => ({ ...TARGET }),
    runTurn: async (turn) => {
      calls.run.push(turn);
      return { ok: true, reply: `echo: ${turn.text}`, proposedDraft: null, proposedMeta: null, durationMs: 7 };
    },
    discussStore: {
      appendMessage: async (flowId, itemId, msg) => { calls.append.push({ flowId, itemId, msg }); },
      setTurnStatus: async (flowId, itemId, status, turnId) => { calls.status.push({ flowId, itemId, status, turnId }); },
      clearPending: async (flowId, itemId) => { calls.status.push({ flowId, itemId, status: "cleared" }); },
      stampMessageError: async (flowId, itemId, turnId, error) => { calls.status.push({ flowId, itemId, status: "error", turnId, error }); },
    },
    sessionIndex: {
      async recordThread(data) { calls.stamp.push(data); },
      async findThread() { return null; },
    },
    recordLineage: async (entry) => { calls.lineage.push(entry); },
    emit: async (event, payload) => { calls.emit.push({ event, payload }); return { ok: true }; },
    logger: { warn: () => {}, info: () => {} },
    ...overrides,
  };
}

beforeEach(() => {
  savedBearer = process.env.MANIFOLD_BEARER;
  process.env.MANIFOLD_BEARER = BEARER;
  sockets = [];
});

afterEach(async () => {
  for (const s of sockets) {
    try { s.close(); } catch {}
  }
  sockets = [];
  if (srv) await srv.stop();
  srv = undefined;
  if (savedBearer === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = savedBearer;
});

async function start(deps) {
  srv = createDialogServer({ port: 0, host: "127.0.0.1", ...deps });
  await srv.start();
  return srv.url().replace(/^ws:/, "http:");
}

function connect(url, qs = "") {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${url.replace(/^http/, "ws")}${qs}`);
    sockets.push(ws);
    ws.inbox = [];
    ws.onmessage = (e) => { ws.inbox.push(JSON.parse(String(e.data))); };
    const timer = setTimeout(() => reject(new Error("ws open timed out")), 5000);
    ws.onopen = () => { clearTimeout(timer); resolve(ws); };
    ws.onerror = () => { /* handshake failures surface as close, not error payloads */ };
    ws.onclose = () => { clearTimeout(timer); resolve(ws); };
  });
}

function collect(ws, count, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (ws.inbox.length >= count) {
        clearInterval(timer);
        resolve(ws.inbox.splice(0, count));
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error(`timed out waiting for ${count} frames, got ${ws.inbox.length}`));
      }
    }, 10);
  });
}

test("rejects connects without (or with a wrong) bearer", async () => {
  const deps = makeDeps();
  const base = await start(deps);
  const noBearer = await connect(base, "");
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(noBearer.inbox[0]?.type, "error");
  assert.equal(noBearer.inbox[0]?.code, "unauthorized");
  const wrong = await connect(base, "?bearer=nope");
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(wrong.inbox[0]?.code, "unauthorized");
});

test("full turn: ack, operator persist, stamp, run, reply, emit", async () => {
  const deps = makeDeps();
  const base = await start(deps);
  const ws = await connect(base, `?bearer=${BEARER}`);
  ws.send(JSON.stringify({ type: "message", id: "c1", flowId: FLOW.flowId, itemId: "i1", text: "hello agent" }));
  const [ack, reply] = await collect(ws, 2);
  assert.equal(ack.type, "ack");
  assert.equal(ack.id, "c1");
  assert.equal(typeof ack.turnId, "string");
  assert.equal(reply.type, "reply");
  assert.equal(reply.id, "c1");
  assert.equal(reply.turnId, ack.turnId);
  assert.equal(reply.text, "echo: hello agent");
  assert.equal(deps.calls.run.length, 1);
  assert.equal(deps.calls.run[0].session, TARGET.session);
  // operator message + running status persisted, session stamped, lineage recorded
  assert.ok(deps.calls.append.some((a) => a.msg.role === "operator" && a.msg.turnId === ack.turnId));
  assert.ok(deps.calls.append.some((a) => a.msg.role === "agent" && a.msg.text === "echo: hello agent"));
  assert.ok(deps.calls.status.some((s) => s.status === "running" && s.turnId === ack.turnId));
  assert.ok(deps.calls.status.some((s) => s.status === "idle"));
  assert.equal(deps.calls.stamp.length, 1);
  assert.equal(deps.calls.lineage.length, 1);
  assert.ok(deps.calls.emit.some((e) => e.event === "session.transcript.completed" && e.payload.turnId === ack.turnId));
});

test("unknown flow, bad target, and run failure surface as error frames", async () => {
  const failing = makeDeps({
    resolveTarget: () => ({ ok: false, status: 404, code: "no_pending_gate", error: "no gate" }),
  });
  const base = await start(failing);
  const ws = await connect(base, `?bearer=${BEARER}`);
  ws.send(JSON.stringify({ type: "message", id: "a", flowId: "missing", text: "hi" }));
  const [notFound] = await collect(ws, 1);
  assert.equal(notFound.code, "flow_not_found");
  ws.send(JSON.stringify({ type: "message", id: "b", flowId: FLOW.flowId, text: "hi" }));
  const [noTarget] = await collect(ws, 1);
  assert.equal(noTarget.code, "no_pending_gate");

  const throwing = makeDeps({
    runTurn: async () => { throw new Error("boom"); },
  });
  await srv.stop();
  srv = undefined;
  const base2 = await start(throwing);
  const ws2 = await connect(base2, `?bearer=${BEARER}`);
  ws2.send(JSON.stringify({ type: "message", id: "c", flowId: FLOW.flowId, text: "hi" }));
  const [ack2, err2] = await collect(ws2, 2);
  assert.equal(ack2.type, "ack");
  assert.equal(err2.type, "error");
  assert.equal(err2.turnId, ack2.turnId);
  assert.ok(throwing.calls.emit.some((e) => e.event === "session.transcript.failed"));
});

test("malformed frames are rejected without killing the socket", async () => {
  const deps = makeDeps();
  const base = await start(deps);
  const ws = await connect(base, `?bearer=${BEARER}`);
  ws.send("not json{{");
  const [bad] = await collect(ws, 1);
  assert.equal(bad.code, "invalid_frame");
  ws.send(JSON.stringify({ type: "ping" }));
  const [unsupported] = await collect(ws, 1);
  assert.equal(unsupported.code, "unsupported_type");
  ws.send(JSON.stringify({ type: "message", flowId: FLOW.flowId }));
  const [missing] = await collect(ws, 1);
  assert.equal(missing.code, "invalid_body");
  // socket still alive: a good turn works afterwards
  ws.send(JSON.stringify({ type: "message", id: "ok", flowId: FLOW.flowId, text: "again" }));
  const [ack, reply] = await collect(ws, 2);
  assert.equal(ack.type, "ack");
  assert.equal(reply.type, "reply");
});

test("requires readFlow, resolveTarget, runTurn, discussStore, sessionIndex", () => {
  const deps = makeDeps();
  for (const key of ["readFlow", "resolveTarget", "runTurn", "discussStore", "sessionIndex"]) {
    const partial = { ...deps };
    delete partial[key];
    assert.throws(() => createDialogServer({ port: 0, ...partial }), new RegExp(key));
  }
});
