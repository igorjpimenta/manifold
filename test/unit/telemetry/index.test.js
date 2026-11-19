import { describe, it, before, mock } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { registerTelemetry } from "../../../telemetry/index.js";

// ── helpers ────────────────────────────────────────────────────────────────────

function makeMockApi() {
  const handlers = {};
  const services = [];
  const routes = {};
  return {
    _handlers: handlers,
    _services: services,
    _routes: routes,
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    on: (event, handler) => { handlers[event] = handler; },
    registerService: (svc) => services.push(svc),
    registerHttpRoute: (spec) => { routes[spec.path] = spec; },
    registerCli: () => {},
  };
}

process.env.MANIFOLD_INGEST_TOKEN = "test-ingest";

// Post one canonical telemetry event through the ingest route, the way a
// harness adapter does.
async function ingest(api, payload) {
  const route = api._routes["/manifold/telemetry/event"];
  const req = {
    method: "POST",
    headers: { authorization: "Bearer test-ingest" },
    async *[Symbol.asyncIterator]() { yield Buffer.from(JSON.stringify(payload)); },
  };
  const res = { statusCode: 0, setHeader() {}, end() {} };
  await route.handler(req, res);
  return res.statusCode;
}

// A canonical cost.usage event for a known provider/model so resolveCost
// returns a non-null result and the insertion path runs to completion.
function makeEvent(overrides = {}) {
  return {
    kind: "cost.usage",
    session: `test-sk-${randomUUID()}`,
    messageId: randomUUID(),
    provider: "openrouter",
    model: "deepseek/deepseek-v4-flash",
    timestamp: Date.now(),
    usage: { input: 1000, output: 500, cost: { total: 0.001 } },
    ...overrides,
  };
}

// ── before_message_write → cost.record.added ───────────────────────────────────

describe("telemetry ingest — cost.record.added broadcast", () => {
  let ingestApi;
  const broadcastFn = mock.fn();

  before(async () => {
    const tmpPath = join(tmpdir(), `telemetry-broadcast-test-${randomUUID()}.sqlite`);
    const api = makeMockApi();
    registerTelemetry(api, { getBroadcast: () => broadcastFn, _dbPath: tmpPath });
    ingestApi = api;
    const svc = api._services.find(s => s.id === "lineage-service");
    await svc.start();
  });

  it("emits 'cost.record.added' when a cost record is inserted", async () => {
    broadcastFn.mock.resetCalls();
    await ingest(ingestApi, makeEvent());
    const call = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.cost.record.added");
    assert.ok(call, "cost.record.added broadcast must fire");
  });

  it("payload includes a numeric costTotal > 0", async () => {
    broadcastFn.mock.resetCalls();
    await ingest(ingestApi, makeEvent());
    const call = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.cost.record.added");
    assert.ok(call, "broadcast must fire");
    const { costTotal } = call.arguments[1];
    assert.ok(typeof costTotal === "number", `costTotal must be a number, got ${typeof costTotal}`);
    assert.ok(costTotal > 0, `costTotal must be positive, got ${costTotal}`);
  });

  it("does NOT emit when event has no cost data (hasCost guard)", async () => {
    broadcastFn.mock.resetCalls();
    const eventNoCost = makeEvent({ usage: undefined });
    await ingest(ingestApi, eventNoCost);
    const call = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.cost.record.added");
    assert.ok(!call, "must not broadcast when usage.cost.total is missing");
  });

  it("uses pre-computed cost.total from usage when model is unknown", async () => {
    // resolveCost falls back to usage.cost.total when the model isn't in the
    // known-models table — so broadcast still fires with costTotal == srcTotal.
    broadcastFn.mock.resetCalls();
    const eventUnknown = makeEvent({ provider: "unknown-provider", model: "unknown-model" });
    await ingest(ingestApi, eventUnknown);
    const call = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.cost.record.added");
    assert.ok(call, "broadcast must fire using pre-computed srcTotal");
    assert.ok(call.arguments[1].costTotal > 0, "costTotal comes from usage.cost.total");
  });

  it("does NOT throw when getBroadcast is undefined", async () => {
    const api = makeMockApi();
    const tmpPath = join(tmpdir(), `telemetry-broadcast-test-${randomUUID()}.sqlite`);
    registerTelemetry(api, { getBroadcast: undefined, _dbPath: tmpPath });
    const svc = api._services.find(s => s.id === "lineage-service");
    await svc.start();
    await assert.doesNotReject(() => ingest(api, makeEvent()));
  });

  it("does NOT throw when getBroadcast returns null", async () => {
    const api = makeMockApi();
    const tmpPath = join(tmpdir(), `telemetry-broadcast-test-${randomUUID()}.sqlite`);
    registerTelemetry(api, { getBroadcast: () => null, _dbPath: tmpPath });
    const svc = api._services.find(s => s.id === "lineage-service");
    await svc.start();
    await assert.doesNotReject(() => ingest(api, makeEvent()));
  });

  it("does NOT throw when getBroadcast throws internally", async () => {
    const api = makeMockApi();
    const tmpPath = join(tmpdir(), `telemetry-broadcast-test-${randomUUID()}.sqlite`);
    registerTelemetry(api, { getBroadcast: () => { throw new Error("exploded"); }, _dbPath: tmpPath });
    const svc = api._services.find(s => s.id === "lineage-service");
    await svc.start();
    await assert.doesNotReject(() => ingest(api, makeEvent()));
  });
});
