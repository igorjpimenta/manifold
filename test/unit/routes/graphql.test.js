/**
 * Unit tests for routes/graphql.js
 *
 * Verifies registration (exactly 1 route, POST /manifold/graphql),
 * method guard before auth, and happy-path execution with stub stores
 * (auth mocked — failures.test.js pattern).
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../plugins/openclaw/manifold/auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
  },
});

const { registerGraphqlRoutes } = await import("../../../routes/graphql.js");

function tmpDb(name) {
  const dir = path.join(os.tmpdir(), `graphql-route-test-${randomUUID()}`);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${name}.sqlite`);
}

async function seedOps(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
  await db.execute(`CREATE TABLE queue_items (
    id TEXT PRIMARY KEY, label TEXT NOT NULL, agent TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, flow_id TEXT, node TEXT
  )`);
  await db.execute(`CREATE TABLE gate_state (
    id TEXT NOT NULL PRIMARY KEY DEFAULT 'singleton', locked INTEGER NOT NULL DEFAULT 0, day TEXT
  )`);
  await db.execute("INSERT INTO gate_state (id, locked) VALUES ('singleton', 0)");
  db.close();
}

function makeDeps(api, overrides = {}) {
  return {
    api,
    failureStore: { list: async () => [] },
    runStore: { listRunsPage: async () => [], get: async () => null },
    runtimeDbPath: tmpDb("runtime"),
    opsDbPath: tmpDb("ops"),
    ...overrides,
  };
}

function postReq(body) {
  const text = typeof body === "string" ? body : JSON.stringify(body);
  return {
    ...createMockReq("POST", "/manifold/graphql"),
    [Symbol.asyncIterator]: async function* () { yield text; },
  };
}

describe("registerGraphqlRoutes — registration", () => {
  test("registers exactly 1 route at POST /manifold/graphql", () => {
    const { api, routes } = createMockApi();
    registerGraphqlRoutes(makeDeps(api));
    assert.equal(routes.length, 1);
    assert.equal(routes[0].path, "/manifold/graphql");
    assert.equal(routes[0].auth, "plugin");
    assert.equal(routes[0].match, "exact");
  });

  test("GET rejected with 405 before auth", async () => {
    const { api, routes } = createMockApi();
    registerGraphqlRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/graphql"), res);
    assert.equal(res.statusCode, 405);
  });

  test("authorized snapshot query executes against stub stores", async () => {
    const { api, routes } = createMockApi();
    const deps = makeDeps(api, {
      failureStore: { list: async () => [{ id: "f1", flow_id: "flow-1" }] },
      runStore: { listRunsPage: async () => [], get: async () => null },
    });
    await seedOps(deps.opsDbPath);
    registerGraphqlRoutes(deps);
    const res = createMockRes();
    await routes[0].handler(postReq({ query: "{ snapshot { failures { id } runningCount } }" }), res);
    assert.equal(res.statusCode, 200);
    const out = JSON.parse(res.body);
    assert.equal(out.data.snapshot.failures[0].id, "f1");
    assert.equal(out.data.snapshot.runningCount, 0);
  });

  test("invalid json body is a 400", async () => {
    const { api, routes } = createMockApi();
    registerGraphqlRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(postReq("{{{"), res);
    assert.equal(res.statusCode, 400);
  });
});
