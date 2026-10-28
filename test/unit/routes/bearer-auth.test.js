// bearer-auth.test.js — the winch-style bearer guard: scheme, secret,
// timing-safe compare, fail-closed unset secret, and the 401 shape.
import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { authorizeBearerRequest, sendUnauthorized } from "../../../routes/bearer-auth.js";
import { createMockRes } from "../../helpers/mock-api.js";

const SECRET = "unit-test-bearer-secret";
let saved;

beforeEach(() => {
  saved = process.env.MANIFOLD_BEARER;
  process.env.MANIFOLD_BEARER = SECRET;
});

afterEach(() => {
  if (saved === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = saved;
});

const req = (authorization) => ({ headers: { authorization } });

test("accepts the exact bearer token", () => {
  assert.deepEqual(authorizeBearerRequest(req(`Bearer ${SECRET}`)), { ok: true });
});

test("rejects a wrong token", () => {
  assert.deepEqual(authorizeBearerRequest(req("Bearer wrong")), { ok: false, reason: "no-match" });
});

test("rejects missing or non-Bearer authorization", () => {
  assert.deepEqual(authorizeBearerRequest({ headers: {} }).reason, "no-bearer");
  assert.deepEqual(authorizeBearerRequest(req("Basic abc")).reason, "no-bearer");
  assert.deepEqual(authorizeBearerRequest(req("Bearer ")).reason, "empty-token");
});

test("fails closed when the secret is unset, warning once", () => {
  delete process.env.MANIFOLD_BEARER;
  const warnings = [];
  const logger = { warn: (m) => warnings.push(m) };
  assert.deepEqual(authorizeBearerRequest(req("Bearer anything"), { logger }).reason, "no-secret");
  assert.deepEqual(authorizeBearerRequest(req("Bearer anything"), { logger }).reason, "no-secret");
  assert.equal(warnings.length, 1);
});

test("sendUnauthorized emits a 401 JSON body", () => {
  const res = createMockRes();
  sendUnauthorized(res, "no-match");
  assert.equal(res.statusCode, 401);
  assert.deepEqual(JSON.parse(res.body), { error: "unauthorized", reason: "no-match" });
});
