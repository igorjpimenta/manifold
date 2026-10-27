// identity.test.js — declaration refused-never-defaulted; per-call stamps.
import { test } from "node:test";
import assert from "node:assert/strict";
import { assertDeclaration, declaredFromEnv, resolveIdentity, identitySessionKey } from "../../../mcp/identity.js";

test("assertDeclaration: missing harness refuses; reserved refuses", () => {
  assert.throws(() => assertDeclaration({}), /missing: harness/);
  assert.throws(() => assertDeclaration({ harness: "unknown" }), /reserved/);
  assert.deepEqual(assertDeclaration({ harness: "openclaw" }), { harness: "openclaw" });
});

test("declaredFromEnv reads MANIFOLD_HARNESS, fails loudly without", () => {
  assert.deepEqual(declaredFromEnv({ MANIFOLD_HARNESS: "my-harness" }), { harness: "my-harness" });
  assert.throws(() => declaredFromEnv({}), /missing: harness/);
});

test("resolveIdentity: _meta.manifold wins; halves never splice", () => {
  const base = { harness: "openclaw" };
  const id = resolveIdentity({ meta: { manifold: { agent: "a", session: "s" } }, args: {}, base });
  assert.equal(id.agent, "a");
  assert.equal(id.session, "s");
  assert.throws(
    () => resolveIdentity({ meta: { manifold: { agent: "a" } }, args: { manifold_session: "s" }, base }),
    /halves in different channels/
  );
});

test("resolveIdentity: stamped args channel works; unstamped refused", () => {
  const base = { harness: "openclaw" };
  const id = resolveIdentity({ meta: null, args: { manifold_agent: "a", manifold_session: "s" }, base });
  assert.equal(idSession(id), "openclaw:a:s");
  assert.throws(() => resolveIdentity({ meta: null, args: {}, base }), /unresolved agent and session/);
});

function idSession(id) {
  return identitySessionKey(id);
}
