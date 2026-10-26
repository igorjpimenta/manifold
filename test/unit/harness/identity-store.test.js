import { describe, it } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { resolveStorePaths } from "../../../state/db.js";
import { createOpenClawHarness } from "../../../harness/openclaw.js";
import { isManifoldOwnedSessionKey } from "../../../engine/executor-helpers.js";

const DEFAULT_RUNTIME = path.join(os.homedir(), ".openclaw", "runtime.sqlite");
const DEFAULT_OPS = path.join(os.homedir(), ".openclaw", "ops.sqlite");

describe("resolveStorePaths", () => {
  it("prefers harness paths over env over default", () => {
    const harness = { id: "stub", storePaths: () => ({ dataDir: "/tmp/manifold-p2-test", runtimeDb: "/tmp/manifold-p2-test/runtime.sqlite", opsDb: "/tmp/manifold-p2-test/ops.sqlite" }) };
    const env = { MANIFOLD_RUNTIME_DB: "/tmp/env-r.sqlite", MANIFOLD_OPS_DB: "/tmp/env-o.sqlite" };
    assert.deepEqual(resolveStorePaths({ harness, env }), {
      runtimeDb: "/tmp/manifold-p2-test/runtime.sqlite",
      opsDb: "/tmp/manifold-p2-test/ops.sqlite",
    });
    assert.deepEqual(resolveStorePaths({ env }), {
      runtimeDb: "/tmp/env-r.sqlite",
      opsDb: "/tmp/env-o.sqlite",
    });
    assert.deepEqual(resolveStorePaths({ env: {} }), {
      runtimeDb: DEFAULT_RUNTIME,
      opsDb: DEFAULT_OPS,
    });
  });

  it("fails loudly on an incomplete harness answer", () => {
    const harness = { storePaths: () => ({ runtimeDb: "/tmp/r.sqlite" }) };
    assert.throws(() => resolveStorePaths({ harness, env: {} }), /storePaths/);
  });
});

describe("openclaw sessionKey identity", () => {
  it("matches :subagent:gf- shape and throws on missing agent", () => {
    const harness = createOpenClawHarness({});
    const flowId = "12345678-aaaa-bbbb-cccc-000000000000";
    const key = harness.sessionKey({ agent: "scout", flowId, nodeId: "PROPOSE" });
    assert.ok(key.includes(":subagent:gf-"), key);
    assert.ok(isManifoldOwnedSessionKey(key), key);
    assert.throws(
      () => harness.sessionKey({ flowId, nodeId: "PROPOSE" }),
      /unstamped|required/
    );
  });
});
