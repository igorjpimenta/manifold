
import { test, describe, before, after, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRealOps } from "../../../mcp/wire-ops.js";

const PIPELINE_SOURCE = `
export default {
  id: "wire_ops_pipe",
  state: {},
  nodes: {
    STEP: { kind: "code", run: async () => ({ STEP: { ok: true } }) },
  },
  edges: { __start__: "STEP", STEP: "__end__" },
};
`;

const SLOW_PIPELINE_SOURCE = `
export default {
  id: "wire_ops_slow",
  state: {},
  nodes: {
    WAIT: {
      kind: "code",
      run: async () => {
        await new Promise((r) => setTimeout(r, 400));
        return { WAIT: { ok: true } };
      },
    },
  },
  edges: { __start__: "WAIT", WAIT: "__end__" },
};
`;

let home;
let pipelinesDir;
let ops;

before(async () => {
  home = mkdtempSync(path.join(os.tmpdir(), "wire-ops-home-"));
  process.env.MANIFOLD_DATA_DIR = home;
  pipelinesDir = path.join(home, "pipelines");
  for (const [name, source] of [["pipe", PIPELINE_SOURCE], ["slow", SLOW_PIPELINE_SOURCE]]) {
    mkdirSync(path.join(pipelinesDir, name), { recursive: true });
    writeFileSync(path.join(pipelinesDir, name, "index.js"), source, "utf8");
  }
  ops = await createRealOps({ dbPath: path.join(home, "runtime.sqlite"), pipelinesDir, logger: null });
});

after(() => {
  delete process.env.MANIFOLD_DATA_DIR;
  try { rmSync(home, { recursive: true, force: true }); } catch {  }
});

async function waitForStatus(flowId, want, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await ops.runStatus({ flowId });
    if (last && want(last)) return last;
    await new Promise((r) => setTimeout(r, 25));
  }
  return last;
}

describe("createRealOps — surface", () => {
  test("exposes the six tool ops plus a logger hook", () => {
    for (const name of ["startRun", "runStatus", "listRuns", "approveGate", "cancelFlow"]) {
      assert.equal(typeof ops[name], "function", `${name} must be callable`);
    }
    assert.ok("logger" in ops);
  });

  test("starting an unknown pipeline rejects instead of hanging", async () => {
    await assert.rejects(() => ops.startRun({ pipelineId: "does_not_exist" }), /does_not_exist|unknown/i);
  });
});

describe("startRun / runStatus", () => {
  test("starts a code pipeline and drives it to succeeded", async () => {
    const record = await ops.startRun({ pipelineId: "wire_ops_pipe", args: { seed: 1 } });
    assert.ok(record.flowId, "startRun returns a flow record with a flowId");
    const flow = await waitForStatus(record.flowId, (f) => f.status === "succeeded");
    assert.equal(flow.status, "succeeded");
    assert.equal(flow.pipelineId, "wire_ops_pipe");
  });

  test("runStatus of an unknown flow returns null, not a throw", async () => {
    assert.equal(await ops.runStatus({ flowId: randomUUID() }), null);
  });
});

describe("listRuns", () => {
  test("returns recent runs and honours the status filter", async () => {
    const all = await ops.listRuns({});
    assert.ok(Array.isArray(all) && all.length >= 1);
    const succeeded = await ops.listRuns({ status: "succeeded" });
    assert.ok(succeeded.every((r) => r.status === "succeeded"));
    const piped = await ops.listRuns({ pipelineId: "wire_ops_pipe" });
    assert.ok(piped.every((r) => r.pipelineId === "wire_ops_pipe"));
  });

  test("limit is clamped to a positive integer, defaulting to 20", async () => {
    assert.ok((await ops.listRuns({ limit: 1 })).length <= 1);
    const def = await ops.listRuns({ limit: 0 });
    assert.ok(def.length <= 20);
  });
});

describe("cancelFlow", () => {
  test("cancels a run that is still executing", async () => {
    const record = await ops.startRun({ pipelineId: "wire_ops_slow" });
    await ops.cancelFlow({ flowId: record.flowId, reason: "test cancel" });
    const flow = await waitForStatus(record.flowId, (f) => f.status === "cancelled" || f.status === "succeeded");
    assert.ok(flow.status === "cancelled", `expected cancelled, got ${flow.status}`);
  });
});
