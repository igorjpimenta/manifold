/**
 * Unit tests for agent/tools/submit-draft.js — registerSubmitDraftTool (submit_draft).
 *
 * Focuses on the jobId auto-resolve behavior (resolveGateContext, mirroring
 * propose-draft.js's pattern) plus the pre-existing structural validation.
 * Uses real temp-dir file writes — the tool's I/O is a couple of small
 * synchronous-ish fs calls, not worth mocking node:fs for.
 */

import { test, mock, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

const telemetryUrl = new URL("../../../../telemetry/index.js", import.meta.url).href;

let fakeResolver = null;

await mock.module(telemetryUrl, {
  exports: {
    getSharedResolver: () => fakeResolver,
  },
});

const { registerSubmitDraftTool } = await import("../../../../agent/tools/submit-draft.js");

function createToolApi() {
  const tools = [];
  const api = {
    registerTool(defOrFactory) { tools.push(defOrFactory); },
    logger: { info: () => {}, warn: () => {}, debug: () => {}, error: () => {} },
  };
  return { api, tools };
}

function findTool(tools, name, ctx = {}) {
  const def = tools.find((t) => {
    if (typeof t === "function") return t({}).name === name;
    return t.name === name;
  });
  assert.ok(def, `tool '${name}' not registered`);
  return typeof def === "function" ? def(ctx) : def;
}

describe("registerSubmitDraftTool — submit_draft", () => {
  let dir;

  beforeEach(async () => {
    fakeResolver = null;
    dir = await mkdtemp(path.join(tmpdir(), "submit-draft-test-"));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function makeTool(ctx = {}, pipelinesRef = undefined) {
    const { api, tools } = createToolApi();
    registerSubmitDraftTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger, pipelinesRef });
    return findTool(tools, "submit_draft", ctx);
  }

  function makePipelinesRef(map) {
    return { value: new Map(Object.entries(map)) };
  }

  function baseParams(overrides = {}) {
    return {
      outPath: path.join(dir, "draft.md"),
      body: "Hello, this is the proposal body.",
      language: "en",
      budget: 500,
      timeframe: "2 weeks",
      ...overrides,
    };
  }

  test("returns toolErr when jobId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("succeeds with an explicit jobId, no ctx needed", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-explicit" }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.match(written, /job-id: job-explicit/);
  });

  test("auto-resolves jobId from ctx.sessionKey when omitted entirely", async () => {
    fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "listing-auto-resolved" }) };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:deadbeef" });
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.match(written, /job-id: listing-auto-resolved/);
  });

  test("explicit jobId is used as-is even when a resolver is present (no override)", async () => {
    fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "should-not-be-used" }) };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:deadbeef" });
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-explicit" }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.match(written, /job-id: job-explicit/);
  });

  test("auto-resolve failure (resolver has nothing for this session) still surfaces a clear error, not a crash", async () => {
    fakeResolver = { resolve: () => null };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:some-random-uuid" });
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("still validates outPath is absolute and ends in .md regardless of jobId source", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", outPath: "relative/path.md" }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("absolute path"));
  });

  test("still validates budget regardless of jobId source", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", budget: -5 }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("budget must be a positive number"));
  });

  test("writes duration to frontmatter when passed (platform-specific, optional)", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", duration: "1 to 3 months" }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.match(written, /duration: 1 to 3 months/);
  });

  test("omits duration from frontmatter when not passed (Workana callers unaffected)", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x" }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.doesNotMatch(written, /duration:/);
  });

  test("writes period to frontmatter when passed (Freelancer.com-only, optional)", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", period: 14 }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.match(written, /period: 14/);
  });

  test("omits period from frontmatter when not passed", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x" }), null);
    assert.equal(result.isError, undefined);
    const written = await readFile(baseParams().outPath, "utf-8");
    assert.doesNotMatch(written, /period:/);
  });

  test("rejects a non-numeric period", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", period: "abc" }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /period must be a plain integer number of days/);
  });

  // Same digit-strip-concatenation bug as propose-draft-meta.js's budget/
  // period fields (see that test file for the real incident this covers):
  // "R$ 2.600 - 3.500" strips to "26003500" without a range guard.
  test("rejects a budget range instead of concatenating it into a mangled number", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", budget: "R$ 2.600 - 3.500" }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /budget must be a single number, not a range/);
    assert.doesNotMatch(result.content[0].text, /26003500/);
  });

  test("rejects a period range instead of concatenating it", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", period: "14-21" }), null);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /period must be a single number, not a range/);
  });

  describe("per-pipeline delivery-field requirement (timeframe vs period)", () => {
    test("default (no pipelinesRef at all) — timeframe required, exactly today's behavior", async () => {
      const toolDef = makeTool();
      const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", timeframe: "" }), null);
      assert.equal(result.isError, true);
      assert.match(result.content[0].text, /timeframe is required/);
    });

    test("pipeline registered but declares no proposalMeta — timeframe still required", async () => {
      const pipelinesRef = makePipelinesRef({ "freelance_workana-proposal": { id: "freelance_workana-proposal" } });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "item-1", pipeline_id: "freelance_workana-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:x" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ timeframe: "" }), null);
      assert.equal(result.isError, true);
      assert.match(result.content[0].text, /timeframe is required/);
    });

    test("pipeline declares requiredDeliveryField:'period' — succeeds with period, no timeframe needed", async () => {
      const pipelinesRef = makePipelinesRef({
        "freelance_freelancer-proposal": { id: "freelance_freelancer-proposal", proposalMeta: { requiredDeliveryField: "period" } },
      });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "item-1", pipeline_id: "freelance_freelancer-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:x" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ timeframe: "", period: 21 }), null);
      assert.equal(result.isError, undefined);
      const written = await readFile(baseParams().outPath, "utf-8");
      assert.match(written, /period: 21/);
      assert.doesNotMatch(written, /timeframe:/);
    });

    test("regression: DRAFT-style dispatch with no item_key on the lineage row still resolves 'period' (2026-09-17 incident)", async () => {
      // A per-item agent node (DRAFT) is dispatched via a subflow with
      // itemKey set, which spawns one CHILD FLOW PER ITEM — flow_id alone
      // already disambiguates the item, so executor-nodes.js's recordSpawn
      // never stamps item_key for this dispatch shape. That's the REAL
      // shape of the lineage row in production (confirmed against
      // session_lineage for the live incident) — every other test above
      // fabricates an item_key that never actually exists here, so none of
      // them would have caught this. Without resolveFlowIdOnly as a
      // fallback, pipelineId resolution silently failed and every
      // Freelancer.com draft that correctly omitted timeframe (per its own
      // skill instructions) was rejected with "timeframe is required".
      const pipelinesRef = makePipelinesRef({
        "freelance_freelancer-proposal": { id: "freelance_freelancer-proposal", proposalMeta: { requiredDeliveryField: "period" } },
      });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", pipeline_id: "freelance_freelancer-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:gf-0f021019-draft" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ jobId: "40717459", timeframe: "", period: 14 }), null);
      assert.equal(result.isError, undefined);
      const written = await readFile(baseParams().outPath, "utf-8");
      assert.match(written, /period: 14/);
      assert.doesNotMatch(written, /timeframe:/);
    });

    test("regression: synthetic subflow pipelineId (e.g. '<root>::PROPOSE') still resolves the root's proposalMeta", async () => {
      // resolveFlowIdOnly can return the SUBFLOW's own compound id (the
      // shape session_lineage actually records for a DRAFT dispatch), not
      // the root pipeline id — loader.js's registerSubflowChildren must
      // have propagated proposalMeta onto that synthetic entry for this
      // lookup to succeed, exactly as it does for the real pipeline
      // registry at runtime.
      const pipelinesRef = makePipelinesRef({
        "freelance_freelancer-proposal::PROPOSE": {
          id: "freelance_freelancer-proposal::PROPOSE",
          _synthetic: true,
          _parentPipelineId: "freelance_freelancer-proposal",
          proposalMeta: { requiredDeliveryField: "period" },
        },
      });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", pipeline_id: "freelance_freelancer-proposal::PROPOSE" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:gf-0f021019-draft" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ jobId: "40717459", timeframe: "", period: 14 }), null);
      assert.equal(result.isError, undefined);
      const written = await readFile(baseParams().outPath, "utf-8");
      assert.match(written, /period: 14/);
    });

    test("pipeline declares requiredDeliveryField:'period' — missing period is a clear error, not a silent pass", async () => {
      const pipelinesRef = makePipelinesRef({
        "freelance_freelancer-proposal": { id: "freelance_freelancer-proposal", proposalMeta: { requiredDeliveryField: "period" } },
      });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "item-1", pipeline_id: "freelance_freelancer-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:x" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ timeframe: "" }), null);
      assert.equal(result.isError, true);
      assert.match(result.content[0].text, /period is required/);
    });

    test("period-requiring pipeline still accepts a redundant timeframe if the agent supplies both", async () => {
      const pipelinesRef = makePipelinesRef({
        "freelance_freelancer-proposal": { id: "freelance_freelancer-proposal", proposalMeta: { requiredDeliveryField: "period" } },
      });
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "item-1", pipeline_id: "freelance_freelancer-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:x" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ period: 21 }), null);
      assert.equal(result.isError, undefined);
      const written = await readFile(baseParams().outPath, "utf-8");
      assert.match(written, /period: 21/);
      assert.match(written, /timeframe: 2 weeks/);
    });

    test("pipelineId resolves to something not in the registry — falls back to timeframe-required, does not crash", async () => {
      const pipelinesRef = makePipelinesRef({});
      fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "item-1", pipeline_id: "freelance_freelancer-proposal" }) };
      const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:x" }, pipelinesRef);
      const result = await toolDef.execute("tc", baseParams({ timeframe: "" }), null);
      assert.equal(result.isError, true);
      assert.match(result.content[0].text, /timeframe is required/);
    });
  });
});
