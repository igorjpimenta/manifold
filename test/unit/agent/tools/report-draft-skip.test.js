/**
 * Unit tests for agent/tools/report-draft-skip.js — registerReportDraftSkipTool
 * (report_draft_skip).
 *
 * Mirrors report-draft-blocked.test.js's structure (same jobId auto-resolve
 * behavior, same real temp-dir file writes) — the two tools share the
 * outPath/jobId plumbing via shared.js, this file focuses on what's actually
 * different: the required `reason` field and the `{ skip: true, reason }`
 * shape written to disk instead of `{ questions }`.
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

const { registerReportDraftSkipTool } = await import("../../../../agent/tools/report-draft-skip.js");

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

describe("registerReportDraftSkipTool — report_draft_skip", () => {
  let dir;

  beforeEach(async () => {
    fakeResolver = null;
    dir = await mkdtemp(path.join(tmpdir(), "report-draft-skip-test-"));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function makeTool(ctx = {}) {
    const { api, tools } = createToolApi();
    registerReportDraftSkipTool({ registerTool: (def, opts) => api.registerTool(def, opts), logger: api.logger });
    return findTool(tools, "report_draft_skip", ctx);
  }

  function baseParams(overrides = {}) {
    return {
      outPath: path.join(dir, "blocked.json"),
      reason: "No Java experience; operator confirmed this is a dealbreaker.",
      ...overrides,
    };
  }

  test("returns toolErr when jobId is missing and cannot be auto-resolved (no ctx/registry)", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("succeeds with an explicit jobId, writes { skip: true, reason } to outPath", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-explicit" }), null);
    assert.equal(result.isError, undefined);
    const written = JSON.parse(await readFile(baseParams().outPath, "utf-8"));
    assert.equal(written.jobId, "job-explicit");
    assert.equal(written.skip, true);
    assert.equal(written.reason, "No Java experience; operator confirmed this is a dealbreaker.");
    assert.equal(written.questions, undefined, "skip-report has no questions array — distinct shape from report_draft_blocked");
  });

  test("auto-resolves jobId from ctx.sessionKey when omitted entirely", async () => {
    fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "listing-auto-resolved" }) };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:deadbeef" });
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, undefined);
    const written = JSON.parse(await readFile(baseParams().outPath, "utf-8"));
    assert.equal(written.jobId, "listing-auto-resolved");
  });

  test("explicit jobId is used as-is even when a resolver is present (no override)", async () => {
    fakeResolver = { resolve: () => ({ flow_id: "fid-1", item_key: "should-not-be-used" }) };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:deadbeef" });
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-explicit" }), null);
    assert.equal(result.isError, undefined);
    const written = JSON.parse(await readFile(baseParams().outPath, "utf-8"));
    assert.equal(written.jobId, "job-explicit");
  });

  test("auto-resolve failure (resolver has nothing for this session) still surfaces a clear error, not a crash", async () => {
    fakeResolver = { resolve: () => null };
    const toolDef = makeTool({ sessionKey: "agent:content-writer:subagent:some-random-uuid" });
    const result = await toolDef.execute("tc", baseParams(), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("could not be auto-resolved"));
  });

  test("still validates outPath is absolute and ends in .json regardless of jobId source", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", outPath: "relative/path.json" }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("absolute path"));
  });

  test("requires a non-empty reason", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", reason: "" }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("reason"));
  });

  test("requires a non-empty reason after trimming whitespace-only input", async () => {
    const toolDef = makeTool();
    const result = await toolDef.execute("tc", baseParams({ jobId: "job-x", reason: "   " }), null);
    assert.equal(result.isError, true);
    assert.ok(result.content[0].text.includes("reason"));
  });
});
