import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { displayRootLabel, displayRootKind, stripTeamPrefix } from "../../../telemetry/labels.js";

// ── stripTeamPrefix ────────────────────────────────────────────────────────────

describe("stripTeamPrefix", () => {
  it("strips the leading <team>_ segment for any team, not just one hardcoded name", () => {
    assert.equal(stripTeamPrefix("freelance_workana-proposal"), "workana-proposal");
    assert.equal(stripTeamPrefix("system_memory-synthesis"), "memory-synthesis");
    assert.equal(stripTeamPrefix("nyxen_some-pipeline"), "some-pipeline");
  });

  it("is a no-op when there's no underscore", () => {
    assert.equal(stripTeamPrefix("Operator runs"), "Operator runs");
  });

  it("passes through non-string input unchanged", () => {
    assert.equal(stripTeamPrefix(null), null);
    assert.equal(stripTeamPrefix(undefined), undefined);
  });
});

// ── displayRootLabel ───────────────────────────────────────────────────────────

describe("displayRootLabel", () => {
  it("returns null when label is falsy", () => {
    assert.equal(displayRootLabel("cron-owner", null), null);
    assert.equal(displayRootLabel("cron-owner", ""), null);
  });

  it("returns null for unknown kind", () => {
    assert.equal(displayRootLabel("unknown", "some-label"), null);
  });

  it("shows cron-owner labels as-is (opaque cron name, no team-prefix guessing)", () => {
    assert.equal(displayRootLabel("cron-owner", "freelance-workana-poll"), "freelance-workana-poll");
    assert.equal(displayRootLabel("cron-owner", "workana-poll"), "workana-poll");
  });

  it("renders telegram-dm as Chat (Telegram)", () => {
    assert.equal(displayRootLabel("telegram-dm", "99887766"), "Chat (Telegram)");
  });

  it("strips the team prefix from operator-trigger labels (the triggering pipelineId), for any team", () => {
    assert.equal(displayRootLabel("operator-trigger", "freelance_freelancer-proposal"), "freelancer-proposal");
    assert.equal(displayRootLabel("operator-trigger", "system_memory-synthesis"), "memory-synthesis");
  });

  it("passes through the legacy literal 'Operator runs' unchanged (pre-fix historical rows)", () => {
    assert.equal(displayRootLabel("operator-trigger", "Operator runs"), "Operator runs");
  });

  it("translates graph-flow-owner explicit to Operator runs", () => {
    assert.equal(displayRootLabel("explicit", "graph-flow-owner"), "Operator runs");
  });

  it("translates skill- explicit to Skill test: ...", () => {
    assert.equal(displayRootLabel("explicit", "skill-deep-research"), "Skill test: deep-research");
  });

  it("collapses UUID-suffixed explicit labels to Ad-hoc sessions", () => {
    assert.equal(displayRootLabel("explicit", "scorer-creation-c61af7cf-3cf1-43ff-873a-40ceb6b2cb17"), "Ad-hoc sessions");
    assert.equal(displayRootLabel("explicit", "test-positive-e3b48771-baf6-4815-8fc9-35b222701b4d"), "Ad-hoc sessions");
  });

  it("collapses a bare UUID explicit label to Ad-hoc sessions", () => {
    assert.equal(displayRootLabel("explicit", "a94cf1f5-9da1-4b45-b764-4f3b6f6014d2"), "Ad-hoc sessions");
  });

  it("skill- prefix still wins over the UUID-shape check when both apply", () => {
    assert.equal(displayRootLabel("explicit", "skill-test-spawn-remote-claude-6973ceb9-72c5-4dfb-8d79-a06862ebc137"), "Skill test: test-spawn-remote-claude-6973ceb9-72c5-4dfb-8d79-a06862ebc137");
  });

  it("passes through unrecognized explicit label", () => {
    assert.equal(displayRootLabel("explicit", "my-custom-label"), "my-custom-label");
  });

  it("renders flow-node as Pipeline (parent expired)", () => {
    assert.equal(displayRootLabel("flow-node", "any"), "Pipeline (parent expired)");
  });

  it("renders flow-triggered using provided label", () => {
    assert.equal(displayRootLabel("flow-triggered", "Workana proposal"), "Workana proposal");
  });

  it("falls back to rootLabel for unrecognized kinds", () => {
    assert.equal(displayRootLabel("brand-new-kind", "my-label"), "my-label");
  });
});

// ── displayRootKind ────────────────────────────────────────────────────────────

describe("displayRootKind", () => {
  it("maps cron-owner and cron-tick to Cron", () => {
    assert.equal(displayRootKind("cron-owner"), "Cron");
    assert.equal(displayRootKind("cron-tick"), "Cron");
  });

  it("maps telegram-dm to Telegram", () => {
    assert.equal(displayRootKind("telegram-dm"), "Telegram");
  });

  it("maps operator-trigger, explicit, agent_tool, cli to Operator", () => {
    assert.equal(displayRootKind("operator-trigger"), "Operator");
    assert.equal(displayRootKind("explicit"), "Operator");
    assert.equal(displayRootKind("agent_tool"), "Operator");
    assert.equal(displayRootKind("cli"), "Operator");
  });

  it("returns unknown for unrecognized kind", () => {
    assert.equal(displayRootKind("flow-node"), "unknown");
    assert.equal(displayRootKind("unknown"), "unknown");
  });
});
