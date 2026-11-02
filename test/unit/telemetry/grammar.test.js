import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { classify, agentIdOf } from "../../../telemetry/grammar.js";

// ── classify ───────────────────────────────────────────────────────────────────

describe("classify", () => {
  it("returns unknown for non-string input", () => {
    assert.equal(classify(null).kind, "unknown");
    assert.equal(classify(42).kind, "unknown");
    assert.equal(classify("").kind, "unknown");
  });

  it("classifies cron-tick with parent hint", () => {
    const r = classify("agent:main:cron:11111111-1111-1111-1111-111111111111:run:aabbcc");
    assert.equal(r.kind, "cron-tick");
    assert.equal(r.agentId, "main");
    assert.equal(r.parentHint?.kind, "cron-id");
    assert.equal(r.parentHint?.cronId, "11111111-1111-1111-1111-111111111111");
  });

  it("classifies cron-owner as root (no parent)", () => {
    const r = classify("agent:main:cron-workana-poll");
    assert.equal(r.kind, "cron-owner");
    assert.equal(r.cronName, "workana-poll");
    assert.equal(r.parentHint, null);
  });

  it("classifies manifold-subagent with a flow parent hint", () => {
    const r = classify("mf-12345678-propose");
    assert.equal(r.kind, "manifold-subagent");
    assert.equal(r.agentId, null);
    assert.equal(r.flowShort, "12345678");
    assert.equal(r.nodeId, "propose");
    assert.equal(r.parentHint?.kind, "flow");
    assert.equal(r.parentHint?.flowShort, "12345678");
  });

  it("stops resolving legacy gateway-shaped keys to flows", () => {
    // Pre-cutover session files keep their gateway keys; they degrade to
    // the generic buckets instead of the flow walk — no crash, no lineage.
    assert.equal(classify("agent:cyrano:subagent:mf-12345678-propose").kind, "unknown");
    assert.equal(classify("agent:main:gf-discuss-a700a3ea-propose-x").kind, "explicit");
  });

  it("classifies plain-text manifold ids with null agent and the same flow hint", () => {
    const r = classify("mf-12345678-propose-item-1");
    assert.equal(r.kind, "manifold-subagent");
    assert.equal(r.agentId, null);
    assert.equal(r.flowShort, "12345678");
    assert.equal(r.nodeId, "propose");
    assert.equal(r.itemKey, "item-1");
    assert.equal(r.parentHint?.kind, "flow");
    assert.equal(r.parentHint?.flowShort, "12345678");
  });

  it("leaves the legacy gf- dispatch shape unmatched", () => {
    const r = classify("agent:cyrano:subagent:gf-12345678-propose");
    assert.equal(r.kind, "unknown");
  });

  it("classifies bare-subagent (parent from spawn hook)", () => {
    const r = classify("agent:scorer:subagent:22222222-2222-2222-2222-222222222222");
    assert.equal(r.kind, "bare-subagent");
    assert.equal(r.agentId, "scorer");
    assert.equal(r.parentHint, null);
  });

  it("classifies telegram-dm as root", () => {
    const r = classify("agent:main:telegram:direct:99887766");
    assert.equal(r.kind, "telegram-dm");
    assert.equal(r.telegramChatId, "99887766");
    assert.equal(r.parentHint, null);
  });

  it("classifies explicit as root", () => {
    const r = classify("agent:main:explicit:graph-flow-owner");
    assert.equal(r.kind, "explicit");
    assert.equal(r.explicitPurpose, "graph-flow-owner");
    assert.equal(r.parentHint, null);
  });

  it("classifies operator-trigger as root", () => {
    const r = classify("agent:operator:ios:33333333-3333-3333-3333-333333333333");
    assert.equal(r.kind, "operator-trigger");
    assert.equal(r.operatorSource, "ios");
    assert.equal(r.parentHint, null);
  });

  it("classifies agent-default as root", () => {
    const r = classify("agent:main:main");
    assert.equal(r.kind, "agent-default");
    assert.equal(r.parentHint, null);
  });

  it("classifies memory-synth-chunk with parent hint", () => {
    const r = classify("agent:main:memory-synth:1716000000:c-3");
    assert.equal(r.kind, "memory-synth-chunk");
    assert.equal(r.chunk, "3");
    assert.equal(r.parentHint?.kind, "memory-synth");
  });

  it("classifies flow-gate-discuss with a flow parent hint, not the explicit-bare catch-all", () => {
    const r = classify("gf-discuss-a700a3ea-propose-some-item-slug");
    assert.equal(r.kind, "flow-gate-discuss");
    assert.equal(r.agentId, null);
    assert.equal(r.flowShort, "a700a3ea");
    assert.equal(r.parentHint?.kind, "flow");
    assert.equal(r.parentHint?.flowShort, "a700a3ea");
  });

  it("classifies plain-text discuss ids with null agent and the same flow hint", () => {
    const r = classify("gf-discuss-a700a3ea-propose-some-item-slug");
    assert.equal(r.kind, "flow-gate-discuss");
    assert.equal(r.agentId, null);
    assert.equal(r.flowShort, "a700a3ea");
    assert.equal(r.parentHint?.kind, "flow");
    assert.equal(r.parentHint?.flowShort, "a700a3ea");
  });

  it("falls back to explicit for bare-shape key without known scope", () => {
    const r = classify("agent:main:some-custom-scope");
    assert.equal(r.kind, "explicit");
    assert.equal(r.explicitPurpose, "some-custom-scope");
  });

  it("returns unknown for unrecognized shape", () => {
    const r = classify("totally:unrecognized:key:format:extra");
    assert.equal(r.kind, "unknown");
  });
});

// ── agentIdOf ──────────────────────────────────────────────────────────────────

describe("agentIdOf", () => {
  it("extracts agent id from well-formed key", () => {
    assert.equal(agentIdOf("agent:scorer:explicit:testing"), "scorer");
  });

  it("returns null for non-string", () => {
    assert.equal(agentIdOf(null), null);
    assert.equal(agentIdOf(42), null);
  });

  it("returns null when key doesn't start with 'agent:'", () => {
    assert.equal(agentIdOf("session:foo:bar"), null);
  });

  it("returns null for malformed key with no second colon", () => {
    assert.equal(agentIdOf("agent:onlyone"), null);
  });
});
