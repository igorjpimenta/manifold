import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { computeCost, resolveCost, knownModels } from "../../../telemetry/pricer.js";

// ── computeCost ────────────────────────────────────────────────────────────────

describe("computeCost", () => {
  it("returns null for missing args", () => {
    assert.equal(computeCost(null, "model", {}), null);
    assert.equal(computeCost("provider", null, {}), null);
    assert.equal(computeCost("provider", "model", null), null);
  });

  it("returns null for unknown provider/model combination", () => {
    assert.equal(computeCost("openrouter", "gpt-9000", { input: 1000, output: 500 }), null);
  });

  it("computes cost for a known model (deepseek-v4-flash via openrouter)", () => {
    const r = computeCost("openrouter", "deepseek/deepseek-v4-flash", {
      input: 1_000_000, output: 1_000_000,
      cacheRead: 0, cacheWrite: 0,
    });
    assert.ok(r !== null);
    assert.ok(Math.abs(r.input  - 0.0983) < 0.0001, `input cost ${r.input}`);
    assert.ok(Math.abs(r.output - 0.1966) < 0.0001, `output cost ${r.output}`);
    assert.ok(Math.abs(r.total  - (0.0983 + 0.1966)) < 0.0001, `total ${r.total}`);
  });

  it("uses zero for missing token fields", () => {
    const r = computeCost("openrouter", "deepseek/deepseek-v4-flash", {});
    assert.ok(r !== null);
    assert.equal(r.total, 0);
  });

  it("computes cost for a known zai-coding model (glm-5.1)", () => {
    const r = computeCost("zai-coding", "glm-5.1", {
      input: 1_000_000, output: 1_000_000,
      cacheRead: 1_000_000, cacheWrite: 0,
    });
    assert.ok(r !== null);
    assert.ok(Math.abs(r.input  - 1.2) < 0.0001, `input cost ${r.input}`);
    assert.ok(Math.abs(r.output - 4.0) < 0.0001, `output cost ${r.output}`);
    assert.ok(Math.abs(r.total  - (1.2 + 4.0 + 0.24)) < 0.0001, `total ${r.total}`);
  });

  it("prices zai glm-4.5-flash at zero (free tier)", () => {
    const r = computeCost("zai", "glm-4.5-flash", { input: 100_000, output: 50_000 });
    assert.ok(r !== null);
    assert.equal(r.total, 0);
  });
});

// ── resolveCost ────────────────────────────────────────────────────────────────

describe("resolveCost", () => {
  it("returns runner cost when srcTotal > 0", () => {
    const r = resolveCost({
      provider: "openrouter",
      model: "deepseek/deepseek-v4-flash",
      usage: { cost: { total: 0.005, input: 0.003, output: 0.002 } },
    });
    assert.equal(r.source, "runner");
    assert.equal(r.total, 0.005);
  });

  it("falls back to computed cost when runner total is 0 but tokens exist", () => {
    const r = resolveCost({
      provider: "openrouter",
      model: "deepseek/deepseek-v4-flash",
      usage: { cost: { total: 0 }, input: 1_000_000, output: 500_000 },
    });
    assert.equal(r.source, "computed");
    assert.ok(r.total > 0);
  });

  it("returns source=unknown when tokens exist but rate is missing", () => {
    const r = resolveCost({
      provider: "openrouter",
      model: "unknown-model-xyz",
      usage: { cost: { total: 0 }, input: 1000, output: 500 },
    });
    assert.equal(r.source, "unknown");
    assert.equal(r.total, 0);
  });

  it("returns runner when there are no tokens (zero cost is correct)", () => {
    const r = resolveCost({
      provider: "openrouter",
      model: "deepseek/deepseek-v4-flash",
      usage: { cost: { total: 0 }, input: 0, output: 0 },
    });
    assert.equal(r.source, "runner");
    assert.equal(r.total, 0);
  });
});

// ── knownModels ────────────────────────────────────────────────────────────────

describe("knownModels", () => {
  it("returns a non-empty array of { provider, model } pairs", () => {
    const models = knownModels();
    assert.ok(Array.isArray(models) && models.length > 0);
    for (const m of models) {
      assert.ok(typeof m.provider === "string" && m.provider.length > 0);
      assert.ok(typeof m.model === "string" && m.model.length > 0);
    }
  });

  it("includes at least one openrouter model", () => {
    const models = knownModels();
    assert.ok(models.some((m) => m.provider === "openrouter"));
  });

  it("includes at least one zai model", () => {
    const models = knownModels();
    assert.ok(models.some((m) => m.provider === "zai"));
  });
});
