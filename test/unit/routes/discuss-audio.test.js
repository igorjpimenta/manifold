import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { tmpdir } from "node:os";
import { mkdirSync, writeFileSync, rmSync, mkdtempSync } from "node:fs";
import { randomUUID } from "node:crypto";
import {
  AUDIO_MIME_EXT,
  EXT_TO_MIME,
  audioDirForFlow,
  audioRootForFlow,
  harnessAudioDir,
  resolveAudioHarness,
  loadAudioMetasForFlow,
} from "../../../routes/discuss-audio.js";

// Every test runs against an isolated store home so the real ~/.manifold
// is never touched. Resolution is per call, so setting the env here (after
// import) still relocates the store.
let prevDataDir;
let dataDir;
beforeEach(() => {
  prevDataDir = process.env.MANIFOLD_DATA_DIR;
  dataDir = mkdtempSync(path.join(tmpdir(), "manifold-discuss-audio-"));
  process.env.MANIFOLD_DATA_DIR = dataDir;
});
afterEach(() => {
  if (prevDataDir === undefined) delete process.env.MANIFOLD_DATA_DIR;
  else process.env.MANIFOLD_DATA_DIR = prevDataDir;
  rmSync(dataDir, { recursive: true, force: true });
});

// ── constants ─────────────────────────────────────────────────────────────────

describe("AUDIO_MIME_EXT", () => {
  it("maps audio/m4a to m4a", () => assert.equal(AUDIO_MIME_EXT["audio/m4a"], "m4a"));
  it("maps audio/mp4 to m4a", () => assert.equal(AUDIO_MIME_EXT["audio/mp4"], "m4a"));
  it("maps audio/mpeg to mp3", () => assert.equal(AUDIO_MIME_EXT["audio/mpeg"], "mp3"));
  it("maps audio/wav to wav",  () => assert.equal(AUDIO_MIME_EXT["audio/wav"], "wav"));
  it("maps audio/webm to webm",() => assert.equal(AUDIO_MIME_EXT["audio/webm"], "webm"));
  it("maps audio/ogg to ogg",  () => assert.equal(AUDIO_MIME_EXT["audio/ogg"], "ogg"));
});

describe("EXT_TO_MIME", () => {
  it("maps m4a to audio/m4a",   () => assert.equal(EXT_TO_MIME["m4a"], "audio/m4a"));
  it("maps mp3 to audio/mpeg",  () => assert.equal(EXT_TO_MIME["mp3"], "audio/mpeg"));
  it("maps wav to audio/wav",   () => assert.equal(EXT_TO_MIME["wav"], "audio/wav"));
  it("maps webm to audio/webm", () => assert.equal(EXT_TO_MIME["webm"], "audio/webm"));
  it("maps ogg to audio/ogg",   () => assert.equal(EXT_TO_MIME["ogg"], "audio/ogg"));
});

// ── audioDirForFlow ───────────────────────────────────────────────────────────

describe("audioDirForFlow", () => {
  it("builds path under <store-home>/media/audio/<harness>/<flowId>/<itemId>", () => {
    const dir = audioDirForFlow("flow-abc", "item-1", "test-harness");
    assert.equal(dir, path.join(dataDir, "media", "audio", "test-harness", "flow-abc", "item-1"));
    assert.equal(harnessAudioDir("test-harness"), path.join(dataDir, "media", "audio", "test-harness"));
  });

  it("namespaces by the connection-declared harness, never a default", () => {
    assert.equal(
      audioDirForFlow("flow-abc", "item-1", "test-harness"),
      path.join(dataDir, "media", "audio", "test-harness", "flow-abc", "item-1"),
    );
  });

  it("resolveAudioHarness reads X-Manifold-Harness and refuses the rest", () => {
    assert.equal(resolveAudioHarness({ headers: { "x-manifold-harness": "test-harness" } }), "test-harness");
    assert.throws(() => resolveAudioHarness({ headers: {} }), "missing header must refuse");
    assert.throws(() => resolveAudioHarness({ headers: { "x-manifold-harness": "unknown" } }), "reserved value must refuse");
    assert.throws(() => resolveAudioHarness({ headers: { "x-manifold-harness": "  " } }), "blank value must refuse");
  });

  it("sanitizes special chars in itemId", () => {
    const dir = audioDirForFlow("flow-abc", "item:with/special?chars", "test-harness");
    assert.ok(!dir.includes(":") && !dir.includes("?"), `special chars should be sanitized: ${dir}`);
  });

  it("truncates itemId at 120 chars", () => {
    const long = "x".repeat(200);
    const dir = audioDirForFlow("flow-abc", long, "test-harness");
    const itemPart = path.basename(dir);
    assert.ok(itemPart.length <= 120, `itemId part should be ≤ 120 chars: ${itemPart.length}`);
  });

  it("falls back to _ for empty itemId", () => {
    const dir = audioDirForFlow("flow-abc", "", "test-harness");
    assert.equal(path.basename(dir), "_");
  });
});

// ── loadAudioMetasForFlow ─────────────────────────────────────────────────────

describe("loadAudioMetasForFlow", () => {
  it("returns empty array for non-existent flowId directory", async () => {
    const result = await loadAudioMetasForFlow("no-such-flow-" + randomUUID(), "test-harness");
    assert.deepEqual(result, []);
  });

  it("returns empty array when item dir has no .meta.json files", async () => {
    const flowId = "test-flow-" + randomUUID().slice(0, 8);
    const dir = audioDirForFlow(flowId, "item-1", "test-harness");
    mkdirSync(dir, { recursive: true });
    writeFileSync(path.join(dir, "audio.m4a"), Buffer.alloc(4));
    const result = await loadAudioMetasForFlow(flowId, "test-harness");
    assert.deepEqual(result, []);
  });

  it("loads and parses .meta.json sidecars from all item subdirs", async () => {
    const flowId = "test-flow-" + randomUUID().slice(0, 8);
    const root = audioRootForFlow(flowId, "test-harness");
    const meta1 = { turnId: "turn-1", itemId: "item-1", audioUrl: "/a/1.m4a", status: "completed", transcript: "hello" };
    const meta2 = { turnId: "turn-2", itemId: "item-2", audioUrl: "/a/2.m4a", status: "pending",   transcript: null };
    mkdirSync(path.join(root, "item-1"), { recursive: true });
    mkdirSync(path.join(root, "item-2"), { recursive: true });
    writeFileSync(path.join(root, "item-1", "turn-1.meta.json"), JSON.stringify(meta1));
    writeFileSync(path.join(root, "item-2", "turn-2.meta.json"), JSON.stringify(meta2));
    const metas = await loadAudioMetasForFlow(flowId, "test-harness");
    assert.equal(metas.length, 2);
    const ids = new Set(metas.map((m) => m.turnId));
    assert.ok(ids.has("turn-1") && ids.has("turn-2"));
  });

  it("skips corrupt .meta.json files without throwing", async () => {
    const flowId = "test-flow-" + randomUUID().slice(0, 8);
    const dir = audioDirForFlow(flowId, "item-1", "test-harness");
    mkdirSync(dir, { recursive: true });
    writeFileSync(path.join(dir, "bad.meta.json"), "not-json{{{{");
    writeFileSync(path.join(dir, "good.meta.json"), JSON.stringify({ turnId: "t1", status: "completed" }));
    const metas = await loadAudioMetasForFlow(flowId, "test-harness");
    assert.equal(metas.length, 1);
    assert.equal(metas[0].turnId, "t1");
  });

  it("returns [] when the store-home root is absent — pre-store-home audio is not read", async () => {
    const flowId = "test-flow-" + randomUUID().slice(0, 8);
    const metas = await loadAudioMetasForFlow(flowId, "test-harness");
    assert.deepEqual(metas, []);
  });
});
