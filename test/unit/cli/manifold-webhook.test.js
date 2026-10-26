// manifold-webhook.test.js — webhook set/test CLI over real processes:
// upsert + verify against the push_targets store + notifyPush, refused and
// ok paths. Each case gets an isolated MANIFOLD_DATA_DIR.
import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import http from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const bin = path.join(import.meta.dirname, "..", "..", "..", "bin", "manifold.js");
let dir;

beforeEach(() => {
  dir = mkdtempSync(path.join(tmpdir(), "manifold-webhook-"));
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

function run(args, extraEnv = {}) {
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      [bin, ...args],
      { env: { ...process.env, MANIFOLD_DATA_DIR: dir, ...extraEnv } },
      (error, stdout, stderr) => {
        resolve({ code: error?.code ?? 0, stdout, stderr });
      }
    );
  });
}

test("webhook set ok + list shows the target (bearer masked on set output)", async () => {
  const set = await run(["webhook", "set", "--harness", "openclaw", "--url", "http://127.0.0.1:9/hook", "--bearer", "s3cret"]);
  assert.equal(set.code, 0);
  const row = JSON.parse(set.stdout);
  assert.equal(row.harness, "openclaw");
  assert.equal(row.bearer, "<set>");
  const list = await run(["webhook", "list"]);
  assert.equal(list.code, 0);
  assert.equal(JSON.parse(list.stdout)[0].url, "http://127.0.0.1:9/hook");
});

test("webhook set refused: missing harness, bad url", async () => {
  const noHarness = await run(["webhook", "set", "--url", "http://x/hook"]);
  assert.notEqual(noHarness.code, 0);
  assert.match(noHarness.stderr, /requires --harness/);
  const badUrl = await run(["webhook", "set", "--harness", "h", "--url", "ftp://x/y"]);
  assert.notEqual(badUrl.code, 0);
  assert.match(badUrl.stderr, /invalid webhook url/);
});

test("webhook test ok: POSTs webhook.test with bearer to the registered URL", async () => {
  const seen = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      seen.push({ auth: req.headers.authorization, body: JSON.parse(body) });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, test: true }));
    });
  });
  await new Promise((res) => server.listen(0, "127.0.0.1", res));
  try {
    const url = `http://127.0.0.1:${server.address().port}/pushed`;
    const set = await run(["webhook", "set", "--harness", "openclaw", "--url", url, "--bearer", "s3cret"]);
    assert.equal(set.code, 0);
    const t = await run(["webhook", "test", "--harness", "openclaw"]);
    assert.equal(t.code, 0);
    assert.equal(seen.length, 1);
    assert.equal(seen[0].auth, "Bearer s3cret");
    assert.equal(seen[0].body.event, "webhook.test");
    assert.match(t.stdout, /"ok": true/);
  } finally {
    server.close();
  }
});

test("webhook test refused without a registered target", async () => {
  const t = await run(["webhook", "test", "--harness", "ghost"]);
  assert.notEqual(t.code, 0);
  assert.match(t.stderr, /no webhook target for harness: ghost/);
});
