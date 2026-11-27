import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const root = path.join(import.meta.dirname, "..", "..", "..");
const adapter = path.join(root, "plugins/openclaw/manifold");

test("root package does not declare a harness extension surface", () => {
  const pkg = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
  assert.equal(pkg.openclaw, undefined);
  assert.equal(existsSync(path.join(root, "openclaw.plugin.json")), false);
});

test("the openclaw adapter keeps its own manifest and id", () => {
  const manifest = JSON.parse(
    readFileSync(path.join(adapter, "openclaw.plugin.json"), "utf8")
  );
  assert.equal(manifest.id, "manifold");
});

test("the adapter is publishable as its own openclaw plugin package", () => {
  const pkg = JSON.parse(readFileSync(path.join(adapter, "package.json"), "utf8"));
  assert.equal(pkg.name, "@igorjpimenta/manifold-openclaw");
  assert.deepEqual(pkg.openclaw?.extensions, ["./index.js"]);
  assert.equal(pkg.private, undefined, "publishable, not a private workspace package");
  const pack = JSON.parse(
    execFileSync("npm", ["pack", "--dry-run", "--json"], { cwd: adapter, encoding: "utf8" })
  );
  const shipped = pack[0].files.map((f) => f.path);
  assert.ok(shipped.includes("openclaw.plugin.json"), "plugin manifest ships in the tarball");
  assert.ok(shipped.includes("tool-results.js"), "turn rendezvous ships in the tarball");
});

test("the adapter opts into startup activation", () => {
  const manifest = JSON.parse(
    readFileSync(path.join(adapter, "openclaw.plugin.json"), "utf8")
  );
  assert.equal(manifest.activation?.onStartup, true);
});

test("no static openclaw imports outside plugins/ and test/", () => {
  const files = execFileSync("git", ["ls-files", "-z", "*.js"], { cwd: root, encoding: "utf8" })
    .split("\0")
    .filter(Boolean)
    .filter((f) => !f.startsWith("test/") && !f.startsWith("dashboard/") && !f.startsWith("plugins/"));
  const STATIC_OPENCLAW_RE = /(?:from\s+["']openclaw\/|import\s*["']openclaw\/)/;
  const offenders = files.filter((f) => STATIC_OPENCLAW_RE.test(readFileSync(path.join(root, f), "utf8"))).sort();
  assert.deepEqual(offenders, [], `static openclaw imports outside plugins/: ${offenders.join(", ")}`);
});

test("no core file imports from a plugins/ path (static or dynamic)", () => {
  const files = execFileSync("git", ["ls-files", "-z", "*.js"], { cwd: root, encoding: "utf8" })
    .split("\0")
    .filter(Boolean)
    .filter((f) => !f.startsWith("test/") && !f.startsWith("plugins/"));
  const STATIC_PLUGINS_RE = /from\s+["'](?:\.\.?\/)+plugins\//;
  const DYNAMIC_PLUGINS_RE = /import\(\s*["'](?:\.\.?\/)?plugins\//;
  const offenders = files.filter((f) => {
    const src = readFileSync(path.join(root, f), "utf8");
    return STATIC_PLUGINS_RE.test(src) || DYNAMIC_PLUGINS_RE.test(src);
  }).sort();
  assert.deepEqual(offenders, [], `core imports from plugins/: ${offenders.join(", ")}`);
});
