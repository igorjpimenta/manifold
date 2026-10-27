// packaging.test.js — the repo root is a harness-free core package, not an
// openclaw extension. A clone under a harness's extension directory must not
// be auto-discovered as a second manifold.
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

// The adapter ships as its own npm package (@igorjpimenta/manifold-openclaw),
// so the plugin manifest lives at THAT package's root — the core package
// stays harness-free. `openclaw plugins install` reads these two fields.
test("the adapter is publishable as its own openclaw plugin package", () => {
  const pkg = JSON.parse(readFileSync(path.join(adapter, "package.json"), "utf8"));
  assert.equal(pkg.name, "@igorjpimenta/manifold-openclaw");
  assert.deepEqual(pkg.openclaw?.extensions, ["./index.js"]);
  assert.ok(pkg.files.includes("openclaw.plugin.json"), "plugin manifest ships in the tarball");
  assert.ok(existsSync(path.join(adapter, "openclaw.plugin.json")));
  assert.equal(pkg.private, undefined, "publishable, not a private workspace package");
});

// Startup activation is explicit: the host boot-loads only manifests that set
// activation.onStartup, and register() is where the tools/routes/services bind.
test("the adapter opts into startup activation", () => {
  const manifest = JSON.parse(
    readFileSync(path.join(adapter, "openclaw.plugin.json"), "utf8")
  );
  assert.equal(manifest.activation?.onStartup, true);
});

// Only the adapter may touch the host SDK, and only via static imports inside
// plugins/ (dynamic import() of a host module from core, e.g. the agent-model
// lookup in routes/operator-discuss.js, stays lazy and is allowed). A static
// `from "openclaw/…"` in core would hard-fail resolution for git-dependency
// consumers that never install openclaw.
// Core must also never import from the adapter: the dependency direction is
// adapter -> core, never core -> plugins/. The adapter injects everything
// core needs (bridge auth, device pairing) as constructor/registrar params.
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
  // Matches static `from "…plugins/…"` (any relative depth and quote
  // style) plus the dynamic `import("…plugins/…")` equivalents.
  const STATIC_PLUGINS_RE = /from\s+["'](?:\.\.?\/)+plugins\//;
  const DYNAMIC_PLUGINS_RE = /import\(\s*["'](?:\.\.?\/)?plugins\//;
  const offenders = files.filter((f) => {
    const src = readFileSync(path.join(root, f), "utf8");
    return STATIC_PLUGINS_RE.test(src) || DYNAMIC_PLUGINS_RE.test(src);
  }).sort();
  assert.deepEqual(offenders, [], `core imports from plugins/: ${offenders.join(", ")}`);
});
