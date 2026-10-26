// Pins winch packaging parity: core carries zero static openclaw SDK imports.
//
// Invariant: no tracked .js file outside test/ and the harness adapters
// statically imports `openclaw/*` (neither `import … from "openclaw/…"` nor
// `require("openclaw/…")`). Static imports make the module graph
// unimportable (and unpackagable) without an installed openclaw runtime.
// The sanctioned seams are dynamic import("openclaw/…") (resolves lazily,
// fails gracefully) and the harness adapters. Comment-only mentions, string
// literals, and dynamic imports never match the static shapes below.

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

// Harness adapters own the SDK boundary; everything else must stay static-free.
const ALLOWLIST = new Set(["harness/openclaw.js", "harness/opencode.js"]);

const STATIC_IMPORT_FROM_RE = /^\s*import[^;]*from\s*["']openclaw\//;
const REQUIRE_RE = /require\(\s*["']openclaw\//;

function walkJsFiles(dir, out) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === ".git") continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (path.relative(REPO_ROOT, full) === "test") continue;
      walkJsFiles(full, out);
    } else if (entry.isFile() && entry.name.endsWith(".js")) {
      out.push(full);
    }
  }
  return out;
}

function isCommentLine(trimmed) {
  return (
    trimmed === "" ||
    trimmed.startsWith("//") ||
    trimmed.startsWith("/*") ||
    trimmed.startsWith("*")
  );
}

function staticViolations(rel, src) {
  const hits = [];
  src.split("\n").forEach((line, idx) => {
    if (isCommentLine(line.trim())) return;
    if (STATIC_IMPORT_FROM_RE.test(line) || REQUIRE_RE.test(line)) {
      hits.push(`${rel}:${idx + 1}: ${line.trim()}`);
    }
  });
  return hits;
}

describe("no static openclaw SDK imports in core", () => {
  it("scans every non-test .js file and finds no static openclaw import", () => {
    const violations = [];
    const scanned = [];
    for (const full of walkJsFiles(REPO_ROOT, [])) {
      const rel = path.relative(REPO_ROOT, full);
      if (ALLOWLIST.has(rel)) continue;
      scanned.push(rel);
      violations.push(...staticViolations(rel, readFileSync(full, "utf8")));
    }
    // Converted modules must be covered by this scan, never allowlisted.
    for (const must of ["channels/manifold-parrot-channel.js", "dashboard/device.js"]) {
      assert.ok(scanned.includes(must), `${must} must be scanned, not allowlisted`);
    }
    assert.deepEqual(violations, [], `static openclaw imports:\n${violations.join("\n")}`);
  });
});
