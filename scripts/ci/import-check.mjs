#!/usr/bin/env node
// import-check.mjs — no tracked source file may import through an absolute
// `file://` URL.
//
// A `file://` specifier is absolute by construction (no relative form), so an
// absolute one only resolves on the machine that wrote it — and in local test
// runs it resolves against the live working tree, where an unstaged file is
// indistinguishable from a committed one. CI checks out only the commit, so
// the check runs where the bug first becomes visible.
//
// Scans import/export/require specifier positions:
//   import x from "file://…" | import "file://…" | import("file://…")
//   export … from "file://…"  | require("file://…")
// Comment-only lines are skipped so documentation can name the pattern.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(?|\brequire\s*\()\s*[`"']file:\/\//;
const COMMENT_LINE = /^\s*(?:\/\/|\/\*|\*|#)/;

const files = execFileSync("git", ["ls-files", "-z", "*.js", "*.mjs", "*.cjs"], {
  encoding: "utf8",
  maxBuffer: 64 * 1024 * 1024,
})
  .split("\0")
  .filter(Boolean);

const hits = [];
for (const rel of files) {
  const lines = readFileSync(rel, "utf8").split("\n");
  for (let i = 0; i < lines.length; i++) {
    if (COMMENT_LINE.test(lines[i])) continue;
    if (SPECIFIER.test(lines[i])) hits.push(`${rel}:${i + 1}: ${lines[i].trim()}`);
  }
}

if (hits.length) {
  console.error(`import-check: ${hits.length} absolute file:// import specifier(s):`);
  for (const h of hits) console.error(`  ${h}`);
  console.error(
    "\nThese resolve only on the author's machine (and, locally, against the" +
      " live working tree). Replace each with a relative import.",
  );
  process.exit(1);
}

console.log(
  `import-check: ${files.length} tracked source files, 0 absolute file:// import specifiers`,
);

// ── homedir seam ─────────────────────────────────────────────────────────────
// Sessions live in the harness, never in core: no shipped source file may
// resolve the filesystem root for session data. state/paths.js is the one
// store-home seam; the openclaw sessions adapter is the one harness edge.
// Own-store readers (manifold's own DBs/logs/config) are grandfathered per
// file below — each entry names what it reads, so a new reach stands out.
// Test files are exempt: they isolate homes via tmp dirs on purpose.
const HOME_RE = /\bos\.homedir\s*\(|process\.env\.HOME\b|process\.env\[\s*['"]HOME['"]\s*\]/;
const HOME_ALLOW = new Set([
  "state/paths.js", // the seam itself
  "plugins/openclaw/manifold/sessions.js", // harness adapter edge
  "bin/cli.js", // own pipelines dir
  "budget/index.js", // own store dir
  "dashboard/cost.js", // own ops.sqlite
  "dashboard/event-log.js", // own runtime.sqlite
  "drizzle.config.js", // own runtime.sqlite (migrations)
  "drizzle.config.ops.js", // own ops.sqlite (migrations)
  "graphql/schema.js", // own runtime/ops.sqlite defaults
  "locks/browser-tabs.js", // own openclaw.json tab lease
  "plugins/openclaw/manifold/hooks/profile-read-gate.js", // harness workspace profile (read)
  "plugins/openclaw/manifold/hooks/profile-write-lock.js", // harness workspace profile (write)
  "state/db-guard.js", // own corruption-alert log
  "routes/health.js", // own runtime/ops.sqlite probe
  "telemetry/store.js", // own ops.sqlite + runtime.sqlite attach
  "telemetry/resolver.js", // own lineage store + cron registry probe
  "telemetry/budget-alert.js", // own alerted-thresholds state + ops.sqlite
]);

const homeHits = [];
for (const rel of files) {
  if (rel.startsWith("test/")) continue;
  if (HOME_ALLOW.has(rel)) continue;
  const lines = readFileSync(rel, "utf8").split("\n");
  for (let i = 0; i < lines.length; i++) {
    if (COMMENT_LINE.test(lines[i])) continue;
    if (HOME_RE.test(lines[i])) homeHits.push(`${rel}:${i + 1}: ${lines[i].trim()}`);
  }
}

if (homeHits.length) {
  console.error(`import-check: ${homeHits.length} filesystem-root reach(es) outside the seam:`);
  for (const h of homeHits) console.error(`  ${h}`);
  console.error(
    "\nSession storage belongs to the harness: resolve store paths via" +
      " state/paths.js, session identity via the harness adapter. Own-store" +
      " reads go through the seam or earn an explicit entry above.",
  );
  process.exit(1);
}

console.log(
  `import-check: ${files.length} tracked source files, 0 filesystem-root reaches outside the seam`,
);
