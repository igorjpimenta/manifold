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
