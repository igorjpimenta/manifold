// Unit tests for the public export surface declared in package.json.
//
// Consumers install `@igorjpimenta/manifold` from GitHub Packages and import
// ONLY through the subpaths below — deep file paths are not a contract. This suite pins the
// map itself: the exact subpath set, each target file's existence, and that
// every subpath still imports through the package's own bare specifier
// (Node package self-reference) yielding the symbols consumers were written
// against.
//
// `openclaw/plugin-sdk/*` resolves only inside an installed openclaw
// runtime, so the import pass registers the test-only resolve hooks that
// redirect those specifiers to test/helpers/openclaw-plugin-sdk-stub.js.

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { registerHooks } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as openclawSdkHooks from "../../helpers/openclaw-plugin-sdk-hooks.js";

const PKG_NAME = "@igorjpimenta/manifold";
const pkg = JSON.parse(readFileSync(new URL("../../../package.json", import.meta.url), "utf8"));
const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));

// The consumer contract. Hardcoded so removing a subpath, renaming a target
// file, or dropping an export consumers rely on fails here rather than in a
// consumer's build.
const PUBLIC_API = {
  ".": { file: "./index.js", exports: ["default"] },
  "./errors": {
    file: "./engine/executor-errors.js",
    exports: [
      "NodeFailure",
      "NodeBlocked",
      "GatewayHandoffNeeded",
      "fetchSessionError",
      "isRequestScopedSubagentError",
      "isBreakerTrippingFailure",
      "REVISION_RETRY_LIMIT",
    ],
  },
  "./elevation": { file: "./elevation.js", exports: ["invokeElevated"] },
  "./routes/bearer-auth": {
    file: "./routes/bearer-auth.js",
    exports: ["authorizeBearerRequest", "sendUnauthorized"],
  },
  "./tool-handoff": {
    file: "./state/tool-handoff.js",
    exports: [
      "writePendingResult",
      "readPendingResult",
      "clearPendingResult",
      "waitForToolResult",
    ],
  },
  "./state/db": {
    file: "./state/db.js",
    exports: ["openDb", "getSharedDb", "isSqliteBusy", "withSharedWrite"],
  },
  "./state/paths": {
    file: "./state/paths.js",
    exports: [
      "resolveStorePaths",
      "resolveDataDir",
      "defaultDataDir",
      "runtimeDbPath",
      "opsDbPath",
      "pipelinesDir",
      "stateFile",
      "ensureMigrated",
      "readMigrationMarker",
    ],
  },
  "./state/batch-writer": {
    file: "./state/batch-writer.js",
    exports: ["enqueue", "enqueueRaw", "flush", "flushAll"],
  },
  // The manifest itself is reachable by name so a consumer can locate assets
  // beside it (the adapter resolves the CLI entry it spawns this way). It
  // exports no symbols; the import pass below skips it.
  "./package.json": { file: "./package.json" },
  "./state/schema": {
    file: "./state/schema.js",
    exports: [
      "flowRuns",
      "flowNodeInvocations",
      "flowNodeExecutions",
      "failureEvents",
      "discussState",
      "flowSessions",
      "nodeHealth",
      "modelHealth",
    ],
  },
};

function specifierFor(subpath) {
  return subpath === "." ? PKG_NAME : `${PKG_NAME}/${subpath.slice(2)}`;
}

// ── the exports map ───────────────────────────────────────────────────────────

describe("package.json exports — declared public surface", () => {
  it("declares exactly the consumer subpaths — no removal, no extras", () => {
    assert.deepEqual(Object.keys(pkg.exports).sort(), Object.keys(PUBLIC_API).sort());
  });

  it("every entry targets the file consumers import, and that file exists", () => {
    for (const [subpath, { file }] of Object.entries(PUBLIC_API)) {
      assert.equal(pkg.exports[subpath], file, `${subpath} must target ${file}`);
      assert.ok(
        existsSync(path.join(repoRoot, file)),
        `${subpath} target file missing: ${file} (repo root: ${repoRoot})`,
      );
    }
  });
});

// ── bare-specifier imports ────────────────────────────────────────────────────

describe("public subpaths — import by bare specifier, expected symbols present", () => {
  registerHooks(openclawSdkHooks);

  for (const [subpath, { exports: names }] of Object.entries(PUBLIC_API)) {
    if (!names) continue;
    it(`import('${specifierFor(subpath)}')`, async () => {
      const mod = await import(specifierFor(subpath));
      for (const name of names) {
        assert.ok(name in mod, `${specifierFor(subpath)} must export ${name}`);
      }
    });
  }
});
