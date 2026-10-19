import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { summarizeModes, readDeployedShas, readDeployLog } from "../../../ios/deploy-status.js";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

const SHA_A = "797e2bda67bb24bf29b8f7fb137e344b64839fbe";
const SHA_B = "26dcfb5700000000000000000000000000000000";

describe("summarizeModes", () => {
  it("reports both modes current when each matches HEAD", () => {
    const out = summarizeModes({
      deployed: { backend: { sha: SHA_A, deployedAt: 1 }, ios: { sha: SHA_A, deployedAt: 2 } },
      headSha: SHA_A,
    });
    assert.equal(out.backend.current, true);
    assert.equal(out.ios.current, true);
  });

  it("reports a mode stale when it lags HEAD — the drift the view exists to show", () => {
    const out = summarizeModes({
      deployed: { backend: { sha: SHA_A, deployedAt: 1 }, ios: { sha: SHA_B, deployedAt: 2 } },
      headSha: SHA_A,
    });
    assert.equal(out.backend.current, true);
    assert.equal(out.ios.current, false);
    assert.equal(out.ios.everDeployed, true, "stale is not the same as never deployed");
  });

  it("distinguishes never-deployed from stale", () => {
    const out = summarizeModes({ deployed: { backend: { sha: SHA_A } }, headSha: SHA_A });
    assert.equal(out.ios.everDeployed, false);
    assert.equal(out.ios.sha, null);
    assert.equal(out.ios.current, false);
  });

  it("refuses to claim currency when HEAD is unknown", () => {
    // git failing must not render a green pill — false is the safe answer.
    const out = summarizeModes({ deployed: { backend: { sha: SHA_A } }, headSha: null });
    assert.equal(out.backend.current, false);
    assert.equal(out.backend.everDeployed, true);
  });

  it("returns an entry for every mode even with no state at all", () => {
    const out = summarizeModes({});
    assert.deepEqual(Object.keys(out).sort(), ["backend", "ios"]);
    assert.equal(out.backend.everDeployed, false);
  });
});

describe("readDeployedShas", () => {
  it("reads per-mode sha files and skips missing/empty ones", () => {
    const home = mkdtempSync(path.join(os.tmpdir(), "deploy-status-"));
    mkdirSync(path.join(home, ".openclaw", "deploy"), { recursive: true });
    writeFileSync(path.join(home, ".openclaw", "deploy", "last-backend"), `${SHA_A}\n`);
    writeFileSync(path.join(home, ".openclaw", "deploy", "last-ios"), "   \n");
    const out = readDeployedShas(home);
    assert.equal(out.backend.sha, SHA_A, "trailing newline must be trimmed");
    assert.ok(typeof out.backend.deployedAt === "number");
    assert.equal(out.ios, undefined, "a whitespace-only file is not a deploy");
  });

  it("returns empty when the deploy dir has never been created", () => {
    const home = mkdtempSync(path.join(os.tmpdir(), "deploy-status-"));
    assert.deepEqual(readDeployedShas(home), {});
  });
});

describe("readDeployLog", () => {
  it("flags a missing log rather than throwing", () => {
    const home = mkdtempSync(path.join(os.tmpdir(), "deploy-status-"));
    const out = readDeployLog({ home });
    assert.equal(out.missing, true);
    assert.deepEqual(out.lines, []);
  });

  it("returns the tail, newest last, and caps the request", () => {
    const home = mkdtempSync(path.join(os.tmpdir(), "deploy-status-"));
    mkdirSync(path.join(home, ".openclaw", "logs"), { recursive: true });
    const body = Array.from({ length: 50 }, (_, i) => `line ${i}`).join("\n");
    writeFileSync(path.join(home, ".openclaw", "logs", "deploy.log"), body);

    const out = readDeployLog({ home, tail: 5 });
    assert.deepEqual(out.lines, ["line 45", "line 46", "line 47", "line 48", "line 49"]);

    // An absurd tail must clamp, not read the whole file into a phone.
    assert.equal(readDeployLog({ home, tail: 99999 }).lines.length, 50);
    // A garbage tail falls back to the default rather than returning nothing.
    assert.equal(readDeployLog({ home, tail: "abc" }).lines.length, 50);
  });
});
