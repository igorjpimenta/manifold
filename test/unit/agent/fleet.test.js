// Unit tests for agent/fleet.js — loadFleet, parseFleetYaml.

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { loadFleet } from "../../../agent/fleet.js";

// ─── helpers ─────────────────────────────────────────────────────────────────

async function makeTempDir() {
  const dir = path.join(os.tmpdir(), `gf-fleet-test-${randomUUID()}`);
  await mkdir(dir, { recursive: true });
  return {
    dir,
    async writeFleet(content) {
      await writeFile(path.join(dir, "fleet.yaml"), content, "utf8");
    },
    async cleanup() {
      await rm(dir, { recursive: true, force: true });
    },
  };
}

// ─── parseFleetYaml (via loadFleet with real temp YAML files) ────────────────

describe("parseFleetYaml — empty input", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("empty file returns empty object", async () => {
    await tmp.writeFleet("");
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result, {});
  });

  test("whitespace-only file returns empty object", async () => {
    await tmp.writeFleet("   \n   \n");
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result, {});
  });
});

describe("parseFleetYaml — top-level tools list", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("parses a top-level tools list correctly", async () => {
    await tmp.writeFleet(`tools:\n  - Bash\n  - Read\n  - Edit\n`);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash", "Read", "Edit"]);
  });

  test("single-item top-level tools list", async () => {
    await tmp.writeFleet(`tools:\n  - Bash\n`);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash"]);
  });
});

describe("parseFleetYaml — nested context with tools and agent sub-keys", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("parses nested discuss context with context-level tools", async () => {
    const yaml = [
      "tools:",
      "  - Bash",
      "discuss:",
      "  tools:",
      "    - Read",
      "    - Edit",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash"]);
    assert.deepEqual(result.discuss.tools, ["Read", "Edit"]);
  });

  test("parses agent sub-key inside discuss context", async () => {
    const yaml = [
      "tools:",
      "  - Bash",
      "discuss:",
      "  tools:",
      "    - Read",
      "  content-writer:",
      "    tools:",
      "      - Write",
      "      - Edit",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash"]);
    assert.deepEqual(result.discuss.tools, ["Read"]);
    assert.deepEqual(result.discuss["content-writer"].tools, ["Write", "Edit"]);
  });

  test("parses multiple agent sub-keys inside discuss context", async () => {
    const yaml = [
      "discuss:",
      "  tools:",
      "    - Bash",
      "  content-writer:",
      "    tools:",
      "      - Read",
      "  reviewer:",
      "    tools:",
      "      - Edit",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.discuss["content-writer"].tools, ["Read"]);
    assert.deepEqual(result.discuss["reviewer"].tools, ["Edit"]);
  });
});

describe("parseFleetYaml — comment stripping", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("strips comment-only lines", async () => {
    const yaml = [
      "# this is a full-line comment",
      "tools:",
      "  - Bash # this tool is very useful",
      "  - Read",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash", "Read"]);
  });

  test("strips inline comments from keys and values", async () => {
    const yaml = [
      "tools: # top-level tools",
      "  - Bash",
      "discuss: # discuss context",
      "  tools:",
      "    - Read # for reading files",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash"]);
    assert.deepEqual(result.discuss.tools, ["Read"]);
  });

  test("blank lines between sections are ignored", async () => {
    const yaml = [
      "tools:",
      "  - Bash",
      "",
      "",
      "discuss:",
      "  tools:",
      "    - Read",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.deepEqual(result.tools, ["Bash"]);
    assert.deepEqual(result.discuss.tools, ["Read"]);
  });
});

describe("parseFleetYaml — scalar values", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("parses scalar string value after a key", async () => {
    await tmp.writeFleet("version: 1\n");
    const result = await loadFleet(tmp.dir);
    assert.equal(result.version, "1");
  });

  test("parses multiple scalar values", async () => {
    const yaml = "name: my-pipeline\nversion: 2\n";
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.equal(result.name, "my-pipeline");
    assert.equal(result.version, "2");
  });
});

describe("parseFleetYaml — null value for key with no child block", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("key with no value and no following child block is null", async () => {
    const yaml = "tools:\ndiscuss:\n";
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.equal(result.discuss, null);
  });

  test("key with no value but followed by sibling key at same indent is null", async () => {
    const yaml = [
      "tools:",
      "discuss:",
      "name: foo",
    ].join("\n");
    await tmp.writeFleet(yaml);
    const result = await loadFleet(tmp.dir);
    assert.equal(result.tools, null);
    assert.equal(result.discuss, null);
    assert.equal(result.name, "foo");
  });
});

describe("parseFleetYaml — full realistic fleet.yaml", () => {
  let tmp;
  before(async () => { tmp = await makeTempDir(); });
  after(async () => { await tmp.cleanup(); });

  test("roundtrip: parse a realistic fleet.yaml and verify all levels", async () => {
    const yaml = [
      "# Fleet config for proposal-writer pipeline",
      "tools:",
      "  - Bash",
      "  - Read",
      "",
      "discuss:",
      "  tools:",
      "    - Read",
      "    - Edit",
      "  content-writer: # the drafter agent",
      "    tools:",
      "      - Read",
      "      - Edit",
      "      - Write",
      "  reviewer:",
      "    tools:",
      "      - Read",
    ].join("\n");

    await tmp.writeFleet(yaml);
    const fleet = await loadFleet(tmp.dir);

    // pipeline-level
    assert.deepEqual(fleet.tools, ["Bash", "Read"]);

    // context-level
    assert.deepEqual(fleet.discuss.tools, ["Read", "Edit"]);

    // agent level
    assert.deepEqual(fleet.discuss["content-writer"].tools, ["Read", "Edit", "Write"]);
    assert.deepEqual(fleet.discuss["reviewer"].tools, ["Read"]);
  });
});

// ─── loadFleet — filesystem edge cases ───────────────────────────────────────

describe("loadFleet — absent file (ENOENT)", () => {
  test("returns null when fleet.yaml does not exist", async () => {
    const tmp = await makeTempDir();
    try {
      const result = await loadFleet(tmp.dir);
      assert.equal(result, null);
    } finally {
      await tmp.cleanup();
    }
  });

  test("returns null for a non-existent directory path", async () => {
    const result = await loadFleet(path.join(os.tmpdir(), `no-such-dir-${randomUUID()}`));
    assert.equal(result, null);
  });
});

describe("loadFleet — non-ENOENT filesystem errors", () => {
  test("rethrows error when readFile fails with a non-ENOENT code", async () => {
    // Create a directory where fleet.yaml is expected but make a subdirectory
    // named fleet.yaml so reading it as a file fails with EISDIR.
    const tmp = await makeTempDir();
    try {
      await mkdir(path.join(tmp.dir, "fleet.yaml"), { recursive: true });
      await assert.rejects(
        () => loadFleet(tmp.dir),
        (err) => {
          assert.notEqual(err.code, "ENOENT", "should not swallow non-ENOENT errors");
          return true;
        }
      );
    } finally {
      await tmp.cleanup();
    }
  });
});

describe("loadFleet — returns parsed object on success", () => {
  test("returns a plain object (not null) when fleet.yaml exists and is valid", async () => {
    const tmp = await makeTempDir();
    try {
      await tmp.writeFleet("tools:\n  - Bash\n");
      const result = await loadFleet(tmp.dir);
      assert.ok(result !== null);
      assert.equal(typeof result, "object");
    } finally {
      await tmp.cleanup();
    }
  });
});
