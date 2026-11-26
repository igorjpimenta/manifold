import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

describe("bin/cli.js", () => {
  test("carries no harness-layout fallback", async () => {
    const src = await readFile(new URL("../../../bin/cli.js", import.meta.url), "utf8");
    assert.ok(!src.includes(".openclaw"), "cli.js must not reference the harness layout");
  });
});
