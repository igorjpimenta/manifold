import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import net from "node:net";
import { checkDatabases, startOrWarn } from "../../../server/app.js";

const logger = { info: () => {}, warn: () => {}, error: () => {} };

function pathsIn(dir) {
  return { runtimeDb: path.join(dir, "runtime.sqlite"), opsDb: path.join(dir, "ops.sqlite") };
}

describe("checkDatabases", () => {
  it("accepts a fresh home — absent files are created on first write", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "manifold-app-check-"));
    try {
      assert.deepEqual(checkDatabases(pathsIn(dir), { logger }), { ok: true });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses an unreadable primary and names the file", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "manifold-app-check-"));
    try {
      const paths = pathsIn(dir);
      writeFileSync(paths.runtimeDb, "not a database", "utf8");
      const result = checkDatabases(paths, { logger });
      assert.equal(result.ok, false);
      assert.equal(result.dbPath, paths.runtimeDb);
      assert.ok(result.error);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

function listenOn(port) {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(port ?? 0, "127.0.0.1", () => resolve(server));
  });
}

describe("startOrWarn", () => {
  it("lets the engine boot when an auxiliary port is already held", async () => {
    const holder = await listenOn(0);
    const takenPort = holder.address().port;
    const warns = [];
    const cleanedUp = [];
    try {
      const up = await startOrWarn(
        { info: () => {}, warn: (msg) => warns.push(msg), error: () => {} },
        {
          label: "manifold dialog ws",
          address: `127.0.0.1:${takenPort}`,
          start: () => listenOn(takenPort).then(() => {}),
          stop: () => { cleanedUp.push("stop"); },
        },
      );
      assert.equal(up, false);
      assert.equal(warns.length, 1);
      assert.match(warns[0], new RegExp(`manifold dialog ws: failed to start on 127\\.0\\.0\\.1:${takenPort}`));
      assert.deepEqual(cleanedUp, ["stop"]);
    } finally {
      await new Promise((resolve) => holder.close(resolve));
    }
  });

  it("reports the service up when the port is free", async () => {
    let bound;
    const up = await startOrWarn(
      { info: () => {}, warn: () => {}, error: () => {} },
      {
        label: "manifold dashboard",
        address: "127.0.0.1:0",
        start: async () => { bound = await listenOn(0); },
        stop: async () => {},
      },
    );
    try {
      assert.equal(up, true);
      assert.ok(bound);
    } finally {
      await new Promise((resolve) => bound.close(resolve));
    }
  });
});
