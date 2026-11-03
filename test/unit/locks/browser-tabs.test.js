// Unit tests for locks/browser-tabs.js — wrapBrowserTabLifecycle.
//
// Mocks node:child_process's spawn() (not fetch — the implementation
// shells out to the `openclaw browser` CLI, not the plugin's HTTP control
// server; see browser-tabs.js's header comment for why) via
// mock.module().

import { describe, it, mock, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";

// The configured profile set is injected per call (opts.browserProfiles) —
// no fixture home, no openclaw.json anywhere. PROFILES is the injected set
// every scoping test below passes explicitly.
const PROFILES = ["freelance"];

// Drives canned CLI responses keyed by the subcommand (tabs/open/close),
// and records every invocation's full argv for assertions.
function makeFakeSpawn({ tabsBefore = [], onOpen, onClose } = {}) {
  const calls = [];
  let currentTabs = tabsBefore;

  const spawnImpl = (bin, args) => {
    calls.push(args.slice());
    const child = new EventEmitter();
    child.stdout = new EventEmitter();
    child.stderr = new EventEmitter();
    child.kill = () => {};

    // args shape: ["browser", "--browser-profile", profile, "--json", <subcommand>, ...rest]
    const subcommand = args[4];
    const rest = args.slice(5);

    queueMicrotask(() => {
      if (subcommand === "tabs") {
        child.stdout.emit("data", JSON.stringify({ tabs: currentTabs }));
        child.emit("exit", 0);
        return;
      }
      if (subcommand === "open") {
        const tab = onOpen ? onOpen() : { targetId: "opened-1", type: "page", url: rest[0] };
        currentTabs = [...currentTabs, tab];
        child.stdout.emit("data", JSON.stringify(tab));
        child.emit("exit", 0);
        return;
      }
      if (subcommand === "close") {
        const targetId = rest[0];
        onClose?.(targetId);
        currentTabs = currentTabs.filter((t) => t.targetId !== targetId);
        child.stdout.emit("data", JSON.stringify({ ok: true }));
        child.emit("exit", 0);
        return;
      }
      child.stderr.emit("data", `unexpected subcommand: ${subcommand}`);
      child.emit("exit", 1);
    });

    return child;
  };

  return { spawnImpl, calls, getCurrentTabs: () => currentTabs };
}

let wrapBrowserTabLifecycle;

beforeEach(async () => {
  mock.restoreAll();
});

afterEach(() => {
  mock.restoreAll();
});

async function loadWithFakeSpawn(spawnImpl) {
  await mock.module("node:child_process", { exports: { spawn: spawnImpl } });
  const mod = await import(`../../../locks/browser-tabs.js?t=${Date.now()}-${Math.random()}`);
  return mod.wrapBrowserTabLifecycle;
}

describe("wrapBrowserTabLifecycle — pass-through", () => {
  it("returns fn unchanged for a non-browser lock name", async () => {
    const { wrapBrowserTabLifecycle: wrap } = await import("../../../locks/browser-tabs.js");
    const fn = async () => "result";
    assert.equal(wrap("db:sqlite", fn), fn);
  });

  it("returns fn unchanged for null/undefined lock name", async () => {
    const { wrapBrowserTabLifecycle: wrap } = await import("../../../locks/browser-tabs.js");
    const fn = async () => "result";
    assert.equal(wrap(null, fn), fn);
    assert.equal(wrap(undefined, fn), fn);
  });

  it("returns fn unchanged for a bare 'browser:' with no profile", async () => {
    const { wrapBrowserTabLifecycle: wrap } = await import("../../../locks/browser-tabs.js");
    const fn = async () => "result";
    assert.equal(wrap("browser:", fn), fn);
  });

  it("returns fn unchanged for a browser:<profile> name when no profiles are injected (empty default)", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [] });
    const wrap = await loadWithFakeSpawn(spawnImpl);
    const fn = async () => "result";
    assert.equal(wrap("browser:freelance", fn), fn);
    assert.equal(calls.length, 0);
  });
});

describe("wrapBrowserTabLifecycle — readiness (open-if-empty)", () => {
  it("opens a tab when the profile has zero page tabs before fn() runs", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [] });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    const fn = async () => "ok";
    const result = await wrap("browser:freelance", fn, { browserProfiles: PROFILES })();

    assert.equal(result, "ok");
    const opens = calls.filter((a) => a[4] === "open");
    assert.equal(opens.length, 1);
    assert.deepEqual(opens[0].slice(0, 3), ["browser", "--browser-profile", "freelance"]);
  });

  it("does NOT open a tab when the profile already has a page tab", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [{ targetId: "existing-1", type: "page" }] });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    await wrap("browser:freelance", async () => "ok", { browserProfiles: PROFILES })();

    assert.equal(calls.filter((a) => a[4] === "open").length, 0);
  });

  it("ignores non-page targets (background_page/service_worker) when checking emptiness", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({
      tabsBefore: [
        { targetId: "bg-1", type: "background_page" },
        { targetId: "sw-1", type: "service_worker" },
      ],
    });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    await wrap("browser:freelance", async () => "ok", { browserProfiles: PROFILES })();

    // Only background_page/service_worker exist — that still counts as
    // zero real page tabs, so a seed tab must be opened.
    assert.equal(calls.filter((a) => a[4] === "open").length, 1);
  });
});

describe("wrapBrowserTabLifecycle — cleanup (diff-based close)", () => {
  it("closes the seed tab it opened when the profile started empty", async () => {
    const closed = [];
    const { spawnImpl } = makeFakeSpawn({ tabsBefore: [], onOpen: () => ({ targetId: "opened-1", type: "page" }), onClose: (id) => closed.push(id) });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    await wrap("browser:freelance", async () => "ok", { browserProfiles: PROFILES })();

    assert.deepEqual(closed, ["opened-1"]);
  });

  it("closes every tab fn() opened, not just one, when several appear in parallel", async () => {
    const closed = [];
    const { spawnImpl, getCurrentTabs } = makeFakeSpawn({
      tabsBefore: [{ targetId: "existing-1", type: "page" }],
      onClose: (id) => closed.push(id),
    });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    const fn = async () => {
      // Simulate a script opening three tabs of its own via its own
      // Playwright client — the wrapper never told it to, and never finds
      // out except by diffing the tab list afterward.
      const current = getCurrentTabs();
      current.push(
        { targetId: "parallel-1", type: "page" },
        { targetId: "parallel-2", type: "page" },
        { targetId: "parallel-3", type: "page" },
      );
      return "ok";
    };
    await wrap("browser:freelance", fn, { browserProfiles: PROFILES })();

    assert.deepEqual(closed.sort(), ["parallel-1", "parallel-2", "parallel-3"]);
  });

  it("never closes a tab that was already open before fn() ran", async () => {
    const closed = [];
    const { spawnImpl } = makeFakeSpawn({
      tabsBefore: [{ targetId: "human-tab", type: "page" }],
      onClose: (id) => closed.push(id),
    });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    await wrap("browser:freelance", async () => "ok", { browserProfiles: PROFILES })();

    assert.equal(closed.includes("human-tab"), false);
  });

  it("still cleans up and rethrows when fn() throws", async () => {
    const closed = [];
    const { spawnImpl } = makeFakeSpawn({ tabsBefore: [], onOpen: () => ({ targetId: "opened-1", type: "page" }), onClose: (id) => closed.push(id) });
    const wrap = await loadWithFakeSpawn(spawnImpl);

    await assert.rejects(wrap("browser:freelance", async () => { throw new Error("script blew up"); }, { browserProfiles: PROFILES })(), /script blew up/);
    assert.deepEqual(closed, ["opened-1"]);
  });
});

describe("wrapBrowserTabLifecycle — degrades gracefully on plugin failure", () => {
  it("still runs fn() and returns its result when the CLI exits non-zero", async () => {
    const spawnImpl = (bin, args) => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {};
      queueMicrotask(() => {
        child.stderr.emit("data", "browser control disabled");
        child.emit("exit", 1);
      });
      return child;
    };
    const wrap = await loadWithFakeSpawn(spawnImpl);
    const logger = { warn: mock.fn() };

    const result = await wrap("browser:freelance", async () => "ok", { logger, browserProfiles: PROFILES })();

    assert.equal(result, "ok");
    assert.ok(logger.warn.mock.callCount() > 0);
  });

  it("does not throw when the post-cleanup tabs listing fails", async () => {
    let listCallCount = 0;
    const spawnImpl = (bin, args) => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {};
      const subcommand = args[4];
      queueMicrotask(() => {
        if (subcommand === "tabs") {
          listCallCount += 1;
          if (listCallCount === 1) {
            child.stdout.emit("data", JSON.stringify({ tabs: [{ targetId: "existing-1", type: "page" }] }));
            child.emit("exit", 0);
          } else {
            child.stderr.emit("data", "gateway restarted mid-run");
            child.emit("exit", 1);
          }
          return;
        }
        child.emit("exit", 1);
      });
      return child;
    };
    const wrap = await loadWithFakeSpawn(spawnImpl);
    const logger = { warn: mock.fn() };

    const result = await wrap("browser:freelance", async () => "ok", { logger, browserProfiles: PROFILES })();

    assert.equal(result, "ok");
  });
});

describe("tab cleanup is skipped once the lock is no longer ours", () => {
  it("wrap: a tab opened during fn() is left alone when isOwned() is false at cleanup", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [{ targetId: "existing-1", type: "page" }] });
    const wrap = await loadWithFakeSpawn(spawnImpl);
    const fn = async () => {
      // Simulates the new holder's tab appearing after a takeover.
      await new Promise((resolve) => {
        const child = spawnImpl("openclaw", ["browser", "--browser-profile", "freelance", "--json", "open", "about:blank"]);
        child.on("exit", resolve);
      });
      return "ok";
    };
    await wrap("browser:freelance", fn, { isOwned: () => false, browserProfiles: PROFILES })();
    assert.equal(calls.filter((a) => a[4] === "close").length, 0);
  });

  it("wrap: still closes new tabs when isOwned() is true", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [{ targetId: "existing-1", type: "page" }] });
    const wrap = await loadWithFakeSpawn(spawnImpl);
    const fn = async () => {
      await new Promise((resolve) => {
        const child = spawnImpl("openclaw", ["browser", "--browser-profile", "freelance", "--json", "open", "about:blank"]);
        child.on("exit", resolve);
      });
    };
    await wrap("browser:freelance", fn, { isOwned: () => true, browserProfiles: PROFILES })();
    assert.deepEqual(calls.filter((a) => a[4] === "close").map((a) => a[5]), ["opened-1"]);
  });

  it("beginTabSession/endTabSession: split form closes only tabs that appeared in between", async () => {
    const { spawnImpl, calls } = makeFakeSpawn({ tabsBefore: [{ targetId: "existing-1", type: "page" }] });
    await mock.module("node:child_process", { exports: { spawn: spawnImpl } });
    const mod = await import(`../../../locks/browser-tabs.js?t=${Date.now()}-${Math.random()}`);
    const session = await mod.beginTabSession("freelance");
    await new Promise((resolve) => {
      const child = spawnImpl("openclaw", ["browser", "--browser-profile", "freelance", "--json", "open", "about:blank"]);
      child.on("exit", resolve);
    });
    await mod.endTabSession(session);
    assert.deepEqual(calls.filter((a) => a[4] === "close").map((a) => a[5]), ["opened-1"]);
  });

  it("browserProfileForLock: only injected browser:<profile> names map to a profile", async () => {
    const { browserProfileForLock } = await import("../../../locks/browser-tabs.js");
    const configured = new Set(["freelance"]);
    assert.equal(browserProfileForLock("browser:freelance", configured), "freelance");
    assert.equal(browserProfileForLock("browser:not-a-profile", configured), null);
    assert.equal(browserProfileForLock("db:sqlite", configured), null);
    assert.equal(browserProfileForLock(undefined, configured), null);
  });

  it("browserProfileForLock: accepts an array or a browser.profiles map, defaults to empty", async () => {
    const { browserProfileForLock } = await import("../../../locks/browser-tabs.js");
    assert.equal(browserProfileForLock("browser:freelance", ["freelance"]), "freelance");
    assert.equal(browserProfileForLock("browser:freelance", { freelance: {} }), "freelance");
    assert.equal(browserProfileForLock("browser:freelance"), null);
    assert.equal(browserProfileForLock("browser:freelance", undefined), null);
    assert.equal(browserProfileForLock("browser:freelance", []), null);
    assert.equal(browserProfileForLock("browser:freelance", {}), null);
  });
});
