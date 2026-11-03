// browser-tabs.js — page/tab lifecycle for crossProcessLock("browser:<profile>")
// nodes. The lock already serializes access to a shared, gateway-managed
// Chrome profile; this wraps the locked work so callers (workana's
// check-login.js, scrape-listings.js, submit-proposal.js, send-reply.js,
// and any future browser-dependent node) never have to open or close a
// tab themselves.
//
// A Chrome profile can end up with zero open page targets (e.g. after the
// last script closed the only tab it had opened). Playwright's
// connectOverCDP performs a browser-level Browser.setDownloadBehavior CDP
// call during its own connection setup; this Chrome build rejects that call
// outright when there are zero page targets — deterministically, in
// milliseconds, not as a timeout. The caller then exhausts its CDP retries
// instantly and falls back to launchPersistentContext, which collides with
// the same Chrome's SingletonLock on the shared profile directory.
//
// Fix: guarantee at least one page exists before the locked work starts,
// and close whatever appeared during it before the lock releases — diffed
// against a before/after snapshot, not a single tracked id, since the
// locked work may open more than one tab (e.g. parallel fetches). Never
// touches a tab that was already open before the snapshot — a human
// working in the same profile to clear a captcha/2FA prompt, or anything
// else pre-existing, is left alone.
//
// Talks to the openclaw-browser plugin via its `openclaw browser` CLI, not
// the plugin's own HTTP control server (port gatewayPort+2), whose vendored
// middleware fails every request — a bug outside this git tree and so not
// fixable here. A direct in-process import of the vendored package's internal
// dispatcher was also considered and rejected: that module's filename is a
// content hash that changes on every `openclaw` version bump, which would
// silently break this on the next upgrade. The CLI is the stable surface (it
// is also what check-login.js's cookie-bridge step already shells out to) —
// see the project's own prior-art note: "openclaw has no MCP client in plugin
// runtime — spawn the CLI directly".
//
// Cost: each CLI invocation carries plugin-boot overhead (no warm-up), so
// invocations aren't cheap. listTabs (before) + openTab (if empty) +
// listTabs (after) are sequential/causally dependent and can't be
// collapsed — but closeTab calls run in parallel (Promise.all), so N closes
// cost roughly the same wall-clock as one.
// Worth preserving rather than re-deriving: each close is a separate CLI
// spawn at the per-invocation cost above, so nothing about the code shape
// implies the parallelism is what keeps it flat.
//
// Scoped to the configured profile set the caller injects (the harness
// passes its browser.profiles down through withLock opts), not any string
// after "browser:" — "browser:" is not
// a reserved prefix, and a crossProcessLock value of "browser:linkedin" is
// a legal arbitrary lock name with no real linkedin profile behind it
// (test/unit/engine/executor-subflow-children-fetch-profiles-e2e.test.js
// declares exactly that). Without this check, such a name would
// shell out to the real CLI for a profile that doesn't exist — breaking
// test isolation and adding multi-second latency for nothing.

import { existsSync } from "node:fs";

// The configured profile set arrives as an injected parameter — a Set, an
// array of names, or the harness config's browser.profiles map — and is
// never read from disk here. Anything else (including unprovided) means
// no scoping: every "browser:" name stays inert.
function normalizeConfiguredProfiles(input) {
  if (input instanceof Set) return input;
  if (Array.isArray(input)) return new Set(input.filter((p) => typeof p === "string" && p.length > 0));
  if (input && typeof input === "object") return new Set(Object.keys(input));
  return new Set();
}

function resolveOpenclawBin() {
  // Mirrors ensure-login-node.js's resolver: cron/gateway-spawned children
  // inherit a PATH that strips /opt/homebrew/bin where Homebrew installs
  // the openclaw CLI.
  if (process.env.OPENCLAW_BIN) return process.env.OPENCLAW_BIN;
  if (existsSync("/opt/homebrew/bin/openclaw")) return "/opt/homebrew/bin/openclaw";
  if (existsSync("/usr/local/bin/openclaw")) return "/usr/local/bin/openclaw";
  return "openclaw";
}

async function runBrowserCli(profile, args, { timeoutMs = 15000 } = {}) {
  const { spawn } = await import("node:child_process");
  return new Promise((resolve, reject) => {
    const child = spawn(
      resolveOpenclawBin(),
      ["browser", "--browser-profile", profile, "--json", ...args],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { child.kill("SIGKILL"); } catch { /* ignore */ }
      reject(new Error(`openclaw browser ${args.join(" ")} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });
    child.on("exit", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) {
        reject(new Error(`openclaw browser ${args.join(" ")} exit=${code}: ${stderr.slice(0, 300)}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (err) {
        reject(new Error(`openclaw browser ${args.join(" ")}: unparseable JSON output: ${err?.message ?? err}`));
      }
    });
  });
}

async function listPageTabs(profile) {
  const result = await runBrowserCli(profile, ["tabs"]);
  const tabs = Array.isArray(result?.tabs) ? result.tabs : [];
  // Only "page" targets count toward the zero-page CDP bug — background
  // pages / service workers (extension internals) aren't navigable tabs
  // and don't satisfy the Browser.setDownloadBehavior handshake the same
  // way, so they're excluded from both the readiness check and the diff.
  return tabs.filter((t) => t?.type === "page" && t?.targetId);
}

async function openBlankTab(profile) {
  return runBrowserCli(profile, ["open", "about:blank"]);
}

async function closeTabs(profile, targetIds) {
  // Independent child processes — run concurrently so N closes cost the
  // same wall-clock time as one, not N times as much.
  await Promise.all(targetIds.map((id) =>
    runBrowserCli(profile, ["close", id]).catch(() => {
      // Best-effort — a tab we can't close isn't worth failing the node
      // over. Worst case it lingers until the next session's diff catches
      // it as pre-existing (harmless) or a human closes it manually.
    })
  ));
}

// The configured profile a "browser:<profile>" lock name refers to, or null
// for any other lock name — including "browser:" names that don't match a
// real profile in the injected set, which stay inert.
export function browserProfileForLock(lockName, configuredProfiles) {
  if (typeof lockName !== "string" || !lockName.startsWith("browser:")) return null;
  const profile = lockName.slice("browser:".length);
  if (!profile || !normalizeConfiguredProfiles(configuredProfiles).has(profile)) return null;
  return profile;
}

// Start of a tab session: snapshot the profile's current page tabs, and open
// a blank one if there are none (the zero-page CDP bug in the header).
// Returns the snapshot endTabSession diffs against.
export async function beginTabSession(profile, { logger } = {}) {
  let beforeIds = new Set();
  try {
    beforeIds = new Set((await listPageTabs(profile)).map((t) => t.targetId));
  } catch (err) {
    logger?.warn?.(`manifold browser-tabs: pre-listTabs(${profile}) failed: ${err?.message ?? err} — proceeding without the readiness guarantee`);
  }
  if (beforeIds.size === 0) {
    try {
      await openBlankTab(profile);
    } catch (err) {
      logger?.warn?.(`manifold browser-tabs: openTab(${profile}) failed: ${err?.message ?? err} — proceeding anyway`);
    }
  }
  return { profile, beforeIds };
}

// End of a tab session: close every page tab that appeared since
// beginTabSession. Skipped when `isOwned()` says the lock is no longer this
// holder's — the tabs that appeared since then belong to whoever holds it now.
export async function endTabSession(session, { logger, isOwned } = {}) {
  if (!session?.profile) return;
  if (isOwned && !isOwned()) {
    logger?.warn?.(`manifold browser-tabs: lock for ${session.profile} is no longer ours — leaving tabs to the current holder`);
    return;
  }
  try {
    const after = await listPageTabs(session.profile);
    const newIds = after.map((t) => t.targetId).filter((id) => !session.beforeIds.has(id));
    await closeTabs(session.profile, newIds);
  } catch (err) {
    logger?.warn?.(`manifold browser-tabs: post-cleanup(${session.profile}) failed: ${err?.message ?? err}`);
  }
}

// Wraps `fn` so that, for a `crossProcessLock` value of "browser:<profile>",
// at least one page exists on that profile's Chrome before `fn()` runs, and
// every tab that appeared while `fn()` ran is closed again before this
// settles — regardless of whether `fn()` opened one tab or several. For any
// other lock name (or no lock at all), `fn` passes through untouched.
//
// Best-effort throughout: a failure to list/open/close tabs logs and falls
// through to running `fn()` anyway — this is a readiness/cleanup guarantee
// layered on top of the lock, not a new failure mode the lock didn't
// already have. Worst case on a plugin hiccup is the script doing its own
// CDP dance, not a newly-broken node.
export function wrapBrowserTabLifecycle(lockName, fn, { logger, isOwned, browserProfiles } = {}) {
  const profile = browserProfileForLock(lockName, browserProfiles);
  if (!profile) return fn;

  return async () => {
    const session = await beginTabSession(profile, { logger });
    try {
      return await fn();
    } finally {
      await endTabSession(session, { logger, isOwned });
    }
  };
}
