// deploy-status.js — read-only view of what this host is actually running.
//
// Deploys are triggered by GitHub (workflow_run -> tailnet SSH ->
// scripts/deploy-on-green.sh) and nothing polls behind them, so "is this Mac
// on the same commit as main?" has no answer anywhere a human looks. The
// deployed SHA per mode lives in ~/.openclaw/deploy/last-{backend,ios}; this
// pairs it with the checkout's own HEAD.
//
// Nothing here writes. The deploy script owns those files; this only reads.

import os from "node:os";
import path from "node:path";
import { readFileSync, statSync, existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const MODES = ["backend", "ios"];

// Bounded so a runaway git invocation can't wedge a request handler.
const GIT_TIMEOUT_MS = 5_000;

async function git(repo, args) {
  try {
    const { stdout } = await execFileAsync("git", ["-C", repo, ...args], {
      timeout: GIT_TIMEOUT_MS,
      maxBuffer: 1 << 20,
    });
    return stdout.trim();
  } catch {
    return null;
  }
}

/**
 * Derive per-mode status from raw inputs. Pure — no IO — so the interesting
 * logic (what counts as current, what counts as never-deployed) is testable
 * without a repo or a filesystem.
 *
 * `current` compares against HEAD rather than against the remote on purpose:
 * it answers "is the code on disk what got deployed?", which is the question
 * that distinguishes a stale gateway from a stale checkout. Drift from the
 * remote is reported separately as `behind`.
 */
export function summarizeModes({ deployed = {}, headSha = null } = {}) {
  const out = {};
  for (const mode of MODES) {
    const entry = deployed[mode];
    if (!entry?.sha) {
      out[mode] = { sha: null, deployedAt: null, current: false, everDeployed: false };
      continue;
    }
    out[mode] = {
      sha: entry.sha,
      deployedAt: entry.deployedAt ?? null,
      // Unknown HEAD means we cannot claim currency — say false rather than
      // guess, since this drives a green pill.
      current: Boolean(headSha) && entry.sha === headSha,
      everDeployed: true,
    };
  }
  return out;
}

/** Read the per-mode SHA files the deploy script writes. */
export function readDeployedShas(home = os.homedir()) {
  const dir = path.join(home, ".openclaw", "deploy");
  const out = {};
  for (const mode of MODES) {
    const file = path.join(dir, `last-${mode}`);
    if (!existsSync(file)) continue;
    try {
      const sha = readFileSync(file, "utf8").trim();
      if (!sha) continue;
      out[mode] = { sha, deployedAt: statSync(file).mtimeMs };
    } catch { /* unreadable == not deployed, not an error worth surfacing */ }
  }
  return out;
}

/**
 * Stashes the deploy created while unblocking a fast-forward. Surfaced because
 * an auto-stash that nobody knows about is indistinguishable from lost work —
 * the whole point of stashing rather than discarding is that it stays visible.
 */
export async function readDeployStashes(repo) {
  const raw = await git(repo, ["stash", "list"]);
  if (!raw) return [];
  return raw
    .split("\n")
    .filter((l) => l.includes("deploy-autostash"))
    .map((l) => {
      const [ref, ...rest] = l.split(":");
      const m = /deploy-autostash ([0-9a-f]+) (\S+)/.exec(l);
      return { ref: ref.trim(), sha: m?.[1] ?? null, at: m?.[2] ?? null, raw: rest.join(":").trim() };
    });
}

export async function readDeployStatus({ home = os.homedir() } = {}) {
  const repo = path.join(home, ".openclaw");
  const [headSha, headSubject, behindRaw] = await Promise.all([
    git(repo, ["rev-parse", "HEAD"]),
    git(repo, ["log", "-1", "--format=%s"]),
    // Against the last-fetched origin/main. Deliberately NOT fetching here:
    // a request handler should not do network IO on a shared repo, and a
    // stale-but-instant answer beats a slow one that can also fail. The
    // deploy itself fetches, so this is accurate right after any deploy.
    git(repo, ["rev-list", "--count", "HEAD..origin/main"]),
  ]);

  const behind = behindRaw != null && /^\d+$/.test(behindRaw) ? Number(behindRaw) : null;

  return {
    head: { sha: headSha, subject: headSubject },
    remote: { behind },
    modes: summarizeModes({ deployed: readDeployedShas(home), headSha }),
    stashes: await readDeployStashes(repo),
  };
}

/**
 * Tail of the deploy log. Capped hard: this is rendered in a phone-sized
 * scroll view and streamed over a device connection, so an unbounded read of
 * a log that grows per deploy is not worth the memory.
 */
export function readDeployLog({ home = os.homedir(), tail = 200 } = {}) {
  const file = path.join(home, ".openclaw", "logs", "deploy.log");
  if (!existsSync(file)) return { lines: [], missing: true };
  const capped = Math.max(1, Math.min(Number(tail) || 200, 1000));
  try {
    const lines = readFileSync(file, "utf8").split("\n").filter(Boolean);
    return { lines: lines.slice(-capped), missing: false };
  } catch (err) {
    return { lines: [], missing: false, error: err?.message ?? String(err) };
  }
}
