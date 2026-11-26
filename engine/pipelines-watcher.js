// pipelines-watcher.js — keep `pipelinesRef` in sync with the on-disk
// pipelines directory handed in as `pipelinesDir`.
//
// loadPipelines otherwise runs only at gateway boot, so new or edited
// pipeline folders would stay invisible until the next restart. Poll every
// `pollMs`, fingerprint each `<dir>/<entry>/index.js` by
// name + mtime, and on change reload and swap into `pipelinesRef.value`.
// The loader cache-busts its own imports (data: URLs), so the swap picks
// up edited code.
//
// Directory mtime alone is not enough: the top-level dir only changes on
// add/remove at depth 1, never when an index.js is edited — hence the
// per-file mtime fingerprint.

import { readdir, stat } from "node:fs/promises";
import path from "node:path";
import { loadPipelines } from "./loader.js";

const DEFAULT_POLL_MS = 1000;

export function createPipelinesWatcher({ pipelinesDir, pipelinesRef, logger, pollMs = DEFAULT_POLL_MS }) {
  let lastFingerprint = null;
  let timer = null;
  let inFlight = false;
  const listeners = new Set();

  async function fingerprint() {
    let entries;
    try {
      entries = await readdir(pipelinesDir, { withFileTypes: true });
    } catch (err) {
      // Directory missing — empty fingerprint; if it appears later, the
      // next tick reloads.
      if (err.code === "ENOENT") return "";
      throw err;
    }
    const parts = [];
    for (const ent of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (ent.name.startsWith(".") || !ent.isDirectory()) continue;
      const idx = path.join(pipelinesDir, ent.name, "index.js");
      try {
        const s = await stat(idx);
        parts.push(`${ent.name}:${s.mtimeMs}`);
      } catch {
        // index.js missing — the loader skips it too.
      }
    }
    return parts.join("|");
  }

  async function tick() {
    if (inFlight) return;
    inFlight = true;
    try {
      const fp = await fingerprint();
      if (lastFingerprint === null) {
        // First tick primes the fingerprint only — the loader service
        // already loaded pipelines at boot.
        lastFingerprint = fp;
        return;
      }
      if (fp === lastFingerprint) return;
      const loaded = await loadPipelines(pipelinesDir, logger);
      // Merge, don't fully replace: a transient reload failure (file
      // mid-write, ESM error) would otherwise silently vanish a live
      // pipeline until its mtime changed again. Stale entries stay live
      // while their index.js is still on disk.
      const merged = new Map(loaded);
      for (const [id, existing] of pipelinesRef.value) {
        if (merged.has(id)) continue;
        // Synthetic subflow-child ids (`parent::node`) have no directory of
        // their own — stat-ing them always throws, so they'd be dropped
        // whenever their real parent transiently fails to reload. Check the
        // parent's directory instead (_parentPipelineId).
        const diskCheckId = existing._parentPipelineId ?? id;
        try {
          await stat(path.join(pipelinesDir, diskCheckId, "index.js"));
          merged.set(id, existing);
          logger?.warn?.(`manifold pipelines-watcher: kept stale entry for "${id}" (failed to reload)`);
        } catch {
          // parent's index.js gone — intentional removal, drop it
        }
      }
      pipelinesRef.value = merged;
      lastFingerprint = fp;
      logger?.info?.(`manifold pipelines-watcher: reloaded ${loaded.size} pipeline(s) after disk change`);
      for (const cb of listeners) {
        try { cb({ size: loaded.size, ids: [...loaded.keys()] }); }
        catch (err) { logger?.warn?.(`manifold pipelines-watcher: listener error: ${err?.message ?? err}`); }
      }
    } catch (err) {
      // Swallow — next tick retries.
      logger?.warn?.(`manifold pipelines-watcher: tick error: ${err?.message ?? err}`);
    } finally {
      inFlight = false;
    }
  }

  return {
    start() {
      if (timer) return;
      timer = setInterval(tick, pollMs);
      if (typeof timer?.unref === "function") timer.unref();
    },
    stop() {
      if (timer) { clearInterval(timer); timer = null; }
      lastFingerprint = null;
      listeners.clear();
    },
    onReload(cb) {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    _tick: tick,
    _fingerprint: fingerprint,
  };
}
