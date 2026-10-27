// pipelines-watcher.js — keep `pipelinesRef` in sync with the on-disk
// pipelines directory (injected `pipelinesDir`: table → env → dataDir default,
// see state/paths.js — never a hardcoded harness path).
//
// Why: without this, `loadPipelines` only runs once at gateway boot (via the
// `manifold-loader` service or `ensurePipelinesLoaded`'s "load once if
// empty" guard). New pipeline folders dropped after boot, or edits to existing
// `index.js` files, are invisible until the next gateway restart.
//
// How: poll every `pollMs` (default 1000). Each tick, compute a fingerprint
// of `<dir>/<entry>/index.js` files keyed by name and mtime. If the
// fingerprint changed since last tick, call `loadPipelines` and swap into
// `pipelinesRef.value`. The loader already cache-busts imports by reading
// each index.js itself and importing it as a base64-encoded `data:` URL
// (each payload is a unique URL, so Node's ESM cache can't return a stale
// module), so the swap picks up edited code.
//
// Note on directory mtime: the top-level `.pipelines/` mtime only changes on
// add/remove at depth 1 — it does NOT change when an `index.js` is edited.
// That's why we fingerprint each `index.js`'s mtime explicitly.

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
      // Directory missing — treat as empty fingerprint. If it later appears,
      // the next tick reloads.
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
        // index.js missing — skip; loader would skip it too.
      }
      const fleet = path.join(pipelinesDir, ent.name, "fleet.yaml");
      try {
        const sf = await stat(fleet);
        parts.push(`${ent.name}:fleet:${sf.mtimeMs}`);
      } catch { /* fleet.yaml optional */ }
    }
    return parts.join("|");
  }

  async function tick() {
    if (inFlight) return;
    inFlight = true;
    try {
      const fp = await fingerprint();
      if (lastFingerprint === null) {
        // First tick: prime the fingerprint without reloading — the loader
        // service has already loaded pipelines at gateway boot.
        lastFingerprint = fp;
        return;
      }
      if (fp === lastFingerprint) return;
      const loaded = await loadPipelines(pipelinesDir, logger);
      // Merge rather than full-replace: if a pipeline that was previously live
      // fails to reload (transient ESM cache error, file mid-write during a git
      // op) its entry would silently vanish from the live Map and stay missing
      // until the next mtime change. Instead, keep stale entries whose index.js
      // is still on disk — they stay live until the disk copy is actually gone.
      const merged = new Map(loaded);
      for (const [id, existing] of pipelinesRef.value) {
        if (merged.has(id)) continue;
        // Synthetic subflow-child pipelines (id = `${parentPipelineId}::${nodeId}`,
        // registered in-memory by loader.js's registerSubflowChildren) are never
        // disk-backed at <pipelinesDir>/<id>/ — stat-ing their own id always
        // throws, so they'd be dropped every time their real parent transiently
        // fails to reload, even though the parent itself survives via this same
        // loop. Check the parent's directory instead (loader.js tags every
        // synthetic entry with _parentPipelineId for exactly this).
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
    // Exposed for tests / probes.
    _tick: tick,
    _fingerprint: fingerprint,
  };
}
