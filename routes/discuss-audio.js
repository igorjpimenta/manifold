// Audio constants and filesystem helpers for discuss audio upload/serve/history.
import path from "node:path";
import { resolveStorePaths, harnessMediaAudioDir } from "../state/paths.js";

export const AUDIO_MIME_EXT = {
  "audio/m4a":   "m4a",
  "audio/mp4":   "m4a",
  "audio/aac":   "m4a",
  "audio/x-m4a": "m4a",
  "audio/mpeg":  "mp3",
  "audio/mp3":   "mp3",
  "audio/wav":   "wav",
  "audio/x-wav": "wav",
  "audio/webm":  "webm",
  "audio/ogg":   "ogg",
};

export const EXT_TO_MIME = {
  m4a:  "audio/m4a",
  mp3:  "audio/mpeg",
  wav:  "audio/wav",
  webm: "audio/webm",
  ogg:  "audio/ogg",
};

// Harness namespace for stored audio, resolved per call like the store
// home itself. Single-harness deployments take the "openclaw" default;
// MANIFOLD_HARNESS relocates it without touching code.
export function audioHarness(env = process.env) {
  return String(env?.MANIFOLD_HARNESS ?? "").trim() || "openclaw";
}

// Media root for this harness: <store-home>/media/audio/<harness>.
// Resolved per call so a MANIFOLD_DATA_DIR change between import and first
// use still relocates the store.
export function harnessAudioDir(harness = audioHarness()) {
  return harnessMediaAudioDir(resolveStorePaths().dataDir, harness ?? audioHarness());
}

export function audioRootForFlow(flowId, harness) {
  return path.join(harnessAudioDir(harness), flowId);
}

// Single search root: this harness's media dir. No cross-harness reads.
export function audioRootsForFlow(flowId, harness) {
  return [audioRootForFlow(flowId, harness)];
}

export function audioDirForFlow(flowId, itemId, harness) {
  const safeItem = String(itemId).replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "_";
  return path.join(audioRootForFlow(flowId, harness), safeItem);
}

// Read all .meta.json sidecars under <store-home>/media/audio/<harness>/<flowId>/
// across every item sub-directory. Returns a flat array of parsed meta objects,
// [] when the root is absent.
export async function loadAudioMetasForFlow(flowId, harness) {
  return (await readMetasUnder(audioRootForFlow(flowId, harness))) ?? [];
}

// Null when the root is absent; an array otherwise.
async function readMetasUnder(root) {
  const { readdir, readFile } = await import("node:fs/promises");
  let itemDirs;
  try { itemDirs = await readdir(root); }
  catch { return null; }
  const metas = [];
  for (const itemDir of itemDirs) {
    let files;
    try { files = await readdir(path.join(root, itemDir)); }
    catch { continue; }
    for (const f of files) {
      if (!f.endsWith(".meta.json")) continue;
      try {
        const text = await readFile(path.join(root, itemDir, f), "utf8");
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object") metas.push(parsed);
      } catch { /* skip corrupt sidecar */ }
    }
  }
  return metas;
}
