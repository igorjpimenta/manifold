// Audio constants and filesystem helpers for discuss audio upload/serve/history.
import path from "node:path";
import os from "node:os";
import { resolveStorePaths } from "../state/paths.js";

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

// Store-home audio root, resolved per call so a MANIFOLD_DATA_DIR change
// between import and first use still relocates the store.
export function discussAudioDir() {
  return resolveStorePaths().discussAudioDir;
}

// Read-only fallback for audio written under the harness home. Writes always
// go under discussAudioDir() above; reads consult this root only when the
// store-home root is absent.
function fallbackAudioDir() {
  return path.join(os.homedir(), ".openclaw", "discuss-audio");
}

export function audioRootForFlow(flowId) {
  return path.join(discussAudioDir(), flowId);
}

function fallbackAudioRootForFlow(flowId) {
  return path.join(fallbackAudioDir(), flowId);
}

// Flow audio roots to search, store home first. Serve paths consult each in
// order and stop at the first file hit.
export function audioRootsForFlow(flowId) {
  const root = audioRootForFlow(flowId);
  const fallback = fallbackAudioRootForFlow(flowId);
  return fallback === root ? [root] : [root, fallback];
}

export function audioDirForFlow(flowId, itemId) {
  const safeItem = String(itemId).replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "_";
  return path.join(audioRootForFlow(flowId), safeItem);
}

// Read all .meta.json sidecars under <store-home>/discuss-audio/<flowId>/
// across every item sub-directory. Returns a flat array of parsed meta objects.
export async function loadAudioMetasForFlow(flowId) {
  const metas = await readMetasUnder(audioRootForFlow(flowId));
  if (metas !== null) return metas;
  return (await readMetasUnder(fallbackAudioRootForFlow(flowId))) ?? [];
}

// Null when the root is absent (caller falls back); an array otherwise, so
// an existing-but-empty root never consults the fallback.
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
