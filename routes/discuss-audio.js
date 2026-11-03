// Audio constants and filesystem helpers for discuss audio upload/serve/history.
import path from "node:path";
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

export function audioRootForFlow(flowId) {
  return path.join(discussAudioDir(), flowId);
}

// Single search root: the store home. Pre-store-home audio is not read.
export function audioRootsForFlow(flowId) {
  return [audioRootForFlow(flowId)];
}

export function audioDirForFlow(flowId, itemId) {
  const safeItem = String(itemId).replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "_";
  return path.join(audioRootForFlow(flowId), safeItem);
}

// Read all .meta.json sidecars under <store-home>/discuss-audio/<flowId>/
// across every item sub-directory. Returns a flat array of parsed meta objects,
// [] when the root is absent.
export async function loadAudioMetasForFlow(flowId) {
  return (await readMetasUnder(audioRootForFlow(flowId))) ?? [];
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
