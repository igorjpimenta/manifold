import path from "node:path";
import { resolveStorePaths, harnessMediaAudioDir } from "../state/paths.js";
import { assertDeclaration } from "../mcp/identity.js";

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

// Audio is namespaced per connection-declared harness (X-Manifold-Harness,
// same assertion as mcp/identity.js) — never env, never defaulted.
export function resolveAudioHarness(req) {
  const header = req?.headers?.["x-manifold-harness"];
  const { harness } = assertDeclaration({ harness: Array.isArray(header) ? header[0] : header });
  return harness;
}

// Resolved per call (not at import) so a MANIFOLD_DATA_DIR change before
// first use still relocates the store.
export function harnessAudioDir(harness) {
  return harnessMediaAudioDir(resolveStorePaths().dataDir, harness);
}

export function audioRootForFlow(flowId, harness) {
  return path.join(harnessAudioDir(harness), flowId);
}

// Single root, this harness's media dir — never cross-harness.
export function audioRootsForFlow(flowId, harness) {
  return [audioRootForFlow(flowId, harness)];
}

export function audioDirForFlow(flowId, itemId, harness) {
  const safeItem = String(itemId).replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "_";
  return path.join(audioRootForFlow(flowId, harness), safeItem);
}

export async function loadAudioMetasForFlow(flowId, harness) {
  return (await readMetasUnder(audioRootForFlow(flowId, harness))) ?? [];
}

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
      } catch {}
    }
  }
  return metas;
}
