// fleet.js — per-pipeline fleet toolset declarations.
//
// A pipeline ships a fleet.yaml alongside its index.js to declare which
// tools each agent should have access to in a given runtime context (e.g.
// "discuss"). The discuss handler reads this to surface the right delegation
// mechanism for agents that need tools unavailable in a default subagent
// context (SUBAGENT_TOOL_DENY_ALWAYS strips most tools from drafter
// sessions; top-level sessions via runEmbeddedAgent get the full allowlist).
//
// Fleet config shape (YAML):
//
//   tools:                # all agents, all contexts
//     - tool_name
//
//   <context>:            # e.g. "discuss"
//     tools:              # all agents in this context
//       - tool_name
//     <agentId>:          # e.g. "content-writer"
//       tools:
//         - tool_name

import { readFile } from "node:fs/promises";
import path from "node:path";

/**
 * Load fleet.yaml from a pipeline directory.
 * Returns the parsed fleet config or null if the file is absent.
 */
export async function loadFleet(pipelineDir) {
  const fleetPath = path.join(pipelineDir, "fleet.yaml");
  try {
    const src = await readFile(fleetPath, "utf8");
    return parseFleetYaml(src);
  } catch (err) {
    if (err.code === "ENOENT") return null;
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Minimal YAML parser — only handles the subset used by fleet.yaml:
//   key:           (nested mapping, empty value)
//   key: scalar
//   - list item
// Indentation-based nesting; comment stripping (#...).
// ---------------------------------------------------------------------------
function parseFleetYaml(src) {
  const lines = src.split("\n");

  // Tokenize: strip blanks + comment-only lines, record indent + content.
  const tokens = [];
  for (const raw of lines) {
    const stripped = raw.replace(/\s*#.*$/, "");
    if (!stripped.trim()) continue;
    const indent = stripped.length - stripped.trimStart().length;
    tokens.push({ indent, content: stripped.trim() });
  }

  let i = 0;

  function parseBlock(baseIndent) {
    if (i >= tokens.length) return null;

    if (tokens[i].content.startsWith("- ")) {
      // List block
      const arr = [];
      while (
        i < tokens.length &&
        tokens[i].indent === baseIndent &&
        tokens[i].content.startsWith("- ")
      ) {
        arr.push(tokens[i].content.slice(2).trim());
        i++;
      }
      return arr;
    }

    // Mapping block
    const obj = {};
    while (i < tokens.length && tokens[i].indent === baseIndent) {
      const line = tokens[i].content;
      const colonIdx = line.indexOf(":");
      if (colonIdx === -1) { i++; continue; }
      const key = line.slice(0, colonIdx).trim();
      const rest = line.slice(colonIdx + 1).trim();
      i++;
      if (rest) {
        obj[key] = rest;
      } else if (i < tokens.length && tokens[i].indent > baseIndent) {
        obj[key] = parseBlock(tokens[i].indent);
      } else {
        obj[key] = null;
      }
    }
    return obj;
  }

  if (tokens.length === 0) return {};
  return parseBlock(tokens[0].indent) ?? {};
}
