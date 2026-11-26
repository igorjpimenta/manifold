import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const REGISTER_TOOL_NAME_RE =
  /api\.registerTool\(\s*(?:\(\w*\)\s*=>\s*\(\s*\{|\{)\s*name:\s*(['"])([^'"\n]+)\1/g;

function trackedSourceJsFiles() {
  return execFileSync("git", ["ls-files", "-z", "*.js"], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  })
    .split("\0")
    .filter(Boolean)
    .filter((file) => !file.startsWith("test/") && !file.startsWith("dashboard/"));
}

function registeredToolNames() {
  const names = new Set();
  for (const file of trackedSourceJsFiles()) {
    const src = readFileSync(path.join(REPO_ROOT, file), "utf8");
    for (const match of src.matchAll(REGISTER_TOOL_NAME_RE)) names.add(match[2]);
  }
  return names;
}


function skipStringLiteral(src, i) {
  const quote = src[i];
  i += 1;
  while (i < src.length) {
    if (src[i] === "\\") { i += 2; continue; }
    if (src[i] === quote) return i + 1;
    i += 1;
  }
  throw new Error("manifest-parity: unterminated string literal");
}

function skipTemplateLiteral(src, i) {
  i += 1;
  while (i < src.length) {
    if (src[i] === "\\") { i += 2; continue; }
    if (src[i] === "`") return i + 1;
    if (src[i] === "$" && src[i + 1] === "{") {
      i = matchBrace(src, i + 1) + 1;
      continue;
    }
    i += 1;
  }
  throw new Error("manifest-parity: unterminated template literal");
}

function matchBrace(src, openIdx) {
  let depth = 0;
  let i = openIdx;
  while (i < src.length) {
    const c = src[i];
    if (c === '"' || c === "'") { i = skipStringLiteral(src, i); continue; }
    if (c === "`") { i = skipTemplateLiteral(src, i); continue; }
    if (c === "/" && src[i + 1] === "/") {
      const nl = src.indexOf("\n", i);
      i = nl === -1 ? src.length : nl + 1;
      continue;
    }
    if (c === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      if (end === -1) throw new Error("manifest-parity: unterminated block comment");
      i = end + 2;
      continue;
    }
    if (c === "{") depth += 1;
    else if (c === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
    i += 1;
  }
  throw new Error("manifest-parity: unbalanced braces");
}

function topLevelEntries(objInner) {
  const entries = [];
  let i = 0;
  while (i < objInner.length) {
    const c = objInner[i];
    if (c === '"' || c === "'") { i = skipStringLiteral(objInner, i); continue; }
    if (c === "`") { i = skipTemplateLiteral(objInner, i); continue; }
    if (c === "/" && objInner[i + 1] === "/") {
      const nl = objInner.indexOf("\n", i);
      i = nl === -1 ? objInner.length : nl + 1;
      continue;
    }
    if (c === "/" && objInner[i + 1] === "*") {
      const end = objInner.indexOf("*/", i + 2);
      if (end === -1) throw new Error("manifest-parity: unterminated block comment");
      i = end + 2;
      continue;
    }
    if (/[A-Za-z_$]/.test(c)) {
      let j = i;
      while (j < objInner.length && /[\w$]/.test(objInner[j])) j += 1;
      const key = objInner.slice(i, j);
      let k = j;
      while (k < objInner.length && /\s/.test(objInner[k])) k += 1;
      if (objInner[k] === ":") {
        k += 1;
        while (k < objInner.length && /\s/.test(objInner[k])) k += 1;
        const valueOpenIdx = objInner[k] === "{" ? k : -1;
        entries.push({ key, valueOpenIdx });
        if (valueOpenIdx >= 0) {
          i = matchBrace(objInner, valueOpenIdx) + 1;
          continue;
        }
      }
      i = j;
      continue;
    }
    i += 1;
  }
  return entries;
}

function collectSchemaPropertyPaths(schemaObjInner, prefix = "") {
  const paths = [];
  const propsEntry = topLevelEntries(schemaObjInner).find((e) => e.key === "properties");
  if (!propsEntry || propsEntry.valueOpenIdx < 0) return paths;
  const propsInner = schemaObjInner.slice(
    propsEntry.valueOpenIdx + 1,
    matchBrace(schemaObjInner, propsEntry.valueOpenIdx),
  );
  for (const entry of topLevelEntries(propsInner)) {
    const full = prefix ? `${prefix}.${entry.key}` : entry.key;
    paths.push(full);
    if (entry.valueOpenIdx >= 0) {
      const valueInner = propsInner.slice(
        entry.valueOpenIdx + 1,
        matchBrace(propsInner, entry.valueOpenIdx),
      );
      paths.push(...collectSchemaPropertyPaths(valueInner, full));
    }
  }
  return paths;
}

function inlineIndexConfigSchemaPaths() {
  const src = readFileSync(path.join(REPO_ROOT, "plugins/openclaw/manifold/index.js"), "utf8");
  const anchor = src.indexOf("configSchema: () => ({");
  assert.notEqual(anchor, -1, "index.js: inline `configSchema: () => ({` anchor not found");
  const openBrace = src.indexOf("({", anchor) + 1;
  return collectSchemaPropertyPaths(src.slice(openBrace + 1, matchBrace(src, openBrace)));
}

function manifestConfigSchemaPaths(manifest) {
  const paths = [];
  const walk = (props, prefix) => {
    for (const [key, val] of Object.entries(props)) {
      const full = prefix ? `${prefix}.${key}` : key;
      paths.push(full);
      if (val && typeof val === "object" && val.properties) walk(val.properties, full);
    }
  };
  walk(manifest.configSchema?.properties ?? {}, "");
  return paths;
}

function readManifest() {
  return JSON.parse(readFileSync(path.join(REPO_ROOT, "plugins/openclaw/manifold/openclaw.plugin.json"), "utf8"));
}

describe("openclaw.plugin.json contract parity", () => {
  test("the plugin registers no raw tools — the pipeline surface is MCP-only", async () => {
    const registered = [...registeredToolNames()].sort();
    assert.deepEqual(
      registered,
      [],
      `the plugin registered raw tools (${registered.join(", ")}) — manifold exposes the pipeline tools over MCP only`,
    );

    const declared = readManifest().contracts?.tools ?? [];
    assert.deepEqual(
      declared,
      [],
      "contracts.tools must stay empty: the plugin contributes no raw tools",
    );

    const { TOOL_DEFS } = await import("../../../mcp/tools.js");
    const mcpToolNames = TOOL_DEFS.map((d) => d.name);
    assert.ok(
      mcpToolNames.includes("manifold_start"),
      `the MCP tool surface lost manifold_start: ${mcpToolNames.join(", ")}`,
    );
  });

  test("manifest configSchema property paths equal index.js inline configSchema", () => {
    const manifestPaths = new Set(manifestConfigSchemaPaths(readManifest()));
    const codePaths = new Set(inlineIndexConfigSchemaPaths());
    assert.ok(codePaths.size > 0, "index.js inline configSchema yielded no property paths");

    const onlyInManifest = [...manifestPaths].filter((p) => !codePaths.has(p)).sort();
    const onlyInCode = [...codePaths].filter((p) => !manifestPaths.has(p)).sort();

    assert.deepEqual(
      onlyInManifest,
      [],
      `configSchema paths in the manifest but not in index.js: ${onlyInManifest.join(", ")}`,
    );
    assert.deepEqual(
      onlyInCode,
      [],
      `configSchema paths in index.js but not in the manifest: ${onlyInCode.join(", ")}`,
    );
  });
});
