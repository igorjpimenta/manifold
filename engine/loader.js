// loader.js — discovery of pipeline definition directories.

import { readdir, stat, readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { loadFleet } from "../agent/fleet.js";
import { ENTRY_FIELDS } from "./needs-resolver.js";

// Registers each subflow node's .flow as an addressable pipeline
// (`${pipelineId}::${nodeId}`) so a real child flow_run can start against it
// via executor.start()/pipelinesRef. Must happen inside loadPipelines(): a
// one-off pipelinesRef.value.set elsewhere is dropped on the next
// pipelines-watcher reload, which only preserves entries backed by a real
// on-disk pipeline directory. Own try/catch per node so one bad subflow
// doesn't block its parent or siblings. Recursive: a synthesized child's
// own nodes can contain kind:"subflow" too, and executor-subflow-children
// computes a spawned child's pipelineId as `${state.pipelineId}::${nodeId}`
// — so a grandchild's id is this function's pipelineId parameter one level
// down.
function registerSubflowChildren(pipelineId, nodes, { result, indexPath, pipelineDir, entName, logger }, ancestorNodeMaps = [], proposalMeta = undefined) {
  for (const [nodeId, node] of Object.entries(nodes ?? {})) {
    if (node?.kind !== "subflow") continue;
    const syntheticId = `${pipelineId}::${nodeId}`;
    try {
      // .flow.kind (e.g. "cascade") carries onto the synthesized child, whose
      // runFlow reads it — that's what gives a cascade-shaped subflow its
      // throw-then-retry-from-start semantics.
      const synthetic = { id: syntheticId, kind: node.flow.kind, state: {}, nodes: node.flow.nodes, edges: node.flow.edges };
      // The child's hop-1 ancestor is `nodes` (the map holding the spawning
      // subflow node), prepended so nested needs: hops resolve against real
      // node maps at load time.
      validatePipeline(synthetic, `${entName}/index.js (${nodeId}.flow)`, [nodes, ...ancestorNodeMaps]);
      // proposalMeta lives on the ROOT pipeline's index.js, but a tool
      // resolving the synthetic child's own pipelineId would never see it —
      // thread it through so those lookups still find the root's config.
      result.set(syntheticId, {
        ...synthetic,
        _filepath: indexPath, _dir: pipelineDir, _fleet: null, _synthetic: true,
        _parentPipelineId: pipelineId, _parentNodeId: nodeId,
        ...(proposalMeta ? { proposalMeta } : {}),
      });
      logger?.info?.(`manifold loader: registered child-flow pipeline "${syntheticId}"`);
      registerSubflowChildren(syntheticId, node.flow.nodes, { result, indexPath, pipelineDir, entName, logger }, [nodes, ...ancestorNodeMaps], proposalMeta);
    } catch (err) {
      logger?.warn?.(`manifold loader: ${entName}/ node "${nodeId}" subflow branch failed to register: ${err.message}`);
    }
  }
}

/**
 * Scan `<pipelinesDir>/<team>_<name>/index.js` (folder per pipeline) and
 * return a Map<pipelineId, pipelineDef> with `_filepath` and `_dir` attached.
 */
export async function loadPipelines(dir, logger) {
  const result = new Map();
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (err) {
    if (err.code === "ENOENT") {
      logger?.info?.(`manifold loader: pipelines dir not found at ${dir} (no pipelines loaded)`);
      return result;
    }
    logger?.warn?.(`manifold loader: cannot read ${dir}: ${err.message}`);
    return result;
  }

  for (const ent of entries) {
    if (ent.name.startsWith(".")) continue;
    if (!ent.isDirectory()) {
      if (ent.name.endsWith(".js")) {
        logger?.warn?.(`manifold loader: skipping legacy flat-file pipeline ${ent.name}; convert to a folder with index.js`);
      }
      continue;
    }
    const pipelineDir = path.join(dir, ent.name);
    const indexPath = path.join(pipelineDir, "index.js");
    try { await stat(indexPath); } catch {
      logger?.warn?.(`manifold loader: ${ent.name}/ has no index.js; skipping`);
      continue;
    }
    try {
      // Cache-bust via data: URL, not `?t=` — Node's long-running ESM loader
      // keys the import cache on the file:// path and would return the stale
      // module on every reload; each data: URL has a unique payload, so the
      // source is read and re-imported fresh. Relative specifiers are
      // rewritten to absolute file:// URLs first so sibling imports work.
      const source = await readFile(indexPath, "utf8");
      const processed = resolveRelativeImports(source, pipelineDir);
      const url = `data:text/javascript;base64,${Buffer.from(processed).toString("base64")}`;
      const mod = await import(url);
      const pipeline = mod.default ?? mod.pipeline ?? mod;
      validatePipeline(pipeline, `${ent.name}/index.js`);
      if (result.has(pipeline.id)) {
        logger?.warn?.(`manifold loader: duplicate pipeline id "${pipeline.id}" in ${ent.name}/; keeping first`);
        continue;
      }
      let _fleet = null;
      try { _fleet = await loadFleet(pipelineDir); } catch {}
      result.set(pipeline.id, { ...pipeline, _filepath: indexPath, _dir: pipelineDir, _fleet });
      logger?.info?.(`manifold loader: loaded pipeline "${pipeline.id}" from ${ent.name}/${_fleet ? " (fleet.yaml found)" : ""}`);

      registerSubflowChildren(pipeline.id, pipeline.nodes, { result, indexPath, pipelineDir, entName: ent.name, logger }, [], pipeline.proposalMeta);
    } catch (err) {
      logger?.warn?.(`manifold loader: ${ent.name}/ failed to load: ${err.message}`);
    }
  }
  return result;
}

/**
 * Memoized ensurePipelinesLoaded() sharing one in-flight/resolved promise
 * across all callers — a per-caller `pipelinesRef.value.size > 0`
 * check-then-act let a caller race ahead of another's still-in-flight load
 * and start a redundant one whose completion nobody awaited, leaving
 * callers to see "pipeline not loaded" mid-boot.
 */
export function createPipelinesLoader(dir, logger, pipelinesRef, { load = loadPipelines } = {}) {
  let pipelinesLoadedPromise = null;
  return function ensurePipelinesLoaded() {
    if (!pipelinesLoadedPromise) {
      pipelinesLoadedPromise = load(dir, logger).then((loaded) => {
        pipelinesRef.value = loaded;
        return loaded;
      });
    }
    return pipelinesLoadedPromise;
  };
}

function validatePipeline(p, filename, ancestorNodeMaps = []) {
  if (!p || typeof p !== "object") throw new Error(`pipeline must export an object`);
  if (!p.id || typeof p.id !== "string") throw new Error(`pipeline ${filename} missing .id`);
  if (!p.state || typeof p.state !== "object") throw new Error(`pipeline ${p.id}: missing .state`);
  if (!p.nodes || typeof p.nodes !== "object") throw new Error(`pipeline ${p.id}: missing .nodes`);
  if (!p.edges || typeof p.edges !== "object") throw new Error(`pipeline ${p.id}: missing .edges`);
  if (!p.edges.__start__) throw new Error(`pipeline ${p.id}: edges must define __start__`);

  const usedGroupIds = new Map();

  for (const [nodeId, node] of Object.entries(p.nodes)) {
    if (!node || typeof node !== "object") throw new Error(`pipeline ${p.id}: node "${nodeId}" not an object`);
    if (!node.kind) throw new Error(`pipeline ${p.id}: node "${nodeId}" missing .kind`);
    if (!["code", "agent", "subflow", "human-gate"].includes(node.kind)) {
      throw new Error(`pipeline ${p.id}: node "${nodeId}" has unknown kind "${node.kind}"`);
    }
    validateNeeds(p, `node "${nodeId}"`, node, ancestorNodeMaps);
    if (node.kind === "code") {
      validateCodeKind(p, `node "${nodeId}"`, node);
    }

    if (node.kind === "agent") {
      validateAgentKind(p, `node "${nodeId}"`, node, { allowSkillShorthand: true });
    }

    if (node.kind === "human-gate") {
      if (typeof node.pendingSelector !== "function") {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (human-gate) missing .pendingSelector function`);
      }
      if (typeof node.approvalRequestBuilder !== "function") {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (human-gate) missing .approvalRequestBuilder function`);
      }
      if (node.apply !== undefined && typeof node.apply !== "function") {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (human-gate) .apply must be a function`);
      }
    }

    if (node.kind === "subflow") {
      // groupId must be the node's own id: listInvocationIdsForNode (keyed
      // by node_id with branch_key IS NOT NULL) would never find a custom
      // groupId's children, permanently deadlocking the parent's
      // resume-trigger. Uniqueness is checked top-level only — nested
      // subflows live under different flowIds, so they can't collide.
      if (node.groupId !== undefined) {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (subflow) must not set .groupId — it is always the node's own id`);
      }
      if (usedGroupIds.has(nodeId)) {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" implicit groupId collides with another subflow node using groupId "${nodeId}"`);
      }
      usedGroupIds.set(nodeId, nodeId);

      validateSubflowNode(p, `node "${nodeId}"`, node, [p.nodes, ...ancestorNodeMaps]);
    }
  }

  // Optional `groups`: dashboard compound parents. Members are node ids or
  // `SUBFLOW.CHILD` qualified ids; all members of one group must share a
  // single scope because a compound can have only one parent.
  if (p.groups !== undefined) {
    if (typeof p.groups !== "object" || Array.isArray(p.groups)) {
      throw new Error(`pipeline ${p.id}: .groups must be an object`);
    }
    const membership = new Map();
    for (const [groupId, group] of Object.entries(p.groups)) {
      if (!group || typeof group !== "object") {
        throw new Error(`pipeline ${p.id}: group "${groupId}" must be an object`);
      }
      if (!Array.isArray(group.members) || group.members.length === 0) {
        throw new Error(`pipeline ${p.id}: group "${groupId}" must have a non-empty .members array`);
      }
      let scopeParent = null;
      for (const m of group.members) {
        if (typeof m !== "string") {
          throw new Error(`pipeline ${p.id}: group "${groupId}" member must be a string, got ${typeof m}`);
        }
        const dot = m.indexOf(".");
        let memberScope = null;
        if (dot === -1) {
          if (!(m in p.nodes)) {
            throw new Error(`pipeline ${p.id}: group "${groupId}" member "${m}" not found in .nodes`);
          }
        } else {
          const parent = m.slice(0, dot);
          const child = m.slice(dot + 1);
          const parentNode = p.nodes[parent];
          if (!parentNode) {
            throw new Error(`pipeline ${p.id}: group "${groupId}" member "${m}" references unknown parent "${parent}"`);
          }
          if (parentNode.kind !== "subflow") {
            throw new Error(`pipeline ${p.id}: group "${groupId}" member "${m}" — parent "${parent}" is "${parentNode.kind}", expected "subflow"`);
          }
          if (!parentNode.flow?.nodes || !(child in parentNode.flow.nodes)) {
            throw new Error(`pipeline ${p.id}: group "${groupId}" member "${m}" — child "${child}" not in flow.nodes of "${parent}"`);
          }
          memberScope = parent;
        }
        if (scopeParent === null && memberScope !== null) scopeParent = memberScope;
        else if (memberScope !== scopeParent) {
          throw new Error(`pipeline ${p.id}: group "${groupId}" mixes scopes — members must all be top-level or all inside the same subflow`);
        }
        if (membership.has(m)) {
          throw new Error(`pipeline ${p.id}: node "${m}" is in multiple groups ("${membership.get(m)}" and "${groupId}")`);
        }
        membership.set(m, groupId);
      }
    }
  }
}

// `.needs: { varName: {hops, nodeId|entry|config} }` — keyed by the local
// variable name resolved via ctx.needs(varName). Validated statically at
// load time against the threaded ancestor chain (synthetic ids chain as
// `${parent}::${nodeId}`, so the whole chain is known), throwing here rather
// than resolving to a runtime "not-found" indistinguishable from a race.
// `gates` is deliberately absent from ENTRY_FIELDS: it has a dedicated
// accessor because reading it through the generic needs path reopens the
// bug that accessor was built to close.
function validateNeeds(p, label, node, ancestorNodeMaps) {
  if (node.needs === undefined) return;
  if (node.kind === "human-gate") {
    throw new Error(`pipeline ${p.id}: ${label} (human-gate) .needs is not yet supported — ctx.needs() is not wired into pendingSelector/approvalRequestBuilder`);
  }
  if (typeof node.needs !== "object" || node.needs === null || Array.isArray(node.needs)) {
    throw new Error(`pipeline ${p.id}: ${label} .needs must be an object keyed by variable name: { varName: {hops, nodeId} }`);
  }
  for (const [varName, need] of Object.entries(node.needs)) {
    if (!need || typeof need !== "object") {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName} must be an object {hops, nodeId}, {hops, entry}, or {hops, config}`);
    }
    const { hops, nodeId, entry, config } = need;
    if (!Number.isInteger(hops) || hops < 0) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName}.hops must be a non-negative integer, got ${JSON.stringify(hops)}`);
    }
    const declared = [nodeId !== undefined, entry !== undefined, config !== undefined].filter(Boolean).length;
    if (declared !== 1) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName} must declare exactly one of .nodeId, .entry, or .config`);
    }
    if (nodeId !== undefined && (typeof nodeId !== "string" || !nodeId)) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName}.nodeId must be a non-empty string`);
    }
    if (entry !== undefined && !ENTRY_FIELDS.has(entry)) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName}.entry "${entry}" is not a recognized entry/bookkeeping field — see needs-resolver.js's ENTRY_FIELDS`);
    }
    // config can't be checked at load time: pipelines load in any order and
    // `state:` declares no schema — an unknown key just resolves undefined.
    if (config !== undefined && (typeof config !== "string" || !config)) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName}.config must be a non-empty string`);
    }
    if (need.fields !== undefined && (!Array.isArray(need.fields) || need.fields.length === 0 || !need.fields.every((f) => typeof f === "string" && f))) {
      throw new Error(`pipeline ${p.id}: ${label} .needs.${varName}.fields must be a non-empty array of non-empty strings — always an array, even for a single field`);
    }
    if (hops === 0) {
      if (nodeId !== undefined && !(nodeId in p.nodes)) {
        throw new Error(`pipeline ${p.id}: ${label} needs.${varName} {hops:0, nodeId:"${nodeId}"} — no node "${nodeId}" in this pipeline`);
      }
      continue;
    }
    const ancestorMap = ancestorNodeMaps[hops - 1];
    if (!ancestorMap) {
      throw new Error(`pipeline ${p.id}: ${label} needs.${varName} {hops:${hops}} but this node's pipeline nesting doesn't go that deep`);
    }
    if (nodeId !== undefined && !(nodeId in ancestorMap)) {
      throw new Error(`pipeline ${p.id}: ${label} needs.${varName} {hops:${hops}, nodeId:"${nodeId}"} — no node "${nodeId}" at that ancestor level`);
    }
  }
}

function validateCodeKind(p, label, obj) {
  if (typeof obj.run !== "function") {
    throw new Error(`pipeline ${p.id}: ${label} (code) missing .run function`);
  }
}

function validateSubflowNode(p, label, node, ancestorNodeMaps) {
  if (typeof node.selector !== "function") {
    throw new Error(`pipeline ${p.id}: ${label} (subflow) missing .selector function`);
  }
  // Required, not defaulted: children going terminal resume the parent
  // regardless of outcome unless mode says otherwise, so authors must pick
  // the propagation policy instead of silently inheriting "never fails".
  if (node.mode !== "any" && node.mode !== "all") {
    throw new Error(`pipeline ${p.id}: ${label} (subflow) requires .mode to be "any" or "all"`);
  }
  if (!node.flow) {
    throw new Error(`pipeline ${p.id}: ${label} (subflow) requires .flow (.flow.nodes/.edges)`);
  }
  if (!node.flow.nodes || typeof node.flow.nodes !== "object") {
    throw new Error(`pipeline ${p.id}: ${label} .flow missing .nodes`);
  }
  if (!node.flow.edges || typeof node.flow.edges !== "object") {
    throw new Error(`pipeline ${p.id}: ${label} .flow missing .edges`);
  }
  if (!node.flow.edges.__start__) {
    throw new Error(`pipeline ${p.id}: ${label} .flow.edges must define __start__`);
  }
  validateFlowSubNodes(p, label, node.flow.nodes, ancestorNodeMaps);
}

function validateFlowSubNodes(p, label, flowNodes, ancestorNodeMaps = []) {
  for (const [subId, sub] of Object.entries(flowNodes)) {
    const subLabel = `${label}.flow.nodes."${subId}"`;
    if (!sub || typeof sub !== "object") {
      throw new Error(`pipeline ${p.id}: ${subLabel} not an object`);
    }
    if (!sub.kind) {
      throw new Error(`pipeline ${p.id}: ${subLabel} missing .kind`);
    }
    if (!["code", "agent", "subflow", "human-gate"].includes(sub.kind)) {
      throw new Error(`pipeline ${p.id}: ${subLabel} has unknown kind "${sub.kind}"`);
    }
    validateNeeds(p, subLabel, sub, ancestorNodeMaps);
    if (sub.kind === "agent") {
      validateAgentKind(p, subLabel, sub, { allowSkillShorthand: false });
    }
    if (sub.kind === "subflow") {
      validateSubflowNode(p, subLabel, sub, [flowNodes, ...ancestorNodeMaps]);
    }
    if (sub.kind === "human-gate") {
      if (typeof sub.pendingSelector !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) missing .pendingSelector function`);
      }
      if (typeof sub.approvalRequestBuilder !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) missing .approvalRequestBuilder function`);
      }
      if (sub.apply !== undefined && typeof sub.apply !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) .apply must be a function`);
      }
    }
  }
}

function validateAgentKind(p, label, obj, { allowSkillShorthand }) {
  if (allowSkillShorthand) {
    if (typeof obj.promptBuilder !== "function" && typeof obj.skill !== "string") {
      throw new Error(`pipeline ${p.id}: ${label} (agent) missing .promptBuilder function`);
    }
  } else if (typeof obj.promptBuilder !== "function") {
    throw new Error(`pipeline ${p.id}: ${label} (agent) missing .promptBuilder function`);
  }
  if (typeof obj.apply !== "function" && typeof obj.applyResult !== "function") {
    throw new Error(`pipeline ${p.id}: ${label} (agent) missing .apply function`);
  }
}

// Rewrites relative specifiers to absolute file:// URLs so data:-URL-loaded
// pipeline modules can import sibling files.
function resolveRelativeImports(source, dir) {
  const dirUrl = pathToFileURL(dir + "/").href;
  return source
    .replace(
      /(\bfrom\s+["'])(\.\.?\/[^"']+)(["'])/g,
      (_, prefix, rel, suffix) => `${prefix}${new URL(rel, dirUrl).href}${suffix}`,
    )
    .replace(
      /(\bimport\s*\(\s*["'])(\.\.?\/[^"']+)(["']\s*\))/g,
      (_, prefix, rel, suffix) => `${prefix}${new URL(rel, dirUrl).href}${suffix}`,
    );
}
