// loader.js — discovery of pipeline definition directories.

import { readdir, stat, readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { loadFleet } from "../agent/fleet.js";
import { ENTRY_FIELDS } from "./needs-resolver.js";

// For every subflow node under `nodes`, register its .flow as an
// addressable pipeline (`${pipelineId}::${nodeId}`) so a real child
// flow_run can be started against it via the normal
// executor.start()/pipelinesRef path. Must happen HERE, inside
// loadPipelines() itself — registering it as a one-off
// pipelinesRef.value.set(...) elsewhere would be silently dropped on the
// next pipelines-watcher reload tick, since that merge only preserves
// stale entries backed by a real on-disk pipeline directory. Own
// try/catch per node so one bad subflow doesn't block the parent pipeline
// (already registered by the caller) or any other subflow node at the
// same nesting level. Renamed from kind:"loop".
//
// Recursive (first real nested-subflow site — freelance_workana-reply's
// REPLY > ENSURE_INFRA): a synthesized child pipeline's own `.nodes` can
// itself contain a `kind:"subflow"` sub-node (e.g.
// `freelance_workana-reply::REPLY::ENSURE_INFRA`), whose OWN children won't
// dispatch correctly unless THAT compound id is registered too —
// engine/executor-subflow-children.js computes a spawned child's
// pipelineId as `${state.pipelineId}::${nodeId}`, so a grandchild's
// pipelineId is exactly this function's own recursive `pipelineId`
// parameter one level down. No depth limit; genuinely unbounded nesting
// works the same way arbitrarily deep, though in practice one level is
// already rare.
function registerSubflowChildren(pipelineId, nodes, { result, indexPath, pipelineDir, entName, logger }, ancestorNodeMaps = [], proposalMeta = undefined) {
  for (const [nodeId, node] of Object.entries(nodes ?? {})) {
    if (node?.kind !== "subflow") continue;
    const syntheticId = `${pipelineId}::${nodeId}`;
    try {
      // .flow.kind is copied through if declared (e.g. "cascade") onto the
      // synthesized child's own `kind`, which that child's runFlow does
      // read — executor.js's `pipeline.kind === "cascade"` throw path is
      // what gives a cascade-shaped subflow its throw-then-retry-from-start
      // semantics. A plain subflow has no .kind and this is simply
      // undefined, so its dispatch is ordinary.
      const synthetic = { id: syntheticId, kind: node.flow.kind, state: {}, nodes: node.flow.nodes, edges: node.flow.edges };
      // This synthetic child's own hop-1 ancestor is
      // `nodes`, the map that contains the subflow node (nodeId) which
      // spawns it; prepend it onto whatever ancestor chain THIS level was
      // itself given, so a needs:{hops:N} declared anywhere inside the
      // synthetic tree can be checked against a real node map, statically,
      // at load time.
      validatePipeline(synthetic, `${entName}/index.js (${nodeId}.flow)`, [nodes, ...ancestorNodeMaps]);
      // proposalMeta (and any future root-level config a tool looks up by
      // pipelineId) is declared once on the ROOT pipeline's own index.js,
      // never on a subflow's .flow — but a per-item agent node dispatched
      // from inside that subflow resolves its OWN pipelineId as this
      // synthetic child's id (e.g. "freelance_freelancer-proposal::PROPOSE"),
      // not the root's. Without carrying it through here, a tool that keys
      // off pipelinesRef.value.get(pipelineId) would silently miss the
      // root's config for every such dispatch. Threaded recursively so it
      // survives arbitrarily deep nesting too.
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
 * Scan a directory for pipeline definitions.
 *
 * Layout: `<pipelinesDir>/<team>_<name>/index.js` (folder per pipeline).
 * Each pipeline folder may also contain `design.md`, `runs.jsonl`, and a
 * `runs/<runId>/` subdirectory for per-run artifacts.
 *
 * Each `index.js` must `export default { id, state, nodes, edges, goal? }`.
 *
 * Returns a Map<pipelineId, pipelineDef> with `_filepath` (the index.js)
 * and `_dir` (the pipeline folder) attached for downstream use.
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
        logger?.warn?.(`manifold loader: skipping flat-file pipeline ${ent.name}; convert to a folder with index.js`);
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
      // Cache-bust strategy: read the file ourselves and import it as a
      // `data:` URL. The original `?t=Date.now()` query-string approach LOOKS
      // like a unique URL each call, but in Node 25's long-running ESM loader
      // the dynamic-import cache still keys on the file:// path and returns
      // the original module on every subsequent import — verified by
      // debug-logging actual pipeline contents across watcher reloads. data:
      // URLs are uncacheable per-path (each has a unique base64 payload) so
      // editing the source file always yields a fresh module.
      //
      // Relative imports (`from "./lib"`) are rewritten to absolute file://
      // URLs before encoding, so pipeline modules can use normal sibling
      // imports without knowing their install path.
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

      // For every subflow node, register its .flow as an
      // addressable pipeline (`${pipeline.id}::${nodeId}`) so a real child
      // flow_run can be started against it via the normal
      // executor.start()/pipelinesRef path. Must happen HERE, inside
      // loadPipelines() itself — registering it as a one-off
      // pipelinesRef.value.set(...) elsewhere would be silently dropped on
      // the next pipelines-watcher reload tick, since that merge only
      // preserves stale entries backed by a real on-disk pipeline
      // directory. Own try/catch per node so one bad subflow doesn't block
      // the parent pipeline (already registered above) or any other
      // subflow node in the same pipeline. Renamed from kind:"loop".
      registerSubflowChildren(pipeline.id, pipeline.nodes, { result, indexPath, pipelineDir, entName: ent.name, logger }, [], pipeline.proposalMeta);
    } catch (err) {
      logger?.warn?.(`manifold loader: ${ent.name}/ failed to load: ${err.message}`);
    }
  }
  return result;
}

/**
 * Build a memoized `ensurePipelinesLoaded()` for a given `pipelinesRef` box.
 * All callers (the loader service, flow-revival, executor's per-flow
 * fallback) share the SAME in-flight/resolved promise instead of each doing
 * its own `pipelinesRef.value.size > 0` check-then-act — that heuristic let
 * a caller race ahead of another's still-in-flight load, intermittently
 * finding `pipelinesRef.value` empty and starting a redundant load whose
 * completion the original caller never awaited — so an orphaned flow
 * revival at boot cannot race the loader service and fail with
 * "pipeline not loaded: <id>".
 *
 * `load` is injectable for tests; defaults to the real `loadPipelines`.
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

  // Track groupIds claimed by subflow nodes so we can catch collisions.
  // Subflow-mode subflows that omit groupId use their nodeId as the implicit key.
  const usedGroupIds = new Map(); // effectiveGroupId → nodeId

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
      // Optional: fired once per decided item, immediately after that
      // item's decision commits (gates/gate-resolver.js's
      // applyResolutionsAndResume) — the root-level-gate counterpart to
      // a subflow's WRITE sub-node, without needing subflow machinery.
      if (node.apply !== undefined && typeof node.apply !== "function") {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (human-gate) .apply must be a function`);
      }
    }

    if (node.kind === "subflow") {
      // groupId is always the node's own id — a custom groupId would make
      // state/node-executions.js's listInvocationIdsForNode (keyed by the
      // subflow's own node_id, plus branch_key IS NOT NULL to exclude
      // ordinary sub-node timing rows) never find this subflow's children,
      // permanently deadlocking the parent's resume-trigger. Top-level-only
      // check (uniqueness across a nested subflow's own id and some other
      // node's id is not a real collision risk — they live under
      // different flowIds — so this isn't extended into
      // validateSubflowNode's recursive nested-node validation below).
      if (node.groupId !== undefined) {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" (subflow) must not set .groupId — it is always the node's own id`);
      }
      if (usedGroupIds.has(nodeId)) {
        throw new Error(`pipeline ${p.id}: node "${nodeId}" implicit groupId collides with another subflow node using groupId "${nodeId}"`);
      }
      usedGroupIds.set(nodeId, nodeId);

      // node.flow.nodes' own hop-1 ancestor, once
      // spawned, is this pipeline itself (p.nodes) — prepend it here so
      // nested needs: entries resolve against a real ancestor chain.
      validateSubflowNode(p, `node "${nodeId}"`, node, [p.nodes, ...ancestorNodeMaps]);
    }
  }

  // Optional `groups` field — logical groupings of nodes the dashboard
  // renders as compound parents. Each group's `members` array must list
  // existing node ids; a single node cannot belong to more than one group.
  //
  // Members may be either:
  //   - top-level node ids (e.g. "SCAN_CODE"), or
  //   - qualified branch-internal ids (e.g. "PROPOSE.SUBMIT_CODE") where
  //     "PROPOSE" is a subflow node and "SUBMIT_CODE" lives in
  //     that node's flow.nodes. The dashboard renders these as a
  //     compound nested inside the subflow node's own compound.
  //
  // Mixing scopes inside a single group is forbidden — every member of a
  // group must live in the same scope (all top-level, or all inside the
  // same subflow). Cytoscape compound nodes can only have
  // one parent, so a group's compound has to live entirely in one scope.
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
      let scopeParent = null; // null = top-level; otherwise the subflow node id
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

// Any code/agent/subflow node, top-level or nested,
// may declare `.needs: { varName: {hops, nodeId}, ... }` — keyed by the
// LOCAL variable name it'll be resolved as via ctx.needs(varName), not by
// nodeId, so it reads like an ORM relation declaration rather than a bare
// fetch list.
// Validated here (not at runtime, per this codebase's own load-time-
// validation convention — see the analogous groups/.members check below)
// using `ancestorNodeMaps`, threaded down from validatePipeline/register
// SubflowChildren: since a subflow's synthetic childPipelineId is always
// `${parentPipelineId}::${nodeId}`, chained at every nesting level, the
// FULL ancestor chain of real node maps is statically knowable at load
// time — a needs: entry naming a hop that doesn't exist, or a nodeId that
// doesn't exist at that ancestor level, throws here rather than resolving
// to a silent runtime "not-found" indistinguishable from a race.
//
// A spec names EXACTLY ONE of `nodeId` (an ancestor's own node output) or
// `entry` (an ancestor flow's own entry/bookkeeping field — runId, args,
// etc., set at that flow's start(), never via any node's resultJson).
// `entry` is checked against ENTRY_FIELDS (needs-resolver.js) — `gates` is
// deliberately not on that list even though it's a real stateJson key; it
// has its own dedicated accessor (gate_records) precisely because reading
// it through the generic needs: path reopens a real bug that mechanism
// was built to close. An `entry`-type need still needs the same hop-depth
// check as a `nodeId`-type one (does the pipeline nest that deep at all)
// but skips the node-existence check, since it isn't naming a node.
//
// human-gate is deliberately excluded — ctx.needs() isn't wired into
// pendingSelector/approvalRequestBuilder yet (no real use case surfaced),
// so a human-gate node declaring .needs would validate fine and then have
// no way to ever call it — throwing here is louder and more honest than
// silently accepting something inert.
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
    // hops:0 = same-flow (the declaring node's own flow — always exists,
    // no nesting-depth check needed). hops>=1 = walk that many parent
    // flows up, same as before.
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
    // config isn't checkable against the target pipeline's own `state:`
    // keys at load time the way entry/nodeId are — pipelines can load in
    // any order and `state:` declares no schema. Just require a non-empty
    // string; an unknown key resolves to `undefined` at runtime, same as
    // reading a missing object property anywhere else in this codebase.
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

// Shared shape validation for a kind:"subflow" node, called both for a
// top-level node and recursively for a nested one (a `flow.nodes` entry
// that is itself kind:"subflow" — first real site: freelance_workana-
// reply's REPLY > ENSURE_INFRA). `label` is already the caller's fully
// qualified prefix (e.g. `node "REPLY"` or `node "REPLY".flow.nodes.
// "ENSURE_INFRA"`), so nesting composes into a readable dotted path
// automatically. groupId-uniqueness is NOT checked here — see the
// top-level call site's own comment for why that stays top-level-only.
function validateSubflowNode(p, label, node, ancestorNodeMaps) {
  if (typeof node.selector !== "function") {
    throw new Error(`pipeline ${p.id}: ${label} (subflow) missing .selector function`);
  }
  // Required, not defaulted — a subflow node's children going terminal
  // resumes the parent regardless of success/failure unless mode says
  // otherwise (see child-resume-trigger.js's maybeResumeParent), so every
  // author has to pick a propagation policy explicitly rather than
  // silently inheriting "never fails the parent."
  if (node.mode !== "any" && node.mode !== "all") {
    throw new Error(`pipeline ${p.id}: ${label} (subflow) requires .mode to be "any" or "all"`);
  }
  // Every subflow node's items become real flow_runs children — requires a
  // .flow definition (.flow.nodes/.edges); there is no other subflow shape.
  // A selector returning a single-item array is the degenerate "spawn
  // exactly one child" case, not a different kind.
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

// The `for (const [subId, sub] of ...)` loop over one subflow's
// flow.nodes, shared between the top-level subflow branch and
// validateSubflowNode's own recursive nested case. `sub.kind ===
// "subflow"` recurses back into validateSubflowNode — arbitrarily deep
// nesting validates the same way at every level.
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
      // Same pattern as the top-level loop: sub.flow.nodes' own hop-1
      // ancestor, once spawned, is `flowNodes` (the map sub itself lives
      // in) — prepend it before recursing.
      validateSubflowNode(p, subLabel, sub, [flowNodes, ...ancestorNodeMaps]);
    }
    if (sub.kind === "human-gate") {
      // A child is an ordinary flow — its gate sub-nodes need the
      // same shape a root-level human-gate node does.
      if (typeof sub.pendingSelector !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) missing .pendingSelector function`);
      }
      if (typeof sub.approvalRequestBuilder !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) missing .approvalRequestBuilder function`);
      }
      // Optional, same as the root-level human-gate check above.
      if (sub.apply !== undefined && typeof sub.apply !== "function") {
        throw new Error(`pipeline ${p.id}: ${subLabel} (human-gate) .apply must be a function`);
      }
    }
  }
}

function validateAgentKind(p, label, obj, { allowSkillShorthand }) {
  // Top-level agent nodes accept a `skill` shorthand string in place of
  // `.promptBuilder`; subflow sub-nodes don't.
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

// Replace relative import specifiers (`from "./lib"`, `from "../shared"`,
// and dynamic `import("./file")`) with absolute file:// URLs so pipeline
// modules loaded via data: URLs can import sibling files normally.
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
