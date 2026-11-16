// pipeline-registry.js — SCD Type 2 history of pipeline/node DEFINITIONS
// (kind/agent/mode), not their execution history (node-executions.js
// already covers that). Answers "what did node X look like at time T" —
// pipelinesRef.value is a live, mutable, single-copy in-memory Map with
// no history of its own, so once a node is renamed/removed or its whole
// pipeline file is deleted, every past flow that dispatched under the
// old shape loses the ability to render correctly, forever, with no
// error — just a silent fallback to kind:null.
//
// Write path: reconcilePipelineVersions(pipelinesMap, {logger}) — called
// once right after the boot-time pipeline load resolves, then again on
// every pipelines-watcher reload tick. Diffs the given Map against the
// currently-active (retired_at IS NULL) row per (pipeline_id[, node_name]):
// content unchanged → no-op; changed or previously absent → retire the
// old active row (if any) and insert a fresh one; a pipeline_id/node_name
// that no longer appears in the Map at all → retire without inserting
// (its last version stays queryable, just marked retired).
//
// Read path: getNodeVersionsBatch([{pipelineId, nodeName, atMs}, ...]) —
// point-in-time lookup, batched to avoid one query per row: fetches every
// version for each distinct (pipeline_id, node_name) pair in one query,
// then matches each request's own atMs against those in memory. (The
// `nodeId` request param is the node's CURRENT name, same convention as
// every other caller in the codebase — see the node-identity note below
// for why it's matched against the `node_name` column internally.)
//
// Explicit scope boundary: this store answers "what was true," never
// "what should happen now." Anything deciding CURRENT behavior (mode
// enforcement in engine/child-resume-trigger.js, dispatch/edge-routing in
// engine/executor.js) keeps reading pipelinesRef directly, unchanged —
// conflating "what did happen" with "what should happen now" would be a
// real correctness bug (a long-running fan-out reacting to a stale mode
// from whenever it first dispatched instead of the pipeline's current
// logic), not a simplification.
//
// content_hash is sha256 over a canonicalized subset of fields only —
// `run`/`edges`/`selector`/etc. are functions, not meaningfully hashable,
// and nothing reads them historically. A reload tick where nothing
// structurally meaningful changed is a no-op, not a new row.
//
// Node identity across a rename: `node_name` is a point-in-time DISPLAY
// LABEL (e.g. "SUBMIT_SUMMARY" — same string as the wider codebase's
// `nodeId`, flow_node_invocations.nodeId, pipeline.nodes' object keys;
// nothing outside this file changes). `node_id` is the node's PERSISTENT
// identity, independent of its current name. A pipeline author can
// optionally declare it directly on a node: `nodes: { SUBMIT_SUMMARY: {
// kind: "code", id: "<any stable string, author's choice>" } }` — when
// present, that declared value IS the node_id, verbatim, every time.
// Renaming a node is then just "change the object key, leave `id` alone"
// — the identity travels with the node's own declaration, no separate
// rename-link step. A node that never declares `id` still gets one
// (minted the first time it's ever reconciled, reused as long as its name
// doesn't change) so getNodeLineageBatch works uniformly either way — an
// undeclared node just can't survive a FUTURE rename without the author
// adding `id` at that point, same trade-off as any opt-in metadata.
// Two (or more) node_names can legitimately share one declared `id` at
// the same time — a genuine N:1 merge falls out for free, no special
// case needed. See dashboard/stats.js's autoFunnelFromNodes for the
// consumer.

import { randomUUID, createHash } from "node:crypto";
import { mkdirSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

function hashOf(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function pipelineContentHash(pipeline) {
  return hashOf({ kind: pipeline.kind ?? null, allowConcurrent: pipeline.allowConcurrent ?? null });
}

function nodeContentHash(node) {
  return hashOf({
    kind: node.kind,
    agent: node.kind === "agent" ? (node.agent ?? null) : null,
    mode: node.kind === "subflow" ? (node.mode ?? null) : null,
    // Declaring (or changing) `id` on an otherwise-unchanged node must
    // still trigger a retire+reinsert — without this, _reconcileOneNode's
    // hash-match short-circuit would return before ever reading node.id,
    // silently ignoring a newly-added declaration on any node whose
    // kind/agent/mode didn't also change (the exact retroactive-backfill
    // case: adding `id` to nodes that already exist and aren't otherwise
    // being edited).
    id: node.id ?? null,
  });
}

let _singleton = null;

function _makeStoreProxy(_p) {
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "_db") return new Proxy({}, { get(__, method) {
      if (method === "then" || typeof method === "symbol") return undefined;
      return (...a) => _p.then(s => s._db[method](...a));
    } });
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`pipeline-registry: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

export function getPipelineRegistryStore(dbPath = defaultDbPath()) {
  if (!_singleton) {
    _singleton = _makeStoreProxy(_createStore(dbPath));
  }
  return _singleton;
}

// Exported for tests: creates an independent store (bypasses the singleton).
export function createPipelineRegistryStore(dbPath) {
  return _makeStoreProxy(_createStore(dbPath));
}

async function _createStore(dbPath) {
  let client;
  try {
    mkdirSync(path.dirname(dbPath), { recursive: true });
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    ({ client } = await getSharedDb(dbPath));
  } catch {
    // Purely additive/historical-rendering feature (see this file's own
    // top comment) — if the DB can't open, degrade to a no-op store
    // rather than take pipeline loading down with it.
    const noop = () => {};
    return {
      _db: null,
      reconcilePipelineVersions: async () => {},
      getNodeVersionAt: async () => null,
      getNodeVersionsBatch: async (requests) => (requests ?? []).map(() => null),
      getPipelineVersionAt: async () => null,
      getNodeLineageBatch: async () => new Map(),
      close: noop,
    };
  }

  async function _activePipelineRow(pipelineId) {
    const { rows } = await client.execute({
      sql: `SELECT id, content_hash FROM pipeline_versions WHERE pipeline_id = ? AND retired_at IS NULL`,
      args: [pipelineId],
    });
    return rows[0] ?? null;
  }

  async function _activeNodeRow(pipelineId, nodeName) {
    const { rows } = await client.execute({
      sql: `SELECT id, content_hash, node_id FROM pipeline_node_versions WHERE pipeline_id = ? AND node_name = ? AND retired_at IS NULL`,
      args: [pipelineId, nodeName],
    });
    return rows[0] ?? null;
  }

  // Reads still happen immediately (against the pre-reconcile committed
  // state — see reconcilePipelineVersions' own comment on why that's safe
  // even with writes deferred). The retire+insert pair itself is pushed
  // onto `queries` instead of executed here — reconcilePipelineVersions
  // commits every pipeline's and every node's pair, plus the orphan-
  // retirement pass, in ONE client.batch() call at the end. That makes the
  // whole SCD2 rotation atomic — otherwise a crash mid-reconcile could
  // leave a pipeline with zero active version rows (retired-with-no-
  // replacement) — and collapses what was N individual round-trips into one.
  async function _reconcileOnePipeline(pipelineId, pipeline, now, queries) {
    const hash = pipelineContentHash(pipeline);
    const active = await _activePipelineRow(pipelineId);
    if (active && active.content_hash === hash) return active.id;
    if (active) {
      queries.push({ sql: `UPDATE pipeline_versions SET retired_at = ? WHERE id = ?`, args: [now, active.id] });
    }
    const id = randomUUID();
    queries.push({
      sql: `INSERT INTO pipelines (id, name, created_at, updated_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
      args: [pipelineId, pipelineId, now, now],
    });
    if (pipeline._parentPipelineId) {
      queries.push({
        sql: `INSERT INTO pipelines (id, name, created_at, updated_at)
              VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
        args: [pipeline._parentPipelineId, pipeline._parentPipelineId, now, now],
      });
    }
    queries.push({
      sql: `INSERT INTO pipeline_versions
              (id, pipeline_id, kind, allow_concurrent, is_synthetic, parent_pipeline_id, source_path, content_hash, registered_at, retired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      args: [
        id, pipelineId, pipeline.kind ?? null,
        pipeline.allowConcurrent !== undefined ? JSON.stringify(pipeline.allowConcurrent) : null,
        pipeline._synthetic ? 1 : 0,
        pipeline._parentPipelineId ?? null,
        pipeline._filepath ?? null,
        hash, now,
      ],
    });
    return id;
  }

  // Resolves the persistent node_id for a row about to be written.
  // `activeRow` is this exact node_name's own currently-active row, if
  // any (already fetched by the caller). `declaredId` is `node.id` read
  // straight off the pipeline definition — author-declared, always wins
  // when present, no lookup needed.
  async function _resolveNodeIdentity(pipelineId, nodeName, activeRow, declaredId) {
    if (declaredId) return declaredId;
    // Name unchanged (content just drifted) — same identity as before.
    if (activeRow) return activeRow.node_id;
    // A node reappearing after removal (no active row, no declared id,
    // but it HAS existed before under this exact name) keeps its old
    // identity; a genuinely brand new, undeclared node mints a fresh one.
    const { rows } = await client.execute({
      sql: `SELECT node_id FROM pipeline_node_versions
             WHERE pipeline_id = ? AND node_name = ? ORDER BY registered_at DESC LIMIT 1`,
      args: [pipelineId, nodeName],
    });
    return rows[0]?.node_id ?? randomUUID();
  }

  // See _reconcileOnePipeline's comment — same deferred-write pattern.
  async function _reconcileOneNode(pipelineVersionId, pipelineId, nodeName, node, now, queries) {
    const hash = nodeContentHash(node);
    const active = await _activeNodeRow(pipelineId, nodeName);
    if (active && active.content_hash === hash) return;
    const nodeId = await _resolveNodeIdentity(pipelineId, nodeName, active, node.id ?? null);
    if (active) {
      queries.push({ sql: `UPDATE pipeline_node_versions SET retired_at = ? WHERE id = ?`, args: [now, active.id] });
    }
    queries.push({
      sql: `INSERT INTO pipeline_node_versions
              (id, pipeline_version_id, pipeline_id, node_name, kind, agent, mode, content_hash, registered_at, retired_at, node_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
      args: [
        randomUUID(), pipelineVersionId, pipelineId, nodeName,
        node.kind, node.kind === "agent" ? (node.agent ?? null) : null,
        node.kind === "subflow" ? (node.mode ?? null) : null,
        hash, now, nodeId,
      ],
    });
  }

  // Reads throughout this pass (_activePipelineRow/_activeNodeRow inside
  // the per-pipeline/per-node loop, plus the two orphan-detection SELECTs
  // below) all see the same pre-reconcile committed state, since every
  // write is deferred into `queries` and only actually committed in the
  // single client.batch() call at the very end — including the two
  // orphan-detection SELECTs, which correctly still see an about-to-be-
  // retired active row as "active" (retired_at IS NULL) and skip it via
  // the seen-set check, same outcome as before, just simpler to reason
  // about since nothing this pass writes is visible to its own reads.
  async function reconcilePipelineVersions(pipelinesMap, { logger } = {}) {
    if (!pipelinesMap || pipelinesMap.size === 0) return;
    const now = Date.now();
    const seenPipelineIds = [];
    const seenNodeKeys = [];
    const queries = [];
    try {
      for (const [pipelineId, pipeline] of pipelinesMap) {
        seenPipelineIds.push(pipelineId);
        const pipelineVersionId = await _reconcileOnePipeline(pipelineId, pipeline, now, queries);
        for (const [nodeName, node] of Object.entries(pipeline.nodes ?? {})) {
          seenNodeKeys.push([pipelineId, nodeName]);
          await _reconcileOneNode(pipelineVersionId, pipelineId, nodeName, node, now, queries);
        }
      }

      // Retire any active row whose pipeline/node no longer appears in
      // the Map at all — a pipeline or node that's been removed. Its
      // last version stays queryable, just marked retired.
      const { rows: activePipelines } = await client.execute(
        `SELECT id, pipeline_id FROM pipeline_versions WHERE retired_at IS NULL`,
      );
      const seenPipelineSet = new Set(seenPipelineIds);
      for (const row of activePipelines) {
        if (!seenPipelineSet.has(row.pipeline_id)) {
          queries.push({ sql: `UPDATE pipeline_versions SET retired_at = ? WHERE id = ?`, args: [now, row.id] });
        }
      }

      const { rows: activeNodes } = await client.execute(
        `SELECT id, pipeline_id, node_name FROM pipeline_node_versions WHERE retired_at IS NULL`,
      );
      const seenNodeSet = new Set(seenNodeKeys.map(([p, n]) => `${p} ${n}`));
      for (const row of activeNodes) {
        if (!seenNodeSet.has(`${row.pipeline_id} ${row.node_name}`)) {
          queries.push({ sql: `UPDATE pipeline_node_versions SET retired_at = ? WHERE id = ?`, args: [now, row.id] });
        }
      }

      if (queries.length > 0) {
        await withSharedWrite(dbPath, () => client.batch(queries));
      }
    } catch (err) {
      logger?.warn?.(`manifold pipeline-registry: reconcile failed: ${err?.message ?? err}`);
    }
  }

  async function getNodeVersionAt({ pipelineId, nodeName, atMs }) {
    const results = await getNodeVersionsBatch([{ pipelineId, nodeName, atMs }]);
    return results[0] ?? null;
  }

  // Batched point-in-time lookup — one query per distinct (pipeline_id,
  // node_name) pair, not one per request. Returns results in the SAME
  // ORDER as `requests`; a pair with NO recorded
  // version at all resolves to null — callers keep their existing
  // kind:null fallback for that case. See resolveVersionAt below for how
  // a timestamp older than any recorded version is handled (NOT null).
  async function getNodeVersionsBatch(requests) {
    if (!Array.isArray(requests) || requests.length === 0) return [];
    const pairKey = (p, n) => `${p} ${n}`;
    const uniquePairs = new Map(); // pairKey -> {pipelineId, nodeName}
    for (const r of requests) uniquePairs.set(pairKey(r.pipelineId, r.nodeName), r);

    const versionsByPair = new Map(); // pairKey -> [{kind, agent, registeredAt, retiredAt}]
    for (const { pipelineId, nodeName } of uniquePairs.values()) {
      const { rows } = await client.execute({
        sql: `SELECT kind, agent, registered_at, retired_at FROM pipeline_node_versions
               WHERE pipeline_id = ? AND node_name = ? ORDER BY registered_at ASC`,
        args: [pipelineId, nodeName],
      });
      versionsByPair.set(pairKey(pipelineId, nodeName), rows.map((r) => ({
        kind: r.kind, agent: r.agent ?? null,
        registeredAt: Number(r.registered_at),
        retiredAt: r.retired_at == null ? null : Number(r.retired_at),
      })));
    }

    return requests.map(({ pipelineId, nodeName, atMs }) => resolveVersionAt(versionsByPair.get(pairKey(pipelineId, nodeName)), atMs));
  }

  // Shared by getNodeVersionsBatch/getPipelineVersionAt. `versions` must
  // already be sorted ascending by registeredAt.
  //
  // atMs predating the EARLIEST recorded version is expected, not an edge
  // case: reconcile only starts recording from whenever this feature first
  // shipped/booted -- every flow that dispatched before that point has
  // real invocations with no real historical record to match. Retire-
  // then-insert always uses one shared `now` for both timestamps (see
  // reconcilePipelineVersions), so once the earliest version exists there
  // are never gaps between versions -- only ever a gap BEFORE the first
  // one. Falling back to that earliest version there (rather than null)
  // is the best available answer, not a guess invented from nothing: the
  // vast majority of nodes are never touched between "this flow ran" and
  // "this feature started tracking pipelines," so the earliest known
  // shape is usually the exact right one. Returning null here -- the
  // naive SCD2 read -- would silently regress correct rendering for
  // every already-existing flow the moment this feature ships, which is
  // a worse outcome than an occasional wrong guess for the rare node
  // that changed in that exact gap.
  function findVersionRowAt(versions, atMs) {
    if (!versions?.length) return null;
    const exact = versions.find((v) => v.registeredAt <= atMs && (v.retiredAt == null || v.retiredAt > atMs));
    return exact ?? (atMs < versions[0].registeredAt ? versions[0] : null);
  }

  function resolveVersionAt(versions, atMs) {
    const match = findVersionRowAt(versions, atMs);
    return match ? { kind: match.kind, agent: match.agent } : null;
  }

  // Pipeline-level (not node-level) point-in-time lookup — e.g. "was this
  // pipeline cascade-kind at time T" (routes/runs.js's cascadeAttemptFor).
  // Single-row lookup, not batched: only one call site needs this today,
  // one per child flow rather than per-node, so an N+1 concern doesn't
  // apply the way it does for getNodeVersionsBatch. Same
  // older-than-earliest-known-version fallback as findVersionRowAt above.
  async function getPipelineVersionAt({ pipelineId, atMs }) {
    const { rows } = await client.execute({
      sql: `SELECT kind, allow_concurrent, registered_at, retired_at FROM pipeline_versions
             WHERE pipeline_id = ? ORDER BY registered_at ASC`,
      args: [pipelineId],
    });
    const versions = rows.map((r) => ({
      kind: r.kind, allow_concurrent: r.allow_concurrent,
      registeredAt: Number(r.registered_at),
      retiredAt: r.retired_at == null ? null : Number(r.retired_at),
    }));
    const match = findVersionRowAt(versions, atMs);
    if (!match) return null;
    let allowConcurrent = null;
    try { allowConcurrent = match.allow_concurrent == null ? null : JSON.parse(match.allow_concurrent); } catch { /* leave null */ }
    return { kind: match.kind, allowConcurrent };
  }

  // Resolves, for each of a pipeline's CURRENT node names, the full set of
  // node_name strings that share its persistent node_id — i.e. every name
  // this identity has ever been called, whether from an author-declared
  // `id` or an auto-minted one. A node with no rename history resolves to
  // a singleton set containing just itself. Two queries total (not one
  // per requested name): first the active rows for the requested names to
  // learn their node_ids, then one query for every row that shares any of
  // those node_ids. Consumed by dashboard/stats.js's autoFunnelFromNodes
  // so an older run's flow_node_invocations.nodeId (recorded under
  // whatever name was live at the time) still counts toward the CURRENT
  // node's funnel stage.
  async function getNodeLineageBatch(pipelineId, nodeIds) {
    if (!Array.isArray(nodeIds) || nodeIds.length === 0) return new Map();
    const { rows: active } = await client.execute({
      sql: `SELECT node_name, node_id FROM pipeline_node_versions
             WHERE pipeline_id = ? AND node_name IN (${nodeIds.map(() => "?").join(",")}) AND retired_at IS NULL`,
      args: [pipelineId, ...nodeIds],
    });
    const identityByNodeName = new Map(active.map((r) => [r.node_name, r.node_id]));
    const identities = [...new Set(identityByNodeName.values())].filter(Boolean);

    const namesByIdentity = new Map();
    if (identities.length > 0) {
      const { rows: all } = await client.execute({
        sql: `SELECT DISTINCT node_name, node_id FROM pipeline_node_versions
               WHERE node_id IN (${identities.map(() => "?").join(",")})`,
        args: identities,
      });
      for (const r of all) {
        if (!namesByIdentity.has(r.node_id)) namesByIdentity.set(r.node_id, new Set());
        namesByIdentity.get(r.node_id).add(r.node_name);
      }
    }

    const result = new Map();
    for (const nodeId of nodeIds) {
      const identity = identityByNodeName.get(nodeId);
      result.set(nodeId, (identity && namesByIdentity.get(identity)) || new Set([nodeId]));
    }
    return result;
  }

  return {
    _db: client,
    reconcilePipelineVersions,
    getNodeVersionAt,
    getNodeVersionsBatch,
    getPipelineVersionAt,
    getNodeLineageBatch,
    close() { try { client.close(); } catch { /* ignore */ } },
  };
}
