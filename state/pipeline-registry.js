// pipeline-registry.js — SCD Type 2 history of pipeline/node DEFINITIONS
// (kind/agent/mode), answering "what did node X look like at time T".

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
    // `id` must stay in the hash: the hash-match short-circuit below
    // returns before ever reading node.id, so dropping it would silently
    // ignore a newly-declared id on an otherwise-unchanged node.
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
    // Historical-rendering feature only: if the DB can't open, degrade to
    // a no-op store rather than take pipeline loading down with it.
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

  // The retire+insert pair is pushed onto `queries` instead of executed
  // here so reconcilePipelineVersions can commit every pair plus the
  // orphan-retirement pass in ONE client.batch() — otherwise a crash
  // mid-reconcile could leave a pipeline with zero active version rows
  // (retired-with-no-replacement).
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

  async function _reconcileOneNode(pipelineVersionId, pipelineId, nodeName, node, now, queries) {
    // The runtime node name is the attribution key the engine writes
    // (flow_node_invocations, node_health, lineage, telemetry), so it must
    // exist before the first turn runs — register it on every reconcile,
    // including a content-green one (hence before the hash check).
    queries.push({
      sql: `INSERT OR IGNORE INTO pipeline_nodes (pipeline_id, node_name, first_seen_at) VALUES (?, ?, ?)`,
      args: [pipelineId, nodeName, now],
    });
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

  // Every read in this pass sees the pre-reconcile committed state:
  // writes are deferred into `queries` and committed only in the single
  // client.batch() at the end, so nothing this pass writes is visible to
  // its own reads — including the orphan-detection SELECTs, which still
  // see an about-to-be-retired row as active and skip it via the seen-set.
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

      // Removed pipeline/node: retire its active row — the last version
      // stays queryable, just marked retired.
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

  // Results come back in the SAME ORDER as `requests`; a pair with no
  // recorded version at all resolves to null.
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
  // case: recording only starts from when this feature first booted, and
  // retire+insert shares one `now` for both timestamps, so gaps exist only
  // BEFORE the first version, never between versions. Falling back to the
  // earliest version there (rather than null) is deliberate: returning null
  // — the naive SCD2 read — would silently regress correct rendering for
  // every already-existing flow the moment this feature ships, a worse
  // outcome than an occasional wrong guess for the rare node that changed
  // in that gap.
  function findVersionRowAt(versions, atMs) {
    if (!versions?.length) return null;
    const exact = versions.find((v) => v.registeredAt <= atMs && (v.retiredAt == null || v.retiredAt > atMs));
    return exact ?? (atMs < versions[0].registeredAt ? versions[0] : null);
  }

  function resolveVersionAt(versions, atMs) {
    const match = findVersionRowAt(versions, atMs);
    return match ? { kind: match.kind, agent: match.agent } : null;
  }

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

  // For each of a pipeline's CURRENT node names, every node_name string
  // that shares its persistent node_id — so invocations recorded under an
  // older name still resolve to the current node. Two queries total, not
  // one per requested name.
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
