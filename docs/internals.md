# Internals

The contract surface for consumers of this repo as a dependency, the code map, and the on-disk stores.

## Public API

Downstream packages install this repo as the
`@igorjpimenta/manifold` dependency and import **only** through these
subpaths. Deep file paths are not a contract — only this table is.

| Subpath | Exports | Consumed by |
| --- | --- | --- |
| `@igorjpimenta/manifold` | plugin entry (default) | openclaw adapter |
| `…/errors` | `NodeFailure`, `NodeBlocked`, `GatewayHandoffNeeded`, error classifiers | oc-config |
| `…/elevation` | `invokeElevated` | oc-config |
| `…/digest` | `sendDigestPush`, `configureApi` | oc-config |
| `…/tool-handoff` | structured result rendezvous (`writePendingResult`, `readPendingResult`, `clearPendingResult`, `waitForToolResult`) | oc-config |
| `…/routes/bearer-auth` | `authorizeBearerRequest`, `sendUnauthorized` (MANIFOLD_BEARER) | sibling extension |
| `…/state/db` | `openDb`, `getSharedDb`, `withSharedWrite`, `isSqliteBusy` | sibling extension (shared `runtime.sqlite`) |
| `…/state/batch-writer` | `enqueue`, `enqueueRaw`, `flush`, `flushAll` | sibling extension |
| `…/state/schema` | drizzle tables (`flowRuns`, `flowNodeExecutions`, …) | sibling extension |

The map is pinned by `test/unit/api/public-exports.test.js`; adding or
removing an entry is a breaking change for the consumers above.


## Quick orientation

> **One-line model:** A `Pipeline` is `{ nodes, edges, state }` loaded
> from a JS module. A `Flow` is one persistent run of a pipeline. The
> `Executor` walks nodes by id, applies their `stateUpdate`, picks the
> next id from `edges[currentId](state)`, persists every revision to
> sqlite, and exits the loop on terminal/blocked status.

| You need…                               | Look at                                        |
| --------------------------------------- | ---------------------------------------------- |
| Neutral core entry (factories)          | `index.js`                                     |
| Harness adapter entry (wires a host)    | `plugins/openclaw/manifold/index.js`           |
| Run loop / node dispatch                | `executor.js: runFlow`, `runStep`              |
| Pipeline file discovery + validation    | `loader.js`                                    |
| sqlite persistence (`flow_runs` table)  | `state-store.js`                               |
| Engine CLI (`serve` modes)              | `bin/cli.js`                                   |
| Agent-facing tools (`TOOL_DEFS`)        | `mcp/tools.js`                                 |
| Approval helpers (dashboard + HTTP)     | `gates.js`                                     |
| Dashboard server                        | `dashboard/server.js`                          |
| Dashboard frontend                      | `dashboard/src/`                               |
| Adapter manifest (config schema)        | `plugins/openclaw/manifold/openclaw.plugin.json` |


## Database migrations

Table shapes live in `state/schema.js` (`runtime.sqlite`) and
`state/schema.ops.js` (`ops.sqlite`) — drizzle definitions, the single
source of truth. `npm run db:generate` snapshots them into
`.migrations/<db>/`; `state/migrate.js` applies pending snapshots at
store open via better-sqlite3 + drizzle, before any libsql handle opens.
Snapshots are the sole schema path. New schema changes go in the defs +
a generated snapshot. `test/unit/state/migrate.test.js` fails the build
if live-booted columns drift from the defs either way, and if any snapshot
file carries an unguarded CREATE or a DROP/RENAME.


## SQLite shape (for direct queries / surgery)

Manifold owns `flow_runs`, `flow_node_invocations`, and
`flow_node_executions`, all in `runtime.sqlite` — the sole read/write
store for the dashboard, revival, gate-resolver, and executor.
`registry.sqlite` is the platform's own store and is never written by
manifold. All direct SQL surgery should target `runtime.sqlite`. Use the
`dql` skill (`oc-dql`) for read-only
inspection — it's the safe default; only reach for raw `sqlite3` when you
actually intend to write.

```
file: <store home>/runtime.sqlite   ← manifold read/write store (use this)
table: flow_runs

flow_id             TEXT PRIMARY KEY
pipeline_id         TEXT  (the pipeline that owns this flow — top-level id,
                            or "<parentPipelineId>::<subflowNodeId>" for a childFlows child)
status              TEXT  ('running' | 'blocked' | 'waiting' | 'succeeded' | 'failed' | 'skipped' | 'cancelled')
current_step        TEXT  (node currently in flight; NULL when idle or terminal)
last_step           TEXT  (last completed node; NULL on creation)
revision            INTEGER
state_json          TEXT  (JSON; the full mutable state)
wait_json           TEXT  (JSON; null when not gated)
status_reason       TEXT  (reason for the current status — blocked/queued/failed/skipped,
                            optional on cancelled; always NULL for running/succeeded)
owner_key           TEXT
parent_invocation_id TEXT  (NULL for a top-level flow; FK → flow_node_invocations.id
                            for a childFlows subflow item — see below)
cancel_requested_at INTEGER
created_at          INTEGER (ms epoch)
updated_at          INTEGER (ms epoch)
ended_at            INTEGER (ms epoch; null until terminal)

indexes: status, owner_key, updated_at, parent_invocation_id (partial, WHERE NOT NULL)

-- "list of runs" queries (dashboard, CLI, companion clients) should filter
-- WHERE parent_invocation_id IS NULL — every childFlows subflow item is its
-- own flow_runs row and is not an independent "run" from an operator's
-- perspective. See state/run-store.js's own listStmt for the canonical
-- filtered query.


table: flow_node_invocations   ← one row per LOGICAL node dispatch (created once)

id          TEXT PRIMARY KEY
flow_id     TEXT NOT NULL       (the PARENT flow — the one that has the subflow node)
pipeline_id TEXT
node_id     TEXT NOT NULL       (the subflow node's own id; also its id in a
                                  root-level node's timing-only invocation)
subflow_key TEXT                (NULL for a root-level node; the item's
                                  derived key for a subflow child — see
                                  pipelines.md "Item keys are kebab-case slugs")
visit_seq   INTEGER NOT NULL DEFAULT 1  (distinguishes a genuine fresh
                                  loop-back pass from a retry/revival
                                  continuation of the same dispatch)
invoked_at  INTEGER NOT NULL

unique index: (flow_id, node_id, COALESCE(subflow_key,''), visit_seq)

-- subflow_key IS NOT NULL is the "this is a subflow-child invocation" signal —
-- flow_runs.parent_invocation_id joins here to find a subflow node's real
-- children: JOIN flow_runs r ON r.parent_invocation_id = flow_node_invocations.id


table: flow_node_executions   ← one row per EXECUTION ATTEMPT of an invocation (one per retry)

id            TEXT PRIMARY KEY
invocation_id TEXT NOT NULL     (FK → flow_node_invocations.id)
entered_at    INTEGER NOT NULL
exited_at     INTEGER NOT NULL
error         TEXT              (NULL on success — this is also the source
                                  of the wire-level errorEntries[], never a
                                  stateJson field, see State conventions)
```

For dry-run-then-commit destructive SQL, wrap in `BEGIN TRANSACTION;
… ; ROLLBACK;` first, inspect, then re-run with `COMMIT;`.
