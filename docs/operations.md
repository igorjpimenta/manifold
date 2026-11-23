# Operations

Running, inspecting and recovering flows: the engine CLI, the MCP tool
surface, HTTP recipes against the engine API, and the symptom/quirk tables.

Every engine route below is served by `manifold serve` on
`http://127.0.0.1:44301` (`MANIFOLD_API_HOST` / `MANIFOLD_API_PORT`) and
requires `Authorization: Bearer $MANIFOLD_BEARER`. Dashboard routes are on
`http://127.0.0.1:44300`.

## Engine CLI

The binary serves; run-manipulation lives on the HTTP API and MCP tools
(see [README → CLI](../README.md#cli) for the `serve` modes).

## Tool surface (agent-facing)

Six tools, declared in `mcp/tools.js` (`TOOL_DEFS`) and registered by both
transports — `mcp/stdio.js` and `mcp/http.js` register every `TOOL_DEFS`
entry, so there is no per-tool wiring:

| Tool | Input | Purpose |
| --- | --- | --- |
| `manifold_start` | `pipelineId`, optional `args` | Start a flow. Returns `flowId`. |
| `manifold_status` | `flowId` | Read one flow (status, currentStep, waitJson). |
| `manifold_runs` | optional filters | List flows. |
| `manifold_approve` | `flowId`, `itemId`, `decision` | Resolve a gate item. |
| `manifold_cancel` | `flowId`, optional `reason` | Cancel a non-terminal flow. |
| `manifold_deliver_result` | JSON `payload`, optional `kind`/`runId` | Structured result rendezvous (`state/tool-handoff.js`): stored under the caller's stamped session key, where the waiting dispatch picks it up. |

Agents reach them through a harness that connects MCP (`manifold serve
--mcp stdio|http`); identity is stamped per call — see
[plugins/README.md](../plugins/README.md). Pipeline-specific tools
(`propose_*`, `submit_*`, `report_*`) belong to companion packages, not
core — core keeps only the harness-agnostic surface.

### Adding a new tool

1. Add a definition to `TOOL_DEFS` in `mcp/tools.js` — name, input schema,
   handler.
2. Both transports register every `TOOL_DEFS` entry; no manifest entry and
   no per-transport registration step (the adapter manifest carries no tool
   contracts by design).
3. Add a test under `test/unit/mcp/` mirroring the definition.

## Common tasks

### Add a new pipeline

1. Create `<pipelinesDir>/<id>/index.js` (default
   `~/.manifold/workspace/.pipelines/<id>/index.js`).
2. Default-export an object matching [Pipeline schema (canonical)](pipelines.md#pipeline-schema-canonical).
3. No restart needed — the engine's `pipelines-watcher.js` reloads on change.
4. Verify with `GET http://127.0.0.1:44300/api/pipelines/<id>` (dashboard
   API) or `GET /manifold/pipelines` on the engine API.

### Run a pipeline once

```bash
curl -X POST -H "Authorization: Bearer $MANIFOLD_BEARER" \
  -H 'Content-Type: application/json' \
  -d '{"pipelineId":"my-pipeline","args":{"someInput":"x"}}' \
  http://127.0.0.1:44301/manifold/start
```

The engine drives the flow to a terminal status or a gate. Agents start
flows through the `manifold_start` MCP tool instead — same route behind it.

### Seed a pipeline with operator-supplied state

When the operator wants to drive a pipeline for a specific input the
pipeline's normal SCAN/ENUMERATE stage wouldn't produce — e.g. "run
the proposal flow for *this* listing URL" — neither `args` nor the
`manifold_start` tool fits: the pipeline's discovery stage still
runs. Instead, insert a `flow_runs` row at `status='running'` +
`current_step='__start__'` with pre-populated `state_json`. The engine's
*revival service* (a `manifold serve` boot service) catches it and drives
forward; the pipeline's start-edge typically short-circuits past discovery
when its readiness condition is already satisfied (e.g.
`PRIME: (s) => s.listings.length > 0 ? "FILTER" : "ENSURE_LOGIN"`).

**⚠ Infra-skip gotcha.** Skipping `ENSURE_LOGIN_CODE` /
`LAUNCH_BROWSER` / etc. via a pre-populated state leaves downstream
CDP-dependent nodes failing with `ECONNREFUSED 127.0.0.1:9222`. Either
run the infra step manually before seeding, or set the row's
`current_step` to that infra node (revival starts there).

### Inspect why a flow failed

```bash
curl -H "Authorization: Bearer $MANIFOLD_BEARER" \
  http://127.0.0.1:44301/manifold/runs/<flowId>
```

Shows status, currentStep and waitJson. Or open the dashboard at
`#/runs/<flowId>`:

- `errors` card lists `errorEntries[*]` (top-level, sourced from `flow_node_executions` — not a `stateJson` field)
- `transcripts` card shows the full subagent conversation per session
- `state` tree shows the full `stateJson`

### Failures feed + operator recovery actions (operator Inbox)

A separate `failures.sqlite` store (sibling of `runtime.sqlite`)
records one row per distinct branch- or flow-level failure on terminal
`__end__`. Powers the operator Inbox's Recover/Done buckets and the dashboard
"Needs Attention" strip. Dedup via `UNIQUE(flow_id, branch_item_id,
failure_node)`; UPSERT semantics on re-records (see
`failure-store.js:215-235`).

```bash
# Open rows (default: not resolved, not ignored)
curl -H "Authorization: Bearer $MANIFOLD_BEARER" \
  http://127.0.0.1:44301/manifold/failures

# Include resolved + ignored — what Inbox clients fetch in one shot
curl -H "Authorization: Bearer $MANIFOLD_BEARER" \
  'http://127.0.0.1:44301/manifold/failures?resolved=all&ignored=all&limit=200'
```

Each row carries an `available_actions[]` array — descriptor list of
operator actions Inbox clients render (kind / label / icon / tint /
destructive). Framework defaults always present; pipeline-declared
actions opt in via the `recoveryActions` map (see [Pipeline schema](pipelines.md#pipeline-schema-canonical)).

**Action dispatch endpoints** (engine API, port 44301):

```bash
# Generic dispatch — for any kind the row's available_actions exposes.
# Includes pipeline-declared kinds (e.g. `skip`) and any future custom
# kinds without code changes here.
curl -X POST -H "Authorization: Bearer $MANIFOLD_BEARER" -H "Content-Type: application/json" \
  -d '{"via":"app"}' \
  http://127.0.0.1:44301/manifold/failures/<id>/action/<kind>

# Framework-default routes — same semantics, dedicated URLs:
curl -X POST .../manifold/failures/<id>/retry      # surgical branch retry
curl -X POST .../manifold/failures/<id>/ignore     # snooze
curl -X POST .../manifold/failures/<id>/unignore   # restore
curl -X POST .../manifold/failures/<id>/skip       # dedicated URL for the skip action
```

`/retry` walks the failure's own flow (a top-level flow, OR a `subflow`
item's own child flow — both retry identically, since a child is just an
ordinary flow with its own `flow_id`) back through its topology to the
first node with a currently-live error, clears that node and everything
downstream from `stateJson`, bumps `_attemptNumber`, and resumes — the
executor re-enters at the failing node only, skipping nothing upstream of
it that already succeeded. Refuses if the flow isn't in a terminal-ish
state (`running`/`blocked`/`waiting`/`succeeded`/`failed`/`cancelled`/
`skipped` only — never races a live executor iteration), or if the
recorded `failure_node` doesn't match a current error (already
resolved by an earlier retry). There is no separate itemId-keyed branch
retry.
Implementation: `routes/operator-retry.js: retryFailureBranch`.

### Resume a gated flow

Three surfaces, same underlying operation:

```
# Dashboard: open #/runs/<flowId> → "waiting on …" card. Approve/reject
# buttons are always rendered; the edit button shows only when the pending
# item has `text` (textarea pre-fills) or `editable: true` (empty textarea).
# The SSE stream refreshes the page once the executor reacts.

# HTTP — what the dashboard sends (human-gate):
curl -X POST -H 'Content-Type: application/json' \
  -d '{"itemId":"<itemId>","decision":"approve"}' \
  http://127.0.0.1:44300/api/runs/<flowId>/approve

# HTTP — gate nodes on the engine API (decision: approve | reject | edit):
curl -X POST -H "Authorization: Bearer $MANIFOLD_BEARER" \
  -H 'Content-Type: application/json' \
  -d '{"flowId":"<flowId>","itemId":"<itemId>","decision":"approve",
       "session":"<caller session>","agentId":"<caller agent>"}' \
  http://127.0.0.1:44301/manifold/gate/resolve

# Agents resolve gates through the manifold_approve MCP tool.
```

`itemId` must match the `id` of an entry returned by the node's
`pendingSelector`. The dashboard endpoint lands in `gates.js: applyApproval`
and `gate/resolve` in `gate-resolver.js: recordItemDecision` — one state
mutation per decision either way.

### Rerun a flow or a single subflow item

Three escalating strategies. Use the lowest one that fits.

**A. Whole flow, fresh from `__start__` (creates a new flow id):**

```bash
curl -X POST -H "Authorization: Bearer $MANIFOLD_BEARER" \
  http://127.0.0.1:44301/manifold/runs/<flowId>/rerun
```

Routes through `executor.rerunFromSource` — copies the source flow's
args, stamps `state.rerunOf = <oldFlowId>` for dashboard lineage,
inserts a fresh `flow_runs` row, and fires `runFlow` on it. The
original flow is untouched. Use when the failure was in `__start__`
discovery or you genuinely want the pipeline to re-enumerate from
scratch.

**B. One item of a `subflow` node (re-fire SEND/SUBMIT/etc. for a
specific item without re-drafting):**

Because every subflow item is a real, independent flow (its own `flowId`,
`childFlowId` in `_subflowGroups[groupId][subflowKey]`), retrying one item is
just retrying that flow — the exact same mechanism as option A/the
failures flow, pointed at the CHILD's own flowId instead of the parent's:

```bash
curl -X POST -H "Authorization: Bearer $MANIFOLD_BEARER" -H "Content-Type: application/json" \
  -d '{"via":"app"}' \
  http://127.0.0.1:44301/manifold/failures/<id>/retry
```

where `<id>` is the failure event recorded against the child's own
`flow_id` — `state/failure-store.js: recordEngineFailure` fires from
within the CHILD's own executor iteration and records this way for any
genuine thrown error. Every `subflow` sub-node failure throws (zero soft
`{ _error }` returns survive anywhere in this codebase), so every
subflow-item failure ends up recorded this way and is directly
retryable. `branch_item_id` on the wire
(derived read-time from `flow_node_invocations.subflow_key`) is purely
descriptive — it
doesn't gate whether a failure can be retried (see
`routes/operator-retry.js`'s own comment).

**One real caveat**:
`recordEngineFailure` *can* also fire against the **parent's** own
`flow_id` — `engine/child-resume-trigger.js`'s `maybeResumeParent`
calls it directly (`failure_kind: "subflow-mode-not-satisfied"`) when
every sibling child has reached a terminal status but the subflow node's own
`mode` ("any"/"all") isn't satisfied. That's a policy decision, not a
throw — nothing in the parent's own dispatch failed. Retrying *that*
failure event doesn't re-run the parent (it already dispatched
correctly); `routes/operator-retry.js`'s `retryFailureBranch` detects
the `subflow-mode-not-satisfied` kind and redirects to the real
underlying child failure instead, recursing through any further
nested propagations first.

This walks
the child's own topology back to the failing node exactly like a
flow-level retry (see *Failures feed + operator recovery actions*
above) — no manual `runtime.sqlite` surgery, no
`branch.step`/`flow.currentStep` rewind math, no engine restart. The
parent flow is untouched and stays blocked on `children-pending` until
this one child (re-)reaches a terminal status.

`routes/operator-retry.js` returns a clean `not_found` for any failure
event whose `itemId` refers to a `flow_branches` row — there is no
branch-row retry path.

**C. Whole pipeline, custom initial state (operator-supplied input
the pipeline's discovery wouldn't produce):** see *Seed a pipeline
with operator-supplied state* above.


## If you see…

| Symptom | Likely cause + where to look |
| --- | --- |
| `pipeline "<id>" already has 1 active flow(s)` | Singleton guard on `POST /manifold/start` / `manifold_start`. Cancel or finish the prior run. `executor.js: start`. |
| `Unrecognized key: "<name>"` when the host validates config | Adapter config not nested under `.config`. See [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them) §3. |
| `manifold: flow not found: <shortId>` | Id lookups take the full UUID, not the dashboard's 8-char display id. |
| Dashboard transcript pane shows long messy nodeId, no item | Pipeline uses a node id with a hyphen. Fix the node id. See [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them) §1. |
| Dashboard pipeline detail renders blank black | `pipeline.nodes` is an array instead of an object keyed by id. Fix the export shape. |
| `itemId "<x>" not in pending list. Pending: [object Object]` | `pendingSelector` returned objects without `.id`. Items must be `{ id: string, … }`. |


## Operational quirks

- **Per-run artifacts** live in `<pipelinesDir>/<id>/runs/<runId>/`,
  not under the dashboard. The dashboard's `/api/runs/:flowId/artifacts`
  endpoint reads from there.
- **State is large.** Avoid stuffing huge blobs into `stateJson` —
  every revision rewrites the row. Use artifacts (file on disk) for
  anything >10KB.
