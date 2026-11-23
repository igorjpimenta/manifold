# Operations

Running, inspecting and recovering flows from the CLI, the agent tool surface, and the symptom/quirk tables.

## CLI

```
openclaw manifold list
openclaw manifold runs
openclaw manifold start <pipelineId> [--args <json>] [--detach]
openclaw manifold status <flowId>
openclaw manifold respond <flowId> <text>
openclaw manifold approve <flowId> <itemId> <decision>
openclaw manifold retry <flowId>
openclaw manifold pause <flowId>
openclaw manifold resume <flowId>
openclaw manifold paused
openclaw manifold dashboard
openclaw manifold node-health status [pipelineId] [--all]
openclaw manifold node-health reset <pipelineId> <nodeId>
```

Implementation: `cli.js`.


## Tool surface (agent-facing)

Declared in `openclaw.plugin.json`, one tool per file under `agent/tools/`
(barrel re-export in `agent/tools/index.js`):

| Tool                      | File                                    | Signature input fields        | Purpose                                             |
| ------------------------- | ---------------------------------------- | ------------------------------ | ---------------------------------------------------- |
| `manifold_start`          | `agent/tools/manifold-start.js`          | `pipelineId`, optional `args` | Start a flow. Returns `flowId`.                     |

Pipeline-specific tools (`propose_*`, `submit_*`, `report_*`) live in
oc-config, not here — core keeps only the harness-agnostic surface.

Shared helpers (`toolErr`) live in `agent/tools/shared.js`.

### Adding a new tool

1. New file under `agent/tools/`, one `registerXTool({ api, ...deps })`
   export per file.
2. Add the tool name to `contracts.tools` in `openclaw.plugin.json` — the
   loader rejects registration for undeclared names.
3. Register with `api.registerTool({ name: "...", ... })` — **prefer the
   object form**. Only reach for the factory form
   (`api.registerTool((ctx) => ({ name: "...", ... }))`, as `manifold_start`
   does) when you actually need `ctx` (e.g. `ctx.sessionKey`) in closure.
4. **If you use the factory form, you MUST pass `{ name: "..." }` as the
   second argument**: `api.registerTool((ctx) => ({...}), { name: "..." })`.
   Object-form registrations get their `names` populated automatically from
   the object's own `.name` field; factory-form registrations do not — the
   core loader never inspects what the factory *returns*, only
   `opts.name`/`opts.names`. Omitting this on a factory-form tool leaves its
   registry entry with an empty `names` array, which falls back to matching
   the *whole plugin's* declared tool list. Combined with `Array.find()`
   picking the *first* matching registry entry, a single factory-form tool
   registered without `opts.name` can silently hijack lookups for every
   *other* tool in the plugin — surfacing as `plugin tool runtime missing
   (manifold): <unrelated-tool-name>` at call time for tools that are
otherwise correctly registered.
5. Wire the register call into `index.js`.
6. Add a test file under `test/unit/agent/tools/<tool-name>.test.js`
   mirroring the source file 1:1.


## Common tasks

### Add a new pipeline

1. Create `~/.openclaw/workspace/.pipelines/<id>/index.js`.
2. Default-export an object matching [Pipeline schema (canonical)](pipelines.md#pipeline-schema-canonical).
3. No restart needed — `pipelines-watcher.js` reloads on change.
4. Verify with `openclaw manifold list` or
   `GET /api/pipelines/<id>`.

### Run a pipeline once

```
openclaw manifold start my-pipeline --args '{"someInput":"x"}'
```

Without `--detach`, the CLI blocks driving the executor loop until
the flow terminates or hits a gate. **With `--detach`, the flow record
is created but never driven** (see *Operational quirks*).

### Seed a pipeline with operator-supplied state

When the operator wants to drive a pipeline for a specific input the
pipeline's normal SCAN/ENUMERATE stage wouldn't produce — e.g. "run
the proposal flow for *this* listing URL" — neither `--args` nor the
`manifold_start` tool fits: the pipeline's discovery stage still
runs. Instead, insert a `flow_runs` row at `status='running'` +
`current_step='__start__'` with pre-populated `state_json`. The
gateway's *revival service* catches it at the next boot (~15s delay)
and drives forward; the pipeline's start-edge typically short-circuits
past discovery when its readiness condition is already satisfied (e.g.
`PRIME: (s) => s.listings.length > 0 ? "FILTER" : "ENSURE_LOGIN"`).

Procedure lives in Vulcan's `seed-pipeline-flow` skill —
`workspace/agents/agent-forger/skills/seed-pipeline-flow/SKILL.md`.

**⚠ Infra-skip gotcha.** Skipping `ENSURE_LOGIN_CODE` /
`LAUNCH_BROWSER` / etc. via a pre-populated state leaves downstream
CDP-dependent nodes failing with `ECONNREFUSED 127.0.0.1:9222`. Either
run the infra step manually before seeding, or set the row's
`current_step` to that infra node (revival starts there).

### Inspect why a flow failed

```
openclaw manifold status <flowId>          # shows status, currentStep, waitJson
```

Or open the dashboard at `#/runs/<flowId>`:
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
curl -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:18789/manifold/failures

# Include resolved + ignored — what Inbox clients fetch in one shot
curl -H "Authorization: Bearer $TOKEN" \
  'http://127.0.0.1:18789/manifold/failures?resolved=all&ignored=all&limit=200'
```

Each row carries an `available_actions[]` array — descriptor list of
operator actions Inbox clients render (kind / label / icon / tint /
destructive). Framework defaults always present; pipeline-declared
actions opt in via the `recoveryActions` map (see [Pipeline schema](pipelines.md#pipeline-schema-canonical)).

**Action dispatch endpoints** (gateway, port 18789):

```bash
# Generic dispatch — for any kind the row's available_actions exposes.
# Includes pipeline-declared kinds (e.g. `skip` on workana-proposal)
# and would include future custom kinds without code changes here.
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"via":"app"}' \
  http://127.0.0.1:18789/manifold/failures/<id>/action/<kind>

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
# CLI
openclaw manifold approve <flowId> <itemId> approve            # human-gate / gate
openclaw manifold approve <flowId> <itemId> edit --text "..."  # replace agent output
openclaw manifold respond <flowId> "the answer"                # needs-info (free-form)

# Dashboard: open #/runs/<flowId> → "waiting on …" card. Approve/reject buttons are
# always rendered; the edit button shows only when the pending item has
# `text` (textarea pre-fills) or `editable: true` (empty textarea). The SSE
# stream refreshes the page once the executor reacts.

# HTTP (what the dashboard sends)
curl -X POST -H 'Content-Type: application/json' \
  -d '{"itemId":"<itemId>","decision":"approve"}' \
  http://127.0.0.1:44300/api/runs/<flowId>/approve
```

`itemId` must match the `id` of an entry returned by the node's
`pendingSelector`. Both the CLI and the dashboard endpoint share
`gates.js: applyApproval` so the resulting state mutation is identical.

### Rerun a flow or a single subflow item

Three escalating strategies. Use the lowest one that fits.

**A. Whole flow, fresh from `__start__` (creates a new flow id):**

```
curl -X POST http://127.0.0.1:18789/manifold/runs/<flowId>/rerun
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

```
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"via":"app"}' \
  http://127.0.0.1:18789/manifold/failures/<id>/retry
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
`branch.step`/`flow.currentStep` rewind math, no gateway restart. The
parent flow is untouched and stays blocked on `children-pending` until
this one child (re-)reaches a terminal status.

`routes/operator-retry.js` returns a clean `not_found` for any failure
event whose `itemId` refers to a `flow_branches` row — there is no
branch-row retry path.

**C. Whole pipeline, custom initial state (operator-supplied input
the pipeline's discovery wouldn't produce):** see *Seed a pipeline
with operator-supplied state* above.


## If you see…

| Symptom                                                                                             | Likely cause + where to look                                                                  |
| --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `pipeline "<id>" already has 1 active flow(s)`                                                       | Singleton guard. Cancel/finish the prior run. `executor.js: start` line 105.                  |
| `Unrecognized key: "<name>"` from `openclaw doctor` or gateway start                                 | Plugin config not nested under `.config`. See [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them) §3.                                |
| Flow stuck at `__start__` in `running` for forever                                                   | `--detach` was used. CLI never invoked `runFlow`. Restart the gateway (revival picks orphans) or re-run without `--detach`. |
| `manifold: flow not found: <shortId>`                                                              | CLI commands take full UUID, not the dashboard's 8-char display id.                            |
| Dashboard transcript pane shows long messy nodeId, no item                                           | Pipeline uses a node id with a hyphen. Fix the node id. See [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them) §1.                  |
| Dashboard pipeline detail renders blank black                                                        | `pipeline.nodes` is an array instead of an object keyed by id. Fix the export shape.            |
| `itemId "<x>" not in pending list. Pending: [object Object]`                                         | `pendingSelector` returned objects without `.id`. Items must be `{ id: string, … }`.            |


## Operational quirks

- **`--detach` is a footgun.** It creates the flow record without
  driving the executor loop. The flow stays at `__start__` in
  `running` until something fires `executor.runFlow(flowId)`. The
  gateway's *revival service* (registered in `index.js: manifold-flow-revival`)
  picks up orphans at gateway boot. Practical fix: run without
  `--detach` inside a background shell instead.
- **No `cancel` CLI command.** Terminal status is only `succeeded` /
  `failed`. To force-stop a flow today, the only path is direct sqlite
  surgery against `runtime.sqlite` (`UPDATE flow_runs SET status='cancelled', ended_at=<ms> …`).
  This is on the *actuation* roadmap (`POST /api/runs/:id/cancel`).
- **Per-run artifacts** live in `<pipelinesDir>/<id>/runs/<runId>/`,
  not under the dashboard. The dashboard's `/api/runs/:flowId/artifacts`
  endpoint reads from there.
- **State is large.** Avoid stuffing huge blobs into `stateJson` —
  every revision rewrites the row. Use artifacts (file on disk) for
  anything >10KB.
