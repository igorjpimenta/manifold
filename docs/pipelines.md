# Pipelines

The static half of manifold: what a pipeline file exports, how to author it, and the rules the loader enforces.

## Glossary

| Term            | Meaning                                                                              |
| --------------- | ------------------------------------------------------------------------------------ |
| **pipeline**    | Static definition: `{ id, goal, state, nodes, edges, groups? }` exported by a module. |
| **node**        | One unit of work. Identified by its key in `pipeline.nodes`. Has a `kind`.            |
| **edge**        | `(state) => nextNodeId`. Lives in `pipeline.edges[fromNodeId]`. Pure function.       |
| **flow**        | One persistent run of a pipeline. Identified by `flowId` (UUID).                     |
| **state**       | Mutable JSON for a single flow. Persisted with every revision.                       |
| **revision**    | Monotonic counter. Increments every time a node returns a `stateUpdate`.             |
| **wait**        | `flow.waitJson` is set; flow is `blocked` or `waiting`. Resumed by external action.   |
| **session**     | One agent conversation. Owned by a harness agent. See [Session keys](lifecycle.md#session-keys-agent-nodes-dispatched-by-manifold).            |
| **item key**    | Slug identifying one item inside a `subflow` fan-out.                               |


## Pipeline schema (canonical)

```js
// ~/.openclaw/workspace/.pipelines/<pipelineId>/index.js
export default {
  id: "my-pipeline",                          // string, file-system safe
  goal: "one-line description",               // shown in the dashboard
  state: { /* initial state shape */ },       // merged with args at start
  nodes: {
    NODE_A: { kind: "code", run: (state) => ({ /* stateUpdate */ }) },
    NODE_B: {
      kind: "agent",
      agent: "scout",
      promptBuilder: (state) => "…",          // string returned to the agent
      applyResult: (state, output) => ({ /* stateUpdate */ }),
    },
  },
  edges: {
    __start__: () => "NODE_A",                // first hop, always required
    NODE_A:    (s) => s.cond ? "NODE_B" : "__end__",
    NODE_B:    () => "__end__",
  },
  // Optional. Logical groupings the dashboard renders as compound parents.
  groups: {
    SCAN: { label: "scan", kind: "pair",
            members: ["SCAN_CODE", "SCAN_AGENT"] },
  },
  // Optional. Pipeline-declared dashboard surfaces (see dashboard.md#declared-views-embed-view-pattern).
  views: [
    { id: "outcomes", kind: "funnel", label: "outcomes (last 30d)",
      source: { kind: "frontmatter-dir", path: SENT_DIR, timeField: "sent-at" },
      stages: [
        { id: "sent",    label: "sent" },
        { id: "replied", label: "replied", outcomeIn: ["replied", "won"] },
        { id: "won",     label: "won",     outcomeEq: "won" },
      ] },
  ],
};
```

**Required exports:** `id`, `state`, `nodes`, `edges`. Validation rules
in `loader.js:80-130`.

**Reserved node ids:** `__start__`, `__end__`. Never appear in `nodes`;
must appear in `edges` (at least `__start__`).

### Optional: `kind: "cascade"` — retry-on-failure as a flow characteristic

A flow-level (not node-level) `kind`, orthogonal to node `kind`. Declared
either directly on a top-level pipeline (`export default { id, kind:
"cascade", ... }`) or on a `subflow` node's `.flow.kind` — the loader
copies it onto the synthesized child pipeline exactly like it copies
`nodes`/`edges`, so a single step of a bigger pipeline can be isolated
into its own cascade-kind subflow.

The mechanic (`executor.js`'s catch block): in a cascade-kind flow, ANY
throw is treated like a completed dispatch, not a failure. `lastStep`
advances to the node that just threw (the failure itself is still
durably recorded via the normal execution-write path), and the loop
`continue`s — so on the very next pass it's **that node's own edge
function** that decides what happens next, exactly the way it would
after a success (typically by checking presence/absence of its own
output in `state`). There is no separate "retry from start" reset and no
engine-level "terminal" check: an edge function that has exhausted every
option it wants to try simply returns no next node, which the
existing `!nextNodeId` → `persistFailRetrying` path already handles
— giving up is just an ordinary edge decision, not a special case.

```js
// A minimal cascade: CODE_LOGIN throws once, its own edge escalates to
// AGENT_LOGIN; if AGENT_LOGIN also throws, its edge gives up (returns
// no next node) and the flow fails normally.
export default {
  id: "ensure-login",
  kind: "cascade",
  state: {},
  nodes: { CODE_LOGIN, AGENT_LOGIN },
  edges: {
    __start__: (state) => (state.AGENT_LOGIN ? "__end__" : "CODE_LOGIN"),
    CODE_LOGIN: (state) => (state.CODE_LOGIN ? "__end__" : "AGENT_LOGIN"),
    AGENT_LOGIN: (state) => (state.AGENT_LOGIN ? "__end__" : null),  // give up
  },
};
```

Author-side responsibilities: exhaustion (when to stop trying and let
the flow fail) and any retry-count cap are entirely up to the edge
functions — nothing bespoke is needed since `state/node-executions.js`'s
`queryErrorsByFlow`/`queryByNode` already let an edge consult prior
attempts on this flow if it needs more than presence/absence in `state`.

**`NodeFailure` and 2-arg edge functions** (`engine/executor-errors.js`,
`engine/executor-helpers.js`'s `resolveEdge` — "Eradicate soft errors"):
a cascade node's own `run()`/`apply()`
throws `NodeFailure(message, { errorType })` instead of soft-returning
`{ok: false, error_type}` on failure — `errorType` reuses the existing
vocabulary (`never_attempted | confirmed_absent | inconclusive |
infra_unavailable | blocked`, or `null` for classification-less failures).
Any plain `Error` works too; `errorType` is just `null` in that case.

An edge function may opt into receiving that thrown error as a **second
parameter** — `resolveEdge` dispatches on `fn.length`: a 2-arg edge
`(state, error) => next` gets `{message, errorType}` (or `null` on a
successful dispatch); a 1-arg edge `(state) => next` is unaffected, the
default and still the overwhelming majority of edges in this codebase.
This is purely additive — no existing edge needed to change.

```js
// CODE's own edge routes on the REAL thrown error, not a soft field —
// give up immediately on a genuine confirmed-absent/inconclusive outcome,
// fall through to an agent-fallback leg for anything else.
edges: {
  CODE: (state, error) => {
    if (state.CODE?.ok) return "__end__";
    if (error?.errorType === "inconclusive") return null;
    if (error?.errorType === "infra_unavailable") return "ENSURE_INFRA";
    return "AGENT_FALLBACK";
  },
},
```

**Caveat — a real footgun**: a default value (`(state, error = null) => ...`)
or a rest param collapses `fn.length` back down, silently falling to
1-arg mode with no error at all. Declare the parameter plainly.

**Attempt counting across dispatches**: there is no supported built-in way
to do this today. `_stepHistory` is not a `state_json` field, and nothing
provides it for pipeline-author use (`ctx` exposes no execution-count
accessor, and `assembleState()` never adds one to `state`). A
`NodeFailure` throw also never merges `state[nodeId]`, so a self-tracked
counter on the throwing node's own output can't survive a failing attempt
either — the same "no room for an arbitrary counter" problem this section
already calls out. An `infra_unavailable` failure ends a cascade
directly, same as any other failure class, and the operator retries
manually via the flow's own Retry action once infra is actually healthy.
If a pipeline genuinely needs a bounded automatic retry count, the only
sound pattern is a sibling `code` node in the same cascade that never
throws and always merges into `state` on every pass — have *that* node's
own `run()` increment its own attempts field, and have the throwing
node's edge read that (capped) instead of trying to count its own
dispatches or looping unconditionally.

**Generalized engine-wide**: every `kind:"agent"` node's `node.retry`
config is inert — `dispatchAgentWithRetry` (`engine/agent-dispatch.js`)
does not retry, for any agent dispatch, anywhere. Every agent node is a
single attempt; a failure throws immediately, records as a normal failure
event, and the operator retries manually — no exceptions, no per-node
opt-out. Any `retry: {...}` left on a node definition is ignored.

**Presence-check pattern for a single-outcome leg**: when a leg either
throws or returns real payload data with no need to classify *why* it
failed (an agent-fallback entry whose own `apply()` throws rather than
soft-returning an error field, say), its edge collapses to a presence
check: `(state) => (state.AGENT_ID !== undefined ? "__end__" : null)` — a
throw never merges `state[nodeId]`, so presence alone (any shape) is the
success signal.

### Optional: `recoveryActions`

Pipelines can declare custom operator-recovery actions surfaced on the
operator Failures inbox. Each entry is a `kind` (URL slug + dispatch
key) → descriptor + handler. The framework merges these with three
always-on defaults (`retry`, `ignore`, `unignore`) and exposes the
result per failure row as `available_actions[]`; clients render verbatim,
no pipeline-id awareness.

```js
recoveryActions: {
  skip: {
    label: "Skip",
    icon: "tray.and.arrow.down.fill",   // SF Symbol — rendered verbatim by clients that support them
    tint: "danger",                     // info | warning | danger | neutral
    destructive: true,                  // optional; UI may confirm before firing
    requiresBranch: true,               // optional; hides on flow-level failures
    appliesTo: ["skip-only", "redraft", "investigate"],  // recovery_hint filter
    handler: async ({ failure, body, api, logger }) => {
      // failure : the failure_events row (see internals.md#sqlite-shape-for-direct-queries--surgery)
      // body    : parsed JSON POST body (e.g. { via?, reason? })
      // returns : { ok: true, ...arbitrary } or throws
    },
  },
},
```

**Kinds reserved for the framework:** `retry`, `ignore`, `unignore`.
Pipelines that declare these are silently dropped from the merge — use
custom kinds (`skip`, `archive`, `mark-spam`, whatever your pipeline's
semantic is).

**Auto-resolve:** after a custom handler succeeds, the framework
resolves any open failure rows for the same `(pipeline_id,
branch_item_id)` tuple with `resolved_via = "auto:operator-<kind>"` —
pipelines don't have to touch the failure store themselves.

Live example: `freelance_workana-proposal/index.js`'s `skip` handler
writes `workspace/.shared/freelance/workana/skipped/<jobId>.md` so
future FILTER passes exclude the listing. Implementation lives in
`recovery-actions.js` (descriptor merge + handler lookup); HTTP
dispatch in `index.js: /manifold/failures/<id>/action/<kind>`.



## Common author mistakes (read this before writing a pipeline)

If you've written pipelines for other graph runtimes (LangGraph, n8n, Airflow, etc.), some of manifold's names and call conventions will not be what you expect. The loader catches the structural mistakes; the runtime catches the rest. Skim this table first — every row below has been hit by a real author shipping a real pipeline.

| Wrong | Right | What breaks |
|---|---|---|
| `initialState: () => ({...})` | `state: { ...staticShape }` | Loader error: `missing .state`. Runtime fills `runId`, `startedAt`, etc. itself — your static shape is merged with run args at flow start. |
| `subflow: { subgraph: { nodes, edges } }` | `subflow: { flow: { nodes, edges } }` | Loader error: `node X (subflow) missing .flow`. |
| `description: "..."` (top-level) | `goal: "..."` | Silently ignored — dashboard renders `goal`. Cosmetic only, but the pipeline list looks empty. |
| `buildBrief: ({ state, item }) => "string"` | `promptBuilder: (state) => "string"` | Runtime calls `node.promptBuilder(state)` positionally, always single-arg — your function gets `state` not `{state, item}`. Subagent receives garbage. Inside a `subflow` sub-node, the item is `state.item`, not a second argument. |
| `apply: ({ state, output }) => ({...})` (object destructure) | `apply: (state, output) => ({...})` | Same as above — runtime calls positionally, always `(state, output)`, whether the node is top-level or a `subflow` sub-node (`applyResult` also accepted for `agent` kind). There is no `subCtx` and no 3-arg form — a `subflow` sub-node is dispatched exactly like a top-level node; the item lives at `state.item`. |
| Gate node with `apply: …`, `buildPayload: …`, `delivery: …`, `buttons: […]` | Gate node with `approvalRequestBuilder: (state, itemId) => ({title, description})` only | Gate's apply path is owned by `gate-resolver.js` (a consumer bridge wires the callback). A decision lands in `state.approvedIds`/`state.rejectedIds` (or `state.approvalPending[itemId]` mid-flight) — read it in your edge function, e.g. `edges.GATE: (state) => state.approvedIds?.includes(state.item?.id) ? "STAMP" : "STAMP_REJECT"`. |
| Edge function reads `flow.data` or a `subCtx` argument inside a `subflow` sub-node | Edge function reads `state` directly — same as top-level | There is no `subCtx`. Inside a subflow sub-node: `edges.GATE: (state) => state.GATE?.decision === "approve" ? "STAMP" : "__end__"` (or read `state.approvedIds`, per the row above) — the subflow item is `state.item`. |
| Fallback pair declared via naming convention only | Add a `groups:` entry tying the pair | The dashboard won't render them as a compound parent. See [Groups (compound parents in the dashboard)](nodes.md#groups-compound-parents-in-the-dashboard). |
| Node id with hyphen (`send-cr`) | Node id with underscore/upper (`SEND_CR`) | See *Node IDs are token-style — no hyphens* below. |

### What the validator catches (loader.js)

These are hard-stops at pipeline load time — the gateway log shows `manifold loader: <id>/ failed to load: <reason>`:

| Loader error | Fix |
|---|---|
| `pipeline must export an object` | `export default { ... }` — not a class, not a function. |
| `missing .id` | Add `id: "team_name"` at the top of the export. |
| `missing .state` | Use `state: { ...staticShape }`, not `initialState`. |
| `missing .nodes` | Provide `nodes: { ... }` even for one-node pipelines. |
| `missing .edges` | Provide `edges: { __start__: (s) => "FIRST_NODE", ... }`. |
| `edges must define __start__` | Always wire `__start__` to your first node. |
| `node "X" has unknown kind "Y"` | Use one of: `code`, `agent`, `subflow`, `human-gate`. |
| `node "X" (subflow) missing .flow` | Use `flow: { nodes, edges }`, NOT `subgraph:`. |
| `node "X" .flow.edges must define __start__` | Inner flow needs its own `__start__` edge — wire it to the first per-item sub-node. |

### What only the runtime catches

Past the loader, these only surface during a real flow:

- **`approvalRequestBuilder` returning a non-object** — runtime can't serialize it for delivery. Return `{ title, description }` at minimum.
- **`promptBuilder` returning a non-string** — the agent will see `[object Object]`. Stringify your inputs into a prompt.
- **`apply` returning a non-object** — runtime expects a `stateUpdate`, wrapped under your own node id, whether the node is top-level or a `subflow` sub-node (see [Subflow nodes (childFlows)](nodes.md#subflow-nodes-childflows)). Returning `null` or `undefined` is fine (no update). Returning a number / string throws.
- **`pendingSelector` for `gate`/`human-gate` returning items that don't match what the operator's decision keys against** — gate-resolver can't map decisions back to items. For a `subflow`'s own gate sub-node, this means returning something other than `state.item?.id` (a childFlows child is always exactly one item).
- **Forgetting `itemKey` on `subflow` when the item's natural key isn't `.id`/`.listingId`** — `deriveSubflowKey` falls through to `String(item)`, which is almost never what you want for an object. See *Item keys are kebab-case slugs* below.

### Two gotchas that *look* like bugs but aren't

These cost an authoring session each. Worth knowing.

#### Edge functions: dashboard parses the source, not the runtime value

The dashboard graph view (`dashboard/pipeline-graph.js`) extracts edge targets by **regex-matching literal node-id strings in the edge function source code**. It cannot follow closure variables, imported constants, or template-string interpolation. If you write:

```js
// Closure variable — invisible to dashboard
import { buildPrimeNodes } from "./lib/prime-nodes.js";
const nextNodeId = "ENSURE_LOGIN_CODE";
const edges = {
  PRIME_SCAN: (state) => state.done ? "PRIME_FILL" : nextNodeId, // ← dashboard sees PRIME_FILL only
};
```

…only the `"PRIME_FILL"` literal is detected — the closure variable disappears. The runtime still routes correctly, but the dashboard graph shows the edge as a dead end (or as `→ ?`).

**Fix:** keep both target ids as literal strings in the edge function body:

```js
const edges = {
  PRIME_SCAN: (state) => state.done ? "PRIME_FILL" : "ENSURE_LOGIN_CODE", // ← both visible
};
```

Factories that compute targets should return only nodes; let each pipeline define its own edges so the literals live in the pipeline source.

#### Pipeline reload only refreshes `index.js`, not its `file:///` imports

The pipelines-watcher reloads each pipeline's `index.js` via a `data:` URL (forces a fresh import per the comment in `loader.js`). But the pipeline's `import { ... } from "file:///.../lib/foo.js"` statements go through Node's normal ESM loader — and **Node caches modules by URL forever within a process**. Edits to shared `lib/` files used by pipelines won't take effect until you restart the gateway.

**Fix:** restart the gateway after editing any `lib/` file:

```sh
launchctl unload ~/Library/LaunchAgents/ai.openclaw.gateway.plist
launchctl load   ~/Library/LaunchAgents/ai.openclaw.gateway.plist
```

If you only edit a pipeline's own `index.js`, the watcher handles the reload — no restart needed.

### Recommended skeleton (copy this and modify)

```js
import path from "node:path";
import os from "node:os";

const ROOT = path.join(os.homedir(), ".openclaw");

export default {
  id: "myteam_my-flow",
  goal: "one-line description shown in the dashboard",
  // Identity surfaces wherever the pipeline shows up in operator UIs.
  //   emoji      — dashboard headers, consumer notifications, Live Activity widgets
  //   sfSymbol   — Inbox pipeline-row icon (any valid SF Symbol name; see
  //                Apple's SF Symbols app). Falls back to "tray.fill" when
  //                omitted, so this field is optional but recommended.
  //   displayName — human label used everywhere a pipeline is named.
  identity: { emoji: "🧩", sfSymbol: "puzzlepiece.fill", displayName: "My flow" },
  schedule: ["0 9 * * *"],            // optional cron(s), GMT-3 if your gateway is in BR

  // Static state shape. Merged with run args at flow start. Don't add an
  // `errors` array here — errors are never a stateJson field; every node
  // failure is recorded to flow_node_executions and served as the
  // top-level errorEntries[] (see lifecycle.md#state-conventions).
  state: {
    items: [],
    parallel: {},
  },

  nodes: {
    PRIME: {
      kind: "code",
      run: async (state) => ({ primedAt: Date.now() }),
    },
    SCAN: {
      kind: "agent",
      agent: "marketplace-scout",
      skill: "scan-some-source",
      promptBuilder: (state) => `Scan and return JSON. Context: ${state.runId}`,
      apply: (state, output) => ({ items: JSON.parse(output.text) }),
    },
    // subflow nodes fan out into real child flows — see docs/nodes.md
    // (Subflow nodes) for the full authoring guide. Never set
    // .groupId; it's always the node's own id ("PROCESS" here).
    PROCESS: {
      kind: "subflow",
      itemKey: "id",
      selector: (state) => state.items ?? [],
      flow: {
        nodes: {
          WRITE: {
            kind: "agent",
            agent: "content-writer",
            promptBuilder: (state) => `Process item ${state.item?.id}.`,
            apply: (state, output) => ({ WRITE: { ...state.item, result: output.text } }),
          },
        },
        edges: {
          __start__: "WRITE",
          WRITE: "__end__",
        },
      },
    },
  },

  edges: {
    __start__: () => "PRIME",
    PRIME: "SCAN",
    SCAN: (state) => (state.items?.length ? "PROCESS" : "__end__"),
    PROCESS: "__end__",
  },

  // Optional. Tie code+agent pairs together for the dashboard.
  groups: {
    SCAN_GROUP: { label: "scan", kind: "pair", members: ["SCAN", "SCAN_AGENT"] },
  },

  // Optional. Run-summary writer.
  runAudit: {
    runsDir: path.join(ROOT, "workspace", ".pipelines", "myteam_my-flow", "runs"),
    summarize: (state) => ({
      runId: state.runId,
      items: state.items?.length ?? 0,
      processed: state.processed?.length ?? 0,
    }),
  },
};
```


## Conventions (and what breaks if you violate them)

### 1. Node IDs are token-style — no hyphens

**Rule:** Node ids match `[A-Za-z0-9_.]+`. Use UPPER_SNAKE, `lowercase`,
or `parent.child` for subflow members. **Never use hyphens.**

**Why:** Node ids surface verbatim in synthetic `${pipelineId}::${nodeId}`
addresses, lineage labels, and human-readable contexts — next to
hyphenated kebab-case item keys. Keeping hyphens out of node ids keeps
those compound names unambiguous to both readers and tooling.

**Examples:** ✅ `SCORE`, `ENSURE_LOGIN_CODE`, `propose.draft`
❌ `ensure-login`, `score-listing`

### 2. Item keys are kebab-case slugs

**Rule:** For `subflow` nodes, each item must expose a
stable string key via `item.id` or `item.listingId` (or `String(item)`
fallback). Slugs may (and usually do) contain hyphens.

**Why:** itemKey is the second component of the session key (rule 1).

### 3. Plugin config nests under `.config`

**Rule:** In `~/.openclaw/openclaw.json`:

```jsonc
"plugins": {
  "entries": {
    "manifold": {
      "enabled": true,
      "config": {                      // ← NOT flat on the entry
        "dashboardHost": "127.0.0.1"
      }
    }
  }
}
```

**Why:** openclaw's config validator at `dist/io-…js: validateConfigObjectWithPluginsBase` builds plugin-config issue paths as `plugins.entries.<id>.config.*`. Flat keys fail strict validation with `Unrecognized key: "<name>"`.

### 4. Singleton — one active run per pipelineId (opt-out via `allowConcurrent`)

**Rule:** `executor.start(pipelineId, args)` throws if any flow for
that pipeline is `running`, `blocked`, or `waiting`. Cancel or finish
the existing flow before starting a new one.

**Why:** Pipelines own real-world resources (a browser profile, a
marketplace login, operator attention via push notification). Parallel runs
race for those. `executor.js: start` lines 95–111.

**Opt-out:** add `allowConcurrent: true` on the pipeline definition.
The singleton check is skipped; multiple active flows for the same
pipelineId become a normal operating state. Use when each flow owns
an independent slice of state — e.g., one flow per inbound reply,
each gated independently by the operator. Don't use for pipelines
that share a real-world resource (browser, login, etc.); the
serialization is doing real work there.
