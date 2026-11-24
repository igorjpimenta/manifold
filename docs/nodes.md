# Nodes

The unit-level half of manifold: the four node kinds, subflow fan-out, and the authoring mechanics that apply to any node at any level.

## Node kinds

| `kind`              | Required fields                                                                      | Returns                | Exits to                                  |
| ------------------- | ------------------------------------------------------------------------------------ | ---------------------- | ----------------------------------------- |
| `code`              | `run(state, ctx) => stateUpdate \| Promise<stateUpdate>` — `ctx` exposes `invokePipeline` (see below) | `stateUpdate`          | normal edge                               |
| `agent`             | `agent`, `promptBuilder(state)`, `applyResult(state, output)`. Optional `circuitBreaker: true` — see *Circuit breaker* below. | `stateUpdate`          | normal edge                               |
| `subflow`           | `selector(state) => Item[]`, `flow: { nodes, edges }` (required — no other shape). Never set `.groupId` — it's always the node's own id; the loader throws if you set it. See *Subflow nodes (childFlows)* below for the full authoring guide. | one real child `flow_runs` per item; parent blocks (`waitJson.kind:"children-pending"`) until every sibling child is terminal, then auto-resumes **if `mode` is satisfied** — otherwise the parent is failed directly, with a real retryable `failure_events` row | normal edge, once all children terminal and `mode` satisfied |
| `human-gate`        | `pendingSelector(state) => Item[]`. Each item is `string` *or* `{ id: string, reason?, hint?, text?, editable? }` — see *Human-gate pending item shape* below | sets `waitJson`; flow → `blocked` | resumes via the dashboard GateForm or the `manifold_approve` MCP tool |
| `gate`              | `pendingSelector` + `approvalRequestBuilder`. Channel-agnostic per-item gate. Delivery is consumer-side: the engine broadcasts `manifold.gate.opened` / `manifold.gate.closed` over its event stream with the full payload, and each host decides how to surface it (OpenClaw: Inbox, Live Activity, …). Optional `discuss: { agent, promptBuilder }` opts the gate into a discuss surface — see *Per-gate discuss opt-in*. | sets `waitJson` (`pending: string[]`, `items: [{itemId, requestPayload}]`) | resumes via any channel's resolver call into `gate-resolver.recordItemDecision` |

A `code`-then-`agent` fallback (deterministic primary path, LLM-driven
recovery leg) is not a distinct node kind — author it as a
single-item `subflow` node (`selector: () => [{ id: "only" }]`) whose own
`flow.kind: "cascade"` child flow routes between an ordinary `code` entry
and an ordinary `agent` entry via hand-authored edges, with `mode: "all"`
so the engine fails the parent automatically if neither leg succeeds. See
the cascade section in [Pipelines](pipelines.md) for the underlying
mechanic and a worked `CODE_LOGIN`/`AGENT_LOGIN` example.

Detailed dispatch logic: `executor.js` lines 380–730.


## Subflow nodes (childFlows)


A `subflow` node fans a `.selector(state)` array out into one real,
independent flow per item — each item's own `flow_runs` row, driven by the
exact same executor as any top-level flow. This is the ONLY subflow shape;
there is no simple/inline mode, and the loader rejects `.groupId` (it's
always the node's own id). Sub-nodes inside `flow.nodes`/`flow.edges` are
dispatched **identically** to root-level nodes — same `run`/`promptBuilder`/
`apply`/`pendingSelector` signatures, no `subCtx`, every return wrapped
under its own node-id key. Outside the subflow, read a child's results at
`state._subflowGroups[groupId][subflowKey]`.


## Node authoring reference

General node-authoring mechanics that apply to any node kind, at any
level (top-level or a subflow's sub-node) — none of this is
subflow-specific, even though earlier revisions of this doc nested it
under *Subflow nodes*.

### Circuit breaker (`node.circuitBreaker`)

An opt-in flag on any `kind:"agent"` node — top-level, or a cascade
sub-node inside a `flow.kind:"cascade"` subflow's own `flow.nodes` — that
fails fast instead of repeatedly dispatching an agent that's known to be
broken:

```js
FLAKY_AGENT: {
  kind: "agent",
  agent: "flaky-thing",
  circuitBreaker: true,               // opt-in; default false/absent = no-op
  promptBuilder: (state) => "…",
  applyResult: (state, output) => ({ FLAKY_AGENT: output }),
}
```

Implemented entirely inside `dispatchAgentEntry` (`engine/executor-nodes.js`)
— the single dispatch path every `kind:"agent"` node goes through,
regardless of whether it's a top-level node or a cascade sub-node running
inside its own child flow. Before doing any work — before the budget gate, before building the
prompt, before spawning a session — it checks
`nodeHealthStore.isBlocked(pipelineId, nodeId)` and throws immediately if
blocked, with a message that names the block:

```
unhealthy agent dispatch: "<nodeId>" is blocked after a prior failure —
clear its node-health block once the underlying issue is confirmed fixed
```

On a genuine dispatch failure it calls `nodeHealthStore.markUnhealthy(...)`
before re-throwing, which blocks every future dispatch of that
`(pipelineId, nodeId)` pair until the node-health store is reset.
Deliberately does
**not** mark unhealthy for `NodeBlocked` (an ordinary budget-gate pause,
not a dispatch failure) or `GatewayHandoffNeeded` (the agent runtime
being unreachable in the current context, not evidence the agent itself
is broken).

Gated purely on the flag — a node that never sets `circuitBreaker` is a
complete no-op through this whole path, so plain agent nodes are
unaffected unless they opt in. Every cascade sub-node needs this flag set
explicitly — see `ensure-login-node.js` factories and `submit-agent.js`
files under `workspace/.pipelines/` and `workspace/.shared/` for worked
examples.

### Gate `approvalRequestBuilder` — editable surfaces

`approvalRequestBuilder(ctx, item)` returns the payload the gate UI renders. Standard fields:

| Field | Surface |
|---|---|
| `title` | Card header on the operator Inbox |
| `description` | Card body — verbose; can include layout markers |
| `editableText` | Prose body the edit-sheet pre-fills with AND the preview card shows. The free-form content the operator commits if they hit Approve unchanged. Body-only — no structured metadata. |
| `editableFields` | Optional `[ { id, label, type, value, options?, prefix?, helper? } ]` — pipeline-declared structured fields rendered as tappable chips above the prose body. Clients pick input UI from `type`. |
| `references` | Array of `{ id, label, body }` chips on the gate row + DiscussView |

**Prose body (`editableText`)**: pipelines with a free-form editable surface (workana-reply, touch-cadence, workana-proposal's prose) set this. The full-prose edit sheet renders + commits its content. If omitted (`undefined`), the executor auto-fills from the subflow's prior code-node outputs — the last `{ ..., text: "…" }` shape wins. Set `editableText: ""` explicitly to opt OUT.

**Structured fields (`editableFields`)**: pipelines that store metadata alongside the prose (workana-proposal's frontmatter: budget / timeframe / language) parse it once at builder time and expose each value as one entry. Clients render a chip per entry; tap → focused single-field editor matching `type`; save → `apply-draft` `fields:` payload → server's `persistRevisedMeta` writes back. Pipelines without structured metadata omit the array.

Field-entry shape:

| Key | Required | Notes |
|---|---|---|
| `id` | yes | Frontmatter key on disk (or any persistence key the pipeline understands) |
| `label` | yes | Display label for the chip + sheet header |
| `type` | yes | `text` / `multiline` / `money` / `enum`. Unknown types degrade to `text` on client. |
| `value` | yes | Current value as a string. Clients render display formatting per type (e.g. `money` gets thousand-separated). |
| `options` | for `enum` | Array of allowed string values. Renders as segmented control (≤4) or picker (>4). |
| `prefix` | optional | For `money`: rendered before the value on both chip + input (e.g. `"R$"`). |
| `helper` | optional | Footer hint shown in the single-field editor. No hardcoded copy on clients — pipelines own the wording. |
| `editable` | optional, default `true` | When `false`, the chip renders read-only (no tap target, muted styling). Useful for display-only context fields the operator should see but not edit (e.g. `source-listing`, `drafted-at`). |

Example (workana-proposal):

```js
return {
  title: `Proposal for ${listing.id} (score ${listing.score})`,
  description: body,                         // long blob with markers
  editableText: prose,                       // body-only
  editableFields: [
    { id: "budget",    label: "Budget",    type: "money", prefix: "R$", value: fm.budget,
      helper: "Numeric BRL the bid form receives." },
    { id: "timeframe", label: "Timeframe", type: "text",  value: fm.timeframe,
      helper: "Delivery duration in the draft's language." },
    { id: "language",  label: "Language",  type: "enum",  value: fm.language,
      options: ["pt-BR", "en", "es"],
      helper: "Language code for the draft body." },
    // Read-only context — operator sees it, can't tap to edit.
    { id: "drafted-at", label: "Drafted", type: "text", value: fm["drafted-at"], editable: false },
  ],
  references,
};
```

### Code-node `ctx` argument

`code`-kind nodes receive a `ctx` object as the LAST positional argument — always `run(state, ctx)`, whether the node is top-level or a `subflow` sub-node (the subflow item, if any, is `state.item` — not a separate positional arg). Existing nodes that ignore the trailing arg keep working unchanged.

`ctx` exposes:

| `ctx.invokePipeline(targetPipelineId, args)` | Start a fresh flow of another pipeline and drive it in the background. Returns immediately. |
| `ctx.flowId` / `ctx.nodeId` / `ctx.pipelineId` | This dispatch's own identifiers, for logging/artifact paths — same values threaded into `log`/`artifact` below, exposed directly too. |
| `ctx.log.info(msg)` / `.warn(msg)` / `.error(msg)` | Logs with the flow/node prefix already applied — prefer this over a bare `console.log` so log lines are attributable. |
| `ctx.artifact(name, content)` | Writes a file under this run's own artifact directory (`<pipelinesDir>/<pipelineId>/runs/<runId>/<name>`). Throws if `state.runId`/`state.pipelineId` aren't set yet (i.e. before `start()` has stamped them). |
| `ctx.readArtifact(name)` | Reads a file back from the same directory. Same throw condition as `ctx.artifact`. |
| `ctx.blocked(reason, hint)` | Throws `NodeBlocked({reason, hint})`. **Despite the name and the class's own doc comment ("blocks the flow with retry-on-resume semantics"), this actually fails the flow, not pauses it** — verified directly against `executor.js`'s own catch logic: a `NodeBlocked` reaches a real wait/pause only if its `_blocked._kind` is set, and `ctx.blocked()` never sets one (`internal/needs-resolver.js`'s `needs:` mechanism does, by explicitly stamping `_kind: "needs-not-ready"` before throwing — that's the one real example of the pause path firing). Calling `ctx.blocked(reason, hint)` today is equivalent to `throw new Error(reason)` with a slightly different message shape, not a way to pause a node — this looks like a real gap between the helper's intent and its actual behavior, not a documented design choice. |
| `ctx.needs(varName)` | Resolves a `needs:` declaration — see below. Only present/meaningful if the node itself declares `needs:`. |

Return shape of `ctx.invokePipeline`:

```js
{ invoked: true,  target, flowId }                  // started
{ invoked: false, reason: "singleton-busy", target } // target's own singleton guard rejected
```

Any other `start()` error propagates (e.g., unknown `pipelineId`). Use this when the parent should fan out to another pipeline without blocking on its lifecycle — e.g., a fast scraper triggering a long-running operator-approval flow. The two pipelines retain independent singletons, so the target can park at a gate without affecting the parent's cron cadence.

#### `needs:` — reaching an ancestor flow's data (subflow children only)

A subflow child's own `state` is deliberately narrow — just `item`/`itemIndex`/`parentItem` (see *Subflow nodes (childFlows)* above), no full clone of the parent. When a node genuinely needs something from an ancestor flow beyond that, declare `needs:` on the node itself:

```js
CHILD_NODE: {
  kind: "code",
  needs: {
    // local variable name → what to fetch
    parentScan: { hops: 1, nodeId: "SCAN" },        // a parent's own node output
    rootRunId:  { hops: 2, entry: "runId" },         // an ancestor's entry/bookkeeping field
    scoreFields: { hops: 1, nodeId: "SCORE", fields: ["score", "reasons"] }, // pick a subset
  },
  run: async (state, ctx) => {
    const scan = await ctx.needs("parentScan");   // throws NodeBlocked (pauses, not fails) if not ready yet
    // ...
  },
},
```

- `hops` counts levels up the subflow-spawn chain (`1` = immediate parent, `2` = grandparent, etc.) — this is the number of times the CHILD's own flow was itself spawned as a subflow item, not a node-graph distance.
- Exactly one of `nodeId` (another node's own output, unwrapped from its `{[nodeId]: value}` shape automatically) or `entry` (an ancestor's entry field — `runId`, `startedAt`, `args`, `triggeredBy`, `pipelineId`, `cost`, `_display`, `approvedIds`, `rejectedIds`, `editedTexts`, `approvalPending`, `approvalResults`, `_cascadeError`; `gates` is deliberately not reachable this way — see [Lifecycle → State conventions](lifecycle.md#state-conventions)) must be declared.
- Optional `fields: ["a","b"]` picks a subset of a `nodeId`-type target's own output instead of the whole thing.
- Resolution is **lazy** — a node with 3 declared `needs` only pays for the ones its own code actually `await`s; a branch that never calls `ctx.needs("x")` never resolves or blocks on `x`.
- `ctx.needs(varName)` on an undeclared `varName` throws a plain `Error` (an author typo) — not a wait state. A declared-but-not-yet-ready target throws `NodeBlocked` with `_kind: "needs-not-ready"` explicitly stamped on — which genuinely pauses the flow (unlike a bare `ctx.blocked()`, see above) — and resolves automatically once the target completes; a declared target that will never exist (walked off the top of the ancestor chain, or the named node was never dispatched by that ancestor) also throws the same stamped `NodeBlocked` — there is no silent `undefined`.
- All of this is validated at pipeline-load time against the real ancestor chain (a hop deeper than the pipeline actually nests, a `nodeId` that doesn't exist at that level, or declaring `needs:` on a `human-gate` node) — a bad declaration fails to load, never a silent runtime miss.

### Human-gate pending item shape

The `pendingSelector` may return raw strings (just itemIds) or objects.
The dashboard's `GateForm` reads these to decide which buttons to show:

| Field         | Type     | Effect on the dashboard form                                          |
| ------------- | -------- | --------------------------------------------------------------------- |
| `id`          | string   | **Required.** Sent as `itemId` to the approve endpoints (dashboard GateForm → `POST /api/runs/:flowId/approve`, harness bridge → `POST /manifold/gate/resolve`). |
| `reason`/`hint` | string | Rendered as a small subtitle under the item row.                       |
| `text`        | string   | Enables the **edit** button. Textarea is pre-filled with this text.   |
| `editable`    | boolean  | If `true`, enables the **edit** button with an empty textarea.        |

If neither `text` nor `editable` is present, the row shows approve/reject only. Approve and reject are always available.

Submitting an edit goes through `applyApproval` (`gates.js`) and writes
`state.editedTexts[itemId]` — downstream nodes that should honor an
edited result must read from there.

### Per-gate discuss opt-in

A `human-gate` node can optionally let the operator chat with an agent
about a pending item before deciding. The chat lives in a dedicated host
surface (OpenClaw: the Inbox screen) and is transported over the engine's
WS dialog door
(`ws://<engine-host>:44302/api/dialog?bearer=<MANIFOLD_BEARER>`, port via
`MANIFOLD_DIALOG_PORT`):
the client sends `{type:"message", flowId, itemId, text}` frames and gets
back `{type:"ack"}`, then `{type:"reply"}` (or `{type:"error"}`).

**Global default** — set `discuss.defaultAgent` in the adapter config
(OpenClaw: `openclaw.json`)
so every gate in every pipeline gets a discuss agent without declaring
its own `discuss` block:

```json
{
  "plugins": {
    "entries": {
      "manifold": {
        "enabled": true,
        "config": {
          "discuss": {
            "defaultAgent": "main"
          }
        }
      }
    }
  }
}
```

**Per-gate override** — add a `discuss` block on the gate node to use a
different agent or inject context into every session turn:

```js
GATE: {
  kind: "human-gate",
  discuss: {
    agent: "main",
    promptBuilder: ({ state, itemId, flowId, pipelineId }) => {
      // Injected as a system prompt for every turn in this gate's session.
      // Give the agent context: the listing, current draft, orchestration rules.
      const draft = state.items?.find(i => i.id === itemId)?.text ?? "";
      return [
        `You are reviewing item ${itemId} for pipeline ${pipelineId}.`,
        `flowId: ${flowId}   itemId: ${itemId}`,
        draft ? `Current draft:\n${draft}` : "",
        `When the operator asks for a revision, hand it to your content`,
        `agent, then end your turn immediately.`,
      ].filter(Boolean).join("\n");
    },
  },
},
```

Without the `discuss` block (and no global default), the Inbox hides
the Discuss row entirely. Gates that aren't reachable for chat behave
exactly as before — Approve / Deny / Edit & approve / Cancel only.

**Mechanics:**

| Concern | Behavior |
|---|---|
| Channel | the WS dialog door — no channel plugin. Operator messages arrive as `{type:"message", flowId, itemId, text}` frames; the door answers `{type:"ack"}`, then `{type:"reply"}` (or `{type:"error"}`). |
| Session key | the harness's own session id, passed through verbatim — locked on the first turn and kept for the gate's lifetime. |
| Session stamp | Recorded in the session roster (`discuss_threads`: flow, gate, item → agent + session key) on first turn, so agent/config changes never break a live chat. |
| First-turn prompt | `promptBuilder` output is injected as a system prompt for the session; called on every turn (not just first). Return a non-string to skip it. |
| Draft proposal | A `discuss.component` event carrying a draft payload — a connected client surfaces an Apply affordance. |
| Meta proposal | A `discuss.component` event carrying a fields payload — updates the chip row on connected clients. |
| subflow (sub-node gate) | Gates inside `flow.nodes` opt in identically. `resolveDiscussTarget` walks `subflow` parents and picks the first `kind: "human-gate"` sub-node with a `discuss` block. |
| Resolution order | Stamped session → node `discuss.agent` → adapter `discuss.defaultAgent`. |
| History | `GET /manifold/runs/<flowId>/discuss?itemId=...` returns message history and pending components for cold-reconnect. |

**Pipeline authors:** `promptBuilder` is your contract surface. Inline the
listing context, current draft, and orchestration rules the agent needs.
Point the agent at the host's discuss tooling — not at `<draft>` tags or
a raw subagent call. Keep the prompt under ~2 KB.

### needs-info: free-text vs button choices

A `code` node that returns `{ _blocked: { reason, needed, hint } }` puts
the flow into a `needs-info` wait (`waitJson.kind="needs-info"`). The
dashboard renders a `RespondForm` with a textarea by default; once the
wait is answered the flow resumes and the node re-runs (the executor
logs `→ waiting (will re-run <node> on resume)`).

Pipeline authors can opt into a **button-only** form by passing
`choices: string[]` in the `_blocked` payload:

```js
return {
  _blocked: {
    reason: "approve to ship this proposal",
    choices: ["approve", "deny"],   // ← opt-in: dashboard renders 2 buttons
  },
};
```

The dashboard's RespondForm renders one button per choice (each
submits the choice label as the response) and skips the textarea. The
node interprets the choice in its next run. If `choices` is omitted,
the textarea remains.

`choices` is a dashboard-form hint, not a contract.


## Groups (compound parents in the dashboard)

`pipeline.groups` is optional. Each entry is a logical grouping of
nodes the dashboard renders as a compound parent. Typical use: a
**code+agent pair** like `(SCAN_CODE, SCAN_AGENT)` where reaching the
second member signals the cheap/fast path failed.

```js
groups: {
  GROUP_ID: {
    label: "human-friendly title",    // optional; defaults to GROUP_ID
    kind:  "pair",                    // optional; styling hint
    members: ["NODE_A", "NODE_B"],    // required, non-empty
  },
}
```

### Member id forms

| Form              | Example                          | Where the node lives                       |
| ----------------- | -------------------------------- | ------------------------------------------ |
| Bare              | `"SCAN_CODE"`                    | top-level `pipeline.nodes`                 |
| Qualified         | `"PROPOSE.SUBMIT_CODE"`          | inside a `subflow` node's `flow.nodes`     |

A qualified id is `<subflowParentId>.<childId>`. The loader
validates: parent exists, has `kind === "subflow"` and has `flow.nodes`, and child
is in `parent.flow.nodes`.

### Same-scope rule

Every member of one group must live in the same scope — either all
top-level or all inside the same subflow. Cytoscape's
compound model allows only one parent per node, so a group's compound
has to be wholly nested in one place.

**Examples:** ✅ `["SCAN_CODE", "SCAN_AGENT"]` (both top-level)
✅ `["PROPOSE.SUBMIT_CODE", "PROPOSE.SUBMIT"]` (both inside PROPOSE)
❌ `["SCORE", "PROPOSE.SUBMIT"]` (mixes scopes — loader throws)
❌ `["A.X", "B.Y"]` (different subflow parents — loader throws)

A node cannot be a member of more than one group.

Code paths: `loader.js: validateGroups` (validation),
`dashboard/src/components/GraphView.tsx` (rendering — nests
subflow-internal group compounds inside their subflow node).
