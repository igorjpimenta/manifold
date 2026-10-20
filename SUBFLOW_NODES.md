# pipeclaw — subflow nodes (childFlows)

Split out of `README.md` to keep that file focused on quick orientation.
Read this when authoring or modifying a `kind:"subflow"` node. For general
node-authoring reference material that applies to any node kind (locks,
`ctx.invokePipeline`, gate payload shape, discuss opt-in, needs-info) see
`README.md`'s *Node authoring reference* section instead — none of that is
subflow-specific even though it used to live under this heading.

A `subflow` node fans a `.selector(state)` array out into one real, independent
flow per item — each item's own `flow_runs` row, driven by the exact same
executor as any top-level flow. This is the ONLY subflow shape; there is no
simple/inline mode, and the loader rejects `.groupId` (it's always the
node's own id).

## Index

- [Minimal shape](#minimal-shape)
- [Sub-nodes use the exact same calling convention as top-level nodes](#sub-nodes-use-the-exact-same-calling-convention-as-top-level-nodes)
- [What a sub-node's `state` actually contains](#what-a-sub-nodes-state-actually-contains)
- [Deriving `subflowKey` (the child's identity)](#deriving-subflowkey-the-childs-identity)
- [Reading a subflow's results downstream](#reading-a-subflows-results-downstream)
- [`flow.display` — identity for iOS/dashboard clients](#flowdisplay--identity-for-iosdashboard-clients)
- [Locks and concurrency](#locks-and-concurrency)
- [Retrying, resuming, and gating one item](#retrying-resuming-and-gating-one-item)
- [`code` node's `ctx.invokePipeline` inside a subflow](#code-nodes-ctxinvokepipeline-inside-a-subflow)
- [Nested subflows](#nested-subflows)

## Minimal shape

```js
SCORE: {
  kind: "subflow",
  itemKey: "id",                          // optional — see "Deriving subflowKey" below
  selector: (state) => state.filtered ?? [],
  flow: {
    display: (item) => ({ title: item.title, url: item.url }),  // optional
    nodes: { CACHE_CHECK, SCORE_LLM, CACHE_SAVE },
    edges: {
      __start__: "CACHE_CHECK",
      CACHE_CHECK: (state) => state.CACHE_CHECK?.hit ? "__end__" : "SCORE_LLM",
      SCORE_LLM: "CACHE_SAVE",
      CACHE_SAVE: "__end__",
    },
  },
},
```

## Sub-nodes use the exact same calling convention as top-level nodes

This is the single most important thing to internalize coming from the old
branch-worker model (or from other graph runtimes): a `flow.nodes` entry
is dispatched **identically** to a root-level node. `code` nodes get
`run(state, ctx)`; `agent` nodes get `promptBuilder(state)` /
`apply(state, output)`; `human-gate` sub-nodes get `pendingSelector(state)` /
`approvalRequestBuilder(state, itemId)`. A code-then-agent cascade sub-node
is just an ordinary `code` entry and an ordinary `agent` entry wired
together by hand-authored edges within the same `flow.kind:"cascade"`
child flow — see the README's *Optional: `kind: "cascade"`* section —
and both still declare `crossProcessLock` directly, same as any other
sub-node. There is no `subCtx`, no `(state, item, output)` three-arg
form, no auto-nesting of return values — **you must wrap every return
under its own node-id key**, same as any top-level node:

```js
// nodes/score/cache-check.js
export const CACHE_CHECK = {
  kind: "code",
  run: async (state) => {
    const listing = state.item;            // ← the subflow item, not a 2nd arg
    // ...
    return { CACHE_CHECK: { hit: true, ...listing, score } };  // ← wrap under your own id
  },
};
```

```js
// nodes/score/score-llm.js
export const SCORE_LLM = {
  kind: "agent",
  agent: "scorer",
  promptBuilder: (state) => buildScoreListingPrompt(state),      // (state) only
  apply: (state, output) => {
    const listing = state.item;
    // ...
    return { SCORE_LLM: { score, reasons } };
  },
};
```

```js
// nodes/propose/gate.js — human-gate sub-node
export const GATE = {
  kind: "human-gate",
  // A childFlows child is always exactly one item, so pendingSelector
  // collapses to "the item's own id, while undecided":
  pendingSelector: (state) => {
    const id = state.item?.id;
    if (!id) return [];
    if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
    return [id];
  },
  approvalRequestBuilder: (state) => ({ title: `Approve ${state.item?.id}?`, description: "…" }),
};
```

Edges inside `flow.edges` read `state` directly too — no `subCtx`:

```js
edges: {
  GATE: (state) => state.approvedIds?.includes(state.item?.id) ? "STAMP" : "STAMP_REJECT",
},
```

## What a sub-node's `state` actually contains

The child's initial state is **not** a clone of the parent's state (that
was the pre-2026-08-17 model — a child used to inherit the parent's whole
state minus a few stripped keys; removed once an audit of every real
subflow node found zero genuine reads of anything beyond what's listed
below). A child's initial state is exactly:

| Key | Value |
|---|---|
| `item` | The selected array element for this child. |
| `itemIndex` | The item's position in `selector(state)`'s output. |
| `parentItem` | The *spawning* flow's own `item`, if the parent flow is itself a subflow child (nested subflow-within-subflow); `null`/`undefined` for a root-level subflow spawn. |

**Need something from an ancestor beyond that?** Declare `needs:` on the
node that wants it — a lazy, per-node accessor resolved just-in-time via
`ctx.needs(varName)`, not an eager clone. See `README.md`'s *Node
authoring reference* section for the full `needs:` contract (`{hops,
nodeId}` for another node's own output, `{hops, entry}` for an ancestor's
entry/bookkeeping field like `runId`/`args`/`cost`). This is the
replacement for what the old full-clone used to make free — it costs an
explicit declaration now, but every genuine cross-boundary read in this
codebase was already using it before the clone was even removed.

Every sub-node along the subflow's own edge path still writes into this
same flat state (`CACHE_CHECK`, `SCORE_LLM`, …), and reading `state.CACHE_CHECK`
after `CACHE_CHECK` dispatches works exactly as it always has — there is no
separate "subflow data" object to read from inside the subflow itself, and
this authoring experience is unchanged by the write-cutover below. What
changed is only the underlying mechanism: the child's own `state_json` no
longer accumulates a key per dispatched sub-node either (same entry-only
model as any top-level flow); each sub-node's own output lives in its own
`flow_node_invocations.result_json` row and is reconstructed on every read
via `assembleState()`, transparently, before your `run`/`selector`/edge
function ever sees `state`. (Downstream, *outside* the subflow, that same
reconstructed state is what `_subflowGroups[groupId][subflowKey].data`
exposes — see `README.md`'s *State conventions* and `ENGINE_INTERNALS.md`'s
SUBFLOW DATA FLOW section.)

## Deriving `subflowKey` (the child's identity)

Each child needs a stable string key, used both as the invocation's
`subflow_key` column and as the key under `_subflowGroups[groupId]` that
downstream nodes read. Resolution order (`executor-subflow-children.js:
deriveSubflowKey`):

1. `node.itemKey` declared → `item[node.itemKey]`
2. else → `item.id ?? item.listingId ?? String(item)`

An item that resolves to an empty/nullish key is skipped (with a logged
warning), not spawned — a malformed item can't silently corrupt another
item's identity.

## Reading a subflow's results downstream

Outside the subflow node, read `state._subflowGroups[groupId][subflowKey]` —
this is unchanged from before childFlows existed, and works identically
whether the group ended up on the new model (real children) or old
(historical `flow_branches`, pre-migration flows only):

```js
// nodes/shortlist.js — downstream of SCORE
const scoredEntries = Object.entries(state._subflowGroups?.SCORE ?? {});
const succeeded = scoredEntries.filter(([, s]) => s && !s.failedAt);
const score = succeeded[0]?.[1]?.data?.SCORE_LLM?.score;   // .data.<subNodeId>
```

A second subflow node's `.selector` can read an earlier subflow's results the
same way — this is how a pipeline chains SCAN → SCORE → PROPOSE, each a
separate subflow over a progressively narrowed item set:

```js
// nodes/propose/index.js — PROPOSE.selector reads SCORE's output
selector: (state) =>
  (state.shortlistedIds ?? [])
    .map((id) => scoreData(state._subflowGroups?.SCORE?.[id]))
    .filter(Boolean),
```

## `flow.display` — identity for iOS/dashboard clients

Optional. `(item) => ({ title, url })` (or any small, pure, I/O-free
projection) called once on a child's first entry and persisted to
`state.parallel.<subflowNodeId>.<subflowKey>._display` — powers the subflow row
label in iOS's Subflows section (RunDetailView/PipelineDetailView) and the
dashboard without those clients needing to know the item's own shape.

## Locks and concurrency

- `crossProcessLock` on a *sub-node* (not the subflow node itself) serializes
  that specific step across every child of the batch, and across every
  other process holding the same lock name — e.g.
  `nodes/score/index.js`'s children each acquire `browser:linkedin` only
  around the one sub-node that actually drives a browser, not for their
  whole lifetime. See `README.md`'s *Lock modifier* for the primitive itself.
- `.concurrency` on the subflow node is a **boolean participation switch**,
  not a numeric width — `true` (the default, including when the field is
  unset) means this subflow's children draw from a single **gateway-wide**
  budget (`childFanoutConcurrency` config, default 8,
  `engine/child-fanout-concurrency.js`) shared by every subflow
  node in every pipeline; `concurrency: false` opts a node out entirely
  (unbounded), for a fan-out cheap enough that throttling it adds latency
  for no protective benefit (e.g. a plain code-only cancellation loop). A
  numeric value (some pipelines still carry old `concurrency: 3`/`5`
  fields, a leftover from an earlier per-node-numeric design this
  collapsed from) is simply truthy — it behaves exactly like `true`, not
  like a per-node cap of that size; the number itself is no longer
  meaningful and should be treated as `true` when touching that pipeline.
  Children beyond the shared budget are created immediately (so
  `getAllSubflowGroups`/dashboards see them right away) but held at
  `status:'queued'` until a slot frees, auto-woken the moment ANY flow
  anywhere goes terminal — no polling, no manual retry needed.
- The process-wide `agentDispatchConcurrency` config (default 8) bounds
  concurrent `kind:"agent"` dispatch across the whole gateway process, root
  and subflow alike — a separate, older mechanism from the child-fanout
  budget above. The two compose: an agent-kind child still has to clear
  both this dispatch semaphore for its own turn AND (unless its subflow
  opted out) the child-fanout budget just to be dispatched as a child at
  all.

## Retrying, resuming, and gating one item

Because a subflow item is a genuine, independent flow, every mechanism that
applies to a normal flow applies to it directly — there is no separate
"subflow retry" API:

- **Resolve its gate**: `openclaw pipeclaw approve <childFlowId> <itemId> approve`
  — same command as any flow, just pointed at the child's own flowId.
- **Retry it after a failure**: the child's own flowId through the normal
  failures/retry flow (`POST /pipeclaw/failures/<id>/retry`) — the same
  code path as a flow-level retry, walking the child's own topology back
  to the failing node. There is no itemId-based subflow retry anymore;
  the old `flow_branches`-era mechanism was removed once every subflow
  became childFlows (`routes/operator-retry.js` returns a clean
  `not_found` if an old failure event still carries a stale `itemId`).
- **Retry a subflow node whose `mode` wasn't satisfied**: this is a
  *different* failure event — recorded against the subflow node's own
  parent flow, `failure_kind: "subflow-mode-not-satisfied"`, since every
  sibling child went terminal but not enough of them succeeded. Retrying
  it does **not** re-run the subflow node itself (it already dispatched
  correctly) — `retryFailureBranch` detects this kind and redirects to
  the real underlying child failure(s) instead, recursing through any
  further nested propagations first. Once the real failure resolves,
  the parent auto-resolves back to `succeeded` on its own — no separate
  action needed on the parent's own failure event.
- **Cancel it**: `executor.cancel(childFlowId)` — cascades correctly; the
  parent's `child-resume-trigger.js` treats a cancelled sibling as
  terminal, same as succeeded/failed/skipped.
- **See its own transcript / raw state**: any dashboard or iOS run-detail
  view works on a childFlowId exactly like a top-level flowId — `data`
  under `_subflowGroups[...][subflowKey].childFlowId` gives you that id
  directly.

## `code` node's `ctx.invokePipeline` inside a subflow

A subflow sub-node can fan out to an *entirely different* pipeline (not a
nested subflow) via `ctx.invokePipeline` — see `README.md`'s *Code-node
`ctx` argument*. Useful for "this subflow item, once approved, should kick off a
long-running second pipeline without blocking this one's own lifecycle."

## Nested subflows

A `flow.nodes` entry can itself be `kind: "subflow"` — the loader validates
it with the exact same rules as a top-level subflow node (still requires its
own `.flow`, still forbids `.groupId`). Each nested child becomes its own
`flow_node_invocations` row keyed by `(nested-child's own flowId, nestedNodeId,
subflowKey)` — the model composes without special-casing. Rare in practice;
most pipelines get the fan-out they need from two sibling top-level subflows
(SCORE then PROPOSE) rather than nesting.
