# Cancel doesn't abort in-flight agent sessions — open gap

**Status: not fixed, documented only (2026-08-12).** `killActiveSessions` — the
scaffolding that used to attempt this, in `engine/executor-cancel.js` — was
deleted the same day as dead code, since it had been a guaranteed no-op
since `branchService`/`flow_branches` were removed. This doc records what
the gap actually is and what a real fix would need, so it's discoverable
next time cancel behavior comes up rather than silently forgotten.

## What cancel actually does today

`engine/executor-cancel.js`'s `completeCancellation()`:

1. Recursively cancels every non-terminal `childFlows` subflow child
   (`cascadeCancelChildren` — walks `nodeExecutionsStore.listSubflowNodeIdsForFlow`
   → `listInvocationIdsForNode` → `runStore.listForParentInvocations`, calling
   itself on each non-terminal child, so a grandchild under a child's own
   subflow is reached too).
2. Flips `flow_runs.status` to `'cancelled'` via `store.cancel()`.

That's it. Neither step touches whatever the flow's *currently executing*
node is actually doing.

## The gap

> Pull-first note: the dispatch chain named here (`dispatchAgentEntry` →
> `dispatchAgentWithRetry` → `runSubagentTurn` in the deleted
> `engine/executor-subagent.js`) is gone with push dispatch. The analogous
> pull-world case is a claimed-but-unreported ticket at cancel time: the
> claimant keeps working until its lease lapses, then the sweeper requeues
> the ticket — but the flow is already terminal, so the work lands
> nowhere. Same shape of gap (in-flight work outlives the cancel), no
> session to kill either way.

If a `kind:"agent"` node is mid-turn (an actual outbound LLM/Claude call
placed by the claiming harness, outside core's reach) when its flow —
root or subflow, doesn't matter — gets cancelled:

- The LLM call is never interrupted. It keeps running to natural completion,
  unaware anything happened.
- When it finishes and the executor tries to persist the result
  (`persistRunningRetrying` → `store.setRunning` → `runStore.casSetRunning`),
  the CAS check (`WHERE flowId=? AND revision=? AND status NOT IN (terminal)`)
  fails because the flow is already `cancelled` — `_casClassify` returns
  `{applied:false, code:"terminal"}`. `persistRunningRetrying` only retries
  on `revision_conflict`, so this returns immediately.
- **No corruption, no un-cancelling, nothing crashes** — but also **no
  record of what happened**. The agent's actual output is discarded, never
  merged into `stateJson`. If the agent's skill had a real side effect mid-run
  (an actual outbound message send, a file write, an API call with
  consequences outside this DB), that side effect already happened and is now
  completely unlinked from any record that the flow was cancelled.
- The only trace left behind: two `logger.warn` lines, plus (depending on
  which persist path handles it) a `flow_node_executions` row that can look
  like a clean completion for a flow whose parent record says `cancelled`.

This applies identically, recursively, to every level of `childFlows`
subflow — `cascadeCancelChildren` re-invokes the exact same
`completeCancellation` on each child, so a child's own in-flight agent
dispatch has the exact same unaddressed gap.

## Why the old mechanism can't just be revived

The deleted `killActiveSessions` step sourced its session-key list
(`activeBranches`) from `branchService`/`flow_branches` — the pre-`childFlows`
subflow model, fully removed in an earlier refactor. There is no equivalent
"list of currently active session keys" table in the current model to
trivially restore that function against.

## What a real fix would need

The raw ingredient does exist: every `kind:"agent"` dispatch writes a row to
`flow_sessions` (`state/session-index.js`, via `sessionIndex.record({flowId,
sessionKey, nodeId})` at `executor-subagent.js:89`) **before** the LLM call
starts. `sessionKey` itself is derived from `entry.sessionKey ??
entry.sessionKeyBuilder?.(state, flowId) ?? defaultAgentSessionKey(agent,
flowId, nodeId)` (`executor-nodes.js`/`executor-helpers.js`).

What's missing is a **completion signal** — `flow_sessions` is write-once,
answering "did a dispatch start" (`hasSessionForNode()`), not "is it still
running." A real fix would need to:

1. Derive "genuinely in-flight" by joining `flow_sessions` against
   `flow_node_executions` (or equivalent) for the target flow, filtering to
   rows with no completion record yet — not by reading a ready-made table.
2. Feed that derived list into a real `killActiveSessions`-equivalent, still
   using `sessions.abort` via `callGatewayFromCli` (the loopback-WS
   self-pairing pattern the old code already worked out — still correct,
   `OPENCLAW_GATEWAY_TOKEN` alone doesn't carry the right scope for a direct
   HTTP/WS call).
3. Apply it recursively through `cascadeCancelChildren`, same as the
   terminal-status cascade already does.
4. Decide what to record when an abort races a dispatch that's already
   finishing (the old code's own unresolved ambiguity: is `"no-active-run"`
   "already done" or "the abort mechanism missed it"?) — worth resolving
   properly this time rather than carrying the same ambiguity forward.

Not scoped or estimated here — this doc exists to make the gap findable, not
to commit to fixing it on any particular timeline.
