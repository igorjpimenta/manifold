# Cancel doesn't abort in-flight agent sessions — open gap

**Status: not fixed, documented only.** There is no `killActiveSessions`
step in `engine/executor-cancel.js` — cancel cannot abort an in-flight
agent session. This doc records what the gap actually is and what a real fix
would need, so it's discoverable next time cancel behavior comes up rather
than silently forgotten.

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

If a `kind:"agent"` node is mid-dispatch (an actual outbound LLM/Claude call,
via `dispatchAgentEntry` → `dispatchAgentWithRetry` → the injected
`runTurn` in `engine/agent-dispatch.js`/`executor-nodes.js`) when its flow — root or
subflow, doesn't matter — gets cancelled:

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

## Why there's no ready-made list to kill from

There is no "list of currently active session keys" table in the current
model, so nothing can enumerate the sessions a cancel would have to kill.

## What a real fix would need

The raw ingredient does exist: every `kind:"agent"` dispatch writes a row to
`flow_sessions` (`state/session-index.js`, via `sessionIndex.record({flowId,
session, nodeId})` at `agent-dispatch.js`) **before** the LLM call
starts. The session id itself is the harness's: `entry.session ??
entry.sessionBuilder?.(state, flowId)` (`executor-nodes.js`) — a node that
declares neither is refused at dispatch.

What's missing is a **completion signal** — `flow_sessions` is write-once,
answering "did a dispatch start" (`hasSessionForNode()`), not "is it still
running." A real fix would need to:

1. Derive "genuinely in-flight" by joining `flow_sessions` against
   `flow_node_executions` (or equivalent) for the target flow, filtering to
   rows with no completion record yet — not by reading a ready-made table.
2. Feed that derived list into a real `killActiveSessions`-equivalent, still
   using `sessions.abort` via `callGatewayFromCli` (loopback-WS
   self-pairing — `OPENCLAW_GATEWAY_TOKEN` alone doesn't carry the right
   scope for a direct HTTP/WS call).
3. Apply it recursively through `cascadeCancelChildren`, same as the
   terminal-status cascade already does.
4. Decide what to record when an abort races a dispatch that's already
   finishing (is `"no-active-run"` "already done" or "the abort mechanism
   missed it"?) — worth resolving properly rather than leaving it ambiguous.

Not scoped or estimated here — this doc exists to make the gap findable, not
to commit to fixing it on any particular timeline.
