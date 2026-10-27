# manifold companion plugins — the identity contract

Manifold core keys every dispatch to three dimensions, and only one of
them is declared: the **harness**, named per push target. The **agent**
and **session** of each turn arrive with the turn itself — the calling
harness's companion wakes them from the push payload. Unstamped
agent/session fail the dispatch; nothing defaults, ever.

| dimension | source | who reads |
| --------- | ------ | --------- |
| harness   | push target registration (`webhookHarness`, default `openclaw`) | core: logs, push routing |
| agent     | pipeline node `entry.agent` (**required** — anonymous dispatch is refused loudly) | core: session attribution, push receiver |
| session   | gateway-native keys (`agent:<agent>:…`, minted by manifold; `entry.sessionKey` or `sessionKeyBuilder`) | core: turn identity; adapter: wake fallback |

## Session-key conventions

Keys are gateway-native: `agent:<agent>:<scope>:<name>`. Manifold mints
them (`defaultAgentSessionKey`); a node may override via `entry.sessionKey`
or `entry.sessionKeyBuilder(state, flowId)`. `entry.agent` is required —
`dispatchAgentEntry` throws on a missing agent rather than dispatching
anonymously. No env fallback, ever.

## openclaw

1. Install the adapter package (its own release line, pulls
   `@igorjpimenta/manifold` as a dependency): `openclaw plugins install
   @igorjpimenta/manifold-openclaw`. Then set `webhookBearer` (any secret)
   in the plugin config — `webhookPublicBaseUrl` (e.g.
   `http://127.0.0.1:18789`) if the gateway port isn't detectable. On boot
   the adapter upserts its own route as this harness's push target and
   verifies it with a test ping — no manual step unless it warns you with
   the exact config snippet. Opt out with `webhookAutoregister: false`.
   Multi-gateway setups: set `webhookHarness` per gateway.
2. Manual registration (what autoregister does for you): set
   `plugins.entries.manifold.pushUrls = { "<harness>": { "url":
   "http://<gateway-host>:<port>/api/manifold/pushed", "bearer":
   "<same-as-webhookBearer>" } }`.

## Push webhook (wake on dispatch)

Polling flow status leaves an idle agent waiting for its next heartbeat.
Instead, core POSTs the handoff the moment a turn is dispatched or a gate
opens, and the adapter opens a **fresh session** for the receiver:

1. On the adapter: set `webhookBearer` in the plugin config. The route
   `POST /api/manifold/pushed` stays disarmed (503) until set.
2. On core: register the callback URL (persistent, checked first at push
   time — autoregister's upsert, or `pushUrls[harness]` / single `pushUrl`
   (+ `pushBearer`) in config). Nothing registered anywhere = no-op;
   dispatches behave exactly as before.
3. On dispatch/gate-open, core POSTs `{event, at, flow, session}` (see
   `events/push.js`); the adapter verifies the bearer, then runs a fresh
   turn in session `manifold-push-<flowId>-<nodeId>` carrying the handoff.
   If the fresh turn fails, it falls back to the originating sessionKey; if
   both fail it logs loudly (the run itself is unaffected). Delivery never
   fails the run (bounded 3s timeout, failures are warnings).

### Event payloads

| event | when | wake |
| ----- | ---- | ---- |
| `agent.dispatched` | agent turn spawned (`session` names the receiver) | fresh session, fallback to originating `sessionKey` |
| `gate.opened` | human-gate record written | fresh session when a receiver agent is known, else report-only (`{woken:false}`) |
| `webhook.test` | autoregister's test ping | none (answers `{ok:true,test:true}`) |

Payload shape: `{event, at, flow: {id, pipelineId, nodeId} | null,
session: {harness, agent, sessionKey}}`. `agent.dispatched` requires
`session.agent` (400 otherwise); `gate.opened` without an agent is
report-only (200, `woken:false`).

## opencode

Why a no-op: opencode's `tool.execute.before` can only mutate the outgoing
call's `args`, but the MCP path builds `callTool({ name, arguments: args })`
— params carry only name + arguments, there is no `params._meta` slot, and
every MCP input schema is forced `additionalProperties: false`, so identity
cannot travel per call. `plugins/opencode/manifold-meta.js` is a registered
no-op documenting the constraint in place. Manifold mints session keys
itself, so the only registration an opencode setup needs is the push URL
core POSTs to: `pushUrls = { "opencode": { "url": "…" } }` (plus `bearer`
when the receiver checks one).

## generic clients

No adapter: register a push URL for your harness name and verify the bearer
(or none) yourself. Core POSTs the same `{event, at, flow, session}`
payloads; wake semantics are yours.
