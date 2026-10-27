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
| session   | opaque core handle (`mf-…`, minted by core) ↔ gateway key (`agent:<agent>:subagent:mf-…`, mapped by adapter); `entry.sessionKey` or `sessionKeyBuilder` overrides pass through verbatim | core: turn identity; adapter: wake fallback |

## Session-key conventions

Core speaks only its own namespace: `dispatchAgentEntry` mints an opaque
handle `mf-<flow8>-<node>[-<item>]` (`mintAgentSessionKey`); a node may
override via `entry.sessionKey` or `entry.sessionKeyBuilder(state, flowId)`,
honored verbatim. `entry.agent` is required — anonymous dispatch is refused
loudly. No env fallback, ever.

The adapter maps at the edge (`session-keys.js`, wrapping
`runtime.subagent` once where it is injected): outgoing `run`/
`waitForRun`/`getSessionMessages` args translate opaque→gateway
(`agent:<agent>:subagent:<opaque>`); incoming runIds/messages translate
back. runId mappings live in memory for the dispatch lifecycle only —
revival re-attaches by session key, never by runId. The session index
stores gateway keys (adapter-facing state, written through conversion);
non-`mf-` keys pass through verbatim in both directions.

## Pipeline directories

Resolution order: `pipeline_dirs` table[harness] → `MANIFOLD_PIPELINES_DIR`
env → config `pipelinesDir` → `<dataDir>/workspace/.pipelines` default. No
new plugin config keys (env + table only, like push).

- `openclaw manifold pipelines register --harness <h> --path <dir>`
  upserts the override (dir must exist); effective on next gateway boot.
- `openclaw manifold pipelines list [--harness <h>]` shows registered
  rows plus the currently effective dir and which tier won.

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

## MCP transport (per harness)

The pipeline tools (`manifold_start`, `manifold_status`, `manifold_runs`,
`manifold_approve`, `manifold_cancel`) ship over MCP stdio and Streamable
HTTP (`bin/manifold.js` → `mcp/stdio.js`, `mcp/http.js`). Identity mirrors
the push contract: the **harness** is declared at construction
(`MANIFOLD_HARNESS` env for stdio, `X-Manifold-Harness` header for HTTP),
**agent**+**session** arrive per call (`_meta.manifold`, or stamped
`manifold_agent`/`manifold_session` args). Unstamped calls are refused;
nothing defaults, ever.

### openclaw

In `~/.openclaw/openclaw.json`, alongside the plugin id:

```json
"mcp": {
  "servers": {
    "manifold": {
      "command": "manifold",
      "args": ["serve", "--mcp", "stdio"],
      "env": { "MANIFOLD_HARNESS": "openclaw" }
    }
  }
}
```

Calls arrive as `mcp__manifold__manifold_start` etc. A companion stamps
`_meta.manifold` (agent+session) per call the way the push adapter stamps
its wake — unstamped calls fail at the boundary, never as poison
downstream. Remote daemon instead: `manifold serve --mcp http`
(`MANIFOLD_BEARER` required) behind TLS, same tools, same stamps.

### generic clients

No plugin: declare the harness and stamp agent/session per call yourself:

```
MANIFOLD_HARNESS=generic manifold serve --mcp stdio
```

```
claude mcp add manifold --env MANIFOLD_HARNESS=generic -- manifold serve --mcp stdio
```

### Ports (all loopback by default; no two defaults collide)

| surface | default | override |
| --- | --- | --- |
| dashboard | `127.0.0.1:7430` | `dashboardPort` plugin config |
| MCP streamable HTTP | `127.0.0.1:38293` | `manifold serve --mcp http --port N` |
| WS event stream | `127.0.0.1:38294` | `manifold serve --ws --port N` |

## WS event stream

`manifold serve --ws [--port N]` (default 38294) broadcasts the in-process
push stream verbatim over WebSocket: `{"type":"welcome"}`, then
`{"type":"history","events":[...]}` (replay, oldest-first, capped), then
one `{"type":"event","event":{...}}` per entry. Track your own seen state —
reconnecting replays history, so a missed frame costs a re-read, never a
lost event. No auth in-process: bind localhost or put a bearer-checked
reverse proxy in front. The dashboard SSE path is untouched; this is the
harness-subscriber door.

## Push webhook (wake on dispatch)

Polling flow status leaves an idle agent waiting for its next heartbeat.
Instead, core POSTs the handoff the moment a turn is dispatched or a gate
opens, and the adapter opens a **fresh session** for the receiver:

1. On the adapter: set `webhookBearer` in the plugin config. The route
   `POST /api/manifold/pushed` stays disarmed (503) until set.
2. On core: register the callback URL (persistent, checked first at push
   time — autoregister's upsert, or `pushUrls[harness]` / single `pushUrl`
   (+ `pushBearer`) in config). Nothing registered anywhere = no-op;
   dispatches behave exactly as before. Manual equivalent (what autoregister
   does for you, and what to run when it warns you):
   `manifold webhook set --harness openclaw --url
   http://<gateway-host>:<port>/api/manifold/pushed --bearer <same-as-webhookBearer>`
   then `manifold webhook test --harness openclaw` (core POSTs
   `{event:"webhook.test"}`; the adapter answers `{ok:true,test:true}`
   without waking anyone).
3. On dispatch/gate-open, core POSTs `{event, at, flow, session}` (see
   `events/push.js`); the adapter verifies the bearer, then runs a fresh
   turn in session `manifold-push-<flowId>-<nodeId>` carrying the handoff.
   If the fresh turn fails, it falls back to the originating sessionKey; if
   both fail it logs loudly (the run itself is unaffected). Delivery never
   fails the run (bounded 3s timeout, failures are warnings).
   There is no pending queue or heartbeat drain on this path (winch parks a
   missed claim for 4h; manifold has no equivalent — a dispatched turn or an
   open gate needs its receiver now). When the fresh session AND the
   originating-session fallback both fail, the adapter logs loudly
   (`manifold: push wake … FAILED`) and the HTTP answer reports
   `{woken:false}` — that log line is the page, not a queue.

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

## generic clients

No adapter: register a push URL for your harness name and verify the bearer
(or none) yourself. Core POSTs the same `{event, at, flow, session}`
payloads; wake semantics are yours.
