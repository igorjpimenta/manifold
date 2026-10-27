# manifold companion plugins — the identity contract

Manifold core keys every claim to three dimensions, and only one of them
is declared: the **harness**, named per push target. The **agent** and
**session** of each turn arrive with the turn itself — the calling
harness's companion wakes them from the push payload or its own poll
loop. Unstamped agent/session fail the claim; nothing defaults, ever.

| dimension | source | who reads |
| --------- | ------ | --------- |
| harness   | push target registration (`webhookHarness`, default `openclaw`) | core: logs, push routing |
| agent     | pipeline node `entry.agent` (**required** — anonymous dispatch is refused loudly) | core: session attribution, push receiver |
| session   | opaque core handle (`mf-…`, minted by core) ↔ gateway key (`agent:<agent>:subagent:mf-…`, mapped by adapter); `entry.sessionKey` or `sessionKeyBuilder` overrides pass through verbatim | core: turn identity; adapter: wake fallback |

## Claim flow (pull-first agent turns)

Agent turns run pull-only: **park → push `agent.ready` → claim via MCP →
execute → report**. No core path places a subagent call for a pipeline
node; the harness wakes its own receiver and the receiver pulls the work.

1. **Park.** An agent node parks its invocation as a `ready` ticket and
   blocks with `waitJson: { kind: "agent-claim", ticket, nodeId }`,
   emitting `agent.claim.parked` on the event bus. Core builds the prompt
   at park (`node.promptBuilder` over the assembled state) and stores it
   with the ticket; the claim response carries that prompt to the claimant.
2. **Push `agent.ready`.** A harness that learns of the ticket (WS event
   stream, `manifold_claim` polling) POSTs `{event:"agent.ready", flow,
   session, claim}` to its webhook to wake a receiver. `claim.ticket` is
   required; `claim.prompt` may carry the work inline.
3. **Claim via MCP.** The receiver calls `manifold_claim`, binding the
   oldest `ready` ticket to its `(harness, agent, session)` triple under
   a lease (default 5 min). Empty hands means someone else holds
   everything — stop, do not execute.
4. **Execute**, then **report** via `manifold_report` with the ticket and
   the result object. Core runs `node.apply(reportedOutput, ctx)` on
   resume, so pipeline-author logic shapes the state update.

Binding rules: the triple is stamped from request identity, never from a
tool param. A heartbeat (`manifold_heartbeat`) or report from any other
triple is refused loudly. A lease that lapses un-renewed returns the
ticket to `ready` via the periodic `sweepExpiredClaims` tick — a crashed
claimant costs a re-queue, never a stuck flow. See the root `README.md`'s
*Claim flow* section for the core-side half of this contract.

## Session-key conventions

The `mf-<flow8>-<node>[-<item>]` opaque handle is the legacy push key
shape: nothing mints it anymore, but core still recognizes it
(`isManifoldOwnedSessionKey`) so boot-time task cleanup can tell
manifold-owned sessions apart. `entry.agent` is required — an anonymous
claim is refused loudly. No env fallback, ever.

Pull-world identity is the ticket plus the claimant triple above, not a
session key. A woken receiver works in a fresh `manifold-claim-<ticket>`
session (falling back to the originating session only if the fresh turn
fails); the session index keeps mapping legacy gateway keys for
attribution, and non-`mf-` keys pass through verbatim.

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

## Push webhook (wake on park / gate-open)

Polling flow status leaves an idle agent waiting for its next heartbeat.
Instead, a push wakes the receiver the moment its work is ready, and the
adapter opens a **fresh session** for it:

1. On the adapter: set `webhookBearer` in the plugin config. The route
   `POST /api/manifold/pushed` stays disarmed (503) until set.
2. On core: register the callback URL (persistent, checked first at push
   time — autoregister's upsert, or `pushUrls[harness]` / single `pushUrl`
   (+ `pushBearer`) in config). Nothing registered anywhere = no-op;
   parking behaves exactly as before. Manual equivalent (what autoregister
   does for you, and what to run when it warns you):
   `manifold webhook set --harness openclaw --url
   http://<gateway-host>:<port>/api/manifold/pushed --bearer <same-as-webhookBearer>`
   then `manifold webhook test --harness openclaw` (core POSTs
   `{event:"webhook.test"}`; the adapter answers `{ok:true,test:true}`
   without waking anyone).
3. On gate-open, core POSTs `{event, at, flow, session}` (see
   `events/push.js`); on a parked claim, the harness POSTs
   `{event:"agent.ready", flow, session, claim}` when the ticket is ready
   for pickup. The adapter verifies the bearer, then runs a fresh
   turn — `manifold-claim-<ticket>` for `agent.ready` (carrying CLAIM
   instructions: claim via `manifold_claim`, execute, report via
   `manifold_report`), `manifold-push-<flowId>-<nodeId>` for gates.
   If the fresh turn fails, it falls back to the originating sessionKey; if
   both fail it logs loudly (the run itself is unaffected). Delivery never
   fails the run (bounded 3s timeout, failures are warnings).
   There is no pending queue or heartbeat drain on this path — a ready
   ticket or an open gate needs its receiver now. When the fresh session
   AND the originating-session fallback both fail, the adapter logs loudly
   (`manifold: push wake … FAILED`) and the HTTP answer reports
   `{woken:false}` — that log line is the page, not a queue.

### Event payloads

| event | when | wake |
| ----- | ---- | ---- |
| `agent.ready` | parked claim ticket ready for pickup (`claim.ticket` names it) | fresh `manifold-claim-<ticket>` session carrying CLAIM instructions |
| `gate.opened` | human-gate record written | fresh session when a receiver agent is known, else report-only (`{woken:false}`) |
| `webhook.test` | autoregister's test ping | none (answers `{ok:true,test:true}`) |

Payload shape: `{event, at, flow: {id, pipelineId, nodeId} | null,
session: {harness, agent, sessionKey}}`, plus `claim: {ticket,
prompt?}` on `agent.ready`. `agent.ready` requires `session.agent` and
`claim.ticket` (400 otherwise); `gate.opened` without an agent is
report-only (200, `woken:false`).

## generic clients

No adapter: register a push URL for your harness name and verify the bearer
(or none) yourself. Core POSTs the same `{event, at, flow, session}`
payloads; wake semantics are yours.
