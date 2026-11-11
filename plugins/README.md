# manifold companion plugins — the identity contract

Manifold core keys every dispatch to three dimensions, and only one of
them is ambient: the **harness**, declared per call (`MANIFOLD_HARNESS`
env for stdio, `X-Manifold-Harness` header for HTTP). The **agent**
and **session** of each turn arrive with the turn itself. Unstamped
agent/session fail the dispatch; nothing defaults, ever. The harness
feeds manifold — core never calls out: no push targets, no wake
callbacks, no receiver sessions.

| dimension | source | who reads |
| --------- | ------ | --------- |
| harness   | per-call declaration (`MANIFOLD_HARNESS` / `X-Manifold-Harness`; `openclaw` default for pipelines-dir resolution) | core: logs, pipelines-dir resolution |
| agent     | pipeline node `entry.agent` (**required** — anonymous dispatch is refused loudly) | core: session attribution |
| session   | harness-owned throughout: dispatch sessions declared by the node (`entry.session` / `entry.sessionBuilder`), discuss ids minted by the harness adapter (`sessions.newDiscussSessionId`) — both pass through verbatim | core: turn identity |

## Session conventions

Core does not mint sessions: the harness owns the format, so an agent
node declares `entry.session` (static) or
`entry.sessionBuilder(state, flowId)` (derived) and `dispatchAgentEntry`
passes it through verbatim. A node that declares neither is refused
loudly — tool-handoff and the lineage ledger both key off this value.
`entry.agent` is required too; no env fallback, ever.

The adapter maps core's `session` to the harness `sessionKey` field on the
way out (`session-keys.js`, wrapping `runtime.subagent` once where it is
injected): only runIds translate symmetrically (core `mf-run-…` ↔ gateway
runId, in memory for the dispatch lifecycle); a harness `sessionKey` echo
maps back to core `session`. Revival re-attaches by session, never by
runId. The session index stores the same plain-text ids the dispatch
used, so any harness can read them without knowing a gateway namespace.

## Pipeline directories

Resolution order: `pipeline_dirs` table[harness] → `MANIFOLD_PIPELINES_DIR`
env → config `pipelinesDir` → `<dataDir>/workspace/.pipelines` default. No
new plugin config keys (env + table only).

- `openclaw manifold pipelines register --harness <h> --path <dir>`
  upserts the override (dir must exist); effective on next gateway boot.
- `openclaw manifold pipelines list [--harness <h>]` shows registered
  rows plus the currently effective dir and which tier won.

## openclaw

1. Install the release tag — the repo root is the plugin surface (root
   `openclaw.plugin.json` plus the root `package.json` `openclaw.extensions`
   entry, which points at the adapter under `plugins/openclaw/manifold/`):
   `openclaw plugins install git:igorjpimenta/manifold@<version> --force
   --accept-capabilities`. No push setup exists: the harness
   feeds manifold through the registered tools and routes, never the
   reverse. Operator routes (`/manifold/*`) require
   `Authorization: Bearer <MANIFOLD_BEARER>` (gateway env, winch-style).
   Multi-gateway setups: the pipelines-dir table resolves per harness
   (`manifold pipelines register --harness`).

## MCP transport (per harness)

The pipeline tools (`manifold_start`, `manifold_status`, `manifold_runs`,
`manifold_approve`, `manifold_cancel`, `manifold_deliver_result`) ship over
MCP stdio and Streamable HTTP (`bin/manifold.js` → `mcp/stdio.js`,
`mcp/http.js`). Identity is declared per call: the **harness** is declared
at construction (`MANIFOLD_HARNESS` env for stdio, `X-Manifold-Harness`
header for HTTP), **agent**+**session** arrive per call (`_meta.manifold`,
or stamped `manifold_agent`/`manifold_session` args). Unstamped calls are
refused; nothing defaults, ever.

Structured results travel the same way: an agent calls
`manifold_deliver_result` with a JSON `payload` (+ optional `kind`/`runId`)
and the result is stored under the caller's stamped session key, where the
waiting dispatch picks it up instead of waiting out the agent run. See
`state/tool-handoff.js` for the rendezvous contract.

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
`_meta.manifold` (agent+session) per call — unstamped calls fail at the
boundary, never as poison downstream. Remote daemon instead:
`manifold serve --mcp http` (`MANIFOLD_BEARER` required) behind TLS,
same tools, same stamps.

### generic clients

No plugin: declare the harness and stamp agent/session per call yourself:

```
MANIFOLD_HARNESS=generic manifold serve --mcp stdio
```

```
claude mcp add manifold --env MANIFOLD_HARNESS=generic -- manifold serve --mcp stdio
```

## WS event stream

`manifold serve --ws [--port N]` (default 38294) broadcasts the in-process
event stream verbatim over WebSocket: `{"type":"welcome"}`, then
`{"type":"history","events":[...]}` (replay, oldest-first, capped), then
one `{"type":"event","event":{...}}` per entry. Track your own seen state —
reconnecting replays history, so a missed frame costs a re-read, never a
lost event. No auth in-process: bind localhost or put a bearer-checked
reverse proxy in front. This is the harness-subscriber door; the dashboard
has its own live WS feed on the dashboard server.

## generic clients

No adapter: declare the harness and stamp agent/session per call yourself —
there is no push URL to register and no wake to implement. Core emits
run/gate transitions on the WS event door above; consume them, don't wait
to be called.
