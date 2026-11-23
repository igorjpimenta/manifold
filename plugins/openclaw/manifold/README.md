# @igorjpimenta/manifold-openclaw

OpenClaw companion for [manifold](https://github.com/igorjpimenta/manifold). Three jobs:

1. **MCP surface** — manifold's pipeline tools (`manifold_start`,
   `manifold_status`, `manifold_runs`, `manifold_approve`,
   `manifold_cancel`, `manifold_deliver_result`) ship as an MCP server
   (`mcp/stdio.js`, `mcp/http.js`). The plugin registers no raw tools of its
   own; the host connects the MCP server. Pipeline-specific tools live in
   companion packages.
2. **Turn bridge** — routes agent and discuss turns into the engine
   (`/api/manifold/turn`, `/api/manifold/_elev/turn`, `/api/manifold/session`);
   the WS dialog door itself is served by the engine (`:44302`).
3. **Spawn and supervise** — starts `manifold serve` as a child (the engine
   runs the loader, watcher, revival, HTTP API and dashboard inside that
   process), restarts it on crash, and registers gateway readiness.

## Install

```sh
openclaw plugins install npm:@igorjpimenta/manifold-openclaw@<version> --force --accept-capabilities
```

The manifest declares `activation.onStartup`, so the plugin loads at gateway
boot (without it the host never calls `register()` and no tool or route
binds).

The plugin depends on `@igorjpimenta/manifold`, so npm resolves and pins the
core runtime version alongside it.

## Config

| key | default | meaning |
|---|---|---|
| `pipelinesDir` | `~/.manifold/workspace/.pipelines` | directory to scan for pipeline definitions |
| `ownerSessionKey` | `agent:main:explicit:manifold-owner` | session key scoping flow-run ownership |
| `dashboardEnabled` | `true` | boot the dashboard with the engine |
| `dashboardPort` / `dashboardHost` | `44300` / `127.0.0.1` | dashboard bind |
| `apiPort` | `44301` | engine API port the adapter spawns against |
| `discuss.defaultAgent` | — | default agent for gate discuss sessions |
| `globalConcurrency` | `8` | shared ceiling for agent dispatch, subflow fan-out, and flow-run concurrency |

Requires OpenClaw ≥ 2026.5.6 (declared as `openclaw.compat.openclawMin`).

## Releases

Ships on its own line, independent of the core package:
`manifold-openclaw-<semver>` tags publish `@igorjpimenta/manifold-openclaw`.
Bump this package's `version` and its `@igorjpimenta/manifold` dependency
floor together.
