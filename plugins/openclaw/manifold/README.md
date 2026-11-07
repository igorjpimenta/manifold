# @igorjpimenta/manifold-openclaw

OpenClaw companion for [manifold](https://github.com/igorjpimenta/manifold). Three jobs:

1. **MCP surface** — manifold's pipeline tools (`manifold_start`, `_status`,
   `_runs`, `_approve`, `_cancel`) ship as an MCP server
   (`mcp/stdio.js`, `mcp/http.js`). The plugin registers no raw tools of its
   own; the host connects the MCP server. Pipeline-specific tools live in
   oc-config.
2. **Discuss surface** — serves the operator discuss routes and the WS dialog
   door (message transport into open discuss sessions; bearer-guarded).
3. **Boot services** — pipeline loader + watcher, flow revival, gateway
   readiness capture, and the PROFILE.md read/write guards.

## Install

```sh
openclaw plugins install @igorjpimenta/manifold-openclaw --accept-capabilities
```

The manifest declares `activation.onStartup`, so the plugin loads at gateway
boot (without it the host never calls `register()` and no tool or route
binds).

The plugin depends on `@igorjpimenta/manifold`, so npm resolves and pins the
core runtime version alongside it.

## Config

| key | default | meaning |
|---|---|---|
| `pipelinesDir` | `~/.openclaw/workspace/.pipelines` | directory to scan for pipeline definitions |
| `ownerSessionKey` | `agent:main:explicit:manifold-owner` | session key scoping flow-run ownership |
| `dashboardEnabled` | `true` | boot the read-only HTTP dashboard service |
| `dashboardPort` / `dashboardHost` | `7430` / `127.0.0.1` | dashboard bind |
| `discuss.defaultAgent` | — | default agent for gate discuss sessions |
| `globalConcurrency` | `8` | shared ceiling for agent dispatch, subflow fan-out, and flow-run concurrency |

Requires OpenClaw ≥ 2026.5.6 (declared as `openclaw.compat.openclawMin`).

## Releases

Ships on its own line, independent of the core package:
`manifold-openclaw-<semver>` tags publish `@igorjpimenta/manifold-openclaw`.
Bump this package's `version` and its `@igorjpimenta/manifold` dependency
floor together.
