# @igorjpimenta/manifold-openclaw

The openclaw plugin for [manifold](https://github.com/igorjpimenta/manifold).

The pipeline schema, node kinds, tool surface, CLI, dashboard, store model and
authoring rules are documented in the [root README](../../../README.md). This
file covers only what is specific to the plugin package.

## What the plugin does

- **Discuss surface** — the operator discuss routes and the WS dialog door
  (message transport into open discuss sessions; bearer-guarded).
- **Boot services** — pipeline loader and watcher, flow revival, gateway
  readiness capture, and the PROFILE.md read/write guards.

The pipeline tools are served over MCP by the core package (`mcp/stdio.js`,
`mcp/http.js`); the plugin registers no raw tools of its own.

## Config

The plugin's config lives under `plugins.entries.manifold.config` in
`~/.openclaw/openclaw.json` — flat keys on the plugin entry fail openclaw's
validator. Keys: `pipelinesDir`, `ownerSessionKey`, `dashboardEnabled`,
`dashboardPort`, `dashboardHost`, `discuss.defaultAgent`, `globalConcurrency`,
`profilePath`, `profileEditResultsDir`. Defaults and meanings live in
`openclaw.plugin.json`.

## Install

Managed installs of a release tag:

```sh
openclaw plugins install git:igorjpimenta/manifold@<version> --force --dangerously-force-unsafe-install
```

## Release line

Ships on its own line: `manifold-openclaw-<semver>` tags publish
`@igorjpimenta/manifold-openclaw`. Bump this package's `version` and its
`@igorjpimenta/manifold` dependency floor together.
