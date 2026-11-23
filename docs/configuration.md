# Configuration

All knobs live in `openclaw.json` under `plugins.entries.manifold.config` — never flat on the entry (see the nesting rule in [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them)). Nothing here is required; every key has the default shown.

## Plugin config (default values)

```jsonc
"plugins.entries.manifold": {
  "enabled": true,
  "config": {
    "pipelinesDir":      "~/.openclaw/workspace/.pipelines",   // discovery root
    "ownerSessionKey":   "agent:main:explicit:manifold-owner",
    "dashboardEnabled":  true,
    "dashboardPort":     44300,
    "dashboardHost":     "127.0.0.1"
  }
}
```

Schema enforced in `openclaw.plugin.json: configSchema`.
