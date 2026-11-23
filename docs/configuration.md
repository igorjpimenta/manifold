# Configuration

Nothing is required: every knob has a default. Two layers — the engine's own
environment (harness-neutral) and the harness adapter's plugin config
(host-side).

## Engine environment

| knob | env | default |
| --- | --- | --- |
| API bind | `MANIFOLD_API_HOST` / `MANIFOLD_API_PORT` | `127.0.0.1` / 44301 |
| Operator bearer | `MANIFOLD_BEARER` | required — API routes and the dialog door fail closed without it |
| Dashboard bind | `MANIFOLD_DASHBOARD_HOST` / `MANIFOLD_DASHBOARD_PORT` | `127.0.0.1` / 44300 |
| Event WS | `manifold serve --ws --port N` | 44303 |
| Dialog door (discuss) | `MANIFOLD_DIALOG_PORT` | 44302 |
| Data dir (store home) | `MANIFOLD_DATA_DIR` | `~/.manifold` |
| Pipelines dir | `MANIFOLD_PIPELINES_DIR` | `<dataDir>/workspace/.pipelines` |
| Discuss default agent | `MANIFOLD_DISCUSS_DEFAULT_AGENT` | unset |
| Concurrency ceiling | `MANIFOLD_GLOBAL_CONCURRENCY` | engine default (adapter config overrides it, below) |

## Adapter plugin config (host-side)

The adapter exposes knobs as host plugin config. On OpenClaw they nest under
`plugins.entries.manifold.config` in `openclaw.json` — never flat on the
entry (see the nesting rule in [Pipelines → Conventions](pipelines.md#conventions-and-what-breaks-if-you-violate-them)).
Schema: `plugins/openclaw/manifold/openclaw.plugin.json`.

| key | default | meaning |
| --- | --- | --- |
| `pipelinesDir` | `<dataDir>/workspace/.pipelines` | pipeline discovery root (env and the pipelines-dir table rank above it — see [plugins/README.md](../plugins/README.md)) |
| `ownerSessionKey` | `agent:main:explicit:manifold-owner` | session key scoping flow-run ownership |
| `dashboardEnabled` | `true` | boot the dashboard with the engine |
| `dashboardPort` / `dashboardHost` | `44300` / `127.0.0.1` | dashboard bind |
| `apiPort` | `44301` | engine API port the adapter spawns against |
| `discuss.defaultAgent` | — | default agent for gate discuss sessions when the gate node declares none |
| `globalConcurrency` | `8` | shared ceiling for agent dispatch, subflow fan-out, and flow-run concurrency (passed to the engine as `MANIFOLD_GLOBAL_CONCURRENCY`) |

The engine env vars above always work standalone; adapter config is the
OpenClaw-shaped way to set the same things when the adapter owns the spawn.
