# Harness matrix

Honest-limits matrix for the Harness port (`harness/Harness.js`): which port
methods each harness actually implements, and where a method is a declared
limitation rather than a working slot.

| Port method         | fake                              | openclaw                          | opencode                          |
| ------------------- | --------------------------------- | --------------------------------- | --------------------------------- |
| `spawn`             | supported (in-memory run)         | supported (delegates to subagent) | unsupported (throws declared error; no run slot — read-only/dry-run mode) |
| `waitForRun`        | supported                         | supported (delegates to subagent) | limited (returns `{ status: "unsupported" }`) |
| `getSessionMessages`| supported                         | supported (delegates to subagent) | limited (returns `{ messages: [] }`) |
| `getSubagent`       | supported (fake runtime)          | supported (gated runtime)         | unsupported (returns `null`; executor falls back to `getWorkingSubagent` / `api.runtime`) |
| `emit`              | supported (records locally)       | supported (forwards to injected emitter) | supported (records locally, forwards when an emitter is injected) |
| `registerTool`      | supported (records locally)       | supported (delegates to api, records locally otherwise) | supported (records locally) |
| `registerHook`      | supported (records locally)       | supported (delegates to api, records locally otherwise) | supported (records locally) |
| `storePaths`        | supported (caller-owned `dataDir`)| supported (`~/.openclaw`, env overrides) | supported (package-owned `~/.manifold` default, env overrides; never `~/.openclaw`) |
| `sessionKey`        | supported (identity triple)       | supported (gateway-native key, fail-loud on missing agent) | supported (gateway-native key via `defaultAgentSessionKey`, fail-loud on missing agent) |

Statuses: **supported** = works; **limited** = returns a declared degraded
value; **unsupported** = throws a declared error or returns `null` so the
caller falls back. No silent no-ops: every limitation is declared at the
call site or in `OPENCODE_SPAWN_UNSUPPORTED`.
