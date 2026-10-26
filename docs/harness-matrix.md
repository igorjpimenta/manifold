# Harness matrix

Honest-limits matrix for the Harness port (`harness/Harness.js`): which port
methods each harness actually implements, and where a method is a declared
limitation rather than a working slot. Tests prove the port with inline
stubs at the call site — there is no fake harness, same as winch.

| Port method         | openclaw                          | opencode                          |
| ------------------- | --------------------------------- | --------------------------------- |
| `spawn`             | supported (delegates to subagent) | unsupported (throws declared error; no run slot — read-only/dry-run mode) |
| `waitForRun`        | supported (delegates to subagent) | limited (returns `{ status: "unsupported" }`) |
| `getSessionMessages`| supported (delegates to subagent) | limited (returns `{ messages: [] }`) |
| `getSubagent`       | supported (gated runtime)         | unsupported (returns `null`; executor falls back to `getWorkingSubagent` / `api.runtime`) |
| `emit`              | supported (forwards to injected emitter) | supported (records locally, forwards when an emitter is injected) |
| `registerTool`      | supported (delegates to api, records locally otherwise) | supported (records locally) |
| `registerHook`      | supported (delegates to api, records locally otherwise) | supported (records locally) |
| `storePaths`        | supported (`~/.openclaw`, env overrides) | supported (package-owned `~/.manifold` default, env overrides; never `~/.openclaw`) |
| `sessionKey`        | supported (gateway-native key, fail-loud on missing agent) | supported (gateway-native key via `defaultAgentSessionKey`, fail-loud on missing agent) |

Statuses: **supported** = works; **limited** = returns a declared degraded
value; **unsupported** = throws a declared error or returns `null` so the
caller falls back. No silent no-ops: every limitation is declared at the
call site or in `OPENCODE_SPAWN_UNSUPPORTED`.
