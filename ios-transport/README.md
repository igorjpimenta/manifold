# manifold-ios-transport

OpenClaw plugin bridging manifold events to the iOS app. Manifold core no
longer pushes to iOS itself; this module owns that lane entirely.

## What it does

Polls manifold state every `pollIntervalMs` (default 15s) and, for arrivals
since the previous cycle:

- **New failures** (`failure_events`) → WS `plugin.failure_event.start`
  `{failureId, flowId, pipelineId?, failureNode?, errorType?,
  failureSummary?}` + push `{kind: "failure", failureId, flowId?}`.
  Manifold core never broadcasts failures; without this the app's failure
  toasts only fire on foreground reconcile.
- **New pending approvals** (`queue_items` status `pending`) → WS
  `plugin.approval.requested` `{id, flowId?}` + push `{kind: "approval",
  flowId, itemId?}` (skipped when the row has no flow — APNs routing
  requires one).
- **Gate lock transitions** (`gate_state`) → push `{kind: "budget"}`.
  Foreground already learns via manifold's canonical `budget.lock.changed`;
  this covers background.

Everything else the app needs foreground arrives over the gateway WS
broadcast from manifold's canonical kinds directly — no bridge involved.

## Semantics

- First cycle baselines silently (marks everything seen, emits nothing),
  so a gateway restart never replays history as new arrivals.
- Reads only: fresh short-lived clients per cycle, tables are manifold's
  to create. Missing tables (manifold never opened) fail a cycle loudly
  in logs and retry next tick.
- Pushes go to the gateway's `POST /manifold/ios/push`
  (`OPENCLAW_GATEWAY_TOKEN`, `OPENCLAW_GATEWAY_PORT`). No token → pushes
  skipped, WS emission continues.
- Broadcast resolves per emission via
  `api.runtime.manifold.getBroadcast()` (set by manifold core), so plugin
  load order never matters; null until manifold captures it.

## Install

Colocated in the manifold repo; install by path. `npm test` runs the
bridge suite (tmp DBs, injected broadcast/push — no gateway needed).
