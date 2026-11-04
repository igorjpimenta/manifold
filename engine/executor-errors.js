export const REVISION_RETRY_LIMIT = 5;

// Dropped legacy: the openclaw tasks DB (~/.openclaw/tasks/runs.sqlite) is
// no longer read — core never reads harness homes. Always resolves null so
// callers fall back to their default detail.
export async function fetchSessionError(sessionKey) {
  void sessionKey;
  return null;
}

/**
 * Thrown (or returned as sentinel) by a node when it detects it can't proceed
 * due to missing info. The executor catches it and blocks the flow with
 * retry-on-resume semantics.
 */
export class NodeBlocked extends Error {
  constructor({ reason, needed, hint } = {}) {
    super(reason ?? "node blocked");
    this.name = "NodeBlocked";
    this._blocked = { reason, needed, hint };
  }
}

/**
 * Thrown internally when a node's agent dispatch hits
 * `RequestScopedSubagentRuntimeError` — the executor is running outside a
 * gateway request context and can't dispatch agents. The flow gets blocked
 * with `wait_json.kind="needs-gateway-handoff"`.
 *
 * There is no automatic resume for this wait kind — confirmed nothing in
 * this codebase (boot-time flow-revival, a periodic scan, or otherwise)
 * ever re-enters a flow blocked this way. A flow that ends up here needs
 * either the underlying scope gap fixed at its actual call site (see
 * elevation.js for the loopback pattern gate resolution uses) or manual
 * task-revival (patch state + flip status) once resumption would actually
 * succeed.
 */
export class GatewayHandoffNeeded extends Error {
  constructor(originalMessage) {
    super(`agent dispatch requires gateway context: ${originalMessage}`);
    this.name = "GatewayHandoffNeeded";
  }
}

/**
 * Thrown by a cascade leg (a `flow.kind:"cascade"` node's own nodes, e.g.
 * SUBMIT_CODE/ENSURE_LOGIN_CODE) to signal failure. `errorType` uses a
 * fixed vocabulary (no new strings): "never_attempted" | "confirmed_absent"
 * | "inconclusive" | "infra_unavailable" | "blocked".
 *
 * The engine's cascade-catch branch in executor.js reads `err.errorType`
 * off this (via `instanceof NodeFailure`) and threads it to the next
 * edge-function call as that edge's 2nd argument — see
 * engine/executor-helpers.js's `resolveEdge`. A plain `Error` (or any
 * non-`NodeFailure` throw) still works in a cascade — the edge just
 * receives `errorType: null`.
 */
export class NodeFailure extends Error {
  constructor(message, { errorType = null } = {}) {
    super(message);
    this.name = "NodeFailure";
    this.errorType = errorType;
  }
}

// The gateway's startup refusal ("<method> unavailable during gateway
// startup") is deliberately NOT matched below: it is transient, and
// the harness's run-turn gate holds every call until the gateway is ready.
// Matching it would park flows in needs-gateway-handoff, which nothing ever
// resumes.
export function isRequestScopedSubagentError(err) {
  if (!err) return false;
  if (err.name === "RequestScopedSubagentRuntimeError") return true;
  const msg = err.message ?? String(err);
  if (msg.includes("only available during a gateway request")) return true;
  if (msg.includes("requires a gateway request scope")) return true;
  // An agent dispatch triggered from a detached continuation (e.g.
  // executor.resume() firing via setImmediate after a gate resolves) has no
  // live HTTP request to inherit operator scope from — the gateway RPC
  // layer rejects it with "missing scope: operator.write", a differently-
  // shaped error than the ones above but the exact same underlying cause
  // (no live gateway-request context). Scoped to "operator.write"
  // specifically, not a bare "missing scope:" match — this function is
  // only ever consulted on errors from the three runtime calls a turn makes
  // getSessionMessages calls in the harness's run-turn.js, never on unrelated
  // RPC scope failures (operator.pairing, operator.admin, etc.) from
  // elsewhere in the gateway, so a narrower-than-it-looks match is safe.
  if (msg.includes("missing scope: operator.write")) return true;
  return false;
}

// Errors that must NOT trip a node's breaker.
//
// A blocked node has no TTL: it stays blocked until an operator resets it.
// That is the right default for a symptom (an agent that times out, or a
// provider that rejects the request) — retrying is pointless and each
// attempt costs a call. It is the wrong default for a standing configuration
// fault, which no retry can clear and which would leave the node parked
// behind a condition only a human can fix, with the reset as the sole
// remedy.
//
// Matched on message text because these originate in different layers (tool
// permission resolution, the gateway handoff contract, the agent bridge)
// and share no error class. Deliberately narrow: an unrecognised error is
// treated as a symptom and DOES trip, so this list can only under-block,
// never silently swallow a genuine health signal.
const NON_TRIPPING_REASONS = [
  // A tool scope the operator has not granted. Retrying cannot grant it.
  /missing scope/i,
  // The node kind requires a live gateway context that this flow does not
  // provide — a structural mismatch, not a transient one.
  /requires gateway context/i,
  // Same family as GatewayHandoffNeeded, raised as plain text by the bridge
  // rather than as that error class.
  /needs-gateway-handoff/i,
];

export function isBreakerTrippingFailure(err) {
  if (err instanceof NodeBlocked) return false;
  if (err instanceof GatewayHandoffNeeded) return false;
  const msg = err?.message ?? String(err ?? "");
  return !NON_TRIPPING_REASONS.some((re) => re.test(msg));
}
