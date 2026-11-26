export const REVISION_RETRY_LIMIT = 5;

// Always resolves null — core never reads harness homes, so callers fall
// back to their default detail.
export async function fetchSessionError(session) {
  void session;
  return null;
}

export class NodeBlocked extends Error {
  constructor({ reason, needed, hint } = {}) {
    super(reason ?? "node blocked");
    this.name = "NodeBlocked";
    this._blocked = { reason, needed, hint };
  }
}

// Thrown when an agent dispatch hits a missing gateway request scope. The
// flow blocks with wait_json.kind="needs-gateway-handoff" and there is NO
// automatic resume for that kind — recovery is a scope-gap fix at the call
// site or manual task-revival.
export class GatewayHandoffNeeded extends Error {
  constructor(originalMessage) {
    super(`agent dispatch requires gateway context: ${originalMessage}`);
    this.name = "GatewayHandoffNeeded";
  }
}

// Thrown by a cascade leg to signal failure. `errorType` is an opaque,
// pipeline-authored string the flow's own edges may route on; the cascade
// catch in executor.js reads it off the error (duck-typed, not instanceof
// — see that site) and threads it to the next edge call as its 2nd
// argument; any other throw reaches the edge with errorType: null.
export class NodeFailure extends Error {
  constructor(message, { errorType = null } = {}) {
    super(message);
    this.name = "NodeFailure";
    this.errorType = errorType;
  }
}

// The gateway's startup refusal ("<method> unavailable during gateway
// startup") is deliberately NOT matched: it is transient (the run-turn
// gate holds calls until the gateway is ready), and matching it would park
// flows in needs-gateway-handoff, which nothing ever resumes.
export function isRequestScopedSubagentError(err) {
  if (!err) return false;
  if (err.name === "RequestScopedSubagentRuntimeError") return true;
  const msg = err.message ?? String(err);
  if (msg.includes("only available during a gateway request")) return true;
  if (msg.includes("requires a gateway request scope")) return true;
  // A dispatch from a detached continuation (e.g. resume() via setImmediate
  // after a gate resolves) has no live HTTP request to inherit operator
  // scope from, so the RPC layer rejects with "missing scope: operator.write"
  // — same underlying cause, differently shaped. Matched narrowly: this
  // function only ever sees errors from a turn's own RPC calls, so a bare
  // "missing scope:" match would swallow unrelated scope failures
  // (operator.pairing, operator.admin, …) from elsewhere.
  if (msg.includes("missing scope: operator.write")) return true;
  return false;
}

// Errors that must NOT trip a node's breaker. A blocked node has no TTL —
// right for a symptom (timeout, provider rejection: retrying is pointless
// and costs a call), wrong for a standing configuration fault no retry can
// clear, which would park the node behind a condition only a human can fix.
// Matched on message text because these originate in layers that share no
// error class. Deliberately narrow: an unrecognised error is treated as a
// symptom and DOES trip, so this list can only under-block, never swallow
// a genuine health signal.
const NON_TRIPPING_REASONS = [
  // A tool scope the operator has not granted. Retrying cannot grant it.
  /missing scope/i,
  // The node kind requires a live gateway context this flow does not
  // provide — a structural mismatch, not a transient one.
  /requires gateway context/i,
  // Same family as GatewayHandoffNeeded, raised as plain text by the
  // bridge rather than as that error class.
  /needs-gateway-handoff/i,
];

export function isBreakerTrippingFailure(err) {
  if (err instanceof NodeBlocked) return false;
  if (err instanceof GatewayHandoffNeeded) return false;
  const msg = err?.message ?? String(err ?? "");
  return !NON_TRIPPING_REASONS.some((re) => re.test(msg));
}
