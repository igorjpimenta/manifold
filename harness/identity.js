const TRIPLE_RE = /^([^:]+):([^:]+):(.+)$/;

export function formatIdentity({ harness, agent, session }) {
  if (!harness || !agent || !session) {
    throw new Error("manifold identity: harness, agent and session are all required (no default keys)");
  }
  return `${harness}:${agent}:${session}`;
}

export function parseIdentity(key) {
  const m = typeof key === "string" ? TRIPLE_RE.exec(key) : null;
  if (!m) throw new Error(`manifold identity: expected harness:agent:session, got ${JSON.stringify(key)}`);
  return { harness: m[1], agent: m[2], session: m[3] };
}

export function stampCallMeta(meta, { harness, agent, session }) {
  const stamped = { ...(meta ?? {}), harness, agent, session };
  if (!stamped.harness || !stamped.agent || !stamped.session) {
    throw new Error("manifold identity: unstamped call refused (agent/session must be stamped by the calling harness)");
  }
  return stamped;
}
