// identity.js — harness declared, agent and session stamped per call.
//
// One thing is declared, two things arrive with the work. The server is
// CONSTRUCTED with the harness it serves (MANIFOLD_HARNESS env or ctor
// base); the agent and session of each call are the calling harness's job
// to stamp — via _meta.manifold on harnesses with request-params access,
// else stamped manifold_agent/manifold_session args. Unstamped calls fail;
// nothing defaults, ever.

function isUsableLayer(v) {
  return (
    typeof v === "string" &&
    v.trim() !== "" &&
    !/^(unknown|instance:.*)$/i.test(v.trim())
  );
}

export function assertCleanTriple(id, who = "identity") {
  const bad = ["harness", "agent", "session"].filter((k) => !isUsableLayer(id?.[k]));
  if (bad.length > 0) {
    throw new Error(`manifold: refused ${who} (bad layers: ${bad.join(", ")}). Provide real harness/agent/session values.`);
  }
  return { harness: id.harness.trim(), agent: id.agent.trim(), session: id.session.trim() };
}

export function assertDeclaration(base) {
  const h = typeof base?.harness === "string" ? base.harness.trim() : "";
  if (h === "") {
    throw new Error(
      "manifold: identity declaration incomplete (missing: harness). " +
        "Declare the consumer at construction — MANIFOLD_HARNESS env, " +
        "X-Manifold-Harness header (HTTP), or startMcpServer({ base: { harness } }). " +
        "Agent and session are never declared: they arrive per call."
    );
  }
  if (/^(unknown|instance:.*)$/i.test(h)) {
    throw new Error(
      `manifold: refused declaration (reserved: harness). ` +
        `"${h}" cannot identify a consumer — declare the real harness name.`
    );
  }
  const pd = typeof base?.pipelinesDir === "string" ? base.pipelinesDir.trim() : "";
  return pd ? { harness: h, pipelinesDir: pd } : { harness: h };
}

export function declaredFromEnv(env = process.env) {
  return assertDeclaration({ harness: env.MANIFOLD_HARNESS });
}

export function resolveIdentity({ meta, args, base }) {
  const declared = assertDeclaration(base);
  const m = meta?.manifold ?? meta ?? null;
  const am = args?._meta?.manifold ?? null;
  const usable = (v) => (isUsableLayer(v) ? String(v).trim() : null);
  const mAgent = usable(m?.agent), mSession = usable(m?.session);
  const amAgent = usable(am?.agent), amSession = usable(am?.session);
  const aAgent = usable(args?.manifold_agent), aSession = usable(args?.manifold_session);
  const pair = (mAgent && mSession)
    ? { agent: mAgent, session: mSession, source: "meta" }
    : (amAgent && amSession)
      ? { agent: amAgent, session: amSession, source: "args-meta" }
      : (aAgent && aSession)
        ? { agent: aAgent, session: aSession, source: "args" }
        : null;
  if (!pair) {
    if ((mAgent || amAgent || aAgent) && (mSession || amSession || aSession)) {
      throw new Error(
        "manifold: refused caller identity (halves in different channels — pairs never splice). " +
          "Stamp a whole agent+session in one channel: _meta.manifold or manifold_agent/manifold_session args."
      );
    }
    const bad = [];
    if (!mAgent && !amAgent && !aAgent) bad.push("agent");
    if (!mSession && !amSession && !aSession) bad.push("session");
    throw new Error(
      `manifold: refused caller identity (unresolved ${bad.join(" and ")}). ` +
        "The calling harness must stamp agent and session on each call. " +
        "Unidentified callers fail; they are never given default keys."
    );
  }
  const out = {
    harness: declared.harness,
    agent: pair.agent,
    session: pair.session,
    source: { harness: "declared", agent: pair.source, session: pair.source },
  };
  if (declared.pipelinesDir) out.pipelinesDir = declared.pipelinesDir;
  return out;
}

export function identitySessionKey(id) {
  const clean = assertCleanTriple(id, "session key");
  return `${clean.harness}:${clean.agent}:${clean.session}`;
}
