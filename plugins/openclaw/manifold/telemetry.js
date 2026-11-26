// The adapter's half of telemetry: translate openclaw's lifecycle hooks into
// manifold's semantic events. Core owns what the events mean (lineage, cost,
// ancestry, budget); this module owns only the harness's field names. Posts
// are fire-and-forget: a missed event costs one row, a blocked hook costs
// the harness.

export function translateHook(hook, event, ctx) {
  if (hook === "before_agent_start" || hook === "session_start") {
    const session = event?.sessionKey || ctx?.sessionKey;
    if (!session) return null;
    return {
      kind: "lineage.session",
      source: hook === "session_start" ? "session_start" : "start",
      session,
      agentId: event?.agentId ?? ctx?.agentId ?? null,
      parentSession: ctx?.requesterSessionKey ?? null,
    };
  }
  if (hook === "subagent_spawned") {
    const session = event?.childSessionKey;
    if (!session) return null;
    return {
      kind: "lineage.spawn",
      session,
      parentSession: ctx?.requesterSessionKey ?? null,
      agentId: event?.agentId ?? null,
    };
  }
  if (hook === "before_message_write") {
    const msg = event?.message;
    if (!msg || typeof msg !== "object") return null;
    const usage = msg.usage ?? msg?.message?.usage;
    if (!usage || typeof usage !== "object") return null;
    return {
      kind: "cost.usage",
      session: event?.sessionKey ?? ctx?.sessionKey ?? null,
      sessionId: ctx?.sessionId ?? msg.sessionId ?? null,
      messageId: msg.id ?? msg.messageId ?? null,
      provider: msg.provider ?? msg?.message?.provider ?? null,
      model: msg.model ?? msg?.message?.model ?? null,
      usage,
      timestamp: msg.timestamp ?? null,
      agentId: ctx?.agentId ?? null,
    };
  }
  return null;
}

export function registerTelemetryHooks(api, { ingestUrl, token, logger = api?.logger ?? null, fetchImpl = globalThis.fetch } = {}) {
  const post = (payload) => {
    if (!ingestUrl || !token) return;
    fetchImpl(ingestUrl, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10_000),
    })
      .then((res) => { if (!res.ok) logger?.warn?.(`manifold telemetry: ingest ${res.status}`); })
      .catch((err) => logger?.warn?.(`manifold telemetry: ${err?.message ?? err}`));
  };

  for (const hook of ["before_agent_start", "session_start", "subagent_spawned", "before_message_write"]) {
    api.on(hook, (event, ctx) => {
      const payload = translateHook(hook, event, ctx);
      if (payload) post(payload);
    });
  }
}
