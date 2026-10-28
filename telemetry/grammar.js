// sessionKey scope grammar — every sessionKey shape we know about,
// with a single `classify` entry point that returns the parent breadcrumb (or
// null for roots). The breadcrumb might be:
//   { parentKind: "flow", flow8: "..." }   → resolver joins to flow_runs
//   { parentKind: "session-key", key: "..." } → resolver walks up
//   { parentKind: "cron-id", cronId: "..." }  → resolver joins to cron jobs
//   { parentKind: "memory-synth", runId: "..." } → root (no on-disk session)
//   null → this is a root, no parent
//
// Adding a new scope: just add a regex + a CLASSIFIERS entry. The rest of the
// pipeline (resolver, store, hooks) is grammar-agnostic.

const REGEX = {
  CRON_RUN:  /^agent:([^:]+):cron:([0-9a-f-]{36}):run:([0-9a-f-]+)$/,
  CRON_OWN:  /^agent:([^:]+):cron-(.+)$/,
  BARE_SUB:  /^agent:([^:]+):subagent:([0-9a-f-]{36})$/,
  // Manifold dispatch sessions: plain-text `mf-<flow8>-<node>[-<item>]`
  // (see engine/executor-helpers.js's mintAgentSessionKey). The embedded
  // flow8 resolves via the "flow" parentHint branch, same as GF_DISCUSS.
  MF_SUB:    /^(mf-[0-9a-f]{8}-.+)$/,
  TG_DM:     /^agent:([^:]+):telegram:direct:(.+)$/,
  EXPLICIT:  /^agent:([^:]+):explicit:(.+)$/,
  MEMSYNTH:  /^agent:([^:]+):memory-synth:([^:]+):c-(\d+)$/,
  // Synthetic discuss-turn session ids for a gate's discuss thread —
  // gf-discuss-<flow8>-<gateNodeId>-<itemId>. The embedded flow8 is a real
  // flow_id prefix — resolves via the same "flow" parentHint branch a
  // pipeline-node subagent's flow:<id> parent uses (classifyWithExternal
  // below), never the EXPLICIT_BARE catch-all.
  GF_DISCUSS: /^gf-discuss-([0-9a-f]{8})-(.+)$/,
  AGENT_DEF: /^agent:([^:]+):main$/,
  // Synthetic operator-trigger keys minted by the `_elev/start` HTTP route
  // when a pipeline is started via iOS button or CLI. Format:
  //   agent:operator:(ios|cli|operator):<flowId-uuid>
  // Treated as root (no parent). The `unknown` source variant is NOT included
  // here because those historic flows were actually cron-triggered and need
  // separate SQL attribution, not the operator label.
  OPERATOR_TRIGGER: /^agent:operator:(ios|cli|operator):([0-9a-f-]{36})$/,
  // Catch-all for legacy / manual sessions whose key is `agent:<id>:<tail>`
  // with no recognized scope prefix — e.g. hand-typed --session-id values
  // that don't follow a known scope. Treated as kind=explicit (same semantics
  // as the prefixed form). Tried LAST so specific rules win.
  EXPLICIT_BARE: /^agent:([^:]+):([^:]+)$/,
};

/** Returns { kind, agentId, pipelineId?, flowShort?, nodeId?, itemKey?, parentHint } */
export function classify(sessionKey) {
  if (typeof sessionKey !== "string" || !sessionKey) {
    return { kind: "unknown", agentId: null, parentHint: null };
  }

  let m;

  if ((m = REGEX.CRON_RUN.exec(sessionKey))) {
    const [, agentId, cronId, runSid] = m;
    return {
      kind: "cron-tick", agentId, cronId, runSid,
      parentHint: { kind: "cron-id", cronId, agentId },
    };
  }
  if ((m = REGEX.CRON_OWN.exec(sessionKey))) {
    const [, agentId, cronName] = m;
    return {
      kind: "cron-owner", agentId, cronName,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.MEMSYNTH.exec(sessionKey))) {
    const [, agentId, runTs, chunk] = m;
    return {
      kind: "memory-synth-chunk", agentId, memorySynthRun: runTs, chunk,
      parentHint: { kind: "memory-synth", runId: runTs, agentId },
    };
  }
  if ((m = REGEX.GF_DISCUSS.exec(sessionKey))) {
    const [, flowShort] = m;
    return {
      kind: "flow-gate-discuss", agentId: null, flowShort,
      parentHint: { kind: "flow", flowShort },
    };
  }
  if ((m = REGEX.MF_SUB.exec(sessionKey))) {
    const [, opaque] = m;
    const flowShort = opaque.slice(3, 11);
    const tail = opaque.slice(12);
    const dash = tail.indexOf("-");
    return {
      kind: "manifold-subagent", agentId: null, opaque, flowShort,
      nodeId: dash === -1 ? tail : tail.slice(0, dash),
      itemKey: dash === -1 ? null : tail.slice(dash + 1),
      parentHint: { kind: "flow", flowShort },
    };
  }
  if ((m = REGEX.BARE_SUB.exec(sessionKey))) {
    const [, agentId, uuid] = m;
    return {
      kind: "bare-subagent", agentId, bareUuid: uuid,
      parentHint: null,                              // Parent must come from spawn hook
    };
  }
  if ((m = REGEX.TG_DM.exec(sessionKey))) {
    const [, agentId, chatId] = m;
    return {
      kind: "telegram-dm", agentId, telegramChatId: chatId,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.OPERATOR_TRIGGER.exec(sessionKey))) {
    const [, source, triggerFlowId] = m;
    return {
      kind: "operator-trigger", agentId: "operator", operatorSource: source, triggerFlowId,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.EXPLICIT.exec(sessionKey))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.AGENT_DEF.exec(sessionKey))) {
    const [, agentId] = m;
    return {
      kind: "agent-default", agentId,
      parentHint: null,                              // ROOT
    };
  }
  // Fallback: bare-shape explicit (no scope prefix). Specific rules above
  // (cron-owner, subagent, telegram-dm, etc.) get first crack, so this only
  // matches truly unprefixed `agent:<id>:<tail>` keys.
  if ((m = REGEX.EXPLICIT_BARE.exec(sessionKey))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,                              // ROOT
    };
  }
  return { kind: "unknown", agentId: agentIdOf(sessionKey), parentHint: null };
}

/** sessionKeys ALWAYS start `agent:<id>:...` — peel the agent off generically. */
export function agentIdOf(sessionKey) {
  if (typeof sessionKey !== "string") return null;
  const i1 = sessionKey.indexOf(":");
  const i2 = sessionKey.indexOf(":", i1 + 1);
  if (i1 !== 5 || i2 === -1) return null;            // must start "agent:"
  return sessionKey.slice(i1 + 1, i2) || null;
}

