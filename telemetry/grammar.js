// session scope grammar — every session shape we know about,
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
  TG_DM:     /^agent:([^:]+):telegram:direct:(.+)$/,
  EXPLICIT:  /^agent:([^:]+):explicit:(.+)$/,
  MEMSYNTH:  /^agent:([^:]+):memory-synth:([^:]+):c-(\d+)$/,
  AGENT_DEF: /^agent:([^:]+):main$/,
  // Synthetic operator-trigger keys minted by the `_elev/start` HTTP route
  // when a pipeline is started via CLI or another harness. Format:
  //   agent:operator:(cli|operator):<flowId-uuid>
  // Treated as root (no parent). The `unknown` source variant is NOT included
  // here because those flows are actually cron-triggered and need separate
  // SQL attribution, not the operator label.
  OPERATOR_TRIGGER: /^agent:operator:(cli|operator):([0-9a-f-]{36})$/,
  // Catch-all for manual sessions whose key is `agent:<id>:<tail>`
  // with no recognized scope prefix — e.g. hand-typed --session-id values
  // that don't follow a known scope. Treated as kind=explicit (same semantics
  // as the prefixed form). Tried LAST so specific rules win.
  EXPLICIT_BARE: /^agent:([^:]+):([^:]+)$/,
};

/** Returns { kind, agentId, pipelineId?, flowShort?, nodeId?, itemKey?, parentHint } */
export function classify(session) {
  if (typeof session !== "string" || !session) {
    return { kind: "unknown", agentId: null, parentHint: null };
  }

  let m;

  if ((m = REGEX.CRON_RUN.exec(session))) {
    const [, agentId, cronId, runSid] = m;
    return {
      kind: "cron-tick", agentId, cronId, runSid,
      parentHint: { kind: "cron-id", cronId, agentId },
    };
  }
  if ((m = REGEX.CRON_OWN.exec(session))) {
    const [, agentId, cronName] = m;
    return {
      kind: "cron-owner", agentId, cronName,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.MEMSYNTH.exec(session))) {
    const [, agentId, runTs, chunk] = m;
    return {
      kind: "memory-synth-chunk", agentId, memorySynthRun: runTs, chunk,
      parentHint: { kind: "memory-synth", runId: runTs, agentId },
    };
  }
  if ((m = REGEX.BARE_SUB.exec(session))) {
    const [, agentId, uuid] = m;
    return {
      kind: "bare-subagent", agentId, bareUuid: uuid,
      parentHint: null,                              // Parent must come from spawn hook
    };
  }
  if ((m = REGEX.TG_DM.exec(session))) {
    const [, agentId, chatId] = m;
    return {
      kind: "telegram-dm", agentId, telegramChatId: chatId,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.OPERATOR_TRIGGER.exec(session))) {
    const [, source, triggerFlowId] = m;
    return {
      kind: "operator-trigger", agentId: "operator", operatorSource: source, triggerFlowId,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.EXPLICIT.exec(session))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,                              // ROOT
    };
  }
  if ((m = REGEX.AGENT_DEF.exec(session))) {
    const [, agentId] = m;
    return {
      kind: "agent-default", agentId,
      parentHint: null,                              // ROOT
    };
  }
  // Fallback: bare-shape explicit (no scope prefix). Specific rules above
  // (cron-owner, subagent, telegram-dm, etc.) get first crack, so this only
  // matches truly unprefixed `agent:<id>:<tail>` keys.
  if ((m = REGEX.EXPLICIT_BARE.exec(session))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,                              // ROOT
    };
  }
  return { kind: "unknown", agentId: agentIdOf(session), parentHint: null };
}

/** Sessions ALWAYS start `agent:<id>:...` — peel the agent off generically. */
export function agentIdOf(session) {
  if (typeof session !== "string") return null;
  const i1 = session.indexOf(":");
  const i2 = session.indexOf(":", i1 + 1);
  if (i1 !== 5 || i2 === -1) return null;            // must start "agent:"
  return session.slice(i1 + 1, i2) || null;
}

