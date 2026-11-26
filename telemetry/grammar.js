// session scope grammar — every session shape we know about, with one
// `classify` entry point returning the parent breadcrumb: { kind: "flow", flow8 }
// (resolver joins flow_runs), { kind: "session-key", key } (resolver walks up),
// { kind: "cron-id", cronId } (resolver joins cron jobs), { kind: "memory-synth",
// runId } (root, no on-disk session), or null (this is a root). New scopes:
// add a regex plus a CLASSIFIERS entry; the rest of the pipeline is
// grammar-agnostic.

const REGEX = {
  CRON_RUN:  /^agent:([^:]+):cron:([0-9a-f-]{36}):run:([0-9a-f-]+)$/,
  CRON_OWN:  /^agent:([^:]+):cron-(.+)$/,
  BARE_SUB:  /^agent:([^:]+):subagent:([0-9a-f-]{36})$/,
  TG_DM:     /^agent:([^:]+):telegram:direct:(.+)$/,
  EXPLICIT:  /^agent:([^:]+):explicit:(.+)$/,
  MEMSYNTH:  /^agent:([^:]+):memory-synth:([^:]+):c-(\d+)$/,
  AGENT_DEF: /^agent:([^:]+):main$/,
  // Adapter-minted node dispatch key: agent:<agentId>:manifold:<flow8>-<node>.
  // The flow segment joins back to flow_runs, so dispatches stay attributable
  // even when the session index row is missing.
  MANIFOLD_NODE: /^agent:([^:]+):manifold:([0-9a-f]{8})-([A-Za-z0-9._-]+)$/,
  // Route-minted operator trigger: agent:operator:(cli|operator):<flowId-uuid>,
  // treated as root. The `unknown` source variant is excluded — those historic
  // flows were cron-triggered and need SQL attribution, not an operator label.
  OPERATOR_TRIGGER: /^agent:operator:(cli|operator):([0-9a-f-]{36})$/,
  // Catch-all for `agent:<id>:<tail>` with no recognized scope prefix
  // (hand-typed --session-id values). Same semantics as the prefixed form;
  // tried LAST so specific rules win.
  EXPLICIT_BARE: /^agent:([^:]+):([^:]+)$/,
};

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
      parentHint: null,
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
      parentHint: null,  // parent must come from the spawn hook
    };
  }
  if ((m = REGEX.TG_DM.exec(session))) {
    const [, agentId, chatId] = m;
    return {
      kind: "telegram-dm", agentId, telegramChatId: chatId,
      parentHint: null,
    };
  }
  if ((m = REGEX.OPERATOR_TRIGGER.exec(session))) {
    const [, source, triggerFlowId] = m;
    return {
      kind: "operator-trigger", agentId: "operator", operatorSource: source, triggerFlowId,
      parentHint: null,
    };
  }
  if ((m = REGEX.MANIFOLD_NODE.exec(session))) {
    const [, agentId, flowShort, nodeId] = m;
    return {
      kind: "manifold-node", agentId, nodeId, flowShort,
      parentHint: { kind: "flow", flowShort },
    };
  }
  if ((m = REGEX.EXPLICIT.exec(session))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,
    };
  }
  if ((m = REGEX.AGENT_DEF.exec(session))) {
    const [, agentId] = m;
    return {
      kind: "agent-default", agentId,
      parentHint: null,
    };
  }
  if ((m = REGEX.EXPLICIT_BARE.exec(session))) {
    const [, agentId, purpose] = m;
    return {
      kind: "explicit", agentId, explicitPurpose: purpose,
      parentHint: null,
    };
  }
  return { kind: "unknown", agentId: agentIdOf(session), parentHint: null };
}

export function agentIdOf(session) {
  if (typeof session !== "string") return null;
  const i1 = session.indexOf(":");
  const i2 = session.indexOf(":", i1 + 1);
  if (i1 !== 5 || i2 === -1) return null;  // must start "agent:"
  return session.slice(i1 + 1, i2) || null;
}

