// transcripts.js — shared store-backed transcript reader for one flow.
//
// Discuss threads come from discuss-store (both directions of every
// dialog-door turn, keyed by (flowId, itemId)); dispatch agent turns come
// from node-executions resultJson (a single assistant turn when the stored
// result carries reply text, metadata-only otherwise — content is never
// invented). Attribution uses stored columns (flow_sessions node/branch
// columns, discuss itemId); the sessionKey passes through opaquely and is
// never parsed here.
//
// Callers own the wire shape: the gateway route wraps the returned array
// in a { transcripts } envelope, the dashboard serves it bare.
//
// lite=true  → turnCount only, no turn text (for list views)
// sessionKey → a single transcript with full turns (for drill-in)
export async function listTranscriptsForFlow(flow, {
  discussStore, sessionIndex, nodeExecutionsStore, pipelinesRef,
  lite = false, sessionKey = null,
} = {}) {
  // Session roster for this flow: one row per dispatch attempt plus one
  // per discuss thread, each carrying its stored node/item attribution.
  const sessionRows = await Promise.resolve(sessionIndex?.listForFlow?.(flow.flowId)).catch(() => []) ?? [];
  const discussThreads = await Promise.resolve(discussStore?.listForFlow?.(flow.flowId)).catch(() => []) ?? [];
  const results = await Promise.resolve(nodeExecutionsStore?.listResultsForFlow?.(flow.flowId)).catch(() => []) ?? [];
  const resultByNode = new Map((results ?? []).map((r) => [r?.nodeId, r]));
  // Agent display name resolves from the loaded pipeline's own node def;
  // null when the pipeline is not loaded — never guessed from key shape.
  const pipelineNodes = pipelinesRef?.value?.get?.(flow.stateJson?.pipelineId)?.nodes ?? {};

  const transcripts = [];
  for (const row of sessionRows) {
    // Discuss identity rows (branchItemId set) are covered by the discuss
    // loop below, which joins them to full turns from discuss-store.
    if (row.branchItemId != null) continue;
    if (sessionKey && row.sessionKey !== sessionKey) continue;
    const result = row.nodeId != null ? resultByNode.get(row.nodeId) : null;
    const turns = lite ? null : dispatchTurns(result);
    transcripts.push({
      sessionKey: row.sessionKey, agent: pipelineNodes?.[row.nodeId]?.agent ?? null,
      nodeId: row.nodeId ?? null, itemKey: row.branchItemId ?? null, sessionId: row.sessionKey,
      startedAt: row.spawnedAt ?? null,
      updatedAt: result?.resultWrittenAt ?? row.spawnedAt ?? null,
      ...(lite ? { turnCount: turnsForCount(result) } : { turns }),
    });
  }
  for (const thread of discussThreads) {
    // A thread predating session-index recording has turns but no stored
    // identity row — it still lists, with null identity fields.
    let identity = null;
    for (const row of sessionRows) {
      if (row.branchItemId === thread.itemId && (!identity || (row.spawnedAt ?? 0) > (identity.spawnedAt ?? 0))) identity = row;
    }
    const key = identity?.sessionKey ?? null;
    if (sessionKey && key !== sessionKey) continue;
    const turns = lite ? null : discussTurns(thread.messages);
    transcripts.push({
      sessionKey: key, agent: null,
      nodeId: identity?.nodeId ?? "discuss", itemKey: thread.itemId ?? null, sessionId: key,
      startedAt: thread.messages?.[0]?.ts ?? thread.updatedAt ?? null,
      updatedAt: thread.updatedAt ?? null,
      ...(lite ? { turnCount: Array.isArray(thread.messages) ? thread.messages.length : 0 } : { turns }),
    });
  }
  transcripts.sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
  return transcripts;
}

// Stored discuss messages use operator/agent roles; the transcript wire
// shape (and its dashboard renderer) uses user/assistant.
function discussTurns(messages) {
  const turns = [];
  if (!Array.isArray(messages)) return turns;
  for (const m of messages) {
    const { text, truncated } = capText(typeof m?.text === "string" ? m.text : "");
    turns.push({
      role: m?.role === "agent" ? "assistant" : "user",
      text, thinking: null, truncated, timestamp: m?.ts ?? null,
    });
  }
  return turns;
}

// The only dispatch content manifold observes durably is the node's own
// resultJson: a bare string, or an object carrying reply text in `text`.
// Anything else yields no turn — the entry lists with metadata only.
function dispatchTurns(result) {
  const text = dispatchTurnText(result?.resultJson);
  if (text == null) return [];
  const capped = capText(text);
  return [{
    role: "assistant", text: capped.text, thinking: null,
    truncated: capped.truncated, timestamp: result?.resultWrittenAt ?? null,
  }];
}

function turnsForCount(result) {
  return dispatchTurnText(result?.resultJson) == null ? 0 : 1;
}

function dispatchTurnText(resultJson) {
  if (typeof resultJson !== "string") return null;
  let parsed = resultJson;
  try { parsed = JSON.parse(resultJson); } catch { return null; }
  if (typeof parsed === "string") return parsed;
  if (parsed && typeof parsed === "object" && typeof parsed.text === "string") return parsed.text;
  return null;
}

function capText(text) {
  const cap = 8 * 1024;
  if (text.length <= cap) return { text, truncated: false };
  return { text: text.slice(0, cap), truncated: true };
}
