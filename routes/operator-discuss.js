
// ── Pure helpers ──────────────────────────────────────────────────────────────

export function stringifyMessageContent(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((c) => c?.type === "text" || typeof c?.text === "string")
      .map((c) => c.text ?? "")
      .join("\n");
  }
  return "";
}

// Takes [{role, content, timestamp}] from a discuss JSONL session and returns
// [{isOperator, text, ts}] with protocol headers/footers stripped from user
// turns and <draft>...</draft> tags stripped from assistant turns.
export function filterDiscussHistoryMessages(msgs) {
  const toEpochMs = (v) => {
    if (v == null) return null;
    if (typeof v === "number") return Number.isFinite(v) ? v : null;
    const n = Date.parse(String(v));
    return Number.isFinite(n) ? n : null;
  };
  const out = [];
  for (const m of msgs) {
    if (m?.role !== "user" && m?.role !== "assistant") continue;
    let text = stringifyMessageContent(m.content);
    if (m.role === "user") {
      const split = text.split(/\n\n---\n\n/);
      if (split.length >= 2) text = split.slice(1).join("\n\n---\n\n");
      if (/^##/.test(text)) {
        const parts = text.split(/\n\n\n+/);
        if (parts.length >= 2) text = parts[parts.length - 1];
      }
      text = text.replace(/\n*\[discuss-protocol\][\s\S]*$/, "").trim();
      if (text) out.push({ isOperator: true, text, ts: toEpochMs(m.timestamp) });
    } else {
      text = text.replace(/<draft>[\s\S]*?<\/draft>/g, "").trim();
      if (text) out.push({ isOperator: false, text, ts: toEpochMs(m.timestamp) });
    }
  }
  return out;
}

// Given a resolved provider chain and a sync isCooldown predicate, returns the
// best {provider, model} to use for this turn. Skips the primary if it's in
// billing cooldown and a healthy fallback is available.
export function selectDiscussProvider({ primarySpec, chain, isCooldown }) {
  const resolveRef = (ref) => {
    const [p, ...mPts] = (ref ?? "").split("/");
    return { provider: p || null, model: mPts.join("/") };
  };
  const nextHealthy = (skip) => {
    for (const ref of chain) {
      const { provider: p, model: m } = resolveRef(ref);
      if (p !== skip && !isCooldown(p)) return { provider: p, model: m };
    }
    return null;
  };
  if (!chain || chain.length === 0) {
    return resolveRef(primarySpec);
  }
  const primary = resolveRef(chain[0]);
  if (chain.length > 1 && primary.provider && isCooldown(primary.provider)) {
    const healthy = nextHealthy(null);
    if (healthy) return healthy;
  }
  return primary;
}

// ── Discuss handler factory ───────────────────────────────────────────────────

export function createDiscussHandler({
  api,
  discussStore,
  sessions,
}) {
  async function handleDiscuss(body) {
    const flowId = typeof body.flowId === "string" ? body.flowId : "";
    const sessionKey = typeof body.sessionKey === "string" ? body.sessionKey : "";
    const sessionId = typeof body.sessionId === "string" ? body.sessionId : "";
    const agentId = typeof body.agentId === "string" ? body.agentId : "";
    const modelOverride = typeof body.modelOverride === "string" && body.modelOverride.trim()
      ? body.modelOverride.trim()
      : null;
    const text = typeof body.text === "string" ? body.text : "";
    if (!flowId || !sessionKey || !sessionId || !agentId || !text) {
      return { status: 400, body: { ok: false, code: "invalid_body", error: "flowId, sessionKey, sessionId, agentId, text required" } };
    }
    const agentRuntime = api.runtime?.agent;
    if (!agentRuntime?.runEmbeddedAgent) {
      return { status: 503, body: { ok: false, code: "agent_runtime_unavailable", error: "agent runtime not bound" } };
    }
    const itemIdForDraft = typeof body.itemId === "string" ? body.itemId : null;
    const systemPrompt = typeof body.systemPrompt === "string" ? body.systemPrompt : null;
    const startedAt = Date.now();
    const sessionFile = sessions.resolveSessionFile(agentId, sessionId);
    const isFirstTurn = !sessions.sessionFileExists(sessionFile);
    if (isFirstTurn) {
      // Touch only — thread identity lives in core's session roster
      // (recorded pre-elev); nothing is stamped here and the id is never
      // parsed.
      try {
        sessions.ensureSessionFile(sessionFile);
      } catch (err) {
        api.logger.warn(`manifold discuss: session-file touch failed for ${sessionFile}: ${err?.message ?? err}`);
      }
    }
    try {
      const workspaceDir = agentRuntime.resolveAgentWorkspaceDir(api.config, agentId)
        ?? agentRuntime.resolveAgentWorkspaceDir(api.config);
      const primarySpec = modelOverride ?? await sessions.resolveAgentEffectiveModelPrimary(api.config, agentId) ?? "";
      const _agentCfg = !modelOverride && (api.config?.agents?.list ?? []).find((a) => a?.id === agentId);
      const _modelCfg = _agentCfg ? (_agentCfg?.model ?? api.config?.agents?.defaults?.model) : null;
      const chain = (_modelCfg && typeof _modelCfg === "object")
        ? [_modelCfg.primary, ...(Array.isArray(_modelCfg.fallbacks) ? _modelCfg.fallbacks : [])]
            .filter((r) => typeof r === "string" && r.length > 0)
        : [];
      const isCooldown = (p) => sessions.isProviderCooldown(agentId, p);
      const { provider: activeProvider, model: activeModel } = selectDiscussProvider({
        primarySpec, chain, isCooldown,
      });
      if (activeProvider !== primarySpec.split("/")[0] && activeProvider) {
        api.logger.info(`manifold discuss: billing cooldown on ${primarySpec.split("/")[0]}, routing to ${activeProvider}/${activeModel}`);
      }
      const _runParams = {
        sessionId, sessionKey, agentId, trigger: "manual", sessionFile, workspaceDir,
        config: api.config, prompt: text, timeoutMs: 300_000,
        ...(systemPrompt ? { extraSystemPrompt: systemPrompt } : {}),
      };
      let result;
      try {
        result = await agentRuntime.runEmbeddedAgent({
          ..._runParams, provider: activeProvider || undefined, model: activeModel || undefined,
        });
      } catch (firstErr) {
        const _errMsg = firstErr?.message ?? "";
        const _isBilling = /billing error|insufficient|out of credits|out of extra usage|no available auth profile|all in cooldown/i.test(_errMsg);
        if (_isBilling && chain.length > 1) {
          const retry = selectDiscussProvider({ primarySpec, chain, isCooldown: (p) => p === activeProvider });
          if (retry.provider && retry.provider !== activeProvider) {
            api.logger.info(`manifold discuss: billing error on ${activeProvider}, retrying with ${retry.provider}/${retry.model}`);
            result = await agentRuntime.runEmbeddedAgent({ ..._runParams, provider: retry.provider, model: retry.model || undefined });
          } else {
            throw firstErr;
          }
        } else {
          throw firstErr;
        }
      }
      // Layer 3: all providers returned empty (context-filter silent rejection)
      {
        const _hasText = (r) => !!(r?.meta?.finalAssistantVisibleText || r?.meta?.finalAssistantRawText
          || (Array.isArray(r?.payloads) && r.payloads.some((p) => p?.text?.trim?.())));
        if (!_hasText(result) && chain.length > 1) {
          const _activeIdx = chain.findIndex((ref) => ref.split("/")[0] === activeProvider);
          const _skipped = _activeIdx > 0 ? chain.slice(0, _activeIdx) : [];
          for (const ref of _skipped) {
            const [_p, ..._mPts] = ref.split("/");
            api.logger.info(`manifold discuss: all providers returned empty (context-filter likely) — retrying with skipped provider ${_p}`);
            try {
              const _l3 = await agentRuntime.runEmbeddedAgent({ ..._runParams, provider: _p, model: _mPts.join("/") || undefined });
              if (_hasText(_l3)) { result = _l3; break; }
            } catch (_l3Err) {
              api.logger.warn(`manifold discuss: layer-3 ${_p} retry failed: ${_l3Err?.message}`);
            }
          }
        }
      }
    // Thread identity is recorded by core before the elev call (session
    // roster); nothing is stamped here and the session id is never parsed.
      let replyText = result?.meta?.finalAssistantVisibleText
        ?? result?.meta?.finalAssistantRawText
        ?? (Array.isArray(result?.payloads)
            ? result.payloads.map((p) => p?.text?.trim?.() ?? "").filter(Boolean).join("\n")
            : "");
      if (!replyText) {
        api.logger.warn(`manifold discuss (elev) [${flowId.slice(0, 8)}]: runEmbeddedAgent returned no assistant text`);
        return { status: 500, body: { ok: false, code: "no_reply_text", error: "agent ran but no assistant text found" } };
      }
      // Child-session harvesting (propose_draft/propose_draft_meta) lives in
      // oc-config now: core never scans harness transcripts for pipeline
      // tool calls. Pending fields/drafts arrive only via explicit tools.
      let proposedDraft = null;
      let proposedMeta = null;
      const currentPending = await discussStore.readPending(flowId, itemIdForDraft);
      if (currentPending) {
        if (typeof currentPending.text === "string" && currentPending.text.length > 0) proposedDraft = currentPending.text;
        if (currentPending.fields && Object.keys(currentPending.fields).length > 0) proposedMeta = currentPending.fields;
      }
      const durationMs = Date.now() - startedAt;
      api.logger.info(
        `manifold discuss (elev) [${flowId.slice(0, 8)}]: ok in ${durationMs}ms (reply=${replyText.length} chars, proposedDraft=${proposedDraft ? proposedDraft.length + " chars" : "none"}, proposedMeta=${proposedMeta ? JSON.stringify(Object.keys(proposedMeta)) : "none"})`
      );
      return { status: 200, body: { ok: true, reply: replyText, durationMs, editDraftUpdated: false, proposedDraft, proposedMeta } };
    } catch (err) {
      api.logger.warn(`manifold discuss (elev) handler error: ${err?.message ?? err}`);
      return { status: 500, body: { ok: false, code: "internal", error: err?.message ?? String(err) } };
    }
  }

  return { handleDiscuss };
}

// ── Discuss-history handler factory ───────────────────────────────────────────

// Caps the synchronous per-line JSON.parse work below to a bounded amount
// regardless of session length. Without this, a long-running discuss session's
// JSONL grows unbounded and this handler's parse loop — running on the same
// single-threaded gateway event loop as every other elevated route, including
// long-running discuss-turn calls — can stall long enough for the client's
// (much tighter) discuss-history GET timeout to fire first. Discuss is a chat
// view; the most recent turns are what matters, so tailing is an acceptable
// trade rather than a silent correctness loss.
const MAX_HISTORY_LINES = 500;

export function createDiscussHistoryHandler({ api, sessions }) {
  async function handleDiscussHistory(body) {
    const flowId = typeof body.flowId === "string" ? body.flowId : "";
    const sessionKey = typeof body.sessionKey === "string" ? body.sessionKey : "";
    const sessionId = typeof body.sessionId === "string" ? body.sessionId : "";
    // The agent travels as its own field: session ids are harness-owned and
    // carry no shape core may read. Without it there is no agent whose
    // transcript to read.
    const agentId = typeof body.agentId === "string" ? body.agentId : "";
    if (!flowId || !sessionKey || !sessionId || !agentId) {
      return { status: 400, body: { ok: false, code: "invalid_body", error: "flowId, sessionKey, sessionId, agentId required" } };
    }
    // Lines already delivered to the caller from the tail of the file, on a
    // prior page. 0 (or omitted) means "give me the most recent page."
    // Passed straight back as `nextOffset` so a "load earlier messages" tap
    // can page further back without the caller needing to track anything
    // beyond the number this handler already gave it.
    const offset = Number.isFinite(body.offset) && body.offset > 0 ? Math.floor(body.offset) : 0;
    try {
      const sessionFile = sessions.resolveSessionFile(agentId, sessionId);
      let msgs = [];
      let hasMore = false;
      let nextOffset = offset;
      if (sessions.sessionFileExists(sessionFile)) {
        const rawContent = await sessions.readSessionFileText(sessionFile);
        const lines = rawContent.split("\n");
        const end = Math.max(0, lines.length - offset);
        const start = Math.max(0, end - MAX_HISTORY_LINES);
        hasMore = start > 0;
        nextOffset = lines.length - start;
        const scanLines = lines.slice(start, end);
        if (hasMore) {
          api.logger.info(`manifold discuss_history (elev) [${sessionId.slice(0, 8)}]: page [${start},${end}) of ${lines.length} lines, more remain before it`);
        }
        for (const line of scanLines) {
          if (!line.trim()) continue;
          try {
            const entry = JSON.parse(line);
            const m = entry?.message;
            if (m && (m.role === "user" || m.role === "assistant")) {
              msgs.push({ role: m.role, content: m.content, timestamp: entry.timestamp });
            }
          } catch { /* skip malformed lines */ }
        }
      }
      return { status: 200, body: { ok: true, messages: filterDiscussHistoryMessages(msgs), hasMore, nextOffset } };
    } catch (err) {
      api.logger.warn(`manifold discuss_history (elev) handler error: ${err?.message ?? err}`);
      return { status: 500, body: { ok: false, code: "internal", error: err?.message ?? String(err) } };
    }
  }

  return { handleDiscussHistory };
}
