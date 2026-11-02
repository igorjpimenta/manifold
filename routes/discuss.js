import path from "node:path";
import os from "node:os";
import { mkdirSync, existsSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { looksLikeMetaNarration, normalizePendingFields } from "./discuss-helpers.js";
import { stampDiscussSession, readDiscussSession } from "./discuss-session.js";
import { persistRevisedDraft, persistRevisedMeta } from "./discuss-draft.js";
import { AUDIO_MIME_EXT, EXT_TO_MIME, audioDirForFlow, loadAudioMetasForFlow } from "./discuss-audio.js";
import { assembleState } from "../engine/executor-helpers.js";

// ── Factory ─────────────────────────────────────────────────────────────────
// All handlers close over the deps passed from index.js / runs.js / operator.js
// rather than using module-level singletons.

// Shared prompt projection for every discuss turn entry point (HTTP route,
// audio route, WS dialog door): feed the gate's promptBuilder the same
// assembleState-enriched state the engine's own dispatch loop reads.
// Returns the prompt string or null (no builder, or the builder threw).
export async function buildDiscussPrompt({ flow, target, runStore, nodeExecutionsStore, logger = null } = {}) {
  if (!target?.promptBuilder) return null;
  try {
    const ctx = {
      // B2.5 — read via assembleState() (resultJson replayed on top of
      // the still-live stateJson) instead of trusting the raw blob
      // directly, matching how the engine's own dispatch loop reads
      // state. Fixes a real gap too: this call site never enriched
      // with _subflowGroups before, so a discuss promptBuilder reading
      // state.loop (the pre-migration field name) always got nothing.
      state: await assembleState(flow.flowId, { runStore, nodeExecutionsStore, baseState: flow.stateJson ?? {} }),
      itemId: target.itemId,
      flowId: flow.flowId,
      pipelineId: flow.stateJson?.pipelineId ?? null,
    };
    return await Promise.resolve(target.promptBuilder(ctx));
  } catch (err) {
    try { logger?.warn?.(`manifold discuss promptBuilder threw for ${flow.flowId.slice(0, 8)}/${target.itemId}: ${err?.message ?? err}`); } catch {}
    return null;
  }
}

export function registerDiscussHandlers({
  api,
  store,
  pipelinesRef,
  emit,
  discussDefaultAgent,
  invokeElevated,
  readPending,
  clearPending,
  discussStore,
  runStore,
  nodeExecutionsStore,
  // Injected gateway adapter, not core state: discuss thread ids come from
  // the harness (sessions.newDiscussSessionId), never minted here — core
  // never imports the gateway's session store (same seam as operator.js).
  sessions,
}) {

  // ── Resolve discuss target ────────────────────────────────────────────────

  function resolveDiscussTarget(sourceFlow, wantItemId) {
    const items = Array.isArray(sourceFlow.waitJson?.items) ? sourceFlow.waitJson.items : [];
    let normalizedItemId = null;
    let gateNodeId = null;
    let branchSubNodeId = null;

    if ((sourceFlow.waitJson?.kind === "human-gate" || sourceFlow.waitJson?.kind === "gate") && items.length > 0) {
      const resolveItemId = (raw) =>
        typeof raw === "string" ? raw : (raw?.id ?? null);
      const item = wantItemId
        ? items.find((it) => resolveItemId(it.itemId) === wantItemId)
        : items[0];
      if (!item) {
        return { ok: false, status: 404, code: "item_not_found", error: `no pending item: ${wantItemId}` };
      }
      normalizedItemId = resolveItemId(item.itemId) ?? String(item.itemId);
      gateNodeId = sourceFlow.waitJson?.nodeId;
    } else if (sourceFlow.status === "running") {
      // Old-model mid-batch path (flow_branches / branchService) removed.
      // All subflow gates now surface their pending items via waitJson.
      return { ok: false, status: 503, code: "gate_transition", error: "gate is transitioning, try again shortly" };
    } else {
      return { ok: false, status: 409, code: "no_pending_gate", error: "flow has no pending gate item to discuss" };
    }

    const pipelineId = sourceFlow.stateJson?.pipelineId;
    const pipeline = pipelineId ? pipelinesRef.value.get(pipelineId) : null;
    if (pipelineId && !pipeline) {
      return { ok: false, status: 503, code: "pipeline_loading", error: "pipeline registry not ready, try again shortly" };
    }
    const topNode = pipeline?.nodes?.[gateNodeId];
    let discuss = null;
    // Cost/lineage attribution wants the MOST SPECIFIC node the discuss
    // session is actually about. For a bare top-level gate that's just
    // gateNodeId; for a gate nested inside a loop's branch, waitJson.nodeId
    // only ever names the LOOP (e.g. "PROPOSE"), never the nested gate sub-
    // node — so every discuss session under any loop's gate was being
    // attributed to the loop itself instead of the specific gate, making
    // real human-review cost look like coarse loop-level activity in the
    // dashboard's cost breakdown. `${gateNodeId}.${subNodeId}` matches the
    // executor's own branch-child naming convention (see nodeKindMap/
    // stepOrder in the dashboard, which key branch children the same way).
    let effectiveNodeId = gateNodeId;
    if (topNode?.kind === "human-gate") {
      discuss = topNode.discuss;
    } else if (topNode?.kind === "subflow") {
      const flowNodes = topNode.flow?.nodes ?? {};
      if (branchSubNodeId && flowNodes[branchSubNodeId]?.kind === "human-gate") {
        discuss = flowNodes[branchSubNodeId].discuss;
        effectiveNodeId = `${gateNodeId}.${branchSubNodeId}`;
      } else {
        for (const [subNodeId, subNode] of Object.entries(flowNodes)) {
          if (subNode?.kind === "human-gate" && subNode?.discuss) {
            discuss = subNode.discuss;
            effectiveNodeId = `${gateNodeId}.${subNodeId}`;
            break;
          }
        }
      }
    }
    const effectiveDiscuss = (discuss && typeof discuss === "object" && discuss.agent)
      ? discuss
      : discussDefaultAgent ? { agent: discussDefaultAgent } : null;
    if (!effectiveDiscuss) {
      return {
        ok: false,
        status: 400,
        code: "discuss_not_supported",
        error: "this gate's pipeline hasn't opted into discussion (set `discuss: { agent, promptBuilder }` on the gate node, or set `discuss.defaultAgent` in the manifold plugin config in openclaw.json)",
      };
    }
    const stamp = readDiscussSession(sourceFlow.flowId, gateNodeId, normalizedItemId);
    const resolvedAgent = stamp?.agent ?? effectiveDiscuss.agent;
    // Discuss thread identity comes from the harness, never from core: a
    // stamped thread keeps its id verbatim (including pre-harness `gf-…`
    // ids, so in-flight threads survive the switch); a new thread asks the
    // harness adapter for one. No minted fallback — a harness that cannot
    // name its own sessions cannot host a discuss thread.
    const sessionId = stamp?.sessionId
      ?? sessions?.newDiscussSessionId?.({
        agentId: resolvedAgent, flowId: sourceFlow.flowId,
        gateNodeId, itemId: normalizedItemId,
      });
    if (typeof sessionId !== "string" || !sessionId) {
      return {
        ok: false,
        status: 503,
        code: "discuss_session_unavailable",
        error: "harness did not provide a discuss session id (sessions.newDiscussSessionId missing or empty)",
      };
    }
    // The key IS the session id — harness-namespaced (`agent:<id>:…`), so
    // the agent rides on the key and history lookups resolve it directly.
    const sessionKey = sessionId;
    return {
      ok: true,
      sessionKey,
      sessionId,
      itemId: normalizedItemId,
      agent: resolvedAgent,
      needsStamp: !stamp,
      // Coarse node id the discuss session itself is keyed/stamped under
      // (readDiscussSession/stampDiscussSession above) — the loop's own id
      // for a branch-mode gate, not the fine-grained per-sub-node id below.
      gateNodeId,
      // Specific node this discuss session is actually about, for cost/
      // lineage attribution — "PROPOSE.GATE", not just "PROPOSE", when the
      // gate lives inside a loop's branch. The sidecar stays keyed on the
      // coarser gateNodeId above (a harness id is opaque to core, so only
      // the (flow, gate, item) coordinates re-find the thread).
      nodeId: effectiveNodeId,
      modelOverride: typeof effectiveDiscuss.model === "string" && effectiveDiscuss.model.trim()
        ? effectiveDiscuss.model.trim()
        : null,
      promptBuilder: typeof effectiveDiscuss.promptBuilder === "function" ? effectiveDiscuss.promptBuilder : null,
    };
  }

  // ── handleDiscuss ─────────────────────────────────────────────────────────

  async function handleDiscuss(req, res, sourceFlow) {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    let body;
    try { body = raw ? JSON.parse(raw) : {}; }
    catch { res.statusCode = 400; res.end("invalid json"); return true; }
    const text = typeof body.text === "string" ? body.text.trim() : "";
    if (!text) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "missing_text", error: "text is required" }));
      return true;
    }
    const wantItemId = typeof body.itemId === "string" ? body.itemId : null;
    const target = resolveDiscussTarget(sourceFlow, wantItemId);
    if (!target.ok) {
      res.statusCode = target.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
      return true;
    }
    let systemPrompt = await buildDiscussPrompt({ flow: sourceFlow, target, runStore, nodeExecutionsStore, logger: api.logger });
    const turnId = randomUUID();
    const flowIdShort = sourceFlow.flowId.slice(0, 8);
    res.statusCode = 202;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, turnId }));

    try {
      await discussStore.appendMessage(sourceFlow.flowId, target.itemId, {
        turnId, role: "operator", text, ts: Date.now(),
      });
      await discussStore.setTurnStatus(sourceFlow.flowId, target.itemId, "running", turnId);
    } catch (err) {
      api.logger.warn(`discuss-store write (operator msg): ${err?.message ?? err}`);
    }

    if (target.needsStamp) {
      stampDiscussSession(sourceFlow.flowId, target.gateNodeId ?? "", target.itemId, target.agent, target.sessionId);
      api.runtime?.lineage?.recordSpawn?.({ childSessionKey: target.sessionKey, flowId: sourceFlow.flowId, nodeId: target.nodeId?.toLowerCase() ?? null, itemKey: target.itemId, pipelineId: sourceFlow.stateJson?.pipelineId ?? null, agentId: target.agent ?? null, kind: "flow-gate-discuss", source: "spawn_hook" });
    }

    setImmediate(async () => {
      try {
        const elev = await invokeElevated("/manifold/_elev/discuss", {
          flowId: sourceFlow.flowId,
          sessionKey: target.sessionKey,
          sessionId: target.sessionId,
          agentId: target.agent,
          gateNodeId: target.gateNodeId,
          modelOverride: target.modelOverride,
          text,
          itemId: target.itemId,
          systemPrompt: typeof systemPrompt === "string" ? systemPrompt : null,
        }, { timeoutMs: 360_000 });
        let payload;
        try { payload = JSON.parse(elev.body || "{}"); } catch { payload = {}; }
        const succeeded = elev.ok && payload.ok;
        try {
          if (succeeded) {
            await discussStore.appendMessage(sourceFlow.flowId, target.itemId, {
              turnId, role: "agent",
              text: payload.reply ?? "",
              proposedDraft: payload.proposedDraft ?? null,
              proposedMeta: payload.proposedMeta ?? null,
              ts: Date.now(),
            });
            await discussStore.clearPending(sourceFlow.flowId, target.itemId);
            await discussStore.setTurnStatus(sourceFlow.flowId, target.itemId, "idle", null);
          } else {
            const errText = payload.error || payload.code || elev.error || `HTTP ${elev.status}`;
            await discussStore.stampMessageError(sourceFlow.flowId, target.itemId, turnId, errText);
            await discussStore.setTurnStatus(sourceFlow.flowId, target.itemId, "error", null);
          }
        } catch (err) {
          api.logger.warn(`discuss-store write (agent reply): ${err?.message ?? err}`);
        }
        const turnKind = succeeded ? "session.transcript.completed" : "session.transcript.failed";
        const emitRes = await emit(turnKind, succeeded
          ? {
              scope: "discuss",
              turnId,
              flowId: sourceFlow.flowId,
              itemId: target.itemId,
              text: payload.reply ?? "",
              durationMs: payload.durationMs ?? null,
              emittedAtMs: Date.now(),
            }
          : {
              scope: "discuss",
              turnId,
              flowId: sourceFlow.flowId,
              itemId: target.itemId,
              error: payload.error || payload.code || elev.error || `HTTP ${elev.status}`,
              emittedAtMs: Date.now(),
            });
        if (!emitRes.ok) {
          api.logger.warn(`manifold discuss async [${flowIdShort}/${turnId.slice(0, 8)}]: canonical emit failed`);
        } else {
          api.logger.info(`manifold discuss async [${flowIdShort}/${turnId.slice(0, 8)}]: ${turnKind} emitted`);
        }
      } catch (err) {
        api.logger.warn(`manifold discuss async [${flowIdShort}/${turnId.slice(0, 8)}]: runner error: ${err?.message ?? err}`);
        try {
          await discussStore.stampMessageError(sourceFlow.flowId, target.itemId, turnId, err?.message ?? String(err));
          await discussStore.setTurnStatus(sourceFlow.flowId, target.itemId, "error", null);
        } catch {}
        await emit("session.transcript.failed", {
          scope: "discuss",
          turnId,
          flowId: sourceFlow.flowId,
          itemId: target.itemId,
          error: err?.message ?? String(err),
        });
      }
    });
    return true;
  }

  // ── transcribeAndDispatch ─────────────────────────────────────────────────
  // Shared by handleDiscussAudio (fresh upload) and handleDiscussAudioRetry
  // (re-transcribe an already-persisted file, e.g. after the operator taps
  // Retry on a failed voice message) — transcribes, updates the meta
  // sidecar, pushes session.transcript.updated, and on success dispatches
  // the agent turn + session.transcript.completed. Callers wrap this in
  // setImmediate since the HTTP response has already been sent by the time
  // it runs.

  async function transcribeAndDispatch({
    sourceFlow, target, turnId, audioPath, audioUrl, metaPath,
    mime, language, systemPrompt, createdAtMs, logPrefix,
  }) {
    const mu = api.runtime?.mediaUnderstanding;
    let transcript = "";
    let transcribeError = null;
    try {
      const muResult = await mu.transcribeAudioFile({ cfg: api.config, filePath: audioPath, mime, language });
      transcript = typeof muResult?.text === "string" ? muResult.text.trim() : "";
    } catch (err) {
      transcribeError = err?.message ?? String(err);
      api.logger.warn(`${logPrefix}: transcribe failed: ${transcribeError}`);
    }

    try {
      const { writeFile: writeFileFs } = await import("node:fs/promises");
      const status = transcribeError ? "failed" : transcript ? "completed" : "empty";
      await writeFileFs(metaPath, JSON.stringify({
        turnId, itemId: target.itemId, audioUrl,
        transcript: transcript || null,
        status, error: transcribeError || null,
        createdAtMs, transcribedAtMs: Date.now(),
      }, null, 2));
    } catch (err) {
      api.logger.warn(`${logPrefix}: meta sidecar update failed: ${err?.message ?? err}`);
    }

    const transcriptStatus = transcribeError ? "failed" : transcript ? "completed" : "empty";
    await emit("session.transcript.updated", {
      scope: "discuss",
      turnId,
      flowId: sourceFlow.flowId,
      itemId: target.itemId,
      status: transcriptStatus,
      text: transcript || null,
      audioUrl,
      error: transcribeError,
      emittedAtMs: Date.now(),
    });

    if (transcriptStatus !== "completed") return;

    try {
      const elev = await invokeElevated("/manifold/_elev/discuss", {
        flowId: sourceFlow.flowId,
        sessionKey: target.sessionKey,
        sessionId: target.sessionId,
        agentId: target.agent,
        gateNodeId: target.gateNodeId,
        modelOverride: target.modelOverride,
        text: transcript,
        itemId: target.itemId,
        systemPrompt: typeof systemPrompt === "string" ? systemPrompt : null,
      }, { timeoutMs: 360_000 });
      let payload;
      try { payload = JSON.parse(elev.body || "{}"); } catch { payload = {}; }
      const turnKind = elev.ok && payload.ok
        ? "session.transcript.completed"
        : "session.transcript.failed";
      const emitRes = await emit(turnKind, elev.ok && payload.ok
        ? {
            scope: "discuss",
            turnId,
            flowId: sourceFlow.flowId,
            itemId: target.itemId,
            text: payload.reply ?? "",
            durationMs: payload.durationMs ?? null,
            operatorAudioUrl: audioUrl,
            operatorTranscript: transcript,
            emittedAtMs: Date.now(),
          }
        : {
            scope: "discuss",
            turnId,
            flowId: sourceFlow.flowId,
            itemId: target.itemId,
            error: payload.error || payload.code || elev.error || `HTTP ${elev.status}`,
            operatorAudioUrl: audioUrl,
            operatorTranscript: transcript,
            emittedAtMs: Date.now(),
          });
      if (!emitRes.ok) {
        api.logger.warn(`${logPrefix}: canonical emit failed`);
      }
    } catch (err) {
      api.logger.warn(`${logPrefix}: runner error: ${err?.message ?? err}`);
      await emit("session.transcript.failed", {
        scope: "discuss",
        turnId,
        flowId: sourceFlow.flowId,
        itemId: target.itemId,
        error: err?.message ?? String(err),
        operatorAudioUrl: audioUrl,
        operatorTranscript: transcript,
        emittedAtMs: Date.now(),
      });
    }
  }

  // ── handleDiscussAudio ────────────────────────────────────────────────────

  async function handleDiscussAudio(req, res, sourceFlow) {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    let body;
    try { body = raw ? JSON.parse(raw) : {}; }
    catch { res.statusCode = 400; res.end("invalid json"); return true; }
    const audioB64 = typeof body.audio === "string" ? body.audio : "";
    const contentType = typeof body.contentType === "string" ? body.contentType.toLowerCase() : "audio/m4a";
    const language = typeof body.language === "string" ? body.language : undefined;
    if (!audioB64) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "missing_audio", error: "audio (base64) is required" }));
      return true;
    }
    const ext = AUDIO_MIME_EXT[contentType] ?? "m4a";
    const wantItemId = typeof body.itemId === "string" ? body.itemId : null;
    const target = resolveDiscussTarget(sourceFlow, wantItemId);
    if (!target.ok) {
      res.statusCode = target.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
      return true;
    }
    const mu = api.runtime?.mediaUnderstanding;
    if (!mu?.transcribeAudioFile) {
      res.statusCode = 503;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "media_understanding_unavailable", error: "no audio transcription provider bound" }));
      return true;
    }
    const turnId = randomUUID();
    const flowIdShort = sourceFlow.flowId.slice(0, 8);
    const audioDir = audioDirForFlow(sourceFlow.flowId, target.itemId);
    const audioPath = path.join(audioDir, `${turnId}.${ext}`);
    try {
      const { mkdir, writeFile } = await import("node:fs/promises");
      await mkdir(audioDir, { recursive: true });
      await writeFile(audioPath, Buffer.from(audioB64, "base64"));
    } catch (err) {
      api.logger.warn(`manifold discuss/audio [${flowIdShort}/${turnId.slice(0, 8)}]: persist failed: ${err?.message ?? err}`);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "audio_persist_failed", error: err?.message ?? String(err) }));
      return true;
    }
    const audioUrl = `/manifold/runs/${sourceFlow.flowId}/discuss/audio/${turnId}.${ext}`;
    const metaPath = path.join(audioDir, `${turnId}.meta.json`);
    const createdAtMs = Date.now();
    try {
      const { writeFile: writeFileFs } = await import("node:fs/promises");
      await writeFileFs(metaPath, JSON.stringify({
        turnId, itemId: target.itemId, audioUrl, transcript: null, status: "pending", createdAtMs,
      }, null, 2));
    } catch (err) {
      api.logger.warn(`manifold discuss/audio [${flowIdShort}/${turnId.slice(0, 8)}]: pending meta sidecar failed: ${err?.message ?? err}`);
    }

    res.statusCode = 202;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, turnId, audioUrl }));

    let systemPrompt = await buildDiscussPrompt({ flow: sourceFlow, target, runStore, nodeExecutionsStore, logger: api.logger });

    setImmediate(() => transcribeAndDispatch({
      sourceFlow, target, turnId, audioPath, audioUrl, metaPath,
      mime: contentType, language, systemPrompt, createdAtMs,
      logPrefix: `manifold discuss/audio async [${flowIdShort}/${turnId.slice(0, 8)}]`,
    }));
    return true;
  }

  // ── handleDiscussAudioRetry ───────────────────────────────────────────────
  // Re-runs transcription on an already-uploaded voice message (identified
  // by turnId) without re-uploading the audio bytes — the operator's Retry
  // tap on a failed/empty transcription bubble. Reuses the same turnId and
  // audioUrl so the existing bubble updates in place via the session
  // transcript events rather than creating a new turn.

  async function handleDiscussAudioRetry(req, res, sourceFlow) {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    let body;
    try { body = raw ? JSON.parse(raw) : {}; }
    catch { res.statusCode = 400; res.end("invalid json"); return true; }
    const turnId = typeof body.turnId === "string" ? body.turnId : "";
    if (!turnId) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "missing_turn_id", error: "turnId is required" }));
      return true;
    }
    const wantItemId = typeof body.itemId === "string" ? body.itemId : null;
    const target = resolveDiscussTarget(sourceFlow, wantItemId);
    if (!target.ok) {
      res.statusCode = target.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
      return true;
    }
    const mu = api.runtime?.mediaUnderstanding;
    if (!mu?.transcribeAudioFile) {
      res.statusCode = 503;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "media_understanding_unavailable", error: "no audio transcription provider bound" }));
      return true;
    }
    const flowIdShort = sourceFlow.flowId.slice(0, 8);
    const audioDir = audioDirForFlow(sourceFlow.flowId, target.itemId);
    const metaPath = path.join(audioDir, `${turnId}.meta.json`);
    let meta;
    try {
      const { readFile } = await import("node:fs/promises");
      meta = JSON.parse(await readFile(metaPath, "utf8"));
    } catch {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "turn_not_found", error: "no audio recorded for this turnId" }));
      return true;
    }
    const ext = (typeof meta.audioUrl === "string" ? meta.audioUrl.split(".").pop() : null) || "m4a";
    const audioPath = path.join(audioDir, `${turnId}.${ext}`);
    if (!existsSync(audioPath)) {
      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "audio_missing", error: "original audio file no longer on disk" }));
      return true;
    }
    const audioUrl = meta.audioUrl;

    let systemPrompt = null;
    if (target.promptBuilder) {
      try {
        const ctx = {
          state: await assembleState(sourceFlow.flowId, { runStore, nodeExecutionsStore, baseState: sourceFlow.stateJson ?? {} }),
          itemId: target.itemId,
          flowId: sourceFlow.flowId,
          pipelineId: sourceFlow.stateJson?.pipelineId ?? null,
        };
        systemPrompt = await Promise.resolve(target.promptBuilder(ctx));
      } catch (err) {
        api.logger.warn(`manifold discuss/audio/retry promptBuilder threw for ${flowIdShort}/${target.itemId}: ${err?.message ?? err}`);
      }
    }

    res.statusCode = 202;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, turnId, audioUrl }));

    setImmediate(() => transcribeAndDispatch({
      sourceFlow, target, turnId, audioPath, audioUrl, metaPath,
      mime: EXT_TO_MIME[ext] ?? "audio/m4a", language: undefined, systemPrompt,
      createdAtMs: typeof meta.createdAtMs === "number" ? meta.createdAtMs : Date.now(),
      logPrefix: `manifold discuss/audio/retry async [${flowIdShort}/${turnId.slice(0, 8)}]`,
    }));
    return true;
  }

  // ── handleDiscussAudioGet ─────────────────────────────────────────────────

  async function handleDiscussAudioGet(req, res, sourceFlow, audioFileName) {
    if (!/^[a-zA-Z0-9-]{8,64}\.[a-zA-Z0-9]{1,8}$/.test(audioFileName)) {
      res.statusCode = 400;
      res.end("invalid audio file name");
      return true;
    }
    const ext = audioFileName.split(".").pop().toLowerCase();
    const { readdir, readFile, stat } = await import("node:fs/promises");
    const flowRoot = path.join(os.homedir(), ".openclaw", "discuss-audio", sourceFlow.flowId);
    let entries;
    try { entries = await readdir(flowRoot); }
    catch { res.statusCode = 404; res.end("not found"); return true; }
    for (const itemDir of entries) {
      const candidate = path.join(flowRoot, itemDir, audioFileName);
      try {
        const st = await stat(candidate);
        if (!st.isFile()) continue;
        const bytes = await readFile(candidate);
        res.statusCode = 200;
        res.setHeader("Content-Type", EXT_TO_MIME[ext] ?? "application/octet-stream");
        res.setHeader("Content-Length", String(bytes.length));
        res.setHeader("Cache-Control", "private, max-age=86400");
        res.end(bytes);
        return true;
      } catch { /* try next */ }
    }
    res.statusCode = 404;
    res.end("not found");
    return true;
  }

  // ── handleDiscussHistory ──────────────────────────────────────────────────

  async function handleDiscussHistory(req, res, sourceFlow) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const wantItemId = url.searchParams.get("itemId");
    const offsetParam = Number(url.searchParams.get("offset"));
    const offset = Number.isFinite(offsetParam) && offsetParam > 0 ? Math.floor(offsetParam) : 0;
    const target = resolveDiscussTarget(sourceFlow, wantItemId);
    if (!target.ok) {
      res.statusCode = target.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
      return true;
    }
    const dsState = await discussStore.getState(sourceFlow.flowId, target.itemId);
    let parsed;
    if (dsState && dsState.messages.length > 0) {
      parsed = {
        ok: true,
        messages: dsState.messages.map((m) => ({
          isOperator: m.role === "operator",
          text: m.text ?? null,
          ts: m.ts ?? null,
          turnId: m.turnId ?? null,
          audioKey: m.audioKey ?? null,
          proposedDraft: m.proposedDraft ?? null,
          proposedMeta: m.proposedMeta ?? null,
          error: m.error ?? null,
        })),
        turnStatus: dsState.turnStatus,
        activeTurnId: dsState.activeTurnId ?? null,
        // In-memory store always returns the full thread — nothing paged
        // out, so there's never more to load from this branch.
        hasMore: false,
      };
      const dsPending = dsState.pending;
      const fsPending = dsPending ? null : readPending(sourceFlow.flowId, target.itemId);
      const pending = dsPending || fsPending;
      if (pending && (pending.text || (pending.fields && Object.keys(pending.fields ?? {}).length > 0))) {
        parsed.pending = {
          text: typeof pending.text === "string" ? pending.text : null,
          fields: pending.fields && typeof pending.fields === "object" ? normalizePendingFields(pending.fields) : null,
          updatedAt: typeof pending.updatedAt === "number" ? pending.updatedAt : null,
        };
      }
    } else {
      const fsPending = readPending(sourceFlow.flowId, target.itemId);
      const hasFsPending = fsPending && (fsPending.text || (fsPending.fields && Object.keys(fsPending.fields ?? {}).length > 0));
      if (hasFsPending) {
        parsed = { ok: true, messages: [] };
        parsed.pending = {
          text: typeof fsPending.text === "string" ? fsPending.text : null,
          fields: fsPending.fields && typeof fsPending.fields === "object" ? normalizePendingFields(fsPending.fields) : null,
          updatedAt: typeof fsPending.updatedAt === "number" ? fsPending.updatedAt : null,
        };
      } else {
        const elev = await invokeElevated("/manifold/_elev/discuss_history", {
          flowId: sourceFlow.flowId,
          sessionKey: target.sessionKey,
          sessionId: target.sessionId,
        });
        try {
          parsed = JSON.parse(elev.body || "{}");
        } catch { parsed = { ok: false, code: "elevation_no_body" }; }
        if (parsed && parsed.ok) {
          const pending = readPending(sourceFlow.flowId, target.itemId);
          if (pending && (pending.text || (pending.fields && Object.keys(pending.fields).length > 0))) {
            parsed.pending = {
              text: typeof pending.text === "string" ? pending.text : null,
              fields: pending.fields && typeof pending.fields === "object" ? normalizePendingFields(pending.fields) : null,
              updatedAt: typeof pending.updatedAt === "number" ? pending.updatedAt : null,
            };
          }
        }
      }
      parsed.turnStatus = dsState?.turnStatus ?? "idle";
      parsed.activeTurnId = dsState?.activeTurnId ?? null;
    }
    try {
      if (parsed && parsed.ok) {
        parsed.messages = Array.isArray(parsed.messages) ? parsed.messages : [];
        const audioMetas = await loadAudioMetasForFlow(sourceFlow.flowId);
        if (audioMetas.length > 0) {
          const matchedTurnIds = new Set();
          for (const msg of parsed.messages) {
            if (!msg || !msg.isOperator) continue;
            const t = typeof msg.text === "string" ? msg.text.trim() : "";
            if (!t) continue;
            const meta = audioMetas.find((m) =>
              typeof m.transcript === "string"
              && m.transcript.trim() === t
              && !matchedTurnIds.has(m.turnId)
            );
            if (meta && typeof meta.audioUrl === "string") {
              msg.audioUrl = meta.audioUrl;
              msg.turnId = meta.turnId;
              matchedTurnIds.add(meta.turnId);
            }
          }
          for (const meta of audioMetas) {
            if (!meta || !meta.turnId || !meta.audioUrl) continue;
            if (matchedTurnIds.has(meta.turnId)) continue;
            if (target.itemId && meta.itemId && meta.itemId !== target.itemId) continue;
            const status = typeof meta.status === "string" ? meta.status : "completed";
            const isPending = status === "pending";
            const audioTs = typeof meta.createdAtMs === "number" ? meta.createdAtMs : 0;
            const hasSubsequentAgentReply = parsed.messages.some(
              (m) => m && !m.isOperator && (m.ts ?? 0) > audioTs
            );
            parsed.messages.push({
              isOperator: true,
              text: typeof meta.transcript === "string" ? meta.transcript : "",
              ts: audioTs || null,
              audioUrl: meta.audioUrl,
              turnId: meta.turnId,
              transcribing: isPending,
              pendingAgentReply: !hasSubsequentAgentReply && (status === "completed" || isPending),
              transcribeStatus: status,
              error: status === "failed" ? (meta.error || null) : null,
            });
          }
          parsed.messages.sort((a, b) => (a.ts ?? 0) - (b.ts ?? 0));
        }
      }
    } catch (err) {
      api.logger.warn(`manifold discuss history enrichment error: ${err?.message ?? err}`);
      if (!parsed) parsed = { ok: false, code: "internal", error: err?.message ?? String(err) };
    }
    if (parsed?.ok) {
      // No channel plugin anymore: replies go back through the same HTTP
      // discuss route this history came from, or the WS dialog door.
      parsed.channelId = "manifold-dialog";
      parsed.channelInboundUrl = null;
    }
    res.statusCode = (parsed?.ok) ? 200 : 500;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify(parsed ?? { ok: false, code: "internal" }));
    return true;
  }

  // ── handleDiscussApplyDraft ───────────────────────────────────────────────

  async function handleDiscussApplyDraft(req, res, sourceFlow) {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    let body;
    try { body = raw ? JSON.parse(raw) : {}; }
    catch { res.statusCode = 400; res.end("invalid json"); return true; }
    let text = typeof body.text === "string" ? body.text.trim() : "";
    let fields = (body.fields && typeof body.fields === "object" && !Array.isArray(body.fields))
      ? body.fields
      : null;
    const source = typeof body.source === "string" ? body.source : null;
    const wantItemId = typeof body.itemId === "string" ? body.itemId : null;
    // Resolve target first so we can use target.itemId for the sidecar
    // lookup before checking whether payload is present.
    const target = resolveDiscussTarget(sourceFlow, wantItemId);
    if (!target.ok) {
      res.statusCode = target.status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
      return true;
    }
    // No explicit payload — fall back to the filesystem sidecar so a client
    // can commit a pre-built proposal with a single tap without
    // needing to fetch and re-POST the full draft text.
    if (!text && !fields) {
      const sidecar = readPending(sourceFlow.flowId, target.itemId);
      if (sidecar?.text) text = sidecar.text;
      if (sidecar?.fields && typeof sidecar.fields === "object" && !Array.isArray(sidecar.fields)) {
        fields = sidecar.fields;
      }
    }
    if (!text && !fields) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: false, code: "missing_payload", error: "text and/or fields is required" }));
      return true;
    }
    const summary = [];
    if (text) {
      const r = await persistRevisedDraft({
        flowId: sourceFlow.flowId,
        itemId: target.itemId,
        newDraft: text,
        source,
        store,
        logger: api.logger,
      });
      if (!r.applied) {
        api.logger.warn(`manifold discuss apply-draft [${sourceFlow.flowId.slice(0, 8)}]: body persist failed: ${r.code}`);
        res.statusCode = r.code === "rejected_meta_narration" ? 400 : 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: r.code }));
        return true;
      }
      summary.push(`body ${text.length}ch`);
    }
    if (fields) {
      const r = await persistRevisedMeta({
        flowId: sourceFlow.flowId,
        itemId: target.itemId,
        fields,
        store,
        logger: api.logger,
      });
      if (!r.applied) {
        api.logger.warn(`manifold discuss apply-draft [${sourceFlow.flowId.slice(0, 8)}]: meta persist failed: ${r.code}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, code: r.code }));
        return true;
      }
      summary.push(`meta=${Object.keys(fields).join("+")}`);
    }
    // INTENTIONAL: no gateResolver.recordItemDecision here.
    // Apply revises; Approve commits. They are separate operator intents.
    try { clearPending(sourceFlow.flowId, target.itemId); } catch {}
    api.logger.info(`manifold discuss apply-draft [${sourceFlow.flowId.slice(0, 8)}]: applied (${summary.join(", ")})`);
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  // ── Return all exported symbols ───────────────────────────────────────────

  return {
    handleDiscuss,
    handleDiscussAudio,
    handleDiscussAudioGet,
    handleDiscussAudioRetry,
    handleDiscussHistory,
    handleDiscussApplyDraft,
    stampDiscussSession,
    resolveDiscussTarget,
    normalizePendingFields,
  };
}
