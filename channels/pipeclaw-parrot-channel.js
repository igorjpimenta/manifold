// pipeclaw-parrot-channel.js — Pipeclaw Discuss channel plugin.
//
// Full SDK channel: inbound via dispatchInboundDirectDmWithRuntime, outbound
// via the deliver callback → SSE (emitSseEvent). The HTTP route is a thin
// bridge — auth, flow/target resolution, system-prompt injection — then
// hands off to the channel runtime for agent dispatch.
//
// Entry point: call createPipeclawParrotChannelEntry(deps).register(api) from
// inside pipeclaw's registerFull, after emitSseEvent is wired up.

import { createChatChannelPlugin, defineChannelPluginEntry } from "openclaw/plugin-sdk/channel-core";
import { dispatchInboundDirectDmWithRuntime } from "openclaw/plugin-sdk/channel-inbound";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { gatewayWireName } from "../events/kinds.js";

export function createPipeclawParrotChannelEntry({
  readStore,
  discussStore,
  resolveDiscussTarget,
  stampDiscussSession,
  emitSseEvent,
  emit,
}) {
  // Canonical-kind fan-out (events roadmap slice 2) with the same
  // emitSseEvent-backed fallback as routes/discuss.js — pre-existing
  // turn tests pin the fallback path unchanged.
  const emitKind = emit ?? (async (kind, payload) => emitSseEvent({ ...payload, type: gatewayWireName(kind) ?? kind }));
  // Captured once during startAccount; used by the HTTP handler.
  let channelRuntimeRef = null;
  let cfgRef = null;

  const plugin = {
    ...createChatChannelPlugin({
      base: {
        id: "pipeclaw-parrot",
        meta: {
          id: "pipeclaw-parrot",
          label: "PipeClaw Parrot",
          selectionLabel: "PipeClaw Parrot",
          docsPath: "",
          blurb: "Full-tool-access agent sessions for operator ↔ agent dialogue",
        },
        capabilities: { chatTypes: ["direct"] },
        config: {
          listAccountIds: (_cfg) => ["default"],
          resolveAccount: (_cfg, accountId) => ({ accountId, enabled: true }),
        },
      },
      outbound: {
        deliveryMode: "direct",
        sendText: async (_ctx) => {
          // Loopback channel — subagent announces are handled internally by the
          // sessions runtime. Returning ok here unblocks sessions_yield without
          // needing to re-inject the payload through a second inbound dispatch.
          return { ok: true, messageId: randomUUID() };
        },
      },
    }),
    gateway: {
      startAccount: async (ctx) => {
        channelRuntimeRef = ctx.channelRuntime;
        cfgRef = ctx.cfg;
        ctx.log?.info(`[pipeclaw-parrot] channel runtime ready`);
        // Keep the account "running" until the gateway shuts down.
        await new Promise((_resolve, reject) => {
          if (ctx.abortSignal?.aborted) { reject(new Error("aborted")); return; }
          ctx.abortSignal?.addEventListener("abort", () => reject(new Error("aborted")));
        });
      },
    },
  };

  return defineChannelPluginEntry({
    id: "pipeclaw-parrot",
    name: "PipeClaw Parrot",
    description: "Full-tool-access channel for operator ↔ agent direct sessions",
    plugin,
    registerFull(api) {
      api.registerHttpRoute({
        path: "/channels/pipeclaw-parrot/message",
        auth: "plugin",
        match: "exact",
        handler: async (req, res) => {
          if (req.method !== "POST") {
            res.statusCode = 405;
            res.setHeader("Allow", "POST");
            res.end("method not allowed");
            return true;
          }

          const { authorizeBridgeRequest, sendUnauthorized } = await import("../ios/auth.js");
          const authz = await authorizeBridgeRequest(req);
          if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }

          let raw = "";
          for await (const chunk of req) raw += chunk;
          let body;
          try { body = raw ? JSON.parse(raw) : {}; }
          catch { res.statusCode = 400; res.end("invalid json"); return true; }

          const flowId = typeof body.flowId === "string" ? body.flowId.trim() : "";
          const text = typeof body.text === "string" ? body.text.trim() : "";
          const wantItemId = typeof body.itemId === "string" ? body.itemId : null;

          if (!flowId || !text) {
            res.statusCode = 400;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ ok: false, code: "invalid_body", error: "flowId and text required" }));
            return true;
          }

          if (!channelRuntimeRef || !cfgRef) {
            res.statusCode = 503;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ ok: false, code: "channel_not_ready", error: "pipeclaw-parrot channel runtime not initialized" }));
            return true;
          }

          const flow = await readStore.get(flowId);
          if (!flow) {
            res.statusCode = 404;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ ok: false, code: "flow_not_found", error: `flow not found: ${flowId}` }));
            return true;
          }

          const target = resolveDiscussTarget(flow, wantItemId);
          if (!target.ok) {
            res.statusCode = target.status;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ ok: false, code: target.code, error: target.error }));
            return true;
          }

          let systemPrompt = null;
          if (target.promptBuilder) {
            try {
              systemPrompt = await Promise.resolve(target.promptBuilder({
                state: flow.stateJson ?? {},
                itemId: target.itemId,
                flowId: flow.flowId,
                pipelineId: flow.stateJson?.pipelineId ?? null,
              }));
            } catch (err) {
              api.logger.warn(`pipeclaw-parrot: promptBuilder threw for ${flow.flowId.slice(0, 8)}/${target.itemId}: ${err?.message ?? err}`);
            }
          }

          const turnId = randomUUID();
          const flowIdShort = flow.flowId.slice(0, 8);
          const gateNodeId = flow.waitJson?.nodeId?.toLowerCase() ?? "gate";
          // peerId encodes flow+gate+item — unique per discuss session.
          const peerId = `${flowIdShort}-${gateNodeId}-${target.itemId}`;

          // systemPrompt is injected via GroupSystemPrompt in extraContext — lands in
          // the agent's system prompt slot via the reply engine, not in the message body.
          const extraContext = systemPrompt ? { GroupSystemPrompt: systemPrompt } : undefined;

          res.statusCode = 202;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: true, turnId }));

          try {
            await discussStore.appendMessage(flow.flowId, target.itemId, {
              turnId, role: "operator", text, ts: Date.now(),
            });
            await discussStore.setTurnStatus(flow.flowId, target.itemId, "running", turnId);
          } catch (err) {
            api.logger.warn(`pipeclaw-parrot: discuss-store write (operator msg): ${err?.message ?? err}`);
          }

          import("node:fs/promises").then(({ mkdir, writeFile }) => {
            const dir = path.join(os.homedir(), ".openclaw", "discuss-turns", flow.flowId);
            return mkdir(dir, { recursive: true }).then(() =>
              writeFile(
                path.join(dir, `${target.itemId}.pending.json`),
                JSON.stringify({ turnId, text, receivedAtMs: Date.now() })
              )
            );
          }).catch(() => {});

          setImmediate(async () => {
            const startMs = Date.now();
            try {
              // Override resolveAgentRoute so each discuss.agent gets its own
              // channel session + tool scope, regardless of static bindings.
              const targetAgent = target.agent;
              const targetSessionKey = `agent:${targetAgent}:pipeclaw-parrot:direct:${peerId}`;
              const result = await dispatchInboundDirectDmWithRuntime({
                cfg: cfgRef,
                channel: "pipeclaw-parrot",
                accountId: "default",
                peer: { kind: "direct", id: peerId },
                runtime: {
                  channel: {
                    ...channelRuntimeRef,
                    routing: {
                      resolveAgentRoute: () => ({
                        agentId: targetAgent,
                        sessionKey: targetSessionKey,
                        accountId: "default",
                      }),
                    },
                  },
                },
                channelLabel: "pipeclaw-parrot",
                conversationLabel: `flow:${flowIdShort}/${target.itemId}`,
                rawBody: text,
                bodyForAgent: text,
                senderId: "operator",
                senderAddress: "operator",
                recipientAddress: "pipeclaw-parrot",
                messageId: turnId,
                timestamp: new Date().toISOString(),
                ...(extraContext ? { extraContext } : {}),
                deliver: async (payload) => {
                  const replyText = typeof payload === "string" ? payload
                    : (payload?.text ?? payload?.body ?? payload?.content ?? "");
                  // Components are seeded by tools during the turn via componentEmitter.
                  // Read them here for: (a) backward-compat proposedDraft/proposedMeta on
                  // the message, (b) including in the discuss.turn SSE for state sync.
                  let proposedDraft = null;
                  let proposedMeta = null;
                  let components = [];
                  try {
                    components = (await discussStore?.getComponents?.(flow.flowId, target.itemId)) ?? [];
                    const draftComp = components.find((c) => c.kind === "proposed_draft");
                    const metaComp  = components.find((c) => c.kind === "proposed_meta");
                    if (draftComp) proposedDraft = draftComp.text ?? null;
                    if (metaComp)  proposedMeta  = metaComp.fields ?? null;
                  } catch {}

                  const durationMs = Date.now() - startMs;
                  try {
                    await discussStore.appendMessage(flow.flowId, target.itemId, {
                      turnId, role: "agent",
                      text: replyText,
                      proposedDraft,
                      proposedMeta,
                      ts: Date.now(),
                    });
                    await discussStore.clearPending(flow.flowId, target.itemId);
                    await discussStore.setTurnStatus(flow.flowId, target.itemId, "idle", null);
                  } catch (err) {
                    api.logger.warn(`pipeclaw-parrot: discuss-store write (agent reply): ${err?.message ?? err}`);
                  }

                  // Canonical text field alongside legacy `reply` (iOS shape).
                  const pushRes = await emitKind("session.transcript.completed", {
                    scope: "discuss",
                    text: replyText,
                    turnId,
                    flowId: flow.flowId,
                    itemId: target.itemId,
                    status: "completed",
                    reply: replyText,
                    proposedDraft,
                    proposedMeta,
                    components,
                    durationMs,
                    emittedAtMs: Date.now(),
                  });
                  if (!pushRes.ok) {
                    api.logger.warn(`pipeclaw-parrot [${flowIdShort}/${turnId.slice(0, 8)}]: SSE push failed`);
                  } else {
                    api.logger.info(`pipeclaw-parrot [${flowIdShort}/${turnId.slice(0, 8)}]: completed → SSE pushed`);
                  }
                },
                onRecordError: (err) => {
                  api.logger.warn(`pipeclaw-parrot [${flowIdShort}]: record error: ${err?.message ?? err}`);
                },
                onDispatchError: async (err) => {
                  try {
                    await discussStore.stampMessageError(flow.flowId, target.itemId, turnId, err?.message ?? String(err));
                    await discussStore.setTurnStatus(flow.flowId, target.itemId, "error", null);
                  } catch {}
                  await emitKind("session.transcript.failed", {
                    scope: "discuss",
                    turnId,
                    flowId: flow.flowId,
                    itemId: target.itemId,
                    status: "failed",
                    error: err?.message ?? String(err),
                  });
                },
              });

              // Stamp after first-turn dispatch so subsequent turns are not re-stamped.
              // Store peerId (not full sessionKey) so resolveDiscussTarget doesn't double-prefix it.
              if (target.needsStamp) {
                stampDiscussSession(flow.flowId, gateNodeId, target.itemId, target.agent, peerId);
                api.runtime?.lineage?.recordSpawn?.({
                  childSessionKey: result?.route?.sessionKey ?? targetSessionKey,
                  flowId: flow.flowId,
                  nodeId: gateNodeId,
                  itemKey: target.itemId,
                  pipelineId: flow.stateJson?.pipelineId ?? null,
                  agentId: target.agent ?? null,
                  kind: "flow-gate-discuss",
                  source: "spawn_hook",
                });
              }

            } catch (err) {
              api.logger.warn(`pipeclaw-parrot [${flowIdShort}/${turnId.slice(0, 8)}]: runner error: ${err?.message ?? err}`);
              try {
                await discussStore.stampMessageError(flow.flowId, target.itemId, turnId, err?.message ?? String(err));
                await discussStore.setTurnStatus(flow.flowId, target.itemId, "error", null);
              } catch {}
              await emitKind("session.transcript.failed", {
                scope: "discuss",
                turnId,
                flowId: flow.flowId,
                itemId: target.itemId,
                status: "failed",
                error: err?.message ?? String(err),
              });
            }

            import("node:fs/promises").then(({ unlink }) =>
              unlink(path.join(os.homedir(), ".openclaw", "discuss-turns", flow.flowId, `${target.itemId}.pending.json`))
            ).catch(() => {});
          });
          return true;
        },
      });
    },
  });
}
