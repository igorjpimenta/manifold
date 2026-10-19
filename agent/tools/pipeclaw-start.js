// pipeclaw-start.js — registers the pipeclaw_start agent tool.

import { toolErr } from "./shared.js";

export function registerGraphFlowTool({ api, executor, pipelinesRef, ensurePipelinesLoaded }) {
  // Factory form — receives a per-invocation tool context with `ctx.sessionKey`,
  // the calling agent's session. Captured in closure so we can record a lineage
  // trigger edge (cron/telegram/operator session → flow_id) right after the
  // executor returns the flowId. Without this the cron-owner lineage row has
  // NULL flow_id and pipeline-run costs can't be attributed back to the cron
  // that fired them. (Object-form registration omits ctx entirely.)
  api.registerTool((ctx) => ({
    name: "pipeclaw_start",
    label: "Pipeclaw: start a pipeline run",
    description:
      "Start a new run of a registered pipeclaw pipeline. Returns the flowId so the caller can watch progress with `openclaw pipeclaw status <flowId>`. The pipeline definition must already be loaded from workspace/.pipelines/.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["pipelineId"],
      properties: {
        pipelineId: {
          type: "string",
          description: "Pipeline identifier matching the `id` field exported by a `workspace/.pipelines/<team>_<name>.js` file (e.g. `freelance_workana-proposal`).",
        },
        args: {
          type: "object",
          additionalProperties: true,
          description: "Optional JSON args merged into the initial state under `state.args`. Use for one-off overrides like `{ mockMode: true }` or `{ maxShortlist: 3 }`.",
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const { pipelineId, args } = params ?? {};
      if (!pipelineId || typeof pipelineId !== "string") {
        return toolErr("pipelineId is required");
      }
      // Loader is async; in fresh CLI-backed processes the agent can hit this
      // tool before the boot-time loadPipelines() has resolved, which manifests
      // as Known: (none loaded). ensurePipelinesLoaded is idempotent and fast
      // on the warm path.
      if (typeof ensurePipelinesLoaded === "function") {
        try { await ensurePipelinesLoaded(); } catch { /* fall through; .has check reports */ }
      }
      if (!pipelinesRef.value.has(pipelineId)) {
        const known = [...pipelinesRef.value.keys()].join(", ") || "(none loaded)";
        return toolErr(`unknown pipeline: ${pipelineId}. Known: ${known}`);
      }
      try {
        const triggeredBy = {
          kind: "agent_tool",
          label: "Agent · pipeclaw_start",
          ts: Date.now(),
        };
        const record = await executor.start(pipelineId, args ?? {}, triggeredBy);
        // Record the trigger edge. Best-effort — a lineage write failure must
        // not break the agent's tool call (recordTrigger swallows its own
        // errors and returns {ok:false}, so awaiting it can't reject). Awaited
        // because it is async — it resolves the root by walking the lineage
        // chain, then upserts session_lineage — so the edge is committed and
        // queryable before runFlow is scheduled below. Unawaited, a
        // single-LLM-call pipeline's first node can fire its
        // before_message_write hook while this write is still suspended, and
        // that cost row resolves against no trigger edge.
        try {
          const parentKey = ctx?.sessionKey ?? null;
          if (!parentKey) {
            api?.logger?.warn?.(`pipeclaw tool: lineage edge skipped for ${record.flowId} — ctx.sessionKey is null`);
          } else if (api?.runtime?.lineage?.recordTrigger) {
            await api.runtime.lineage.recordTrigger({
              flowId: record.flowId,
              runId: record.stateJson?.runId ?? null,
              pipelineId,
              parentSessionKey: parentKey,
            });
          }
        } catch (err) {
          try { api?.logger?.warn?.(`pipeclaw tool: lineage edge skipped: ${err?.message ?? err}`); } catch {}
        }
        // Fire the executor loop async — the tool returns immediately so the
        // calling agent isn't blocked for the duration of the pipeline run.
        setImmediate(() => {
          executor.runFlow(record.flowId, "fresh").catch((err) => {
            try { (api?.logger ?? console).warn?.(`pipeclaw tool: runFlow [${record.flowId.slice(0, 8)}] error: ${err?.message ?? err}`); } catch {}
          });
        });
        return {
          content: [
            { type: "text", text: `Started ${pipelineId}\n  flowId: ${record.flowId}\n  runId: ${record.stateJson?.runId}\n  watch: openclaw pipeclaw status ${record.flowId}` },
          ],
        };
      } catch (err) {
        return toolErr(`start failed: ${err?.message ?? err}`);
      }
    },
  }), { name: "pipeclaw_start" });
}
