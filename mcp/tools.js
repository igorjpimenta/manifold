// tools.js — agent-facing pipeline tools: manifold_start / _status /
// _runs / _approve / _cancel.
//
// `createManifoldToolHandlers(ops)` returns static TOOL_DEFS (name,
// description, JSON-schema inputSchema) plus a `handle(name, args,
// identity)` dispatcher. Ops are injected by the host — stdio/HTTP servers
// pass the real executor/run-store wiring, tests pass stubs:
//
//   startRun({ pipelineId, args, triggeredBy }) -> flow record
//   runStatus({ flowId })                       -> flow record | null
//   listRuns({ pipelineId, status, limit })     -> flow record[]
//   approveGate({ flowId, itemId, decision, text }) -> { ok, ... }
//   cancelFlow({ flowId, reason })                 -> { cancelled, ... }
//   deliverResult({ sessionKey, runId, agent, kind, payload }) -> { ok }
//   logger                                       -> { info, warn, error } (optional)
//
// The caller identity is the resolved `identity` — never a tool param —
// so agents cannot start runs or approve gates under another agent's name.

import { identitySessionKey, assertCleanTriple } from "./identity.js";

function ok(text, details) {
  return { content: [{ type: "text", text }], details };
}

function err(text, reason, extra = {}) {
  return { content: [{ type: "text", text }], details: { status: "rejected", reason, ...extra } };
}

export const TOOL_DEFS = [
  {
    name: "manifold_start",
    description:
      "Start a new run of a pipeline. Returns the flow record (flowId, status, revision). " +
      "The caller is attributed by request identity, never by a tool param.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["pipelineId"],
      properties: {
        pipelineId: { type: "string", description: "Pipeline id (workspace/.pipelines/ definition)." },
        args: { type: "object", description: "JSON args merged into the initial state." },
      },
    },
  },
  {
    name: "manifold_status",
    description:
      "Read one flow's status: flowId, pipelineId, status, lastStep, revision. " +
      "Read-only; a missing flow is refused as not_found, never defaulted.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["flowId"],
      properties: {
        flowId: { type: "string", description: "Flow id returned by manifold_start." },
      },
    },
  },
  {
    name: "manifold_runs",
    description:
      "List recent flow runs, optionally filtered by pipeline and status. Read-only.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        pipelineId: { type: "string", description: "Only runs of this pipeline." },
        status: { type: "string", description: "Only runs in this status." },
        limit: { type: "integer", description: "Max rows (default 20)." },
      },
    },
  },
  {
    name: "manifold_approve",
    description:
      "Record a human-gate decision: approve, reject, or edit (with text) one pending item. " +
      "The decision is attributed to the caller — speaking for another agent is refused.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["flowId", "itemId", "decision"],
      properties: {
        flowId: { type: "string", description: "Blocked flow id." },
        itemId: { type: "string", description: "Pending gate item id." },
        decision: { type: "string", enum: ["approve", "reject", "edit"] },
        text: { type: "string", description: "Edited text (only with decision=edit)." },
      },
    },
  },
  {
    name: "manifold_cancel",
    description:
      "Cancel a flow run. Terminal flows and missing flows are refused, never silently ok.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["flowId"],
      properties: {
        flowId: { type: "string", description: "Flow id to cancel." },
        reason: { type: "string", description: "Why the run is cancelled (audit trail)." },
      },
    },
  },
  {
    name: "manifold_deliver_result",
    description:
      "Deliver a structured tool result for the calling session: the dispatch " +
      "waiting on this session's turn picks it up instead of waiting out the " +
      "agent run. The session is identified from the caller's stamped identity, " +
      "never from a parameter — a result cannot be filed under another session. " +
      "Latest write wins per session; the dispatch consumes and clears it.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["payload"],
      properties: {
        payload: { description: "JSON result payload the node's apply() receives as output.toolResult.payload." },
        kind: { type: "string", description: "Result kind (e.g. score, draft) — routed opaquely, never interpreted here." },
        runId: { type: "string", description: "Agent run id, when known (traceability only)." },
      },
    },
  },
];

export function createManifoldToolHandlers(ops = {}) {
  const { startRun, runStatus, listRuns, approveGate, cancelFlow, deliverResult, logger } = ops;

  function need(fn, name) {
    if (typeof fn !== "function") throw new Error(`manifold: op ${name} is not wired (no ${name} in injected ops)`);
    return fn;
  }

  const handlers = {
    async manifold_start(args, identity) {
      const pipelineId = args?.pipelineId;
      if (typeof pipelineId !== "string" || pipelineId.trim() === "") {
        return err("manifold_start: pipelineId is required", "rejected");
      }
      try {
        const record = await need(startRun, "startRun")({
          pipelineId: pipelineId.trim(),
          args: args?.args ?? {},
          triggeredBy: { kind: "mcp", label: `MCP · ${identitySessionKey(identity)}`, ts: Date.now() },
        });
        logger?.info?.(`manifold: ${identitySessionKey(identity)} started ${record.flowId} (${record.pipelineId ?? pipelineId})`);
        return ok(JSON.stringify(record), { status: "ok", flowId: record.flowId });
      } catch (e) {
        return err(`manifold_start failed: ${e?.message ?? e}`, "error");
      }
    },

    async manifold_status(args, identity) {
      const flowId = args?.flowId;
      if (typeof flowId !== "string" || flowId.trim() === "") {
        return err("manifold_status: flowId is required", "rejected");
      }
      try {
        const flow = await need(runStatus, "runStatus")({ flowId: flowId.trim() });
        if (!flow) return err(`manifold_status: not found: ${flowId}`, "not_found", { flowId });
        return ok(JSON.stringify(flow), { status: "ok", flowId: flow.flowId ?? flowId });
      } catch (e) {
        return err(`manifold_status failed: ${e?.message ?? e}`, "error");
      }
    },

    async manifold_runs(args, identity) {
      void identity;
      try {
        const rows = await need(listRuns, "listRuns")({
          pipelineId: args?.pipelineId ?? null,
          status: args?.status ?? null,
          limit: args?.limit ?? 20,
        });
        return ok(JSON.stringify(rows), { status: "ok", count: Array.isArray(rows) ? rows.length : 0 });
      } catch (e) {
        return err(`manifold_runs failed: ${e?.message ?? e}`, "error");
      }
    },

    async manifold_approve(args, identity) {
      const { flowId, itemId, decision } = args ?? {};
      if (typeof flowId !== "string" || !flowId) return err("manifold_approve: flowId is required", "rejected");
      if (typeof itemId !== "string" || !itemId) return err("manifold_approve: itemId is required", "rejected");
      if (!["approve", "reject", "edit"].includes(decision)) {
        return err("manifold_approve: decision must be approve|reject|edit", "rejected");
      }
      if (decision === "edit" && (typeof args.text !== "string" || args.text.trim() === "")) {
        return err("manifold_approve: decision=edit requires text", "rejected");
      }
      try {
        const r = await need(approveGate, "approveGate")({
          flowId, itemId, decision,
          text: args?.text ?? null,
          actor: identity.agent,
        });
        if (!r?.ok) return err(`manifold_approve refused: ${r?.code ?? "unknown"}`, r?.code ?? "rejected", { flowId, itemId });
        return ok(JSON.stringify(r), { status: "ok", flowId, itemId, decision });
      } catch (e) {
        return err(`manifold_approve failed: ${e?.message ?? e}`, "error");
      }
    },

    async manifold_cancel(args, identity) {
      const flowId = typeof args?.flowId === "string" ? args.flowId : null;
      if (!flowId) return err("manifold_cancel: flowId is required", "rejected");
      try {
        const r = await need(cancelFlow, "cancelFlow")({ flowId, reason: args?.reason ?? null });
        if (!r?.cancelled) return err(`manifold_cancel refused: ${r?.reason ?? "unknown"}`, "rejected", { flowId });
        logger?.info?.(`manifold: ${identitySessionKey(identity)} cancelled ${flowId}`);
        return ok(JSON.stringify(r), { status: "ok", flowId });
      } catch (e) {
        return err(`manifold_cancel failed: ${e?.message ?? e}`, "error", { flowId });
      }
    },

    async manifold_deliver_result(args, identity) {
      if (args?.payload === undefined) {
        return err("manifold_deliver_result: payload is required", "rejected");
      }
      try {
        await need(deliverResult, "deliverResult")({
          sessionKey: identity.session,
          runId: typeof args?.runId === "string" ? args.runId : null,
          agentId: identity.agent,
          kind: typeof args?.kind === "string" ? args.kind : null,
          payload: args.payload,
        });
        return ok("delivered", { status: "ok", sessionKey: identity.session });
      } catch (e) {
        return err(`manifold_deliver_result failed: ${e?.message ?? e}`, "error");
      }
    },
  };

  async function handle(name, args, identity) {
    assertCleanTriple(identity, `caller identity for ${name}`);
    const fn = handlers[name];
    if (!fn) return err(`unknown tool: ${name}`, "unknown_tool");
    return fn(args, identity);
  }

  return { TOOL_DEFS, handle };
}
