// tools.js — agent-facing pipeline tools: manifold_start / _status /
// _runs / _approve / _cancel plus pull-claim tools manifold_claim /
// _report / _heartbeat.
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
//   cancelFlow({ flowId, reason })              -> { cancelled, ... }
//   claimNext({ harness, agent, session })      -> ticket | null
//   reportClaim({ ticket, harness, agent, session, result }) -> { ok, ... }
//   renewClaim({ ticket, harness, agent, session }) -> { ok, ... }
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
    name: "manifold_claim",
    description:
      "Claim the highest-priority ready pull ticket for the caller. Empty hands " +
      "is an ok empty response, never an error — poll again later.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {},
    },
  },
  {
    name: "manifold_report",
    description:
      "Report a claimed ticket's result. The ticket must be claimed by the " +
      "caller — tickets claimed by another caller are refused.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["ticket", "result"],
      properties: {
        ticket: { type: "string", description: "Ticket id returned by manifold_claim." },
        result: { type: "object", description: "JSON result stored for the node." },
      },
    },
  },
  {
    name: "manifold_heartbeat",
    description:
      "Renew a claimed ticket's lease. The ticket must be claimed by the " +
      "caller — tickets claimed by another caller are refused.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["ticket"],
      properties: {
        ticket: { type: "string", description: "Ticket id returned by manifold_claim." },
      },
    },
  },
];

export function createManifoldToolHandlers(ops = {}) {
  const { startRun, runStatus, listRuns, approveGate, cancelFlow, claimNext, reportClaim, renewClaim, logger } = ops;

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

    async manifold_claim(args, identity) {
      void args;
      try {
        const won = await need(claimNext, "claimNext")({
          harness: identity.harness, agent: identity.agent, session: identity.session,
        });
        if (!won) return ok("manifold_claim: no work ready", { status: "empty" });
        logger?.info?.(`manifold: ${identitySessionKey(identity)} claimed ${won.ticket}`);
        return ok(JSON.stringify(won), { status: "ok", ticket: won.ticket, flowId: won.flowId, nodeId: won.nodeId, leaseExpiresAt: won.leaseExpiresAt ?? null });
      } catch (e) {
        return err(`manifold_claim failed: ${e?.message ?? e}`, "error");
      }
    },

    async manifold_report(args, identity) {
      const ticket = args?.ticket;
      if (typeof ticket !== "string" || !ticket) return err("manifold_report: ticket is required", "rejected");
      if (args?.result === undefined || args?.result === null || typeof args.result !== "object") {
        return err("manifold_report: result is required", "rejected");
      }
      try {
        const r = await need(reportClaim, "reportClaim")({
          ticket,
          harness: identity.harness, agent: identity.agent, session: identity.session,
          result: args.result,
        });
        logger?.info?.(`manifold: ${identitySessionKey(identity)} reported ${ticket}`);
        return ok(JSON.stringify(r), { status: "ok", ticket, flowId: r?.flowId ?? null, nodeId: r?.nodeId ?? null });
      } catch (e) {
        return err(`manifold_report refused: ${e?.message ?? e}`, "refused", { ticket });
      }
    },

    async manifold_heartbeat(args, identity) {
      const ticket = args?.ticket;
      if (typeof ticket !== "string" || !ticket) return err("manifold_heartbeat: ticket is required", "rejected");
      try {
        const r = await need(renewClaim, "renewClaim")({
          ticket,
          harness: identity.harness, agent: identity.agent, session: identity.session,
        });
        return ok(JSON.stringify(r), { status: "ok", ticket, leaseExpiresAt: r?.leaseExpiresAt ?? null });
      } catch (e) {
        return err(`manifold_heartbeat refused: ${e?.message ?? e}`, "refused", { ticket });
      }
    },

    async manifold_cancel(args, identity) {
      const flowId = args?.flowId;
      if (typeof flowId !== "string" || !flowId) return err("manifold_cancel: flowId is required", "rejected");
      try {
        const r = await need(cancelFlow, "cancelFlow")({ flowId, reason: args?.reason ?? null });
        if (!r?.cancelled) return err(`manifold_cancel refused: ${r?.reason ?? "unknown"}`, "rejected", { flowId });
        logger?.info?.(`manifold: ${identitySessionKey(identity)} cancelled ${flowId}`);
        return ok(JSON.stringify(r), { status: "ok", flowId });
      } catch (e) {
        return err(`manifold_cancel failed: ${e?.message ?? e}`, "error");
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
