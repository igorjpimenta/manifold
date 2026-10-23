// graphql/schema.js — client-agnostic query layer over manifold state.
//
// Any consumer (iOS, dashboard, CLI) picks the fields it needs; the edge
// stays iOS-specific (push, Live Activities) and never duplicates this.
// Query-only by design: writes go through the POST routes, so reads
// never emit domain events (single-writer).
//
// Leaf types inherit the database schema (columns.js builds fields from
// the drizzle table defs — the same files migrations generate from), so
// a renamed column breaks loudly instead of drifting. Reads come from
// the domain stores where they exist (failureStore, runStore) and raw
// sqlite where no store reader exists (queue_items, gate_state,
// gate_records — same precedent as budget/index.js).
import os from "node:os";
import path from "node:path";
import {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLList,
  GraphQLNonNull,
  GraphQLID,
  execute,
  parse,
  validate,
} from "graphql";
import { createClient } from "@libsql/client";
import { failureEvents, flowRuns } from "../state/schema.js";
import { queueItems, gateState } from "../state/schema.ops.js";
import { objectType } from "./columns.js";

export const DEFAULT_RUNTIME_DB = () => path.join(os.homedir(), ".openclaw", "runtime.sqlite");
export const DEFAULT_OPS_DB = () => path.join(os.homedir(), ".openclaw", "ops.sqlite");

// Public leaf subsets: one word to publish a new column, internals
// (state_json, tokens, handles) stay out by default. Joined extras
// declared with explicit types beside the table list.
const Failure = objectType({
  name: "Failure",
  table: failureEvents,
  columns: ["id", "flow_id", "failed_at", { name: "failure_summary", as: "summary" }, "error_type"],
  extras: { pipelineId: GraphQLString, nodeId: GraphQLString },
});

const Approval = objectType({
  name: "Approval",
  table: queueItems,
  columns: ["id", "flow_id", "node", "label", "agent", "created_at"],
});

const Gate = objectType({
  name: "Gate",
  table: gateState,
  columns: ["locked", "day"],
});

const FlowRun = objectType({
  name: "FlowRun",
  table: flowRuns,
  columns: ["flow_id", "pipeline_id", "status", "created_at", "updated_at"],
  extras: { flowId: new GraphQLNonNull(GraphQLID) },
});

const Snapshot = new GraphQLObjectType({
  name: "Snapshot",
  fields: {
    failures: { type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(Failure))) },
    approvals: { type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(Approval))) },
    gate: { type: Gate },
    runningCount: { type: new GraphQLNonNull(GraphQLInt) },
    emittedAtMs: { type: GraphQLFloat },
  },
});

const InboxItem = new GraphQLObjectType({
  name: "InboxItem",
  fields: {
    activityId: { type: new GraphQLNonNull(GraphQLID) },
    flowId: { type: new GraphQLNonNull(GraphQLString) },
    itemId: { type: new GraphQLNonNull(GraphQLString) },
    nodeId: { type: GraphQLString },
    pipelineId: { type: GraphQLString },
    agentName: { type: GraphQLString },
    title: { type: GraphQLString },
    emittedAtMs: { type: GraphQLFloat },
    gateKind: { type: GraphQLString },
    payload: { type: new GraphQLNonNull(GraphQLString) },
  },
});

function openDb(dbPath) {
  return createClient({ url: `file:${dbPath}` });
}

function toFailure(r) {
  return {
    id: r.id,
    flowId: r.flow_id ?? r.flowId ?? null,
    pipelineId: r.pipeline_id ?? r.pipelineId ?? null,
    nodeId: r.failure_node ?? r.nodeId ?? null,
    summary: r.failure_summary ?? r.summary ?? null,
    errorType: r.error_type ?? r.errorType ?? null,
    failedAt: r.failed_at ?? r.failedAt ?? null,
  };
}

function toFlowRun(r) {
  if (!r) return null;
  return {
    flowId: r.flowId ?? r.flow_id,
    pipelineId: r.pipelineId ?? r.pipeline_id ?? null,
    status: r.status ?? null,
    createdAt: r.createdAtMs ?? r.createdAt ?? r.created_at ?? null,
    updatedAt: r.updatedAtMs ?? r.updatedAt ?? r.updated_at ?? null,
  };
}

export function createRootValue({
  failureStore = null,
  runStore = null,
  runtimeDbPath = DEFAULT_RUNTIME_DB(),
  opsDbPath = DEFAULT_OPS_DB(),
  logger = null,
} = {}) {
  const clamp = (limit) => (Number.isInteger(limit) && limit > 0 ? Math.min(limit, 200) : 50);

  async function queryFailures(limit = 50) {
    try {
      const rows = await failureStore.list({ resolved: false, limit: clamp(limit) });
      return (rows ?? []).map(toFailure);
    } catch (err) {
      logger?.warn?.(`graphql: failures query failed: ${err?.message ?? err}`);
      return [];
    }
  }

  async function queryApprovals() {
    let db = null;
    try {
      db = openDb(opsDbPath);
      await db.execute("PRAGMA busy_timeout = 30000");
      const { rows } = await db.execute(
        "SELECT id, flow_id, node, label, agent, created_at FROM queue_items WHERE status = 'pending' ORDER BY created_at DESC LIMIT 200",
      );
      return rows.map((r) => ({
        id: r.id,
        flowId: r.flow_id ?? null,
        node: r.node ?? null,
        label: r.label ?? null,
        agent: r.agent ?? null,
        createdAt: r.created_at ?? null,
      }));
    } catch (err) {
      logger?.warn?.(`graphql: approvals query failed: ${err?.message ?? err}`);
      return [];
    } finally {
      try { db?.close(); } catch {}
    }
  }

  async function queryGate() {
    let db = null;
    try {
      db = openDb(opsDbPath);
      await db.execute("PRAGMA busy_timeout = 30000");
      const { rows } = await db.execute("SELECT locked, day FROM gate_state WHERE id = 'singleton' LIMIT 1");
      const g = rows[0];
      return g ? { locked: g.locked ?? 0, day: g.day ?? null } : null;
    } catch (err) {
      logger?.warn?.(`graphql: gate query failed: ${err?.message ?? err}`);
      return null;
    } finally {
      try { db?.close(); } catch {}
    }
  }

  async function queryRuns(status = null, limit = 50) {
    try {
      const rows = await runStore.listRunsPage({
        status: typeof status === "string" && status.length > 0 ? status : undefined,
        limit: clamp(limit),
      });
      return (rows ?? []).map(toFlowRun);
    } catch (err) {
      logger?.warn?.(`graphql: runs query failed: ${err?.message ?? err}`);
      return [];
    }
  }

  async function queryRun(flowId) {
    try {
      return toFlowRun(await runStore.get(flowId));
    } catch (err) {
      logger?.warn?.(`graphql: run query failed: ${err?.message ?? err}`);
      return null;
    }
  }

  // Open gate items for triage surfaces, read from persisted gate_records
  // (the engine writes requestPayload incl. layout at gate-block time;
  // consumed records carry consumedAt). Context-joined from flow_runs
  // (pipeline) and queue_items (agent/label) — both LEFT, missing
  // context degrades to nulls consumers already tolerate.
  async function queryInbox() {
    let gateRows = [];
    const pipeByFlow = {};
    try {
      const db = openDb(runtimeDbPath);
      try {
        await db.execute("PRAGMA busy_timeout = 30000");
        gateRows = (await db.execute("SELECT flow_id, node_id, record_json, updated_at FROM gate_records")).rows;
        try {
          const f = await db.execute("SELECT flow_id, pipeline_id FROM flow_runs");
          for (const r of f.rows) pipeByFlow[r.flow_id] = r.pipeline_id ?? null;
        } catch {}
      } finally {
        db.close();
      }
    } catch (err) {
      logger?.warn?.(`graphql: inbox gate read failed: ${err?.message ?? err}`);
      return [];
    }
    const agentByItem = {};
    const labelByItem = {};
    try {
      const db = openDb(opsDbPath);
      try {
        await db.execute("PRAGMA busy_timeout = 30000");
        const q = await db.execute("SELECT id, label, agent FROM queue_items WHERE status = 'pending'");
        for (const r of q.rows) {
          agentByItem[r.id] = r.agent ?? null;
          labelByItem[r.id] = r.label ?? null;
        }
      } finally {
        db.close();
      }
    } catch {}
    const out = [];
    for (const g of gateRows) {
      let record;
      try {
        record = JSON.parse(g.record_json);
      } catch {
        continue;
      }
      if (record?.consumedAt != null) continue;
      for (const item of record?.items ?? []) {
        if (!item?.itemId) continue;
        const rp = item.requestPayload ?? {};
        out.push({
          activityId: `${g.flow_id}:${item.itemId}`,
          flowId: g.flow_id,
          itemId: item.itemId,
          nodeId: g.node_id ?? null,
          pipelineId: pipeByFlow[g.flow_id] ?? null,
          agentName: agentByItem[item.itemId] ?? null,
          title: rp.title ?? labelByItem[item.itemId] ?? null,
          emittedAtMs: g.updated_at ?? null,
          gateKind: record.kind ?? null,
          payload: JSON.stringify(rp),
        });
        if (out.length >= 200) return out;
      }
    }
    return out;
  }

  async function runningCount() {
    const list = await queryRuns("running", 200);
    return list.length;
  }

  return {
    failures: ({ limit }) => queryFailures(limit),
    approvals: () => queryApprovals(),
    gate: () => queryGate(),
    runs: ({ status, limit }) => queryRuns(status, limit),
    run: ({ flowId }) => queryRun(flowId),
    snapshot: async () => ({
      failures: await queryFailures(50),
      approvals: await queryApprovals(),
      gate: await queryGate(),
      runningCount: await runningCount(),
      emittedAtMs: Date.now(),
    }),
    inbox: () => queryInbox(),
  };
}

export const schema = new GraphQLSchema({
  // No custom field resolvers: the root value's functions are invoked
  // by the default resolver with (args), so the query layer stays a
  // thin projection over createRootValue.
  query: new GraphQLObjectType({
    name: "Query",
    fields: {
      failures: {
        type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(Failure))),
        args: { limit: { type: GraphQLInt, defaultValue: 50 } },
      },
      approvals: {
        type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(Approval))),
      },
      gate: { type: Gate },
      runs: {
        type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(FlowRun))),
        args: { status: { type: GraphQLString }, limit: { type: GraphQLInt, defaultValue: 50 } },
      },
      run: {
        type: FlowRun,
        args: { flowId: { type: new GraphQLNonNull(GraphQLID) } },
      },
      snapshot: { type: new GraphQLNonNull(Snapshot) },
      inbox: {
        type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(InboxItem))),
      },
    },
  }),
  types: [Failure, Approval, Gate, FlowRun, Snapshot, InboxItem],
});

export async function executeQuery({ root, query, variables = {} }) {
  let document;
  try {
    document = parse(query);
  } catch (err) {
    return { errors: [{ message: err?.message ?? "invalid query" }] };
  }
  try {
    const validationErrors = validate(schema, document);
    if (validationErrors.length > 0) return { errors: validationErrors.map((e) => ({ message: e.message })) };
    return await execute({ schema, document, rootValue: root, variableValues: variables ?? {} });
  } catch (err) {
    return { errors: [{ message: err?.message ?? "execution failed" }] };
  }
}
