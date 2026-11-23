// wire-ops.js — real core ops for the MCP servers.
//
// createRealOps builds the six tool ops from live core pieces: the
// run-store for status/runs/cancel, the executor + gate-resolver for
// start/approve, and the tool-results store for deliver_result.
// Everything is store-backed — no stubs, no fakes.
//
// Standalone note: `serve --mcp` runs outside the gateway, so there is no
// agent runtime. Code/manual/subflow nodes drive fine; an agent-node
// dispatch refuses loudly at dispatch time (the injected runTurn throws),
// naming the missing runtime instead of hanging. Status/runs/cancel always
// work — they are pure store reads/writes.

import { createRunStore } from "../state/run-store.js";
import { createStateStore } from "../state/state-store.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { createFailureStore } from "../state/failure-store.js";
import { writePendingResult } from "../state/tool-handoff.js";
import { createAdmissionQueueStore } from "../state/admission-queue-store.js";
import { createNodeHealthStore } from "../state/node-health-store.js";
import { createSessionIndex } from "../state/session-index.js";
import { createExecutor } from "../engine/executor.js";
import { createGateResolver } from "../gates/gate-resolver.js";
import { loadPipelines } from "../engine/loader.js";
import { createEmitter } from "../events/emit.js";
import { getGlobalConcurrencyLimit } from "../engine/global-concurrency.js";
import { resolveStorePaths } from "../state/paths.js";

export async function createRealOps({ dbPath = null, pipelinesDir = null, config = null, logger = null } = {}) {
  const paths = resolveStorePaths();
  const runtimeDb = dbPath ?? paths.runtimeDb;
  const pipesDir = pipelinesDir ?? paths.pipelinesDir;

  const runStore = createRunStore({ logger, dbPath: runtimeDb });
  const nodeExecutionsStore = getNodeExecutionsStore(runtimeDb);
  const failureStore = createFailureStore({ logger, nodeExecutionsStore });
  const admissionQueueStore = createAdmissionQueueStore({ logger, getLimit: getGlobalConcurrencyLimit });
  const nodeHealthStore = await createNodeHealthStore({ logger }).catch(() => null);
  const sessionIndex = await createSessionIndex({ logger }).catch(() => null);

  const api = {
    logger: logger ?? console,
    runtime: {
      subagent: {
        run: async () => {
          throw new Error("manifold standalone serve: agent dispatch needs a gateway subagent runtime");
        },
      },
    },
  };
  // Pipelines cache keyed by dir — the default dir loads at startup;
  // per-connection dirs (X-Manifold-Pipelines-Dir) load lazily on first use.
  const pipelinesCache = new Map();
  const loadPipelinesForDir = async (dir) => {
    const key = dir ?? pipesDir;
    if (pipelinesCache.has(key)) return pipelinesCache.get(key);
    const loaded = await loadPipelines(key, logger);
    pipelinesCache.set(key, loaded);
    return loaded;
  };
  const pipelinesRef = { value: await loadPipelinesForDir(pipesDir).catch((err) => {
    logger?.warn?.(`manifold: pipelines failed to load (${err?.message ?? err}); manifold_start will refuse unknown pipelines`);
    return new Map();
  }) };
  const ensurePipelinesLoaded = async (dir = null) => {
    const key = dir ?? pipesDir;
    if (!pipelinesCache.has(key)) {
      const loaded = await loadPipelines(key, logger).catch((err) => {
        logger?.warn?.(`manifold: pipelines failed to load from ${key} (${err?.message ?? err})`);
        return new Map();
      });
      pipelinesCache.set(key, loaded);
    }
    if (key === pipesDir) pipelinesRef.value = pipelinesCache.get(key);
    return pipelinesCache.get(key);
  };

  const store = createStateStore(api, "mcp:standalone", {
    runStore,
    nodeExecutionsStore,
    resumeParentRef: {
      resume: async () => {
        throw new Error("manifold standalone serve: no parent-resume runtime");
      },
    },
    pipelinesRef,
    failureStore,
    admissionQueueStore,
    logger,
  });
  const emit = createEmitter({ getBroadcast: () => null, logger });

  const executor = createExecutor({
    api,
    config: config ?? {},
    approvalQueue: null,
    pipelinesDir: pipesDir,
    store,
    pipelinesRef,
    ensurePipelinesLoaded,
    logger: logger ?? console,
    // No agent runtime outside the gateway: an agent-node dispatch refuses
    // here, naming the missing runtime instead of hanging.
    runTurn: async () => {
      throw new Error("manifold standalone serve: agent dispatch needs a gateway agent runtime");
    },
    // No harness alongside the MCP stdio transport either, so a node that
    // declares no session is refused by name rather than silently dispatched.
    sessions: null,
    failureStore,
    nodeHealthStore,
    notifyFlowEnteredWaiting: async () => {},
    sessionIndex,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
    emit: emit.emit,
  });

  const gateResolver = createGateResolver({
    api,
    store,
    executor,
    getBroadcast: () => null,
    readStore: runStore,
    runStore,
    pipelineLookup: (id, dir = null) => {
      const cache = pipelinesCache.get(dir ?? pipesDir);
      return (cache ?? pipelinesRef.value)?.get(id) ?? null;
    },
    nodeExecutionsStore,
    admissionQueueStore,
  });

  return {
    async startRun({ pipelineId, args, triggeredBy, pipelinesDir = null }) {
      const dir = pipelinesDir || pipesDir;
      const loaded = await ensurePipelinesLoaded(dir);
      if (loaded && !pipelinesRef.value?.has(pipelineId)) pipelinesRef.value = loaded;
      const record = await executor.start(pipelineId, args ?? {}, triggeredBy ?? { kind: "mcp", ts: Date.now() });
      setImmediate(() => {
        executor.runFlow(record.flowId, "fresh").catch((err) => {
          logger?.warn?.(`manifold: detached runFlow ${record.flowId} failed: ${err?.message ?? err}`);
        });
      });
      return store.get(record.flowId).then((f) => f ?? record);
    },
    async runStatus({ flowId }) {
      return runStore.get(flowId);
    },
    async listRuns({ pipelineId, status, limit }) {
      const q = {};
      if (pipelineId) q.pipelineId = pipelineId;
      if (status) q.status = status;
      q.limit = Number.isInteger(limit) && limit > 0 ? limit : 20;
      try {
        return await runStore.listRunsPage(q);
      } catch {
        const all = await runStore.list();
        return all.slice(0, q.limit);
      }
    },
    async approveGate({ flowId, itemId, decision, text, actor }) {
      return gateResolver.recordItemDecision({
        flowId, itemId, decision,
        source: "mcp",
        editedText: text ?? null,
        reason: actor ? `mcp approval by ${actor}` : "mcp approval",
        awaitCompletion: false,
      });
    },
    async cancelFlow({ flowId, reason }) {
      return runStore.cancelFlow(flowId, reason);
    },
    async deliverResult({ session, runId, agentId, kind, payload }) {
      await writePendingResult({ session, runId, agentId, kind, payload });
      return { ok: true, session };
    },
    logger,
  };
}
