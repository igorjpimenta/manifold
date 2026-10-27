// wire-ops.js — real core ops for the MCP servers.
//
// createRealOps builds the eight tool ops from live core pieces: the
// run-store for status/runs/cancel, the executor + gate-resolver for
// start/approve, and the node-executions store for claim/report/heartbeat.
// Everything is store-backed — no stubs, no fakes.
//
// Standalone note: `serve --mcp` runs outside the gateway. Agent nodes
// park as claim tickets (claimed via manifold_claim, completed via
// manifold_report, lease renewed via manifold_heartbeat) — parking needs
// no dispatch runtime, only the ticket store. Status/runs/cancel always
// work — they are pure store reads/writes.

import { createRunStore } from "../state/run-store.js";
import { createStateStore } from "../state/state-store.js";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { createFailureStore } from "../state/failure-store.js";
import { createAdmissionQueueStore } from "../state/admission-queue-store.js";
import { createNodeHealthStore } from "../state/node-health-store.js";
import { createSessionIndex } from "../state/session-index.js";
import { createPushTargetsStore } from "../state/push-targets.js";
import { createExecutor } from "../engine/executor.js";
import { createGateResolver } from "../gates/gate-resolver.js";
import { loadPipelines } from "../engine/loader.js";
import { resolvePushTarget, notifyPush as pushNotify } from "../events/push.js";
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
  const pushTargetsStore = await createPushTargetsStore({ logger, dbPath: runtimeDb }).catch(() => null);

  const api = { logger: logger ?? console };
  const pipelinesRef = { value: new Map() };
  const ensurePipelinesLoaded = async () => {
    if (pipelinesRef.value && pipelinesRef.value.size > 0) return;
    pipelinesRef.value = await loadPipelines(pipesDir, logger);
  };
  await ensurePipelinesLoaded().catch((err) => {
    logger?.warn?.(`manifold: pipelines failed to load (${err?.message ?? err}); manifold_start will refuse unknown pipelines`);
  });

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
    failureStore,
    nodeHealthStore,
    notifyFlowEnteredWaiting: async () => {},
    notifyPush: async (payload) => {
      try {
        const harness = payload?.session?.harness ?? null;
        const target = resolvePushTarget({
          pushTargets: harness ? await pushTargetsStore?.get(harness).catch(() => null) : null,
          harnessId: harness,
          config: config ?? {},
        });
        if (target) await pushNotify({ target, payload, logger });
      } catch (err) {
        logger?.warn?.(`manifold: standalone notifyPush failed: ${err?.message ?? err}`);
      }
    },
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
    pipelineLookup: (id) => pipelinesRef.value?.get(id) ?? null,
    nodeExecutionsStore,
    admissionQueueStore,
  });

  return {
    async startRun({ pipelineId, args, triggeredBy }) {
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
    async claimNext({ harness, agent, session }) {
      return nodeExecutionsStore.claimNext({ harness, agent, session });
    },
    async reportClaim({ ticket, harness, agent, session, result }) {
      return nodeExecutionsStore.reportClaim({ ticket, harness, agent, session, result });
    },
    async renewClaim({ ticket, harness, agent, session }) {
      return nodeExecutionsStore.renewClaim({ ticket, harness, agent, session });
    },
    logger,
  };
}
