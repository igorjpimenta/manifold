// executor.js — drives a manifold pipeline against a managed flow_run row.

import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { nodeEmitter } from "../events.js";
import { resolveStorePaths } from "../state/paths.js";

import {
  isTerminal, isBlocked, isQueued, shortSummary, deepClone, resolveEdge,
  validateGateLayout, isFreshNodeEntry, describeWaitFallback, assembleState,
} from "./executor-helpers.js";
import { NodeBlocked, GatewayHandoffNeeded } from "./executor-errors.js";
import { REVIVAL_MSG } from "./revival-messages.js";
import { createBudgetGate } from "./executor-budget.js";
import { createPersist } from "./executor-persist.js";
import { createAgentDispatch } from "./agent-dispatch.js";
import { dispatchSubflowChildren } from "./executor-subflow-children.js";
import { createNodeRunner } from "./executor-nodes.js";
import { completeCancellation } from "./executor-cancel.js";

export function createExecutor({ api, config, approvalQueue, pipelinesDir, store, pipelinesRef, ensurePipelinesLoaded, logger, runTurn, failureStore, nodeHealthStore, notifyFlowEnteredWaiting, sessionIndex, nodeExecutionsStore, runStore, admissionQueueStore, emit, sessions }) {
  const _config = config ?? api?.config ?? null;
  const _lineage = api?.runtime?.lineage ?? null;
  const _pipelinesDir = pipelinesDir ?? _config?.pipelinesDir ?? resolveStorePaths().pipelinesDir;
  const nodeExecutions = nodeExecutionsStore ?? getNodeExecutionsStore();

  // These call sites write the execution row before persistFailRetrying, so
  // a throw from write() would skip the fail status entirely — swallow it.
  async function safeWriteExecution(args) {
    try {
      return (await nodeExecutions.write(args)) ?? {};
    } catch (err) {
      logger.warn(`manifold [${args.flowId.slice(0, 8)}] nodeExecutions.write threw: ${err?.message ?? err}`);
      return {};
    }
  }

  const { maybeBudgetGate } = createBudgetGate({
    getApprovalQueue: () => approvalQueue ?? api?.runtime?.approvalQueue,
    logger,
  });

  const { persistRunningRetrying, persistWaitingRetrying, persistSkipRetrying, persistFinishRetrying, persistFailRetrying } = createPersist({
    store, failureStore, notifyFlowEnteredWaiting, logger,
  });

  const { dispatchAgentWithRetry } = createAgentDispatch({
    runTurn,
    sessionIndex,
    logger,
    emit,
  });

  // Assigned below once start/runFlow exist; runners read it via getter.
  let nodeCtx;

  async function executeSubflowNode({ node, nodeId, flowId, state }) {
    // Subflow selectors may read earlier nodes' aggregated results
    // (state._subflowGroups), which dispatch never enriches — assembleState
    // here is what makes them visible. Its resultJson overlay is a no-op
    // against real flows.
    const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
    return dispatchSubflowChildren({
      node, nodeId, flowId, state: enrichedState, store, runStore, nodeExecutionsStore: nodeExecutions,
      maybeBudgetGate, runFlow, logger, lineage: _lineage, pipelinesRef, admissionQueueStore,
    });
  }

  const { executeNode } = createNodeRunner({
    maybeBudgetGate, dispatchAgentWithRetry, executeSubflowNode,
    api, config: _config, lineage: _lineage, pipelinesDir: _pipelinesDir, logger,
    getNodeCtx: () => nodeCtx,
    runStore, nodeExecutionsStore: nodeExecutions,
    nodeHealthStore, pipelinesRef,
    sessions,
  });

  async function start(pipelineId, args = {}, triggeredBy = null) {
    const pipeline = pipelinesRef.value.get(pipelineId);
    if (!pipeline) throw new Error(`unknown pipeline: ${pipelineId}`);

    const runId = randomUUID();
    const initialState = {
      runId,
      startedAt: Date.now(),
      args,
      triggeredBy: triggeredBy ?? null,
      ...deepClone(pipeline.state),
    };

    const record = await store.create({ pipelineId, initialState });
    logger.info(`manifold [${record.flowId.slice(0, 8)}] start: pipeline=${pipelineId} runId=${runId.slice(0, 8)}`);

    // Default is unlimited; allowConcurrent:false rejects this run before it
    // competes for an admission slot if a non-terminal run already exists.
    if (pipeline.allowConcurrent === false) {
      const others = await runStore.listRunsPage({ pipelineId });
      const active = others.find((r) => r.flowId !== record.flowId && !isTerminal(r.status));
      if (active) {
        logger.info(`manifold [${record.flowId.slice(0, 8)}] start: skipped — ${pipelineId} already has an active run (flowId=${active.flowId.slice(0, 8)}, status=${active.status})`);
        await store.skip({
          flowId: record.flowId,
          expectedRevision: record.revision,
          stateJson: record.stateJson,
          reason: `another run of ${pipelineId} is already active (flowId=${active.flowId}, status=${active.status})`,
        });
        return await store.get(record.flowId);
      }
    }

    // Shared admission pool (same one subflow child-dispatch draws from); a
    // full pool queues the run instead of rejecting it.
    const admission = await admissionQueueStore.admit({ kind: "flow-start", flowId: record.flowId });
    if (admission.status === "queued") {
      logger.info(`manifold [${record.flowId.slice(0, 8)}] start: queued — no admission slot available yet`);
      await store.setQueued({
        flowId: record.flowId,
        expectedRevision: record.revision,
        lastStep: record.lastStep,
        stateJson: record.stateJson,
        waitJson: { kind: "flow-start" },
        statusReason: "queued: waiting for a shared admission slot",
      });
      return await store.get(record.flowId);
    }
    return record;
  }

  async function runFlow(flowId, kind) {
    while (true) {
      const flow = await store.get(flowId);
      if (!flow) {
        logger.warn(`manifold [${flowId.slice(0, 8)}] vanished mid-execution`);
        return;
      }
      if (isTerminal(flow.status)) {
        logger.info(`manifold [${flowId.slice(0, 8)}] terminal (${flow.status})`);
        return;
      }
      if (isBlocked(flow.status)) {
        logger.info(`manifold [${flowId.slice(0, 8)}] ${flow.status} at ${flow.lastStep} on ${flow.waitJson?.kind ?? "?"}`);
        return;
      }
      // A flow queued for flow-start/child-dispatch admission isn't cleared to
      // run; its admission check lives outside this loop, so a fresh call here
      // would dispatch lastStep's edge and bypass the pool. Return — the
      // admission queue's release/promotion wakes it later.
      if (isQueued(flow.status) && (flow.waitJson?.kind === "flow-start" || flow.waitJson?.kind === "child-dispatch")) {
        logger.info(`manifold [${flowId.slice(0, 8)}] queued at ${flow.lastStep} on ${flow.waitJson?.kind}`);
        return;
      }

      const state = flow.stateJson ?? {};
      if (state._paused) {
        logger.info(`manifold [${flowId.slice(0, 8)}] paused at ${flow.lastStep}`);
        return;
      }
      const pipelineId = state.pipelineId;
      if (pipelineId && !pipelinesRef.value.get(pipelineId) && typeof ensurePipelinesLoaded === "function") {
        try { await ensurePipelinesLoaded(); } catch (err) {
          logger.warn(`manifold [${flowId.slice(0, 8)}] ensurePipelinesLoaded failed: ${err?.message ?? err}`);
        }
      }
      const pipeline = pipelineId ? pipelinesRef.value.get(pipelineId) : null;
      if (!pipeline) {
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `pipeline not loaded: ${pipelineId}`,
          failureNode: "<engine:pipeline-load>",
        });
        return;
      }

      const lastStep = flow.lastStep ?? "__start__";
      // Transient cascade-leg error, persisted in state_json to survive a
      // restart; the nodeId===lastStep guard stops a stale marker from an
      // earlier loop-back being read after lastStep moved past it.
      const cascadeError = state?._cascadeError?.nodeId === lastStep ? state._cascadeError : null;
      // Separate local: `state` is what gets persisted below; only the value
      // handed to the edge function changes.
      const edgeState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
      const nextNodeId = resolveEdge(pipeline.edges, lastStep, edgeState, {
        logger, flowId,
        error: cascadeError ? { message: cascadeError.message, errorType: cascadeError.errorType } : null,
      });
      if (!nextNodeId) {
        // With a cascadeError, reuse its invocation/execution ids so the
        // failure upserts onto the row the throw already wrote instead of
        // creating a second, less-detailed one.
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: cascadeError?.message ?? `edge ${lastStep} returned no next node`,
          failureNode: lastStep,
          invocationId: cascadeError?.invocationId ?? null,
          executionId: cascadeError?.executionId ?? null,
          errorType: cascadeError?.errorType ?? null,
        });
        return;
      }
      if (nextNodeId === "__end__") {
        // state_json is entry-only: assembleState replays node output
        // (resultJson) and folds in _subflowGroups so display() and
        // successCriteria() can read node results here.
        const enrichedState = await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: state });
        await runAuditIfConfigured({ flowId, pipeline, state: enrichedState, logger });
        // finalState must derive from enrichedState — raw state lacks node
        // output such as _successCriteriaReason. The persist layer's
        // toPersistableState narrows it back to entry-only fields anyway.
        let finalState = enrichedState;
        if (typeof pipeline.display === "function") {
          try {
            const out = await Promise.resolve(pipeline.display(enrichedState));
            if (out && typeof out === "object") {
              finalState = {
                ...enrichedState,
                _display: {
                  title: typeof out.title === "string" ? out.title : null,
                  summary: typeof out.summary === "string" ? out.summary : null,
                  funnels: Array.isArray(out.funnels) ? out.funnels : null,
                  metrics: Array.isArray(out.metrics) ? out.metrics : null,
                },
              };
            }
          } catch (err) {
            logger?.warn?.(`manifold [${flowId.slice(0, 8)}] pipeline.display() threw: ${err?.message ?? err}`);
          }
        }
        const successCriteria = pipeline.runAudit?.successCriteria;
        if (typeof successCriteria === "function") {
          let verdict;
          try {
            verdict = await Promise.resolve(successCriteria({ ...enrichedState, ...finalState }));
          } catch (err) {
            logger?.warn?.(`manifold [${flowId.slice(0, 8)}] successCriteria threw: ${err?.message ?? err}`);
          }
          if (verdict === "failed") {
            await persistFailRetrying({
              flowId, revision: flow.revision,
              state: finalState,
              message: finalState._successCriteriaReason ?? "successCriteria: failed",
              failureNode: "__end__",
            });
            logger.info(`manifold [${flowId.slice(0, 8)}] __end__ → failed (successCriteria)`);
            return;
          }
          if (verdict === "skipped") {
            await persistSkipRetrying({
              flowId, revision: flow.revision,
              state: finalState,
              reason: finalState._successCriteriaReason ?? "successCriteria: skipped",
            });
            logger.info(`manifold [${flowId.slice(0, 8)}] __end__ → skipped (successCriteria)`);
            return;
          }
        }
        await persistFinishRetrying({ flowId, revision: flow.revision, state: finalState });
        logger.info(`manifold [${flowId.slice(0, 8)}] __end__`);
        return;
      }

      const node = pipeline.nodes[nextNodeId];
      if (!node) {
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `unknown node: ${nextNodeId}`,
          failureNode: nextNodeId,
        });
        return;
      }

      logger.info(`manifold [${flowId.slice(0, 8)}] → ${nextNodeId} (${node.kind})`);

      const enteredAt = Date.now();
      // Fresh node entry is always "fresh"; only a re-entry at the node the
      // flow already sat at spends the caller-supplied kind.
      const lastNodeId = await nodeExecutions.findCurrentStep(flowId);
      const forceNew = isFreshNodeEntry(lastNodeId, nextNodeId);
      const execKind = forceNew ? "fresh" : kind;
      // enter() before execution so the in-flight node is identifiable
      // mid-dispatch, not only after completion.
      const enterResult = await nodeExecutions.enter({
        flowId, pipelineId: state.pipelineId ?? null,
        nodeId: nextNodeId, subflowKey: null, kind: execKind,
      });
      const invocationId = enterResult?.invocationId ?? null;
      const isNew       = enterResult?.isNew          ?? (execKind === "fresh");
      process.nextTick(() => nodeEmitter.emit("node.executing", {
        flowId, nodeId: nextNodeId,
        pipelineId: state.pipelineId ?? null,
        parentInvocationId: flow.parentInvocationId ?? null,
      }));
      let stateUpdate = {};
      let waitJson = null;
      let blockedInfo = null;
      let skippedInfo = null;
      let reentrant = false;
      let exitedAt = null;
      try {
        const result = await executeNode({ node, nodeId: nextNodeId, state, flowId, invocationId });
        exitedAt = Date.now();
        stateUpdate = result.stateUpdate ?? {};
        waitJson = result.waitJson ?? null;
        blockedInfo = result.blockedInfo ?? null;
        skippedInfo = result.skippedInfo ?? null;
        reentrant = result.reentrant === true;
      } catch (err) {
        exitedAt ??= Date.now();
        if (err instanceof NodeBlocked) {
          blockedInfo = err._blocked;
        } else if (err instanceof GatewayHandoffNeeded) {
          blockedInfo = {
            reason: `node ${nextNodeId} needs gateway context: ${err.message}`,
            needed: [],
            hint: "No automatic resume — requires a scope-gap fix at the call site or manual task-revival.",
            _kind: "needs-gateway-handoff",
          };
          logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} → needs-gateway-handoff: ${err.message}`);
        } else {
          const msg = err?.message ?? String(err);
          // Duck-typed, not instanceof: the engine and pipeline code can
          // load executor-errors.js through different module registries,
          // giving structurally-identical but reference-distinct
          // NodeFailure classes. A plain Error never sets .errorType.
          const errorType = err?.errorType ?? null;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          if (pipeline.kind === "cascade") {
            // A cascade leg treats any throw as a completed dispatch: lastStep
            // advances to the throwing node and its own edge function decides
            // next (retry vs fallback vs give-up), reading the real error via
            // the transient _cascadeError key. Every throw still records a
            // failure_events row immediately — a cascade-continuing throw never
            // reaches persistFailRetrying, and if this is the final attempt the
            // !nextNodeId branch reuses these ids so the upsert collapses
            // onto the same row.
            try {
              await failureStore?.recordEngineFailure?.({
                flowId, pipelineId: state.pipelineId ?? null, failureNode: nextNodeId,
                message: msg, state, invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
              });
            } catch (recErr) {
              logger?.warn?.(`manifold [${flowId.slice(0, 8)}] recordEngineFailure (cascade) threw: ${recErr?.message ?? recErr}`);
            }
            logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} → cascading (its own edge decides what happens next)`);
            await persistRunningRetrying({
              flowId, revision: flow.revision, lastStep: nextNodeId,
              state: {
                ...state,
                _cascadeError: {
                  nodeId: nextNodeId, message: msg, errorType,
                  invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
                },
              },
            });
            continue;
          }
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
          });
          return;
        }
      }

      if (blockedInfo) {
        if (!blockedInfo._kind) {
          const msg = blockedInfo.reason ?? `${nextNodeId} blocked without declaring a wait kind`;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        const waitJsonForBlock = {
          kind: blockedInfo._kind,
          nodeId: nextNodeId,
          reason: blockedInfo.reason ?? "node blocked",
          needed: blockedInfo.needed ?? [],
          hint: blockedInfo.hint ?? "",
          ...(blockedInfo.responder ? { responder: blockedInfo.responder } : {}),
          ...(Array.isArray(blockedInfo.choices) && blockedInfo.choices.length > 0
                ? { choices: blockedInfo.choices.map((c) => String(c)) }
                : {}),
          ...(Array.isArray(blockedInfo.items) ? { items: blockedInfo.items } : {}),
          renderedAt: Date.now(),
          retry: true,
        };
        try {
          validateGateLayout(waitJsonForBlock.items);
        } catch (err) {
          const msg = err?.message ?? String(err);
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: flow.revision, state,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        const summary = `${nextNodeId}: ${waitJsonForBlock.reason}`;
        logger.info(`manifold [${flowId.slice(0, 8)}] ${waitJsonForBlock.kind} → waiting (will re-run ${nextNodeId} on resume)\n  reason: ${waitJsonForBlock.reason}\n  hint: ${waitJsonForBlock.hint}`);
        await persistWaitingRetrying({
          flowId, revision: flow.revision, lastStep,
          blockingNodeId: nextNodeId,
          state,
          waitJson: waitJsonForBlock,
          statusReason: shortSummary(summary),
        });
        nodeExecutions.write({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      if (skippedInfo) {
        logger.info(`manifold [${flowId.slice(0, 8)}] ${nextNodeId}: _skipped — ${skippedInfo.reason}`);
        await persistSkipRetrying({
          flowId, revision: flow.revision,
          state,
          reason: skippedInfo.reason ?? "skipped",
        });
        nodeExecutions.write({ flowId, pipelineId: state.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      const freshAfterNode = (await store.get(flowId)) ?? { stateJson: state, revision: flow.revision };
      // Entry-only state as persisted (toPersistableState strips node output,
      // which reaches result_json via writeResult instead) — do not merge
      // stateUpdate in here; it would never reach the database.
      let entryState = freshAfterNode.stateJson ?? state;
      // Success clears any _cascadeError left by an earlier failed attempt at
      // this same node, so the next edge call doesn't read a stale error.
      if (entryState._cascadeError?.nodeId === nextNodeId) {
        const { _cascadeError, ...rest } = entryState;
        entryState = rest;
      }
      const persistRevision = freshAfterNode.revision ?? flow.revision;

      if (waitJson) {
        try {
          validateGateLayout(waitJson.items);
        } catch (err) {
          const msg = err?.message ?? String(err);
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: persistRevision, state: entryState,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
          });
          return;
        }
        const summary = describeWaitFallback(waitJson, nextNodeId);
        logger.info(`manifold [${flowId.slice(0, 8)}] ${reentrant ? "reentrant gate" : "gate"} → blocked\n${summary}`);
        await persistWaitingRetrying({
          flowId, revision: persistRevision,
          lastStep: reentrant ? lastStep : nextNodeId,
          blockingNodeId: nextNodeId,
          state: entryState,
          waitJson,
          statusReason: shortSummary(summary),
        });
        nodeExecutions.write({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
        return;
      }

      // Write the node's result BEFORE advancing last_step: boot-time orphan
      // revival and concurrency dequeue trust last_step as "completed" and
      // skip re-running apply(), so a crash before this write would advance
      // past a node whose output never landed. Also awaited, not
      // fire-and-forget: state_json is entry-only, so the next iteration's
      // assembleState() is the only way this output becomes visible — a read
      // racing this write makes the edge function see it as missing and
      // re-route, risking a busy-loop.
      const resultWrite = await nodeExecutions.writeResult(invocationId, stateUpdate).catch(() => null);
      if (!resultWrite || !resultWrite.ok) {
        const reason = resultWrite?.reason ?? "threw";
        // Only "race" means a concurrent writer already committed this
        // invocation's content (advancing is correct). Any other reason means
        // nobody wrote: advancing would let resolveEdge dead-end with "no
        // next node" on the next iteration, hiding the real cause.
        if (reason !== "race") {
          const msg = `resultJson write failed (${reason})`;
          logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} ${msg}`);
          const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
          await persistFailRetrying({
            flowId, revision: persistRevision, state: entryState,
            message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
            invocationId: written.invocationId ?? invocationId, executionId: written.executionId ?? null,
          });
          return;
        }
        logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} resultJson write failed (race) — another writer already completed this invocation`);
      }

      const runResult = await persistRunningRetrying({
        flowId, revision: persistRevision, lastStep: nextNodeId,
        state: entryState,
      });
      if (!runResult.applied) {
        const msg = `state persist failed: ${runResult.code}`;
        logger.warn(`manifold [${flowId.slice(0, 8)}] ${nextNodeId} failed: ${msg}`);
        const written = await safeWriteExecution({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, kind: execKind, invocationId, isNewInvocation: isNew });
        await persistFailRetrying({
          flowId, revision: persistRevision, state: entryState,
          message: `${nextNodeId}: ${msg}`, failureNode: nextNodeId,
          invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
        });
        return;
      }
      nodeExecutions.write({ flowId, pipelineId: entryState.pipelineId ?? null, nodeId: nextNodeId, enteredAt, exitedAt, error: null, kind: execKind, invocationId, isNewInvocation: isNew });
    }
  }

  nodeCtx = Object.freeze({
    async invokePipeline(targetPipelineId, args = {}) {
      const record = await start(targetPipelineId, args);
      // A queued record would be promoted and run unobserved — cancel it and
      // report singleton-busy so the caller retries deliberately.
      if (record.status === "queued") {
        await cancel(record.flowId, "invokePipeline: shared admission pool was full").catch(() => {});
        return {
          invoked: false, reason: "singleton-busy", target: targetPipelineId,
          message: `pipeline "${targetPipelineId}" is at its concurrency limit — queued instead of started`,
        };
      }
      setImmediate(() => {
        runFlow(record.flowId, "fresh").catch((err) => {
          logger?.warn?.(`manifold invokePipeline(${targetPipelineId}) [${record.flowId.slice(0, 8)}]: runFlow failed: ${err?.message ?? err}`);
        });
      });
      return { invoked: true, target: targetPipelineId, flowId: record.flowId };
    },
  });

  async function resume(flowId) {
    return runFlow(flowId, "recheck");
  }

  async function rerun(sourceFlowId, argsOverride) {
    const source = await store.get(sourceFlowId);
    if (!source) throw new Error(`source flow not found: ${sourceFlowId}`);
    return rerunFromSource(source, argsOverride);
  }

  async function rerunFromSource(sourceFlow, argsOverride) {
    if (!sourceFlow) throw new Error(`source flow not found`);
    const sourceFlowId = sourceFlow.flowId;
    const pipelineId = sourceFlow.stateJson?.pipelineId;
    if (!pipelineId) throw new Error(`source flow has no pipelineId in state; cannot determine which pipeline to rerun`);
    const args = argsOverride !== undefined ? argsOverride : (sourceFlow.stateJson?.args ?? {});
    const triggeredBy = {
      kind: "rerun",
      label: `Rerun of ${sourceFlowId.slice(0, 8)}`,
      parentFlowId: sourceFlowId,
      ts: Date.now(),
    };
    const record = await start(pipelineId, args, triggeredBy);
    try {
      const fresh = await store.get(record.flowId);
      if (fresh) {
        await store.setRunning({
          flowId: record.flowId,
          expectedRevision: fresh.revision,
          lastStep: fresh.lastStep,
          stateJson: { ...fresh.stateJson, rerunOf: sourceFlowId },
        });
      }
    } catch (err) {
      logger?.warn?.(`manifold rerun: failed to stamp rerunOf on [${record.flowId.slice(0, 8)}]: ${err?.message ?? err}`);
    }
    return record;
  }

  async function cancel(flowId, reason = null) {
    const flow = await store.get(flowId);
    if (!flow) return { cancelled: false, reason: "Flow not found." };
    return completeCancellation({
      store,
      flowId,
      stateJson: flow.stateJson ?? {},
      lastStep: flow.lastStep,
      revision: flow.revision,
      cfg: _config,
      logger,
      runStore,
      nodeExecutionsStore: nodeExecutions,
      reason,
    });
  }

  // Unlike a code-kind orphan (safe to re-enter via runFlow), an agent-kind
  // node may have already dispatched a completed/partial turn; re-running
  // promptBuilder() risks a duplicate side effect. Send REVIVAL_MSG on the
  // original session key so it can check its own prior turns. nodeId is the
  // in-flight node (lastStep is the last COMPLETED node); omitting it falls
  // back to lastStep.
  async function reviveAgentKindOrphan(flow, nodeId = flow.lastStep) {
    const flowId = flow.flowId;
    const flowId8 = flowId.slice(0, 8);
    const state = flow.stateJson ?? {};
    const pipeline = pipelinesRef.value.get(state.pipelineId);
    const node = pipeline?.nodes?.[nodeId];
    if (!node || node.kind !== "agent") {
      logger.warn(`manifold revival [${flowId8}]: reviveAgentKindOrphan called for non-agent node "${nodeId}"`);
      return;
    }

    logger.info(`manifold revival [${flowId8}]: reviving interrupted agent-node "${nodeId}"`);
    const enteredAt = Date.now();
    // Resolve the original invocation up front (not after executeNode) so
    // tool calls during the revived dispatch register under the same
    // invocationId; no fresh enter() — that row already exists.
    const invocation = await nodeExecutions.getLatestInvocationForNode(flowId, nodeId, null);
    const invocationId = invocation?.id ?? null;
    let result;
    try {
      result = await executeNode({ node, nodeId, state, flowId, reviveMessage: REVIVAL_MSG, invocationId });
    } catch (err) {
      if (err instanceof GatewayHandoffNeeded) {
        await persistWaitingRetrying({
          flowId, revision: flow.revision, lastStep: nodeId, blockingNodeId: nodeId,
          state,
          waitJson: {
            kind: "needs-gateway-handoff", nodeId, reason: err.message, needed: [],
            hint: "No automatic resume — requires a scope-gap fix at the call site or manual task-revival.",
            renderedAt: Date.now(), retry: true,
          },
          statusReason: shortSummary(`${nodeId}: needs gateway context: ${err.message}`),
        });
        return;
      }
      const msg = err?.message ?? String(err);
      const errorType = err?.errorType ?? null;
      logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" revival failed: ${msg}`);
      // Revival always continues this same node — no edge resolution — so
      // kind is fixed rather than derived from isFreshNodeEntry.
      const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
      await persistFailRetrying({
        flowId, revision: flow.revision, state,
        message: `${nodeId}: revival failed: ${msg}`, failureNode: nodeId,
        invocationId: written.invocationId ?? null, executionId: written.executionId ?? null, errorType,
      });
      return;
    }

    const { stateUpdate = {}, blockedInfo, skippedInfo } = result;

    if (blockedInfo) {
      if (!blockedInfo._kind) {
        const msg = blockedInfo.reason ?? `${nodeId} blocked without declaring a wait kind`;
        logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" ${msg}`);
        const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `${nodeId}: ${msg}`, failureNode: nodeId,
          invocationId: written.invocationId ?? null, executionId: written.executionId ?? null,
        });
        return;
      }
      const waitJsonForBlock = {
        kind: blockedInfo._kind, nodeId,
        reason: blockedInfo.reason ?? "node blocked", needed: blockedInfo.needed ?? [],
        hint: blockedInfo.hint ?? "", renderedAt: Date.now(), retry: true,
      };
      await persistWaitingRetrying({
        flowId, revision: flow.revision, lastStep: nodeId, blockingNodeId: nodeId,
        state,
        waitJson: waitJsonForBlock,
        statusReason: shortSummary(`${nodeId}: ${waitJsonForBlock.reason}`),
      });
      return;
    }

    if (skippedInfo) {
      await persistSkipRetrying({
        flowId, revision: flow.revision,
        state,
        reason: skippedInfo.reason ?? "skipped",
      });
      return;
    }

    let entryState = state;
    if (entryState._cascadeError?.nodeId === nodeId) {
      const { _cascadeError, ...rest } = entryState;
      entryState = rest;
    }
    // The main loop writes a result before advancing last_step, but this
    // revival resumes the original interrupted dispatch (no new enter(), no
    // writeResult of its own) — without this write, last_step already points
    // at nodeId while its result_json stays empty, so the next runFlow()
    // iteration's assembleState() reads nothing and resolveEdge dead-ends
    // with "no next node" on every revived agent-node whose edge depends on
    // its output.
    const resultWrite = await nodeExecutions.writeResult(invocationId, stateUpdate).catch(() => null);
    if (!resultWrite || !resultWrite.ok) {
      const reason = resultWrite?.reason ?? "threw";
      if (reason !== "race") {
        const msg = `resultJson write failed (${reason})`;
        logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" ${msg}`);
        const written = await safeWriteExecution({ flowId, pipelineId: state.pipelineId ?? null, nodeId, enteredAt, exitedAt: Date.now(), kind: "revival" });
        await persistFailRetrying({
          flowId, revision: flow.revision, state,
          message: `${nodeId}: ${msg}`, failureNode: nodeId,
          invocationId: written.invocationId ?? invocationId, executionId: written.executionId ?? null,
        });
        return;
      }
      logger.warn(`manifold revival [${flowId8}]: agent-node "${nodeId}" resultJson write failed (race) — another writer already completed this invocation`);
    }
    await persistRunningRetrying({ flowId, revision: flow.revision, lastStep: nodeId, state: entryState });
    logger.info(`manifold revival [${flowId8}]: agent-node "${nodeId}" revived — continuing`);
    return runFlow(flowId, "revival");
  }

  return { start, runFlow, resume, cancel, rerun, rerunFromSource, reviveAgentKindOrphan };
}

// ── audit ─────────────────────────────────────────────────────────────────────

async function runAuditIfConfigured({ flowId, pipeline, state, logger }) {
  const audit = pipeline?.runAudit;
  if (!audit) return;
  if (typeof audit.runsDir !== "string" || typeof audit.summarize !== "function") {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit malformed (need runsDir:string + summarize:function); skipping`);
    return;
  }
  const runId = state?.runId;
  if (!runId) {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit: state.runId missing; skipping`);
    return;
  }
  try {
    const runDir = path.join(audit.runsDir, runId);
    await mkdir(runDir, { recursive: true });
    await writeFile(path.join(runDir, "final-state.json"), JSON.stringify(state, null, 2), "utf-8");
    let summary;
    try { summary = await Promise.resolve(audit.summarize(state)); }
    catch (err) {
      logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit.summarize threw: ${err?.message ?? err}`);
      return;
    }
    if (summary && typeof summary === "object") {
      await writeFile(
        path.join(audit.runsDir, "log.jsonl"),
        JSON.stringify(summary) + "\n",
        { flag: "a", encoding: "utf-8" },
      );
    }
  } catch (err) {
    logger?.warn?.(`manifold [${flowId.slice(0, 8)}] runAudit failed: ${err?.message ?? err}`);
  }
}
