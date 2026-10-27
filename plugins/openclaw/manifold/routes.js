import { readPending, clearPending, setPendingText, mergePendingFields } from "../../../state/pending-store.js";
import { costForAllPipelines } from "../../../dashboard/cost.js";
import { invokeElevated } from "../../../elevation.js";
import { createManifoldParrotChannelEntry } from "./channels/manifold-parrot-channel.js";
import { registerDiscussHandlers } from "../../../routes/discuss.js";
import { createComponentEmitter } from "../../../routes/discuss-component.js";
import { registerRunRoutes } from "../../../routes/runs.js";
import { registerOperatorRoutes } from "../../../routes/operator.js";
import { resumeFlowFromGate } from "../../../routes/operator-resume.js";
import { registerFailureRoutes } from "../../../routes/failures.js";
import { registerGraphqlRoutes } from "../../../routes/graphql.js";
import { registerHealthRoutes } from "../../../routes/health.js";
import { registerPipelineRoutes } from "../../../routes/pipelines.js";
import { authorizeBridgeRequest, sendUnauthorized } from "./auth/bridge.js";
import { registerBudgetRoutes } from "../../../budget/index.js";
import { registerTelemetry } from "../../../telemetry/index.js";
import * as telemetrySources from "./telemetry-sources.js";

export function registerRoutes(api, { cfg, stores, runtime, pipelinesRef, resumeParentRef }) {
  const { discussDefaultAgent, ownerSessionKey } = cfg;
  const {
    readStore, store, runStore, nodeExecutionsStore, discussStore, failureStore,
    pipelineRegistryStore, ensurePipelinesLoaded, admissionQueueStore,
    gatewayBroadcastRef, emitCanonical,
  } = stores;
  const { executor, gateResolver } = runtime;
  // ── componentEmitter + gateway ping ──────────────────────────────────────

  // Proposal-artifact fan-out (routes/discuss-component.js): persists the
  // component, then emits canonical proposed + derived pending through
  // the shared emitter. Egress carries canonical kinds.
  const componentEmitter = createComponentEmitter({ discussStore, emit: emitCanonical });

  api.registerGatewayMethod("plugin.manifold.ping", (opts) => {
    if (!gatewayBroadcastRef.current) {
      gatewayBroadcastRef.current = opts.context.broadcast;
      api.logger.info("manifold: WS broadcaster captured");
    }
    opts.respond(true, { ok: true });
  }, { scope: "operator.read" });
  // ── Discuss handlers ─────────────────────────────────────────────────────

  const discussHandlers = registerDiscussHandlers({
    api,
    store,
    pipelinesRef,
    emit: emitCanonical,
    discussDefaultAgent,
    invokeElevated,
    readPending,
    clearPending,
    discussStore,
    runStore,
    nodeExecutionsStore,
  });

  // ── Route modules ────────────────────────────────────────────────────────

  registerRunRoutes({
    api,
    auth: { authorizeBridgeRequest, sendUnauthorized },
    readStore,
    costForAllPipelines,
    runStore,
    pipelinesRef,
    pipelineRegistryStore,
    invokeElevated,
    discussStore,
    failureStore,
    handleDiscuss:           discussHandlers.handleDiscuss,
    handleDiscussHistory:    discussHandlers.handleDiscussHistory,
    handleDiscussApplyDraft: discussHandlers.handleDiscussApplyDraft,
    handleDiscussAudio:      discussHandlers.handleDiscussAudio,
    handleDiscussAudioGet:   discussHandlers.handleDiscussAudioGet,
    handleDiscussAudioRetry: discussHandlers.handleDiscussAudioRetry,
  });

  registerOperatorRoutes({
    api,
    auth: { authorizeBridgeRequest, sendUnauthorized },
    store,
    readStore,
    executor,
    gateResolver,
    failureStore,
    pipelinesRef,
    ensurePipelinesLoaded,
    invokeElevated,
    ownerSessionKey,
    runStore,
    admissionQueueStore,
    resumeParentRef,
    stampDiscussSession:  discussHandlers.stampDiscussSession,
    normalizePendingFields: discussHandlers.normalizePendingFields,
    readPending,
    mergePendingFields,
    setPendingText,
    componentEmitter,
    getBroadcast: () => gatewayBroadcastRef.current,
  });

  registerFailureRoutes({
    api,
    auth: { authorizeBridgeRequest, sendUnauthorized },
    failureStore,
    invokeElevated,
  });

  registerGraphqlRoutes({
    api,
    auth: { authorizeBridgeRequest, sendUnauthorized },
    failureStore,
    runStore,
  });

  registerHealthRoutes({ api, auth: { authorizeBridgeRequest, sendUnauthorized } });

  registerPipelineRoutes({
    api,
    auth: { authorizeBridgeRequest, sendUnauthorized },
    pipelinesRef,
    readStore,
    ensurePipelinesLoaded,
  });

  registerBudgetRoutes(api, {
    getBroadcast: () => gatewayBroadcastRef.current,
    // In-process resume for flow-bound budget approvals (no HTTP loopback).
    onFlowResume: ({ flowId, nodeId, decision }) => resumeFlowFromGate({
      store, readStore, admissionQueueStore, runStore, executor,
      logger: api.logger, api, flowId, nodeId, decision,
    }),
  });
  registerTelemetry(api, { getBroadcast: () => gatewayBroadcastRef.current, storePaths, sources: telemetrySources });

  // ── Parrot channel (takes the shared emitter directly) ────────────────────

  createManifoldParrotChannelEntry({
    readStore,
    discussStore,
    resolveDiscussTarget: discussHandlers.resolveDiscussTarget,
    stampDiscussSession:  discussHandlers.stampDiscussSession,
    emit: emitCanonical,
  }).register(api);
  return { componentEmitter, discussHandlers };
}
