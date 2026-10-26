// manifold — neutral core entry: pipeline runtime factories. The gateway
// plugin entry that wires these factories to a live api lives in
// plugins/openclaw/manifold/index.js.

import { createExecutor } from "./engine/executor.js";
import { createPipelinesLoader } from "./engine/loader.js";
import { createStateStore } from "./state/state-store.js";
import { createRunStore } from "./state/run-store.js";
import { createFailureStore } from "./state/failure-store.js";
import { createDiscussStore } from "./state/discuss-store.js";
import { createSessionIndex } from "./state/session-index.js";
import { createNodeHealthStore } from "./state/node-health-store.js";
import { createAdmissionQueueStore } from "./state/admission-queue-store.js";
import { getNodeExecutionsStore } from "./state/node-executions.js";
import { createGateResolver } from "./gates/gate-resolver.js";
import { registerCliCommands } from "./bin/cli.js";
import { invokeElevated } from "./elevation.js";

export {
  createExecutor,
  createPipelinesLoader,
  createStateStore,
  createRunStore,
  createFailureStore,
  createDiscussStore,
  createSessionIndex,
  createNodeHealthStore,
  createAdmissionQueueStore,
  getNodeExecutionsStore,
  createGateResolver,
  registerCliCommands,
  invokeElevated,
};

export default {
  createExecutor,
  createPipelinesLoader,
  createStateStore,
  createRunStore,
  createFailureStore,
  createDiscussStore,
  createSessionIndex,
  createNodeHealthStore,
  createAdmissionQueueStore,
  getNodeExecutionsStore,
  createGateResolver,
  registerCliCommands,
  invokeElevated,
};
