// manifold — LangGraph-style pipeline executor for openclaw.
//
// Loads pipeline definitions from `workspace/.pipelines/<team>_<name>.js`,
// drives them against managed `flow_runs`, and dispatches each node:
//   - `code`        : pure function (state) => stateUpdate
//   - `single`      : one runtime.subagent.run, output parsed by `apply`
//   - `subflow`     : bounded-parallel fan-out over `.selector(state)`; slot-per-item writes
//   - `human-gate`  : flow.status='blocked' + wait_json; resumed by CLI approve
//   - `parallel`    : fixed set of named `.tasks`, all dispatched concurrently

import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { configSchema, readPluginConfig } from "./config.js";
import { createStores } from "./stores.js";
import { createRuntime } from "./runtime.js";
import { registerRoutes } from "./routes.js";
import { registerTools } from "./tools.js";
import { registerServices } from "./services.js";
import { registerDashboard } from "./dashboard.js";

export default definePluginEntry({
  id: "manifold",
  name: "Manifold Pipeline Engine",
  description:
    "LangGraph-style pipeline executor: loads workspace/.pipelines/<team>_<name>.js definitions and runs them against managed flow_runs. Nodes (code / single / subflow / human-gate / parallel) advance via explicit edges with conditional routing. CLI surface: `openclaw manifold list / start / status / approve / runs`. Agent tool: `manifold_start`.",
  configSchema,
  register: (api) => {
    const cfg = readPluginConfig(api);
    const pipelinesRef = { value: new Map() };
    // resumeParentRef.resume is set to executor.resume once the executor
    // exists below — store construction has to precede it, so the
    // resume-trigger (state-store.js's finish/skip/fail/cancel) reads
    // through this mutable box rather than a captured closure. Currently
    // dormant: no real flow sets parentInvocationId yet, so this hook never
    // actually fires today.
    const resumeParentRef = { resume: null };
    const stores = createStores(api, cfg, pipelinesRef, resumeParentRef);
    const runtime = createRuntime(api, cfg, stores, pipelinesRef, resumeParentRef);
    const { componentEmitter } = registerRoutes(api, { cfg, stores, runtime, pipelinesRef, resumeParentRef });
    registerTools(api, { cfg, stores, runtime, pipelinesRef, componentEmitter });
    registerServices(api, { cfg, stores, runtime, pipelinesRef, resumeParentRef });
    registerDashboard(api, { cfg, stores, runtime, pipelinesRef });
  },
});
