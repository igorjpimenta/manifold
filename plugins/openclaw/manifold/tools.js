import { registerCliCommands } from "../../../bin/cli.js";
import {
  registerGraphFlowTool,
  registerProposeDraftTool,
  registerProposeDraftMetaTool,
  registerSubmitDraftTool,
  registerSubmitScoreTool,
  registerReportDraftBlockedTool,
  registerReportDraftSkipTool,
} from "./agent/tools/index.js";
import { registerProfileReadGate } from "./hooks/profile-read-gate.js";
import { registerProfileWriteLock } from "./hooks/profile-write-lock.js";
import { registerManifoldMetaHook } from "./meta.js";

export function registerTools(api, { cfg, stores, runtime, pipelinesRef, componentEmitter }) {
  const {
    profilePath, profileEditResultsDir, dashboardHost, dashboardPort,
    ownerSessionKey, pushHarness, pipelinesConfigDir,
  } = cfg;
  const {
    store, runStore, nodeExecutionsStore, admissionQueueStore, pipelineDirsStore,
    storePaths, ensurePipelinesLoaded, nodeHealthStore, readStore, discussStore,
  } = stores;
  const { executor } = runtime;
  // Registered here (rather than deferred) since PROFILE.md reads/edits
  // can happen from the very first dispatched pipeline run after boot.
  // Both path options fall through to each hook's own os.homedir() default
  // when the config key is absent or undefined, so a zero-config install
  // acquires byte-identical lock names to the workspace-side PROFILE.md
  // writers (gap-answers.js, prime-nodes.js) it shares that lock with.
  registerProfileReadGate(api, { profilePath });
  registerProfileWriteLock(api, { profilePath, resultsDir: profileEditResultsDir });
  registerManifoldMetaHook(api);
  // ── CLI + tools ─────────────────────────────────────────────────────────

  api.registerCli(
    (ctx) => registerCliCommands({
      program: ctx.program,
      executor,
      store,
      pipelinesRef,
      logger: ctx.logger ?? api.logger,
      dashboardUrl: `http://${dashboardHost}:${dashboardPort}`,
      api,
      nodeHealthStore,
      ownerSessionKey,
      runStore,
      nodeExecutionsStore,
      admissionQueueStore,
      pipelineDirsStore,
      resolvePipelinesDir: (harness) => pipelineDirsStore.resolveDir({
        harness: harness ?? pushHarness,
        env: process.env,
        dataDir: storePaths.dataDir,
        configDir: pipelinesConfigDir,
      }),
    }),
    {
      commands: ["manifold"],
      descriptors: [{
        name: "manifold",
        description: "Run state-graph pipelines from workspace/.pipelines/",
        hasSubcommands: true,
      }],
    },
  );
  registerGraphFlowTool({ api, executor, store, pipelinesRef, ensurePipelinesLoaded });

  registerSubmitDraftTool({ api, pipelinesRef });
  registerSubmitScoreTool({ api });
  registerReportDraftBlockedTool({ api });
  registerReportDraftSkipTool({ api });
  registerProposeDraftTool({ api, readStore, componentEmitter, discussStore });
  registerProposeDraftMetaTool({ api, readStore, componentEmitter, discussStore });
}
