import path from "node:path";
import os from "node:os";

const DEFAULT_OWNER_SESSION_KEY = "agent:main:explicit:manifold-owner";
const DEFAULT_DASHBOARD_PORT = 7430;
const DEFAULT_DASHBOARD_HOST = "127.0.0.1";

const TASKFLOW_RUNS_DB = process.env.MANIFOLD_TASKFLOW_RUNS_DB
  || path.join(os.homedir(), ".openclaw", "tasks", "runs.sqlite");

export const configSchema = () => ({
    type: "object",
    additionalProperties: false,
    properties: {
      pipelinesDir: {
        type: "string",
        description: "Directory to scan for pipeline definition files. Default: <dataDir>/workspace/.pipelines (MANIFOLD_DATA_DIR or ~/.manifold).",
      },
      ownerSessionKey: {
        type: "string",
        description: "Session key used to scope flow_run ownership. Default: agent:main:explicit:manifold-owner",
      },
      dashboardEnabled: {
        type: "boolean",
        description: "Whether the read-only HTTP dashboard service should boot. Default: true.",
      },
      dashboardPort: {
        type: "integer",
        minimum: 1024,
        maximum: 65535,
        description: `Port the dashboard listens on (bound to 127.0.0.1 only). Default: ${DEFAULT_DASHBOARD_PORT}.`,
      },
      dashboardHost: {
        type: "string",
        description: "Host/interface to bind. Default: 127.0.0.1 (loopback only). Override at your own risk.",
      },
      manifoldParrot: {
        type: "object",
        description: "manifold-parrot channel config. manifold-parrot is the built-in discuss channel a consumer opens when a human-gate is blocked.",
        additionalProperties: false,
        properties: {
          defaultAgent: {
            type: "string",
            description: "Default agent for the manifold-parrot channel. Used when the blocked gate node does not declare its own discuss.agent.",
          },
        },
      },
      taskflowRevive: {
        type: "object",
        description: "Config for boot-time task orphan cleanup.",
        additionalProperties: false,
        properties: {
          startupDelayMs: { type: "number", minimum: 0 },
        },
      },
      globalConcurrency: {
        type: "integer",
        minimum: 1,
        description: "Shared ceiling for every flow-run concurrency mechanism — agent dispatch, subflow fan-out, and whole-flow-run concurrency alike (engine/global-concurrency.js). Default: 8.",
      },
      pushUrls: {
        type: "object",
        description: "Per-harness push targets core POSTs dispatch/gate events to (each: {url, bearer?, timeoutMs?}). DB-registered targets win over this map.",
      },
      pushUrl: {
        type: "string",
        description: "Single fallback push URL for all harnesses when pushUrls has no entry.",
      },
      pushBearer: {
        type: "string",
        description: "Bearer core sends as Authorization on fallback pushUrl POSTs.",
      },
      pushTimeoutMs: {
        type: "integer",
        minimum: 1,
        description: "Bounded push POST timeout. Default: 3000.",
      },
      webhookBearer: {
        type: "string",
        description: "Shared secret core must send as Authorization: Bearer on POST /api/manifold/pushed. Must match the registered push target bearer. Route stays disarmed (503) until set.",
      },
      webhookPublicBaseUrl: {
        type: "string",
        description: "Gateway address core can reach, e.g. http://127.0.0.1:18789. Used by self-registration to build the route URL.",
      },
      webhookHarness: {
        type: "string",
        description: "Harness name registered for push delivery. Default: openclaw.",
      },
      webhookAutoregister: {
        type: "boolean",
        description: "Register the route URL as this harness's push target on boot (upsert + test ping). Default true when webhookBearer is set.",
      },
      profilePath: {
        type: "string",
        description: "Absolute path of the PROFILE.md file guarded by the before_tool_call read gate and edit lock (hooks/profile-read-gate.js, hooks/profile-write-lock.js). Default: ~/.openclaw/workspace/.shared/freelance/profile/PROFILE.md",
      },
      profileEditResultsDir: {
        type: "string",
        description: "Directory the edit lock writes one <runId>.json outcome marker per guarded PROFILE.md edit into (hooks/profile-write-lock.js). Default: ~/.openclaw/locks/profile-edit-results",
      },
    },
});

export function readPluginConfig(api) {
  const userCfg = api.pluginConfig ?? {};
  const pushHarness = userCfg.webhookHarness ?? "openclaw";
  const ownerSessionKey = userCfg.ownerSessionKey || DEFAULT_OWNER_SESSION_KEY;
  const discussDefaultAgent = typeof userCfg.manifoldParrot?.defaultAgent === "string" ? userCfg.manifoldParrot.defaultAgent : null;
  const dashboardEnabled = userCfg.dashboardEnabled !== false;
  const dashboardPort = Number.isInteger(userCfg.dashboardPort) ? userCfg.dashboardPort : DEFAULT_DASHBOARD_PORT;
  const dashboardHost = userCfg.dashboardHost || DEFAULT_DASHBOARD_HOST;
  const pushConfig = {
    pushUrls: userCfg.pushUrls,
    pushUrl: userCfg.pushUrl,
    pushBearer: userCfg.pushBearer,
    pushTimeoutMs: userCfg.pushTimeoutMs,
  };
  // Optional extra delay before the scan, for operators who want one.
  // Dispatches already wait for gateway readiness at the executor's gate,
  // so it defaults to 0.
  const taskflowStartupDelayMs = Number(userCfg.taskflowRevive?.startupDelayMs) >= 0
    ? Number(userCfg.taskflowRevive?.startupDelayMs)
    : 0;
  return {
    userCfg, pushHarness, ownerSessionKey, discussDefaultAgent,
    dashboardEnabled, dashboardPort, dashboardHost, pushConfig,
    taskflowStartupDelayMs, globalConcurrency: userCfg.globalConcurrency,
    profilePath: userCfg.profilePath, profileEditResultsDir: userCfg.profileEditResultsDir,
    pipelinesConfigDir: userCfg.pipelinesDir || null,
  };
}
