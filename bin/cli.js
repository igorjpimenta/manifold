import { loadPipelines } from "../engine/loader.js";
import path from "node:path";
import os from "node:os";
import { getNodeExecutionsStore } from "../state/node-executions.js";
import { drainDetachedResumes } from "../engine/admission-promotion-resume.js";
import { parseArgs } from "node:util";
import { startMcpServer } from "../mcp/stdio.js";
import { createRealOps } from "../mcp/wire-ops.js";
import { createEventStream } from "../events/stream.js";
import { startWsServer, DEFAULT_WS_PORT } from "../events/serve-ws.js";
import { createPushTargetsStore } from "../state/push-targets.js";
import { resolvePushTarget, notifyPush } from "../events/push.js";
import { resolveStorePaths } from "../state/paths.js";
import { registerRunCommands } from "./cli-runs.js";
import { registerPipelineCommands } from "./cli-pipelines.js";
import { registerGateCommands } from "./cli-gates.js";
import { registerOpsCommands } from "./cli-ops.js";

// cli.js — registers the `openclaw manifold <subcommand>` CLI surface.
//
// IMPORTANT: CLI handlers run in a separate openclaw process from the gateway,
// so the `pipelinesRef` populated by the gateway's loader-service is NOT
// visible here. Every CLI handler that needs pipeline definitions must
// re-load them from disk on demand via ensurePipelinesLoaded(). Similarly,
// the executor loop runs in-process here (sync await), so CLI commands
// `start`, `rerun`, `retry`, and `approve` all block until the flow reaches
// terminal or blocked.


export const PIPELINES_DIR = path.join(os.homedir(), ".openclaw", "workspace", ".pipelines");

export async function ensurePipelinesLoaded(pipelinesRef, logger, dir = null) {
  if (pipelinesRef.value && pipelinesRef.value.size > 0) return;
  const loaded = await loadPipelines(dir ?? PIPELINES_DIR, logger);
  pipelinesRef.value = loaded;
}

// No subcommand exits on its own after a successful run: something opened
// during the plugin boot phase, before any subcommand-specific code even
// runs, holds the process open regardless of which subcommand ran, and it
// isn't a leftover socket or timer that Node's own introspection exposes,
// so waiting it out isn't an option. An explicit exit is the only reliable
// fix.
//
// Only safe to wrap an action once its own async work is FULLY done —
// fire-and-forget follow-on work scheduled as setImmediate/setTimeout (fine
// for a long-lived HTTP/Telegram/dashboard/iOS caller) is dropped by the
// exit below. Every command must await its own work to completion: retry
// via routes/operator-retry.js's awaitCompletion, approve via
// gates/gate-resolver.js's awaitCompletion (which also covers
// applyResolutionsAndResume's own multi-round setTimeout-based retry chain
// on sustained revision contention, not just its setImmediate resume).
// Drains detached promotion resumes before exiting: any command that
// releases an admission can promote another flow, whose run is scheduled on
// a later tick (engine/admission-promotion-resume.js) — process.exit()
// right after would drop it with the row already flipped to 'running'.
export function withExit(action) {
  return async (...args) => {
    await action(...args);
    await drainDetachedResumes();
    process.exit(0);
  };
}

export function registerCliCommands({ program, executor, store, pipelinesRef, logger, dashboardUrl, api, nodeHealthStore, ownerSessionKey, runStore, nodeExecutionsStore, admissionQueueStore, pipelineDirsStore = null, resolvePipelinesDir = null }) {
  const root = program.command("manifold");
  root.description("Run state-graph pipelines defined in workspace/.pipelines/");
  const deps = { program, executor, store, pipelinesRef, logger, dashboardUrl, api, nodeHealthStore, ownerSessionKey, runStore, nodeExecutionsStore, admissionQueueStore, pipelineDirsStore, resolvePipelinesDir };
  registerRunCommands(root, deps);
  registerPipelineCommands(root, deps);
  registerGateCommands(root, deps);
  registerOpsCommands(root, deps);
}

export function safeParseJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

export function browserOpenCommand(url) {
  // Per-platform opener. macOS `open`, Linux `xdg-open`, Windows `start` via
  // cmd /c. Returns { cmd, args } or { cmd: null } if the platform is unknown.
  if (process.platform === "darwin") return { cmd: "open", args: [url] };
  if (process.platform === "win32")  return { cmd: "cmd",  args: ["/c", "start", "", url] };
  if (process.platform === "linux")  return { cmd: "xdg-open", args: [url] };
  return { cmd: null, args: [] };
}

const USAGE = `usage: manifold <command>

  serve --mcp stdio|http [--port N] [--host H]
                                       serve the pipeline tools over MCP stdio or Streamable HTTP
                                       (stdio harness from MANIFOLD_HARNESS env only; agent+session stamped per call;
                                       http additionally needs MANIFOLD_BEARER + X-Manifold-Harness per session)
  serve --ws [--port N] [--host H]       stream push events over WebSocket (verbatim + history replay)
  webhook set --harness H --url U [--bearer B] [--timeout-ms N]
                                            register a harness push URL (checked first at push time)
  webhook list                             list registered push URLs
  webhook remove --harness H               drop a registered push URL
  webhook test --harness H                 POST a test ping to the harness target`;

function fail(msg, code = 2) {
  console.error(msg);
  process.exit(code);
}

function stderrLogger() {
  return {
    info: (...a) => console.error(...a),
    warn: (...a) => console.error(...a),
    error: (...a) => console.error(...a),
  };
}

function validTimeoutMs(v) {
  if (v === undefined || v === null) return null;
  const n = Number(v);
  if (!Number.isInteger(n) || n <= 0) fail(`manifold: --timeout-ms must be a positive integer, got ${JSON.stringify(v)}`);
  return n;
}

function validPushUrl(url) {
  try {
    const parsed = new URL(String(url));
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error("not http(s)");
    return String(url).trim();
  } catch {
    fail(`manifold: invalid webhook url (must be http(s)://…): ${url}`, 1);
  }
}

export async function runCli(argv) {
  const [cmd, ...rest] = argv;
  switch (cmd) {
    case undefined:
    case "--help":
    case "-h":
    case "help":
      console.log(USAGE);
      return;

    case "serve": {
      let values;
      try {
        ({ values } = parseArgs({
          args: rest,
          options: {
            ws: { type: "boolean" },
            mcp: { type: "string" },
            port: { type: "string" },
            host: { type: "string" },
          },
        }));
      } catch (err) {
        if (/Unknown option|ERR_PARSE_ARGS_UNKNOWN_OPTION/.test(String(err?.message ?? err))) {
          fail("manifold serve --mcp takes no --harness flag: set MANIFOLD_HARNESS in the environment.");
        }
        throw err;
      }
      if (values.mcp !== undefined && values.mcp !== "stdio" && values.mcp !== "http") {
        fail(`manifold: --mcp must be stdio or http, got ${JSON.stringify(values.mcp)}`);
      }
      const modes = ["ws", "mcp"].filter((k) => values[k]);
      if (modes.length !== 1) {
        fail("usage: manifold serve (--ws | --mcp stdio|http) [--port N] [--host H]");
      }
      const logger = stderrLogger();
      const host = values.host ?? "127.0.0.1";
      const port = values.port ? Number(values.port) : undefined;
      if (port !== undefined && !(Number.isInteger(port) && port >= 1 && port <= 65535)) {
        fail(`manifold: --port must be 1-65535, got ${JSON.stringify(values.port)}`);
      }
      if (values.mcp === "stdio") {
        const ops = await createRealOps({ logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        // Harness is env-only here by design: the spawner owns identity.
        await startMcpServer({ ops, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      if (values.mcp === "http") {
        const { startMcpHttpServer } = await import("../mcp/http.js");
        const ops = await createRealOps({ logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        await startMcpHttpServer({ ops, port, host, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      const stream = createEventStream();
      await startWsServer({ stream, port: port ?? DEFAULT_WS_PORT, host, logger });
      logger.info(`manifold: events on ws://${host}:${port ?? DEFAULT_WS_PORT} (verbatim; subscribers track their own state)`);
      return;
    }

    case "webhook": {
      const [sub, ...subRest] = rest;
      if (!["set", "list", "remove", "test"].includes(sub)) {
        fail("usage: manifold webhook (set|list|remove|test) …");
      }
      const dbPath = resolveStorePaths().runtimeDb;
      const pushTargets = await createPushTargetsStore({ dbPath });
      try {
        if (sub === "list") {
          console.log(JSON.stringify(await pushTargets.getAll(), null, 2));
          return;
        }
        const { values } = parseArgs({
          args: subRest,
          options: {
            harness: { type: "string" },
            url: { type: "string" },
            bearer: { type: "string" },
            "timeout-ms": { type: "string" },
          },
        });
        const harness = values.harness;
        if (!harness || !harness.trim()) fail(`manifold webhook ${sub} requires --harness <name>`);
        if (sub === "remove") {
          const { removed } = await pushTargets.remove(harness);
          if (!removed) fail(`no webhook registered for harness: ${harness}`, 1);
          console.log(JSON.stringify({ removed: harness }));
          return;
        }
        if (sub === "set") {
          if (!values.url) fail("manifold webhook set requires --url <http(s)://…>");
          const timeoutMs = validTimeoutMs(values["timeout-ms"]);
          try {
            await pushTargets.upsert({
              harness,
              url: validPushUrl(values.url),
              bearer: values.bearer ?? null,
              timeoutMs,
            });
            const row = await pushTargets.get(harness);
            console.log(JSON.stringify({ ...row, bearer: row.bearer ? "<set>" : null }, null, 2));
          } catch (err) {
            fail(String(err?.message ?? err), 1);
          }
          return;
        }
        // test: resolve exactly like a push would (DB-registered target) and
        // POST a test ping. The adapter answers {ok:true,test:true}.
        const hook = resolvePushTarget({ pushTargets: await pushTargets.get(harness), harnessId: harness });
        if (!hook) fail(`no webhook target for harness: ${harness} (register one with webhook set first)`, 1);
        const r = await notifyPush({
          target: hook,
          payload: { event: "webhook.test", at: Date.now(), session: { harness } },
          logger: stderrLogger(),
        });
        console.log(JSON.stringify(r, null, 2));
        if (!r.ok) process.exitCode = 1;
        return;
      } finally {
        pushTargets.close();
      }
    }

    default:
      fail(`unknown command: ${cmd}\n\n${USAGE}`);
  }
}
