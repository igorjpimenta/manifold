// cli.js — the engine CLI: runCli(argv) boots the app, MCP, or WS modes
// in a standalone process (bin/manifold.js's thin entry calls it).
// This module imports no harness SDK, so it loads under plain node for
// standalone use and node:test.
import { resolveStorePaths } from "../state/paths.js";
import { parseArgs } from "node:util";
import { startMcpServer } from "../mcp/stdio.js";
import { createRealOps } from "../mcp/wire-ops.js";
import { createEventStream } from "../events/stream.js";
import { startWsServer, DEFAULT_WS_PORT } from "../events/serve-ws.js";

// Precedence: explicit --pipelines-dir flag wins, then the adapter seam
// (resolvePipelinesDir: table -> env -> config -> default), then the store
// seam (resolveStorePaths().pipelinesDir, honoring MANIFOLD_PIPELINES_DIR).
// Throws when nothing resolves — callers fail loudly naming the flag/env
// instead of silently falling back to a harness layout.
export function resolveCliPipelinesDir({ flagDir = null, resolvePipelinesDir = null, storePaths = null, env = process.env } = {}) {
  if (flagDir) return { dir: flagDir, source: "flag" };
  try {
    const adapter = resolvePipelinesDir?.();
    if (adapter?.dir) return adapter;
  } catch { /* adapter unavailable — fall through to the store seam */ }
  const fromStore = storePaths?.pipelinesDir ?? resolveStorePaths({ env }).pipelinesDir;
  if (fromStore) return { dir: fromStore, source: env?.MANIFOLD_PIPELINES_DIR && !storePaths?.pipelinesDir ? "env" : "store" };
  throw new Error("manifold: no pipelines directory resolved — pass --pipelines-dir <dir> or set MANIFOLD_PIPELINES_DIR");
}

const USAGE = `usage: manifold <command>

  serve [--pipelines-dir DIR]          boot the manifold app (API, executor, dashboard)
  serve --mcp stdio|http [--port N] [--host H] [--pipelines-dir DIR]
                                       serve the pipeline tools over MCP stdio or Streamable HTTP
                                       (stdio harness from MANIFOLD_HARNESS env only; agent+session stamped per call;
                                       http additionally needs MANIFOLD_BEARER + X-Manifold-Harness per session)
  serve --ws [--port N] [--host H]       stream push events over WebSocket (verbatim + history replay)`;

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
            "pipelines-dir": { type: "string" },
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
      if (modes.length > 1) {
        fail("usage: manifold serve [--pipelines-dir DIR] | --ws | --mcp stdio|http");
      }
      const logger = stderrLogger();
      if (modes.length === 0) {
        // No mode flag: the full app — stores, executor, pipelines watcher,
        // revival, HTTP API and dashboard — as its own process. The harness
        // adapter spawns exactly this and bridges agent turns over HTTP.
        if (values["pipelines-dir"]) process.env.MANIFOLD_PIPELINES_DIR = values["pipelines-dir"];
        const { serve } = await import("../server/serve.js");
        await serve({ logger }).catch((err) => fail(`manifold serve: ${err?.message ?? err}`, 1));
        return;
      }
      const host = values.host ?? "127.0.0.1";
      const port = values.port ? Number(values.port) : undefined;
      if (port !== undefined && !(Number.isInteger(port) && port >= 1 && port <= 65535)) {
        fail(`manifold: --port must be 1-65535, got ${JSON.stringify(values.port)}`);
      }
      const servePipelinesDir = values["pipelines-dir"] ?? null;
      if (values.mcp === "stdio") {
        const ops = await createRealOps({ pipelinesDir: servePipelinesDir, logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        // Harness is env-only here by design: the spawner owns identity.
        await startMcpServer({ ops, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      if (values.mcp === "http") {
        const { startMcpHttpServer } = await import("../mcp/http.js");
        const ops = await createRealOps({ pipelinesDir: servePipelinesDir, logger }).catch((err) => fail(`manifold: ops wiring failed: ${err?.message ?? err}`, 1));
        await startMcpHttpServer({ ops, port, host, logger }).catch((err) => fail(String(err?.message ?? err)));
        return;
      }
      const stream = createEventStream();
      await startWsServer({ stream, port: port ?? DEFAULT_WS_PORT, host, logger });
      logger.info(`manifold: events on ws://${host}:${port ?? DEFAULT_WS_PORT} (verbatim; subscribers track their own state)`);
      return;
    }

    default:
      fail(`unknown command: ${cmd}\n\n${USAGE}`);
  }
}
