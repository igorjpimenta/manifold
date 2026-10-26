#!/usr/bin/env node
// manifold.js — standalone core CLI: serve the MCP tools and the WS event
// pipe, manage per-harness push targets. No harness imports: everything
// here is core-owned (run-store, push-targets, executor wiring).
//
// stdout is the data channel for every subcommand; diagnostics go to
// stderr. MANIFOLD_DATA_DIR relocates the whole store (state/paths.js).

import { parseArgs } from "node:util";
import { startMcpServer } from "../mcp/stdio.js";
import { createRealOps } from "../mcp/wire-ops.js";
import { createEventStream } from "../events/stream.js";
import { startWsServer, DEFAULT_WS_PORT } from "../events/serve-ws.js";
import { createPushTargetsStore } from "../state/push-targets.js";
import { resolvePushTarget, notifyPush } from "../events/push.js";
import { resolveStorePaths } from "../state/paths.js";

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

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  runCli(process.argv.slice(2)).catch((err) => fail(String(err?.stack ?? err), 1));
}
