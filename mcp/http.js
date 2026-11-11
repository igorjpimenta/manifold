// http.js — shared Streamable HTTP MCP server.
// Same tools + same identity contract as stdio (mcp/stdio.js), different
// door: one TCP socket, many harnesses as HTTP clients.
//
// Auth: `Authorization: Bearer <MANIFOLD_BEARER>` checked before any tool
// runs (MANIFOLD_BEARER required; missing/empty refuses to start). Terminate
// TLS in front (reverse proxy / ingress) — the server itself speaks plain
// HTTP. Identity: `X-Manifold-Harness: <name>` declares the harness per
// session; agent+session still arrive per call via _meta.manifold or stamped
// args — identical to stdio resolution. GET /health is unauthenticated.

import http from "node:http";
import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import * as z from "zod/v4-mini";
import { TOOL_DEFS, createManifoldToolHandlers } from "./tools.js";
import { resolveIdentity, identitySessionKey, assertDeclaration } from "./identity.js";
import { toolErrorResult } from "./stdio.js";

function fieldFromJson(schema) {
  if (!schema || typeof schema !== "object") return z.unknown();
  if (schema.enum && Array.isArray(schema.enum)) return z.enum(schema.enum);
  if (schema.type === "string") return z.string();
  if (schema.type === "boolean") return z.boolean();
  if (schema.type === "integer") return z.number();
  if (schema.type === "array") return z.array(fieldFromJson(schema.items));
  if (schema.type === "object") return z.object(shapeFromJson(schema));
  return z.unknown();
}

function shapeFromJson(schema) {
  const shape = {};
  const required = new Set(schema.required ?? []);
  for (const [key, sub] of Object.entries(schema.properties ?? {})) {
    const field = fieldFromJson(sub);
    shape[key] = required.has(key) ? field : z.optional(field);
  }
  shape.manifold_agent = z.optional(z.string());
  shape.manifold_session = z.optional(z.string());
  shape._meta = z.optional(z.unknown());
  return shape;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      if (chunks.length === 0) return resolve(undefined);
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

export async function startMcpHttpServer({ ops, port = null, host = "127.0.0.1", logger } = {}) {
  const bearer = process.env.MANIFOLD_BEARER;
  if (!bearer || bearer.trim() === "") {
    throw new Error("manifold: serve --mcp http refuses without MANIFOLD_BEARER. Set a bearer token in the environment.");
  }
  const listenPort = port ?? 38293;
  const sessions = new Map();

  async function sessionFor(req, base) {
    const sid = req.headers["mcp-session-id"];
    if (typeof sid === "string" && sessions.has(sid)) return sessions.get(sid);
    const { handle } = createManifoldToolHandlers(ops);
    const server = new McpServer({ name: "manifold", version: "0.1.0" });
    for (const def of TOOL_DEFS) {
      server.registerTool(
        def.name,
        { description: def.description, inputSchema: z.object(shapeFromJson(def.inputSchema)) },
        async (args, extra) => {
          try {
            const identity = resolveIdentity({ meta: extra?._meta, args, base });
            logger?.info?.(`manifold: ${def.name} from ${identitySessionKey(identity)}`);
            const res = await handle(def.name, args, identity);
            return { content: res.content };
          } catch (err) {
            return toolErrorResult(err);
          }
        }
      );
    }
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (id) => sessions.set(id, { server, transport }),
    });
    transport.onclose = () => {
      for (const [id, entry] of sessions) {
        if (entry.transport === transport) sessions.delete(id);
      }
    };
    await server.connect(transport);
    return { server, transport };
  }

  const httpServer = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://x");
      if (url.pathname === "/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      if (url.pathname !== "/mcp") {
        res.writeHead(404, { "content-type": "text/plain" });
        res.end("manifold mcp-http: POST /mcp only\n");
        return;
      }
      const auth = req.headers.authorization ?? "";
      if (auth !== `Bearer ${bearer}`) {
        res.writeHead(401, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "unauthorized: bad or missing bearer" }));
        return;
      }
      const harness = req.headers["x-manifold-harness"];
      const pipelinesDir = req.headers["x-manifold-pipelines-dir"];
      let base;
      try {
        base = assertDeclaration({
          harness: Array.isArray(harness) ? harness[0] : harness,
          pipelinesDir: Array.isArray(pipelinesDir) ? pipelinesDir[0] : pipelinesDir,
        });
      } catch {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "missing or reserved X-Manifold-Harness" }));
        return;
      }
      const { transport } = await sessionFor(req, base);
      let body;
      if (req.method === "POST") body = await readBody(req);
      await transport.handleRequest(req, res, body);
    } catch (err) {
      logger?.warn?.(`manifold: mcp-http request failed: ${err?.message ?? err}`);
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      }
    }
  });

  return new Promise((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(listenPort, host, () => {
      logger?.info?.(`manifold: mcp-http on http://${host}:${httpServer.address().port}/mcp (Streamable HTTP; bearer + X-Manifold-Harness)`);
      resolve({
        port: httpServer.address().port,
        host,
        close: () => new Promise((res) => httpServer.close(() => res())),
      });
    });
  });
}
