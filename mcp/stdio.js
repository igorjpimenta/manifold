// stdio.js — stdio MCP server exposing the pipeline tools. stdout is the
// JSON-RPC channel: every log line goes to stderr, and nothing else may be
// written to stdout.
//
// Request `_meta` surfaces on the handler's second argument (`extra._meta`,
// the original request's params._meta — see the SDK's RequestHandlerExtra).
// `extra._meta.manifold` is the trusted companion-plugin channel; models
// cannot forge it because the plugin overwrites it unconditionally.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4-mini";
import { TOOL_DEFS, createManifoldToolHandlers } from "./tools.js";
import { resolveIdentity, identitySessionKey, assertDeclaration } from "./identity.js";

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

export function toolErrorResult(err) {
  const message = `manifold: tool failed: ${String(err?.message ?? err)}`;
  if (process.env.MANIFOLD_MCP_DEBUG) {
    console.error(message);
  }
  return {
    content: [{ type: "text", text: JSON.stringify({ error: message }) }],
    isError: true,
  };
}

export async function startMcpServer({ ops, base, logger }) {
  // Identity is refused, never defaulted, at both layers: an undeclared
  // server will not start, and an unstamped, unusable, or mismatched call
  // will not run. Harness comes from the ctor arg or MANIFOLD_HARNESS env.
  const declared = assertDeclaration(base ?? { harness: process.env.MANIFOLD_HARNESS, pipelinesDir: process.env.MANIFOLD_PIPELINES_DIR, concurrency: process.env.MANIFOLD_CONCURRENCY });
  const { handle } = createManifoldToolHandlers(ops);
  const server = new McpServer({ name: "manifold", version: "0.1.0" });

  for (const def of TOOL_DEFS) {
    server.registerTool(
      def.name,
      { description: def.description, inputSchema: z.object(shapeFromJson(def.inputSchema)) },
      async (args, extra) => {
        try {
          const identity = resolveIdentity({ meta: extra?._meta, args, base: declared });
          logger?.info?.(`manifold: ${def.name} from ${identitySessionKey(identity)}`);
          const res = await handle(def.name, args, identity);
          return { content: res.content };
        } catch (err) {
          return toolErrorResult(err);
        }
      }
    );
  }

  const transport = new StdioServerTransport();
  await server.connect(transport);
  logger?.info?.("manifold: MCP server connected on stdio");
  return server;
}
