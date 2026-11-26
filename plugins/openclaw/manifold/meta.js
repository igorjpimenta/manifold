// Pure before_tool_call identity stamping. No plugin-SDK import, so it loads
// under `node --test` outside the openclaw gateway.

// openclaw exposes MCP tools as mcp__<server>__<tool> (e.g.
// mcp__manifold__manifold_start); the substring match survives a prefix change.
export function isManifoldTool(name) {
  return typeof name === "string" && name.includes("manifold");
}

// Session resolution order is most stable first: live session key, session
// id, single-run id, then the same fields off the event. A null dimension
// stays null: mcp/identity.js treats it as a miss and refuses the call, so a
// half-stamped call fails loudly instead of minting a default key. With no
// resolvable dimension the hook returns undefined and the call proceeds
// unstamped, for the server to refuse.
export function buildManifoldMeta(event, ctx) {
  const toolName = event?.toolName ?? ctx?.toolName;
  if (!isManifoldTool(toolName)) return undefined;
  const agent = ctx?.agentId ?? event?.agentId ?? null;
  const session = ctx?.sessionKey ?? ctx?.sessionId ?? ctx?.runId ?? event?.sessionId ?? event?.runId ?? null;
  if (agent === null && session === null) return undefined;
  const params = event?.params ?? {};
  return {
    params: {
      ...params,
      _meta: {
        ...(params._meta ?? {}),
        manifold: { harness: "openclaw", agent, session },
      },
    },
  };
}

// The before_tool_call return channel carries the stamp: a returned { params }
// replaces the outgoing params; undefined leaves the call untouched.
export function registerManifoldMetaHook(api) {
  api.on("before_tool_call", (event, ctx) => buildManifoldMeta(event, ctx));
}
