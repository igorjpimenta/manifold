// manifold-meta.js — opencode companion for manifold identity.
//
// Mechanics: opencode's `tool.execute.before` can mutate the outgoing
// call's `args` (the plugin types declare `output: { args: any }`, return
// `Promise<void>` — mutation is the only channel), but opencode's MCP path
// builds the request as `callTool({ name, arguments: args })`: params carry
// only name + arguments, there is no `params._meta` slot, and every MCP
// input schema is forced `additionalProperties: false`, so an `args._meta`
// injection is stripped or rejected before the wire. Manifold keys every
// dispatch to harness/agent/session and refuses unstamped calls rather
// than attributing them to a configured default, so there is no per-call
// identity channel to stamp here.
//
// Manifold mints session keys itself (gateway-native `agent:<agent>:…`
// keys); the only registration an opencode setup needs is the push URL
// core POSTs dispatch/gate events to (plugins/README.md: `pushUrls`).
//
// The export stays a registered no-op so the hook point exists for an
// opencode version that grows request-level meta mutation.
export default {
  id: "manifold-meta",
  server: async () => ({}),
};
