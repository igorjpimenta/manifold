// shared.js — helpers shared by manifold's own agent tools. Pipeline-specific
// helpers (propose/submit/report field handling, gate-context resolution)
// live in oc-config alongside the tools that use them.

export function toolErr(message) {
  return {
    isError: true,
    content: [{ type: "text", text: message }],
  };
}
