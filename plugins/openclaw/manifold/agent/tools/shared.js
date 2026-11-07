// shared.js — helpers shared by manifold's own agent tools.

export function toolErr(message) {
  return {
    isError: true,
    content: [{ type: "text", text: message }],
  };
}
