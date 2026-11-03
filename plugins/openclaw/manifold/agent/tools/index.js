// index.js — barrel re-export of manifold's own agent tools. Pipeline-specific
// tools (propose_*, submit_*, report_*) live in oc-config, not here.

export { registerGraphFlowTool } from "./manifold-start.js";
