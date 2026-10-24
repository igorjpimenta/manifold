// health.js — manifold's own liveness, no harness contact.
//
// The app's connectivity probe and any consumer's readiness check land
// here instead of the harness status surface: version + uptime prove
// the plugin is loaded and serving, which is all a client can act on.
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const MANIFOLD_VERSION = require("../package.json").version;

export function registerHealthRoutes({ api }) {
  api.registerHttpRoute({
    path: "/manifold/health",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../auth/bridge.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, manifold: MANIFOLD_VERSION, uptimeMs: Math.round(process.uptime() * 1000) }));
      return true;
    },
  });
}
