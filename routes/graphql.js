// graphql.js — client-agnostic query surface over manifold state.
//
// POST /manifold/graphql replaces per-view REST polling for read models:
// the consumer picks fields, the core stays the single writer. Mutations
// are intentionally absent — writes go through the existing POST routes.
import { createRootValue, executeQuery } from "../graphql/schema.js";

export function registerGraphqlRoutes({
  api,
  failureStore,
  runStore,
  runtimeDbPath,
  opsDbPath,
}) {
  // ── POST /manifold/graphql ───────────────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/graphql",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405;
        res.setHeader("Allow", "POST");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../plugins/openclaw/manifold/auth/bridge.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      let raw = "";
      try {
        for await (const chunk of req) raw += chunk;
      } catch {
        res.statusCode = 400;
        res.end("unreadable body");
        return true;
      }
      let payload;
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch {
        res.statusCode = 400;
        res.end("invalid json");
        return true;
      }
      const root = createRootValue({
        failureStore,
        runStore,
        ...(runtimeDbPath ? { runtimeDbPath } : {}),
        ...(opsDbPath ? { opsDbPath } : {}),
        logger: api.logger,
      });
      const out = await executeQuery({ root, query: payload?.query, variables: payload?.variables });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Cache-Control", "no-cache");
      res.end(JSON.stringify(out));
      return true;
    },
  });
}
