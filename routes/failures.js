// failures.js — iOS-facing HTTP surface over failure-store.js.
//
// Ignore is a pure iOS device-local annotation with no server round trip
// at all, so it has no route here. Only GET (list) and POST .../retry
// (implemented in routes/operator-retry.js) remain.

export function registerFailureRoutes({ api, failureStore, invokeElevated }) {
  // ── GET /manifold/failures ─────────────────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/failures",
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (req.method !== "GET") {
        res.statusCode = 405;
        res.setHeader("Allow", "GET");
        res.end("method not allowed");
        return true;
      }
      const { authorizeBridgeRequest, sendUnauthorized } = await import("../plugins/openclaw/manifold/auth/bridge.js");
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const resolved = url.searchParams.get("resolved");
      const limit = Math.min(200, Number(url.searchParams.get("limit") ?? 50) || 50);
      const sinceMs = Number(url.searchParams.get("since")) || undefined;
      try {
        const resolvedFilter = resolved === "true" ? true
                            : resolved === "all" || resolved === "null" ? null
                            : false;
        const rows = await failureStore.list({
          resolved: resolvedFilter,
          limit,
          since: sinceMs,
        });
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ rows, count: rows.length }));
      } catch (err) {
        api.logger.warn(`manifold /failures handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: err?.message ?? String(err) }));
      }
      return true;
    },
  });

  // ── POST /manifold/failures/<id>/retry ──────────────────────────────────
  api.registerHttpRoute({
    path: "/manifold/failures/",
    auth: "plugin",
    match: "prefix",
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

      const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
      const tail = url.pathname.slice("/manifold/failures/".length);
      let raw = "";
      for await (const chunk of req) raw += chunk;
      let body = {};
      try { body = raw ? JSON.parse(raw) : {}; }
      catch { res.statusCode = 400; res.end("invalid json"); return true; }
      const via = typeof body?.via === "string" ? body.via : "app";

      try {
        const retryMatch = tail.match(/^([^/]+)\/retry$/);
        if (retryMatch) {
          const id = decodeURIComponent(retryMatch[1]);
          try {
            const elev = await invokeElevated("/manifold/_elev/retry-failure", { id, via });
            res.statusCode = elev.status || 500;
            res.setHeader("Content-Type", "application/json");
            res.end(elev.body || JSON.stringify({ ok: false, code: "elevation_no_body" }));
          } catch (err) {
            api.logger.warn(`manifold retry handler error: ${err?.message ?? err}`);
            res.statusCode = 500;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ ok: false, code: "internal", error: err?.message ?? String(err) }));
          }
          return true;
        }
        res.statusCode = 404;
        res.end("unknown failures subroute");
      } catch (err) {
        api.logger.warn(`manifold /failures/ handler error: ${err?.message ?? err}`);
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: false, error: err?.message ?? String(err) }));
      }
      return true;
    },
  });
}
