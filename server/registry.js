// The HTTP route registry the route modules already speak: route modules
// call `api.registerHttpRoute({ path, auth, match, handler })` where
// handler(req, res) ends the response and returns true — the same method
// the gateway supplies, so `manifold serve` mounts identical handlers.
// `auth` is ignored here: every handler guards itself with the shared
// MANIFOLD_BEARER (routes/bearer-auth.js) and the server binds loopback.
// Host/Origin is checked here, once, before the route table — per-route
// bearer checks cannot see a rebinding attempt that never reaches them
// (see local-request.js).

import { isLocalRequest } from "./local-request.js";

export function createRouteRegistry({ logger = console, boundHost = null } = {}) {
  const routes = [];

  function registerHttpRoute(spec) {
    if (!spec?.path || typeof spec.handler !== "function") {
      throw new Error("registerHttpRoute: path and handler are required");
    }
    routes.push({ auth: "plugin", match: "exact", ...spec });
  }

  function find(pathname) {
    for (const route of routes) {
      if (route.match === "prefix" ? pathname.startsWith(route.path) : pathname === route.path) {
        return route;
      }
    }
    return null;
  }

  async function handle(req, res) {
    if (!isLocalRequest(req, { boundHost })) {
      res.statusCode = 403;
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ error: "forbidden", reason: "non-local host or origin" }));
      return;
    }
    const url = new URL(req.url, `http://${req.headers.host ?? "127.0.0.1"}`);
    const route = find(url.pathname);
    if (!route) {
      res.statusCode = 404;
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ error: "not found", path: url.pathname }));
      return;
    }
    try {
      const handled = await route.handler(req, res);
      if (handled !== true && !res.writableEnded) res.end();
    } catch (err) {
      logger?.warn?.(`manifold api: handler error on ${url.pathname}: ${err?.message ?? err}`);
      if (!res.writableEnded) {
        res.statusCode = 500;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ error: "internal", message: err?.message ?? String(err) }));
      }
    }
  }

  return { registerHttpRoute, routes, find, handle };
}
