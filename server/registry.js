// registry.js — the HTTP route registry the route modules already speak.
//
// Route modules call `api.registerHttpRoute({ path, auth, match, handler })`
// where handler(req, res) ends the response and returns true. The gateway
// supplies that method today; this registry supplies the same one for a
// standalone server, so `manifold serve` mounts the identical handlers.
//
// `auth` is ignored here: every handler guards itself with the shared
// MANIFOLD_BEARER (routes/bearer-auth.js), and the server binds loopback.

export function createRouteRegistry({ logger = console } = {}) {
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
