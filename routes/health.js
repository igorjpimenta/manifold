// Liveness plus per-file DB reachability: `ok` is the AND of every check —
// a degraded database reports ok:false with the failing file named, never a
// blanket ok over silent rot. No harness contact.
import { createRequire } from "node:module";
import { checkDb } from "../state/db-health.js";
import { resolveStorePaths } from "../state/paths.js";
import { noteDbFailure } from "../state/db-guard.js";
import { authorizeBearerRequest, sendUnauthorized } from "./bearer-auth.js";

const require = createRequire(import.meta.url);
const MANIFOLD_VERSION = require("../package.json").version;

// This process's own store home (MANIFOLD_DATA_DIR / MANIFOLD_*_DB), never
// a harness default — the health answer must describe the files we read.
const DB_PATHS = () => resolveStorePaths();

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
      const authz = authorizeBearerRequest(req, { logger: api.logger });
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const db = {};
      let ok = true;
      const paths = DB_PATHS();
      for (const [name, dbPath] of [["runtime", paths.runtimeDb], ["ops", paths.opsDb]]) {
        const check = checkDb(dbPath);
        db[name] = check.ok ? { ok: true, latencyMs: check.latencyMs } : { ok: false, error: check.error };
        if (!check.ok) {
          ok = false;
          const err = new Error(check.error);
          err.code = check.code;
          noteDbFailure(err, { dbPath, logger: api.logger });
        }
      }
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok, manifold: MANIFOLD_VERSION, uptimeMs: Math.round(process.uptime() * 1000), db }));
      return true;
    },
  });
}
