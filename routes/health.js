// health.js — manifold's own liveness plus per-file DB reachability.
//
// The app's connectivity probe and any consumer's readiness check land
// here instead of the harness status surface. `ok` is the AND of every
// check: a degraded database reports ok:false with the failing file
// named, never a blanket ok over silent rot. No harness contact.
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { checkDb } from "../state/db-health.js";
import { noteDbFailure } from "../state/db-guard.js";

const require = createRequire(import.meta.url);
const MANIFOLD_VERSION = require("../package.json").version;

const RUNTIME_DB = () => path.join(os.homedir(), ".openclaw", "runtime.sqlite");
const OPS_DB = () => path.join(os.homedir(), ".openclaw", "ops.sqlite");

export function registerHealthRoutes({ api, auth }) {
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
      const { authorizeBridgeRequest, sendUnauthorized } = auth;
      const authz = await authorizeBridgeRequest(req);
      if (!authz.ok) { sendUnauthorized(res, authz.reason); return true; }
      const db = {};
      let ok = true;
      for (const [name, dbPath] of [["runtime", RUNTIME_DB()], ["ops", OPS_DB()]]) {
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
