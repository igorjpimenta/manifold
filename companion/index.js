// companion/index.js — thin gateway harness for the manifold sidecar.
//
// Opens no database. Every /manifold/* route (except sibling namespaces
// and sidecar internals) proxies to the sidecar; agent dispatch runs here
// against the gateway runtime and fulfills sidecar ops; canonical events
// relay to gateway broadcast. A down sidecar degrades proxy routes to 502
// with the harness otherwise unaffected.
import os from "node:os";
import path from "node:path";
// Resolved in the gateway; identity fallback standalone (tests), same as
// the manifold entry.
let definePluginEntry = (entry) => entry;
try {
  ({ definePluginEntry } = await import("openclaw/plugin-sdk/plugin-entry"));
} catch {}
import { existsSync, readFileSync } from "node:fs";
import { authorizeCompanionRequest, sendUnauthorized } from "./auth.js";
import { proxyDecision, forwardToSidecar } from "./proxy.js";
import { startDispatchRunner } from "./runner.js";
import { startEventRelay } from "./relay.js";

const DEFAULT_SIDECAR_URL = "http://127.0.0.1:7431";

function resolveServiceToken({ userCfg } = {}) {
  if (typeof userCfg?.sidecarToken === "string" && userCfg.sidecarToken) return userCfg.sidecarToken;
  const p = path.join(os.homedir(), ".manifold", ".api-token");
  if (existsSync(p)) return readFileSync(p, "utf8").trim();
  return "";
}

export default definePluginEntry({
  id: "manifold",
  name: "Manifold Companion",
  description:
    "Thin gateway harness for the manifold sidecar: proxies /manifold/* to the sidecar, runs agent dispatches, relays canonical events. Opens no database.",
  configSchema: () => ({
    type: "object",
    additionalProperties: false,
    properties: {
      sidecarUrl: { type: "string" },
      sidecarToken: { type: "string" },
    },
  }),
  register: (api) => {
    const userCfg = api.pluginConfig ?? {};
    const sidecarUrl = userCfg.sidecarUrl || DEFAULT_SIDECAR_URL;
    const serviceToken = resolveServiceToken({ userCfg });
    if (!serviceToken) {
      api.logger.error("manifold companion: no sidecar token (config sidecarToken or ~/.manifold/.api-token); proxy routes will 502 until the sidecar boots");
    }
    const forward = forwardToSidecar({ sidecarUrl, serviceToken, logger: api.logger });

    let gatewayBroadcast = null;
    api.registerGatewayMethod("plugin.manifold.ping", (opts) => {
      if (!gatewayBroadcast) {
        gatewayBroadcast = opts.context.broadcast;
        api.logger.info("manifold companion: WS broadcaster captured");
      }
      opts.respond(true, { ok: true });
    }, { scope: "operator.read" });

    const proxyHandler = async (req, res) => {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const decision = proxyDecision(url.pathname);
      if (decision === "deny") {
        res.statusCode = 404;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "not found" }));
        return true;
      }
      // Sibling namespaces (e.g. /manifold/ios/*): not ours. Returning
      // false leaves gateway matching to their owners.
      if (decision === "passthrough") return false;
      const authz = await authorizeCompanionRequest(req);
      if (!authz.ok) {
        sendUnauthorized(res, authz.reason);
        return true;
      }
      if (!serviceToken) {
        res.statusCode = 502;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "sidecar unavailable" }));
        return true;
      }
      await forward(req, res);
      return true;
    };

    api.registerHttpRoute({ path: "/manifold/", auth: "plugin", match: "prefix", handler: proxyHandler });
    api.registerHttpRoute({ path: "/manifold", auth: "plugin", match: "exact", handler: proxyHandler });

    let dispatchSub = null;
    let relaySub = null;
    api.registerService({
      id: "manifold-companion-link",
      start: async () => {
        if (!serviceToken) {
          api.logger.warn("manifold companion: link not started (no sidecar token)");
          return;
        }
        dispatchSub = startDispatchRunner({ api, sidecarUrl, serviceToken, logger: api.logger });
        relaySub = startEventRelay({ sidecarUrl, serviceToken, getBroadcast: () => gatewayBroadcast, logger: api.logger });
        api.logger.info("manifold companion: sidecar link started");
      },
      stop: async () => {
        try {
          dispatchSub?.stop();
        } catch {}
        try {
          relaySub?.stop();
        } catch {}
      },
    });
  },
});
