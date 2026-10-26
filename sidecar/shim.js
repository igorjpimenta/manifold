// sidecar/shim.js — the gateway `api` surface the sidecar replays so
// index.js's register() runs unmodified outside the gateway.
//
// Parity rules:
//   - registerService / registerHttpRoute / registerGatewayMethod: real
//     registries; the server starts services and routes HTTP through them.
//   - registerCli / registerTool: collected and reported; agent tools and
//     the CLI execute in a harness (gateway companion), never in the sidecar.
//   - on(): collected only. Tool-call hooks have nothing to observe in a
//     process that serves no tool calls; staying inert here is correct, and
//     staying LOUD (warn at boot) keeps a future real subscription honest.
//   - runtime: { subagent } only. approvalQueue/lineage stay absent so the
//     engine's optional chains skip harness affordances instead of
//     half-using them.
export function createSidecarApi({ logger = null, pluginConfig = {}, runtime = {} } = {}) {
  const services = [];
  const routes = [];
  const cliRegistrations = [];
  const tools = [];
  const gatewayMethods = new Map();
  const eventHandlers = new Map();

  const api = {
    logger: logger ?? console,
    pluginConfig,
    runtime,
    registerService(def) {
      services.push(def);
      return def;
    },
    registerHttpRoute(def) {
      routes.push(def);
      return def;
    },
    registerCli(fn, opts) {
      cliRegistrations.push({ fn, opts });
      return { fn, opts };
    },
    registerTool(fn) {
      tools.push(fn);
      return fn;
    },
    registerGatewayMethod(name, handler, opts) {
      gatewayMethods.set(name, { handler, opts });
      return { name, handler, opts };
    },
    on(event, handler) {
      let set = eventHandlers.get(event);
      if (!set) {
        set = new Set();
        eventHandlers.set(event, set);
      }
      set.add(handler);
      return () => {
        set.delete(handler);
      };
    },
  };

  return {
    api,
    services,
    routes,
    cliRegistrations,
    tools,
    gatewayMethods,
    eventHandlers,
    // Longest-prefix wins across exact + prefix routes, mirroring gateway
    // route selection closely enough that handler behavior never depends
    // on registration order here.
    findRoute(urlPath) {
      let best = null;
      let bestLen = -1;
      for (const r of routes) {
        if (r.match === "exact") {
          if (r.path === urlPath && r.path.length > bestLen) {
            best = r;
            bestLen = r.path.length;
          }
        } else {
          if (urlPath === r.path || urlPath.startsWith(r.path.endsWith("/") ? r.path : `${r.path}/`)) {
            if (r.path.length > bestLen) {
              best = r;
              bestLen = r.path.length;
            }
          }
        }
      }
      return best;
    },
  };
}
