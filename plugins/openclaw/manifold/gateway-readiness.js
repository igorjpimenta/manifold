// Manifold's one "the gateway serves agent RPCs" signal, awaited by the
// run-turn gate before every agent dispatch.
//
// For the first seconds after every start the gateway refuses the methods in
// openclaw's STARTUP_UNAVAILABLE_GATEWAY_METHODS ("<method> unavailable
// during gateway startup") — agent.wait among them, while the `agent` RPC
// that starts a run is not. openclaw starts plugin services one by one
// (await service.start()) and releases those methods only after the last
// start() returns; gateway_start fires right after. So:
//   - manifold's services are part of the gateway's startup. Readiness must
//     never be awaited inside a service start(): the gateway can't become
//     ready until that start() returns.
//   - gateway_start is the only thing that marks readiness. It fires once per
//     gateway process, so readiness is process-wide state on globalThis (jiti
//     and native ESM can load this file twice, and a plugin reload in the
//     same process re-registers): a later registration finds it already set.
//   - only the gateway starts plugin services. A process where manifold's
//     services never started (the CLI) isn't the gateway: there is nothing
//     to wait for, and whenReady() resolves at once.

const SHARED_KEY = "__manifold_gateway_readiness__";

function sharedState() {
  if (!globalThis[SHARED_KEY]) globalThis[SHARED_KEY] = createReadinessState();
  return globalThis[SHARED_KEY];
}

export function createReadinessState() {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { ready: false, gatewayProcess: false, promise, resolve };
}

// Wires one plugin registration into the shared readiness state:
// whenReady() resolves once gateway_start has fired, or at once where no
// manifold service has started (not the gateway).
export function registerGatewayReadiness(api, { logger, state = sharedState() } = {}) {
  if (!state.ready && typeof api?.on === "function") {
    api.on("gateway_start", () => {
      if (state.ready) return;
      state.ready = true;
      state.resolve();
      logger?.info?.("manifold: gateway ready");
    });
  }
  return {
    markGatewayProcess() {
      state.gatewayProcess = true;
    },
    whenReady() {
      if (!state.ready && !state.gatewayProcess) return Promise.resolve();
      return state.promise;
    },
    isReady: () => state.ready,
  };
}
