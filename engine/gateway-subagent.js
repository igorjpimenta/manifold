// gateway-subagent.js — the engine's subagent runtime, gated on gateway
// readiness.
//
// For the first seconds after every gateway start, agent.wait is refused
// ("agent.wait unavailable during gateway startup") while the RPC that
// starts a run already works, so a dispatch landing in that window starts
// its agent run and then can't wait on it. Every subagent call the engine
// makes goes through this wrapper, which awaits the shared readiness wait
// (engine/gateway-readiness.js) before calling — a settled promise once the
// gateway is up, so free after boot. A call refused anyway surfaces as an
// ordinary error; it is not retried.

const GATED_METHODS = ["run", "waitForRun", "getSessionMessages"];

/**
 * Returns gate(rawSubagent) → a subagent whose run/waitForRun/
 * getSessionMessages wait for gateway readiness. Other members pass through
 * to the raw runtime unchanged; a missing runtime stays missing (null /
 * undefined), so "no subagent here" keeps its own handling downstream.
 */
export function createGatewaySubagentGate({ whenGatewayReady } = {}) {
  // One wrapper per raw runtime object, so repeated resolves don't allocate.
  const wrappers = new WeakMap();

  return function gate(raw) {
    if (!raw) return raw;
    let gated = wrappers.get(raw);
    if (!gated) {
      gated = Object.create(raw);
      for (const method of GATED_METHODS) {
        if (typeof raw[method] === "function") {
          gated[method] = async (...args) => {
            if (whenGatewayReady) await whenGatewayReady();
            return raw[method](...args);
          };
        }
      }
      wrappers.set(raw, gated);
    }
    return gated;
  };
}
