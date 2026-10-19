// ESM resolve hook: redirects `openclaw/plugin-sdk/*` to the local stub so
// unit tests can import the plugin's modules without an installed openclaw
// runtime. Applied in-thread via `module.registerHooks()` from the test file —
// node:test isolates each file in its own process, so the redirect never
// escapes into other suites.

const STUB_URL = new URL("./openclaw-plugin-sdk-stub.js", import.meta.url).href;
const PREFIX = "openclaw/plugin-sdk/";

export function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith(PREFIX)) {
    return { url: STUB_URL, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
