
const STUB_URL = new URL("./openclaw-plugin-sdk-stub.js", import.meta.url).href;
const PREFIX = "openclaw/plugin-sdk/";

export function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith(PREFIX)) {
    return { url: STUB_URL, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
