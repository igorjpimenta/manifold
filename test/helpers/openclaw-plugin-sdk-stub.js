// Minimal stand-ins for the `openclaw/plugin-sdk/*` subpaths the plugin
// imports statically. Real modules exist only inside an installed openclaw
// runtime; without these, importing the package graph (and any test that
// touches it) fails resolution long before the code under test runs.

export function definePluginEntry(entry) {
  return entry;
}

export function defineChannelPluginEntry(entry) {
  return entry;
}

export function createChatChannelPlugin(spec) {
  return spec;
}

export async function dispatchInboundDirectDmWithRuntime() {
  return undefined;
}
