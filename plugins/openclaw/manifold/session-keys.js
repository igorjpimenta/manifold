// session-keys.js — adapter-owned gateway key mapping. The winch-shaped
// boundary: core speaks ONLY its own namespace (opaque `mf-<flow8>-<node>`
// handles minted by engine/executor-helpers.js); the gateway speaks
// `agent:<agent>:subagent:<opaque>`. This module translates at the edge.
//
// Wrap ONCE per raw runtime (wrapSubagentKeys memoizes by identity, same
// posture as engine/gateway-subagent.js's gate): outgoing run/waitForRun/
// getSessionMessages args map opaque→gateway, incoming runIds/messages map
// back. runId mapping is dispatch-scoped and in-memory (coreRunId→gateway
// gatewayRunId both ways, dropped when its waitForRun settles) — a restart
// wipes it, which is safe because revival re-attaches by session key,
// never by runId.
//
// Non-mf keys pass through verbatim in both directions: explicit
// entry.sessionKey overrides and gateway-native keys are never reshaped.

import { randomBytes } from "node:crypto";

// Pure: opaque core handle + agent → gateway key. Throws on empty input —
// a dispatch without both is a caller bug, fail loudly at the edge.
export function toGatewayKey({ opaque, agent } = {}) {
  if (!opaque || !agent) throw new Error("session-keys: toGatewayKey needs { opaque, agent }");
  return `agent:${agent}:subagent:${opaque}`;
}

// Pure: gateway key → opaque core handle, or null for foreign shapes.
// Never throws — callers use null to mean "not ours, pass through".
export function toOpaqueKey(gatewayKey) {
  if (typeof gatewayKey !== "string") return null;
  const m = /^agent:([^:]+):subagent:(.+)$/.exec(gatewayKey);
  return m ? m[2] : null;
}

// Core-minted handles start here; everything else is foreign/verbatim.
function isOpaqueHandle(key) {
  return typeof key === "string" && key.startsWith("mf-");
}

// Outbound: opaque→gateway, gateway/foreign verbatim.
export function toGatewaySessionKey(key, agent) {
  if (!isOpaqueHandle(key)) return key;
  if (!agent) return key;
  return toGatewayKey({ opaque: key, agent });
}

// Inbound: gateway mf-shape→opaque, everything else verbatim.
export function toCoreSessionKey(key) {
  const opaque = toOpaqueKey(key);
  if (opaque !== null && isOpaqueHandle(opaque)) return opaque;
  return key;
}

// runId↔runId registry for the dispatch lifecycle. Module-level (not
// per-wrapper): the score-tool wake hook resolves gateway runIds from
// gateway events without holding the wrapper.
const coreToGatewayRunId = new Map();
const gatewayToCoreRunId = new Map();

// Opaque↔gateway session registry, filled by run() (the only call that
// always carries agentId). getSessionMessages carries no agent field, so it
// resolves the opaque handle through this table instead. Entries outlive
// the dispatch — revival re-attaches by session key — but never cross a
// restart; a fresh process re-registers on its first run(), which always
// precedes any getSessionMessages in runSubagentTurn.
const coreToGatewaySession = new Map();

export function coreRunIdForGateway(gatewayRunId) {
  if (!gatewayRunId) return null;
  return gatewayToCoreRunId.get(gatewayRunId) ?? null;
}

function mintCoreRunId() {
  return `mf-run-${randomBytes(6).toString("hex")}`;
}

function rememberRunMapping(coreRunId, gatewayRunId) {
  coreToGatewayRunId.set(coreRunId, gatewayRunId);
  gatewayToCoreRunId.set(gatewayRunId, coreRunId);
}

function forgetRunMappingByCore(coreRunId) {
  const gatewayRunId = coreToGatewayRunId.get(coreRunId);
  coreToGatewayRunId.delete(coreRunId);
  if (gatewayRunId) gatewayToCoreRunId.delete(gatewayRunId);
}

function mapRunIdOut(runId) {
  return coreToGatewayRunId.get(runId) ?? runId;
}

function mapRunIdIn(runId) {
  return gatewayToCoreRunId.get(runId) ?? runId;
}

// Translate a `sessionKey`/`session_key`/`runId`/`run_id` string field when
// it carries a mapped value; leave anything else (foreign shapes, missing)
// untouched.
function mapMessageFields(msg) {
  if (!msg || typeof msg !== "object") return msg;
  const out = Array.isArray(msg) ? [...msg] : { ...msg };
  const items = Array.isArray(out) ? out : [out];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    for (const k of ["sessionKey", "session_key"]) {
      if (typeof item[k] === "string") item[k] = toCoreSessionKey(item[k]);
    }
    for (const k of ["runId", "run_id"]) {
      if (typeof item[k] === "string") item[k] = mapRunIdIn(item[k]);
    }
  }
  return out;
}

const wrappers = new WeakMap();

export function wrapSubagentKeys(raw) {
  if (!raw) return raw;
  const cached = wrappers.get(raw);
  if (cached) return cached;
  const wrapped = Object.create(raw);

  if (typeof raw.run === "function") {
    wrapped.run = async (args) => {
      const gatewayKey = toGatewaySessionKey(args?.sessionKey, args?.agentId);
      if (isOpaqueHandle(args?.sessionKey) && gatewayKey !== args.sessionKey) {
        coreToGatewaySession.set(args.sessionKey, gatewayKey);
      }
      const out = await raw.run({
        ...args,
        sessionKey: gatewayKey,
      });
      const coreRunId = mintCoreRunId();
      if (out && typeof out === "object" && typeof out.runId === "string") {
        rememberRunMapping(coreRunId, out.runId);
        return { ...out, runId: coreRunId };
      }
      return out;
    };
  }

  if (typeof raw.waitForRun === "function") {
    wrapped.waitForRun = async (args) => {
      try {
        const out = await raw.waitForRun({
          ...args,
          runId: mapRunIdOut(args?.runId),
        });
        if (out && typeof out === "object" && typeof out.runId === "string") {
          return { ...out, runId: mapRunIdIn(out.runId) };
        }
        return out;
      } finally {
        if (typeof args?.runId === "string") forgetRunMappingByCore(args.runId);
      }
    };
  }

  if (typeof raw.getSessionMessages === "function") {
    wrapped.getSessionMessages = async (args) => {
      // Opaque handles resolve through the run()-registered table; anything
      // unregistered (explicit overrides, gateway-native keys) passes
      // through verbatim — the gateway stored exactly that string.
      const sessionKey = isOpaqueHandle(args?.sessionKey)
        ? (coreToGatewaySession.get(args.sessionKey) ?? args.sessionKey)
        : args?.sessionKey;
      const out = await raw.getSessionMessages({
        ...args,
        sessionKey,
      });
      if (out && typeof out === "object" && Array.isArray(out.messages)) {
        return { ...out, messages: mapMessageFields(out.messages) };
      }
      return out;
    };
  }

  wrappers.set(raw, wrapped);
  return wrapped;
}
