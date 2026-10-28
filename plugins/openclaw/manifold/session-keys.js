// session-keys.js — adapter-owned session identity helpers. Session ids
// are plain text, harness-neutral strings minted by core (`mf-<flow8>-…`
// dispatches, `gf-discuss-<flow8>-…` discuss threads): no `agent:` prefix,
// no `subagent` infix, no gateway namespace anywhere in the contract.
// The gateway stores exactly the string it is given, so reads (session
// transcripts, trajectories) key off the same plain id the dispatch used.
//
// Two legacy bridges remain for gateway-shaped keys minted before the
// cutover (`agent:<agent>:subagent:<opaque>`): toOpaqueKey/toCoreSessionKey
// map them back so in-flight pre-cutover flows keep resolving. Nothing
// mints gateway-shaped keys anymore.
// runId mapping below is untouched: gateway runIds are native gateway
// events, so the dispatch-lifecycle registry stays regardless of key shape.

import { randomBytes } from "node:crypto";

// Inbound: legacy gateway mf-shape→opaque, everything else verbatim.
// Only gateway-shaped keys minted before the cutover still match; current
// dispatches already carry the opaque handle, which passes through.
export function toCoreSessionKey(key) {
  const opaque = toOpaqueKey(key);
  if (opaque !== null && opaque.startsWith("mf-")) return opaque;
  return key;
}

// Legacy map-back for gateway-shaped keys minted before the cutover
// (`agent:<agent>:subagent:<opaque>` → `<opaque>`), or null for anything
// else. Never throws — callers use null to mean "not ours, pass through".
// Nothing mints this shape anymore; this stays only so in-flight
// pre-cutover flows keep resolving until they drain.
export function toOpaqueKey(gatewayKey) {
  if (typeof gatewayKey !== "string") return null;
  const m = /^agent:([^:]+):subagent:(.+)$/.exec(gatewayKey);
  return m ? m[2] : null;
}

// runId↔runId registry for the dispatch lifecycle. Module-level (not
// per-wrapper): the score-tool wake hook resolves gateway runIds from
// gateway events without holding the wrapper.
const coreToGatewayRunId = new Map();
const gatewayToCoreRunId = new Map();

// No session registry: session ids pass through verbatim, so
// getSessionMessages needs no lookup table (runIds still map below).

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
      const out = await raw.run({ ...args });
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
      const out = await raw.getSessionMessages({ ...args });
      if (out && typeof out === "object" && Array.isArray(out.messages)) {
        return { ...out, messages: mapMessageFields(out.messages) };
      }
      return out;
    };
  }

  wrappers.set(raw, wrapped);
  return wrapped;
}
