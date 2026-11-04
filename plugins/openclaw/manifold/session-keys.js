// session-keys.js — adapter-owned session identity helpers.
//
// Run ids are a separate namespace: gateway runIds are native gateway
// events, so the dispatch-lifecycle registry maps them independently of
// whatever shape a session key carries.

import { randomBytes } from "node:crypto";

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

// Translate a `runId`/`run_id` string field when it carries a mapped value;
// leave anything else (foreign shapes, missing) untouched. Session keys
// pass through verbatim — there is no gateway shape left to map back.
function mapMessageFields(msg) {
  if (!msg || typeof msg !== "object") return msg;
  const out = Array.isArray(msg) ? [...msg] : { ...msg };
  const items = Array.isArray(out) ? out : [out];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
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
