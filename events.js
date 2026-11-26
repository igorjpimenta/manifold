// In-process event bus for flow state changes. ESM caches the instance so
// every importer shares the same emitter object in-process.
import { EventEmitter } from "node:events";

export const flowStateEmitter = new EventEmitter();
flowStateEmitter.setMaxListeners(20);

// Fired by failure-store when a new engine failure row is inserted —
// consumers get notified immediately instead of polling the DB.
export const failureEmitter = new EventEmitter();
failureEmitter.setMaxListeners(10);

// Structured state-boundary emitters — full payloads so a consumer can
// render without a DB scan.
// gate.opened  { flowId, nodeId, ... } — local trigger only (test
// wait-for-event; the wire kind is produced thin at the WS boundary,
// forwarded over WS as manifold.gate.opened.
// gate.closed  { flowId, nodeId } — forwarded as manifold.gate.closed
export const gateEmitter = new EventEmitter();
gateEmitter.setMaxListeners(10);

// node.executing  { flowId, nodeId, pipelineId, parentInvocationId }
export const nodeEmitter = new EventEmitter();
nodeEmitter.setMaxListeners(10);

// flow.step.changed  { flowId, lastStep, stateJson, status }
// flow.terminal      { flowId, status, stateJson }
export const flowEmitter = new EventEmitter();
flowEmitter.setMaxListeners(10);
