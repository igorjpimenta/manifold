// events.js — cross-plugin in-process event bus for flow state changes.
// pipeclaw (emitter) and ios module (subscriber) both import this;
// ESM caches the instance so they share the same emitter object in-process.
import { EventEmitter } from "node:events";

export const flowStateEmitter = new EventEmitter();
// Two plugins subscribe; headroom for future additions.
flowStateEmitter.setMaxListeners(20);

// Fired by failure-store when a new engine failure row is inserted.
// Consumers get notified immediately and don't need to poll the DB.
export const failureEmitter = new EventEmitter();
failureEmitter.setMaxListeners(10);

// Fired by ios/index.js's relay.broadcast patch — forwards every
// live_activity.* / failure_event.start SSE message to WS clients.
// ios/index.js emits; pipeclaw/index.js (pipeclaw-ws-run-events) subscribes.
export const liveActivityEmitter = new EventEmitter();
liveActivityEmitter.setMaxListeners(10);

// Structured state-boundary emitters — carry full payloads so subscribers
// can project live_activity.* messages without a DB scan.

// gate.opened  { flowId, nodeId, items, waitJson, stateJson, lastStep, _suppressApns? }
// gate.closed  { flowId, nodeId }
export const gateEmitter = new EventEmitter();
gateEmitter.setMaxListeners(10);

// node.executing  { flowId, nodeId, pipelineId, parentInvocationId }
export const nodeEmitter = new EventEmitter();
nodeEmitter.setMaxListeners(10);

// flow.step.changed  { flowId, lastStep, stateJson, status }
// flow.terminal      { flowId, status, stateJson }
export const flowEmitter = new EventEmitter();
flowEmitter.setMaxListeners(10);
