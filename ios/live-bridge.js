// live-bridge.js — event-push iOS bridge, replaces poller.js + poller-flows.js.
//
// Subscribes to structured state-boundary emitters (gateEmitter, nodeEmitter,
// flowEmitter, failureEmitter) and immediately projects live_activity.* WS
// messages from the event payload — no DB scan per event. Only touches the DB
// to fetch subflow group data (one targeted query per gate.opened/node.executing)
// and for the one-shot startup seed that populates tracking sets on boot.
import { gateEmitter, nodeEmitter, flowEmitter, failureEmitter } from "../events.js";
import { getAllSubflowGroups, getSubflowGroup } from "../state/subflow-state.js";
import { endAgentApproval, endAgentRunning, startFailureEvent } from "./projectors.js";
import {
  projectGateItem,
  projectAgentRunning, projectAgentRunningUpdate,
  agentRunningContent, buildSubflowAggregateSummary,
} from "./gate-projector.js";

const GATE_PUSH_COALESCE_THRESHOLD = 3;

export function createLiveBridge({
  relay,
  pipelineLookup, agentLookup, nodeAgentLookup, nodeDiscussLookup,
  enableAgentRunning,
  apnsPushApproval, apnsPushFailure,
  runStore, nodeExecutionsStore,
  logger,
}) {
  const deps = { pipelineLookup, agentLookup, nodeAgentLookup, nodeDiscussLookup };

  // flowId:itemId keys for every open AgentApproval card
  const openGateItems    = new Set();
  // `${flowId}:${nodeId}` → last content JSON string for dedup
  const openAgentRunning = new Map();

  const listeners = [];
  let stopped = false;

  function on(emitter, event, fn) {
    emitter.on(event, fn);
    listeners.push({ emitter, event, fn });
  }

  // ── AgentApproval ────────────────────────────────────────────────────────────

  on(gateEmitter, "gate.opened", async ({ flowId, nodeId, items, waitJson, stateJson, _suppressApns }) => {
    if (stopped) return;
    const allSubflowGroups = await getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore });
    // Sole source for blockedAtMs — see gate-projector.js's comment.
    const gateInvokedAtMs = await nodeExecutionsStore.getLatestInvocationInvokedAt(flowId, nodeId).catch(() => null);
    const flow = { flowId, nodeId, stateJson, waitJson, lastStep: nodeId, gateInvokedAtMs };
    const newItems = [];
    for (const item of items) {
      const key = `${flowId}:${item.itemId}`;
      if (openGateItems.has(key)) continue;
      if (stateJson?.approvalPending?.[item.itemId]?.decision) continue;
      try {
        relay.broadcast(projectGateItem(flow, item, allSubflowGroups, deps));
        openGateItems.add(key);
        newItems.push(item);
      } catch (err) {
        logger?.warn?.(`live-bridge gate.opened: ${err?.message ?? err}`);
      }
    }
    // APNs push (skip on startup seed to avoid duplicate pushes on restart)
    if (!_suppressApns && apnsPushApproval && newItems.length > 0) {
      const pushFlow = { flowId, stateJson, waitJson };
      if (newItems.length > GATE_PUSH_COALESCE_THRESHOLD) {
        Promise.resolve(apnsPushApproval({ flow: pushFlow, items: newItems })).catch(() => {});
      } else {
        for (const item of newItems) {
          Promise.resolve(apnsPushApproval({ flow: pushFlow, item })).catch(() => {});
        }
      }
    }
  });

  on(gateEmitter, "gate.closed", ({ flowId }) => {
    if (stopped) return;
    for (const key of [...openGateItems]) {
      if (!key.startsWith(flowId + ":")) continue;
      try { relay.broadcast(endAgentApproval({ flowId, itemId: key.slice(37), dismissalPolicy: "after:30s" })); } catch {}
      openGateItems.delete(key);
    }
  });

  // PipelineProgress (live_activity.start/update/end for activityType
  // "PipelineProgress") used to be projected here from flow.step.changed/
  // flow.terminal. Removed — pipeclaw.run.started/updated/ended (see
  // index.js's buildRunPatch) now carries the same information
  // (pipelineDisplayName/pipelineEmoji/currentNodeId added specifically
  // for this) on the SAME per-revision cadence flow.step.changed used to
  // drive, so this was pure duplication.

  on(flowEmitter, "flow.terminal", ({ flowId }) => {
    if (stopped) return;
    for (const [key] of [...openAgentRunning]) {
      if (!key.startsWith(flowId + ":")) continue;
      try { relay.broadcast(endAgentRunning({ flowId, nodeId: key.slice(37), dismissalPolicy: "after:5s" })); } catch {}
      openAgentRunning.delete(key);
    }
  });

  // ── AgentRunning ─────────────────────────────────────────────────────────────

  if (enableAgentRunning) {
    on(nodeEmitter, "node.executing", async ({ flowId, nodeId, pipelineId, parentInvocationId }) => {
      if (stopped || parentInvocationId) return;
      const nodeMeta = nodeAgentLookup?.(pipelineId, nodeId);
      if (!nodeMeta?.agent && nodeMeta?.kind !== "subflow") return;

      // End prior AgentRunning for a different node on this flow
      for (const [key] of [...openAgentRunning]) {
        if (!key.startsWith(flowId + ":") || key === `${flowId}:${nodeId}`) continue;
        try { relay.broadcast(endAgentRunning({ flowId, nodeId: key.slice(37), dismissalPolicy: "after:5s" })); } catch {}
        openAgentRunning.delete(key);
      }

      let subflows = null;
      if (nodeMeta.kind === "subflow") {
        try {
          const group = await getSubflowGroup({ flowId, groupId: nodeId, runStore, nodeExecutionsStore });
          subflows = buildSubflowAggregateSummary(group, { nodeAgentLookup, agentLookup, pipelineId, parentNodeId: nodeId });
          if (!subflows || subflows.total === 0) return;
        } catch { return; }
      }

      const key = `${flowId}:${nodeId}`;
      const newContent = agentRunningContent(nodeMeta, subflows);
      const newJson = JSON.stringify(newContent);
      const prev = openAgentRunning.get(key);
      if (!prev) {
        try {
          const stateJson = (await runStore.get(flowId))?.stateJson;
          relay.broadcast(projectAgentRunning(flowId, nodeId, stateJson, nodeMeta, subflows, deps));
        } catch {}
        openAgentRunning.set(key, newJson);
      } else if (newJson !== prev) {
        try { relay.broadcast(projectAgentRunningUpdate(flowId, nodeId, nodeMeta, subflows)); } catch {}
        openAgentRunning.set(key, newJson);
      }
    });
  }

  // ── Failures ─────────────────────────────────────────────────────────────────

  on(failureEmitter, "failure.created", (row) => {
    if (stopped) return;
    try { relay.broadcast(startFailureEvent({ row })); } catch {}
    if (apnsPushFailure) Promise.resolve(apnsPushFailure(row)).catch(() => {});
  });

  // ── Lifecycle ─────────────────────────────────────────────────────────────────

  async function start() {
    // One-shot startup seed: load only active (non-terminal) flows to populate
    // tracking sets for flows already in-flight before this process started.
    // After this, listeners are the sole update path — no scan loop.
    const flows = await runStore.listActive();
    for (const flow of flows) {
      if (flow.waitJson?.kind === "human-gate") {
        // Synthetic gate.opened — _suppressApns avoids duplicate pushes on restart
        gateEmitter.emit("gate.opened", {
          flowId:   flow.flowId,
          nodeId:   flow.waitJson?.nodeId ?? flow.lastStep,
          items:    flow.waitJson.items ?? [],
          waitJson: flow.waitJson,
          stateJson: flow.stateJson,
          _suppressApns: true,
          updatedAt: flow.updatedAt,
        });
      }
      // The `else if (flow.status === "running")` branch that used to seed
      // PipelineProgress tracking via a synthetic flow.step.changed emit
      // is gone — that tracking no longer exists (pipeclaw.run.* is the
      // sole source now, and it doesn't need per-process Set/Map seeding
      // the way the old dedup-by-Map live-push did). A freshly-connected
      // client still learns about already-running flows via snapshot()'s
      // own runStore.listActive() query on its first reconcile.
    }
  }

  async function snapshot() {
    // Called on WS reconnect / app foreground. Reconstructs current open
    // activities from tracking sets + individual runStore.get() per open item.
    // O(active-open-items), not O(all-flows).
    const messages = [];

    for (const key of openGateItems) {
      const flowId = key.slice(0, 36);
      const itemId = key.slice(37);
      const flow = await runStore.get(flowId);
      if (!flow) continue;
      if (flow.stateJson?.approvalPending?.[itemId]?.decision) continue;
      const item = (flow.waitJson?.items ?? []).find((i) => i.itemId === itemId);
      if (!item) continue;
      try {
        const allSubflowGroups = await getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore });
        const nodeId = flow.waitJson?.nodeId ?? flow.lastStep;
        // Sole source for blockedAtMs — see gate-projector.js's comment.
        // Same cost class as getAllSubflowGroups above (one query per item).
        const gateInvokedAtMs = await nodeExecutionsStore.getLatestInvocationInvokedAt(flowId, nodeId).catch(() => null);
        messages.push(projectGateItem(
          { flowId, nodeId, stateJson: flow.stateJson, waitJson: flow.waitJson, lastStep: flow.lastStep, gateInvokedAtMs },
          item, allSubflowGroups, deps,
        ));
      } catch {}
    }

    // PipelineProgress's live-push half moved to pipeclaw.run.* (see
    // index.js's buildRunPatch) — no more per-step-change broadcast
    // duplication. The reconnect/snapshot half does NOT live here
    // anymore either: `messages` (this function's return value) feeds
    // /pipeclaw/ios/state's `open` array, which InboxAPI.swift's
    // InboxClient.fetch() strictly decodes as [InboxActivity] — every
    // element MUST have activityId/activityType/attributes/contentState.
    // A `{ type: "pipeclaw.run.updated", ...buildRunPatch(...) }` entry
    // (tried here previously) has none of those fields, so the whole
    // array decode throws the moment one appears — a real regression
    // (client showed "pipeclaw/ios/state decode: ... is missing").
    // ios/index.js's STATE_PATH handler now builds a separate
    // `runningPipelines` array straight from its own already-live
    // _runningFlows map instead, so `open` stays homogeneous.
    if (enableAgentRunning) {
      for (const [key] of openAgentRunning) {
        const flowId = key.slice(0, 36);
        const nodeId = key.slice(37);
        const flow = await runStore.get(flowId);
        if (!flow || flow.status !== "running") continue;
        const nodeMeta = nodeAgentLookup?.(flow.stateJson?.pipelineId, nodeId);
        if (!nodeMeta) continue;
        let subflows = null;
        if (nodeMeta.kind === "subflow") {
          try {
            const group = await getSubflowGroup({ flowId, groupId: nodeId, runStore, nodeExecutionsStore });
            subflows = buildSubflowAggregateSummary(group, {
              nodeAgentLookup, agentLookup,
              pipelineId: flow.stateJson?.pipelineId, parentNodeId: nodeId,
            });
          } catch {}
        }
        try { messages.push(projectAgentRunning(flowId, nodeId, flow.stateJson, nodeMeta, subflows, deps)); } catch {}
      }
    }

    return messages;
  }

  async function stop() {
    stopped = true;
    for (const { emitter, event, fn } of listeners) emitter.off(event, fn);
    openGateItems.clear();
    openAgentRunning.clear();
  }

  return { start, stop, snapshot };
}
