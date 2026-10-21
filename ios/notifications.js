// notifications.js — single source of truth for user-facing notification
// kinds the bridge can push.
//
// Adding a new notification surface (e.g. cost-spike, on-call handoff)
// is one entry in the KINDS table below + one call to `pushKind(...)`
// from wherever the trigger lives (live-bridge, intent router, async hook).
// Two parallel things stay in lockstep:
//   1. The APNs payload shape (`aps.category`, `aps.thread-id`,
//      payload userInfo keys) — server-side, this file.
//   2. The iOS routing dispatch keyed by `aps.category` — client-side,
//      Sources/Shared/NotificationKinds.swift mirrors this table.
//
// Add a new kind by appending to KINDS here AND adding the matching
// case to NotificationKind on iOS. Forgetting either side surfaces
// as a notification that delivers but doesn't navigate.
//
// Notification kinds are NOT manifold concepts (error_type / gate wait
// kinds live one level up); they're delivery surfaces.

const KIND_APPROVAL = "openclaw.approval";
const KIND_FAILURE  = "openclaw.failure";
const KIND_BUDGET   = "openclaw.budget";
const KIND_DIGEST   = "openclaw.digest";

// Keyed on failure_events.error_type (state/failure-store.js) — undefined/
// null (an ordinary engine-caught throw with no further classification)
// falls back to the generic "Failure" title below.
const FAILURE_KIND_LABELS = {
  infra_unavailable:            "Infrastructure unavailable",
  confirmed_absent:              "Verified absent",
  blocked:                       "Submit blocked",
  inconclusive:                  "Inconclusive",
  never_attempted:               "Never attempted",
  "subflow-mode-not-satisfied":  "Subflow incomplete",
};

// Each kind declares: how to build the APNs payload from a context
// object, the iOS-side category id (= the same string), and a thread-id
// builder for iOS notification grouping. `userInfo` carries everything
// the iOS routing layer needs to navigate after a tap.
const KINDS = {
  [KIND_APPROVAL]: {
    category: KIND_APPROVAL,
    threadId: (ctx) => `approval-${ctx.flowId ?? ""}`,
    build: (ctx) => ({
      title: ctx.pipelineDisplayName || "Pending action",
      body:  ctx.body || "Pending action",
      userInfo: {
        kind: KIND_APPROVAL,
        flowId: ctx.flowId ?? "",
        itemId: ctx.itemId ?? "",
        activityId: ctx.activityId ?? ctx.flowId ?? "",
      },
    }),
  },
  [KIND_FAILURE]: {
    category: KIND_FAILURE,
    threadId: (ctx) => `failure-${ctx.flowId ?? ""}`,
    build: (ctx) => {
      const kindLabel = FAILURE_KIND_LABELS[ctx.errorType] ?? "Failure";
      const body = ctx.failureSummary
        ? ctx.failureSummary
        : `${ctx.pipelineId ?? "Pipeline"} · ${ctx.failureNode ?? "node"}`;
      return {
        title: `⚠️ ${kindLabel}`,
        body,
        userInfo: {
          kind: KIND_FAILURE,
          failureId: ctx.failureId ?? "",
          flowId: ctx.flowId ?? "",
          pipelineId: ctx.pipelineId ?? "",
          branchItemId: ctx.branchItemId ?? "",
          failureNode: ctx.failureNode ?? "",
          errorType: ctx.errorType ?? "",
        },
      };
    },
  },
  [KIND_BUDGET]: {
    category: KIND_BUDGET,
    threadId: () => "budget",
    build: (ctx) => ({
      title: ctx.title,
      body: ctx.body,
      userInfo: {
        kind: KIND_BUDGET,
      },
    }),
  },
  [KIND_DIGEST]: {
    category: KIND_DIGEST,
    threadId: () => "digest",
    build: (ctx) => ({
      title: ctx.title,
      body: ctx.body,
      userInfo: {
        kind: KIND_DIGEST,
      },
    }),
  },
};

export const NotificationKinds = Object.freeze({
  APPROVAL: KIND_APPROVAL,
  FAILURE:  KIND_FAILURE,
  BUDGET:   KIND_BUDGET,
  DIGEST:   KIND_DIGEST,
});

/**
 * Build a fully-formed APNs JSON payload for a given notification kind.
 * Throws if the kind is unknown — every kind must be registered before
 * push code can fire it (this is the design-system guard).
 *
 * Returns the wire-shape APNs payload, ready for `apns.send`.
 */
export function buildPayload(kindId, ctx) {
  const def = KINDS[kindId];
  if (!def) throw new Error(`notifications: unknown kind ${kindId}`);
  const built = def.build(ctx);
  return {
    aps: {
      alert: { title: built.title, body: built.body },
      sound: "default",
      "content-available": 1,
      category: def.category,
      "thread-id": def.threadId(ctx),
    },
    ...built.userInfo,
  };
}

/**
 * Push a kind to every registered APNs device. The pusher is the
 * generic `apnsSendToAll` from index.js — passed in to keep this
 * module dependency-free (testable in isolation, no http2 setup).
 *
 * Returns a Promise that resolves once all per-device sends settle.
 * Errors are logged via the supplied logger; never thrown.
 */
export async function pushKind({ kindId, ctx, sendToAll, logger }) {
  let payload;
  try {
    payload = buildPayload(kindId, ctx);
  } catch (err) {
    logger?.warn?.(`notifications: build failed for ${kindId}: ${err?.message ?? err}`);
    return;
  }
  try {
    await sendToAll(payload);
  } catch (err) {
    logger?.warn?.(`notifications: pushKind ${kindId} sendToAll threw: ${err?.message ?? err}`);
  }
}
