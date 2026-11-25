import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { persistRevisedDraft, persistRevisedMeta } from "../../../routes/discuss-draft.js";

// The real `store` (state/state-store.js) has every method declared `async`.
// A synchronous mock can't catch a missing await (a plain return value and an
// unawaited Promise are both "not null" to a caller that doesn't read a field
// off either), so every mock below is async to exercise the real await path.

// ── helpers ───────────────────────────────────────────────────────────────────

function makeFlow(overrides = {}) {
  return {
    flowId: "12345678-0000-0000-0000-000000000001",
    status: "waiting",
    revision: 1,
    lastStep: "NODE_A",
    stateJson: { pipelineId: "team_test" },
    statusReason: null,
    waitJson: {
      kind: "human-gate",
      nodeId: "PROPOSE",
      items: [
        {
          itemId: "item-1",
          requestPayload: {
            title: "Approve?",
            editableText: "original draft",
          },
        },
      ],
    },
    ...overrides,
  };
}

function makeStore(flow) {
  let current = { ...flow };
  let rev = flow.revision ?? 1;
  return {
    get: async (id) => (id === current.flowId ? { ...current } : null),
    setWaiting: async ({ expectedRevision, waitJson }) => {
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, waitJson, revision: ++rev };
      return { applied: true };
    },
  };
}

// ── persistRevisedDraft ───────────────────────────────────────────────────────

describe("persistRevisedDraft", () => {
  it("returns flow_not_found when flow does not exist", async () => {
    const store = { get: async () => null, setWaiting: async () => ({ applied: true }) };
    const r = await persistRevisedDraft({
      flowId: "nonexistent", itemId: "item-1", newDraft: "Updated draft text that is long enough to not look like meta",
      store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "flow_not_found");
  });

  it("returns not_a_gate when flow waitJson kind is not human-gate", async () => {
    const flow = makeFlow({ waitJson: { kind: "needs-info" } });
    const store = makeStore(flow);
    const r = await persistRevisedDraft({
      flowId: flow.flowId, itemId: "item-1", newDraft: "Updated draft text that is long enough to not look like meta",
      store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "not_a_gate");
  });

  it("returns item_not_found when itemId is missing from waitJson", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const r = await persistRevisedDraft({
      flowId: flow.flowId, itemId: "no-such-item", newDraft: "A draft with enough text to pass the meta check",
      store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "item_not_found");
  });

  it("applies draft update to the correct item", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const r = await persistRevisedDraft({
      flowId: flow.flowId, itemId: "item-1", newDraft: "Updated draft text that is long enough to not look like meta",
      store, logger: null,
    });
    assert.equal(r.applied, true);
  });

  it("rejects meta narration", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    // Short text triggers looksLikeMetaNarration
    const r = await persistRevisedDraft({
      flowId: flow.flowId, itemId: "item-1", newDraft: "Short",
      store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "rejected_meta_narration");
  });
});


// ── persistRevisedMeta ────────────────────────────────────────────────────────

describe("persistRevisedMeta", () => {
  it("returns empty_fields for null fields", async () => {
    const store = makeStore(makeFlow());
    const r = await persistRevisedMeta({
      flowId: "any", itemId: "item-1", fields: null, store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "empty_fields");
  });

  it("returns empty_fields for empty object", async () => {
    const store = makeStore(makeFlow());
    const r = await persistRevisedMeta({
      flowId: "any", itemId: "item-1", fields: {}, store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "empty_fields");
  });

  it("returns flow_not_found when store returns null", async () => {
    const store = { get: async () => null };
    const r = await persistRevisedMeta({
      flowId: "nonexistent", itemId: "item-1", fields: { budget: "300" }, store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "flow_not_found");
  });

  it("applies field update — editableMeta set", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { budget: "750" }, store, logger: null,
    });
    assert.equal(r.applied, true);
    // Verify the store received an update with the patched field
    const updated = await store.get(flow.flowId);
    const updatedItem = updated.waitJson.items[0];
    assert.equal(updatedItem.requestPayload.editableMeta?.budget, "750");
  });

  it("adds new fields to editableMeta", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { newField: "hello" }, store, logger: null,
    });
    assert.equal(r.applied, true);
    const updated = await store.get(flow.flowId);
    assert.equal(updated.waitJson.items[0].requestPayload.editableMeta?.newField, "hello");
  });

  // Every current gate-layout.js (freelance_workana-proposal etc.) declares
  // requestPayload.layout directly (executor-helpers.js enforces this).
  // gate-projector.js serves `layout` verbatim to clients, so a field-group
  // edit has to land there or the chip never updates, regardless of
  // editableMeta being patched correctly.
  it("patches a field-group entry inside requestPayload.layout", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [
                { kind: "draft", id: "draft", label: "Proposed text", value: "prose" },
                {
                  kind: "field-group", id: "fields",
                  fields: [
                    { id: "budget", label: "Budget", type: "money", prefix: "R$", value: "500" },
                    { id: "timeframe", label: "Timeframe", type: "text", value: "4 semanas" },
                  ],
                },
              ],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    // timeframe is not numerically-typed, so a plain string is valid here.
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { timeframe: "2 semanas" }, store, logger: null,
    });
    assert.equal(r.applied, true);
    const updated = await store.get(flow.flowId);
    const layout = updated.waitJson.items[0].requestPayload.layout;
    const group = layout.find((entry) => entry.kind === "field-group");
    assert.equal(group.fields.find((f) => f.id === "timeframe")?.value, "2 semanas");
    // Untouched fields in the same group must survive the patch.
    assert.equal(group.fields.find((f) => f.id === "budget")?.value, "500");
  });

  it("patches a flat kind:\"field\" layout entry", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [{ kind: "field", id: "timeframe", label: "Timeframe", type: "text", value: "4 semanas" }],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { timeframe: "2 semanas" }, store, logger: null,
    });
    assert.equal(r.applied, true);
    const updated = await store.get(flow.flowId);
    const layout = updated.waitJson.items[0].requestPayload.layout;
    assert.equal(layout.find((entry) => entry.id === "timeframe")?.value, "2 semanas");
  });

  // propose_draft_meta (the agent tool Cyrano calls during a discuss
  // session) stores budget/period as JS numbers, not strings — the
  // layout patch requires a real number for any field the pipeline
  // declares type: "money"/"number", so this is the only shape that
  // succeeds for such a field.
  it("patches a field-group entry when the proposed value is a number", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [
                {
                  kind: "field-group", id: "fields",
                  fields: [{ id: "budget", label: "Budget", type: "money", prefix: "R$", value: "500" }],
                },
              ],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { budget: 750 }, store, logger: null,
    });
    assert.equal(r.applied, true);
    const updated = await store.get(flow.flowId);
    const group = updated.waitJson.items[0].requestPayload.layout.find((entry) => entry.kind === "field-group");
    assert.equal(group.fields.find((f) => f.id === "budget")?.value, "750");
  });

  it("patches a flat number-typed field (e.g. Freelancer's delivery period) when the value is a number", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [{ kind: "field", id: "period", label: "Delivery period (days)", type: "number", value: "14" }],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { period: 21 }, store, logger: null,
    });
    assert.equal(r.applied, true);
    const updated = await store.get(flow.flowId);
    const layout = updated.waitJson.items[0].requestPayload.layout;
    assert.equal(layout.find((entry) => entry.id === "period")?.value, "21");
  });

  // A field the pipeline declares numeric MUST arrive as a real JS number.
  // A numeric-looking string is rejected outright, not silently coerced —
  // matches the strict shape propose_draft_meta already enforces at the
  // tool-call boundary, rather than re-permitting a lenient string here.
  it("rejects a string value for a field-group entry declared type: \"money\"", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [
                {
                  kind: "field-group", id: "fields",
                  fields: [{ id: "budget", label: "Budget", type: "money", prefix: "R$", value: "500" }],
                },
              ],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { budget: "750" }, store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "invalid_field_type");
    assert.equal(r.field, "budget");
    // Rejected outright — the field-group entry must be untouched.
    const updated = await store.get(flow.flowId);
    const group = updated.waitJson.items[0].requestPayload.layout.find((entry) => entry.kind === "field-group");
    assert.equal(group.fields.find((f) => f.id === "budget")?.value, "500");
  });

  it("rejects a string value for a flat field declared type: \"number\"", async () => {
    const flow = makeFlow({
      waitJson: {
        kind: "human-gate",
        nodeId: "PROPOSE",
        items: [
          {
            itemId: "item-1",
            requestPayload: {
              title: "Approve?",
              layout: [{ kind: "field", id: "period", label: "Delivery period (days)", type: "number", value: "14" }],
            },
          },
        ],
      },
    });
    const store = makeStore(flow);
    const r = await persistRevisedMeta({
      flowId: flow.flowId, itemId: "item-1", fields: { period: "21" }, store, logger: null,
    });
    assert.equal(r.applied, false);
    assert.equal(r.code, "invalid_field_type");
    assert.equal(r.field, "period");
  });
});
