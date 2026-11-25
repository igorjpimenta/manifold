// Draft persistence helpers for discuss handlers.
// These functions mutate flow waitJson (via store.setWaiting).
import { looksLikeMetaNarration } from "./discuss-helpers.js";

export async function persistRevisedDraft({ flowId, itemId, newDraft, store, logger }) {
  if (looksLikeMetaNarration(newDraft)) {
    return { applied: false, code: "rejected_meta_narration" };
  }
  {
    const flow = await store.get(flowId);
    if (!flow) return { applied: false, code: "flow_not_found" };
  }
  for (let attempt = 0; attempt < 5; attempt++) {
    const flow = await store.get(flowId);
    if (!flow) return { applied: false, code: "flow_not_found" };
    if (flow.waitJson?.kind !== "human-gate" && flow.waitJson?.kind !== "gate") return { applied: false, code: "not_a_gate" };
    const items = Array.isArray(flow.waitJson?.items) ? flow.waitJson.items : [];
    const targetItemId = itemId ?? items[0]?.itemId;
    const idx = items.findIndex((it) => it?.itemId === targetItemId);
    if (idx < 0) return { applied: false, code: "item_not_found" };
    const nextItems = items.map((it, i) => {
      if (i !== idx) return it;
      return {
        ...it,
        requestPayload: {
          ...(it.requestPayload ?? {}),
          editableText: newDraft,
        },
      };
    });
    const result = await store.setWaiting({
      flowId,
      expectedRevision: flow.revision,
      lastStep: flow.lastStep,
      stateJson: flow.stateJson,
      waitJson: { ...flow.waitJson, items: nextItems },
      statusReason: flow.statusReason,
    });
    if (result.applied) return { applied: true };
    if (result.code !== "revision_conflict") return { applied: false, code: result.code };
    await new Promise((r) => setTimeout(r, 25 + attempt * 25));
  }
  return { applied: false, code: "retry_exhausted" };
}

// Field `type`s that a gate-layout.js declares for genuinely numeric data
// (currency amounts, day counts). A value destined for one of these MUST
// arrive as a real JS number — a numeric string (e.g. from a lenient
// client) is rejected outright rather than silently coerced, so a malformed
// edit fails loudly instead of writing a value nothing validated.
const NUMERIC_FIELD_TYPES = new Set(["money", "number"]);

function collectLayoutFieldTypes(layout) {
  const types = {};
  if (!Array.isArray(layout)) return types;
  for (const entry of layout) {
    if (!entry) continue;
    if (entry.kind === "field" && typeof entry.id === "string" && typeof entry.type === "string") {
      types[entry.id] = entry.type;
    } else if (entry.kind === "field-group" && Array.isArray(entry.fields)) {
      for (const f of entry.fields) {
        if (f && typeof f.id === "string" && typeof f.type === "string") types[f.id] = f.type;
      }
    }
  }
  return types;
}

export async function persistRevisedMeta({ flowId, itemId, fields, store, logger }) {
  if (!fields || typeof fields !== "object" || Object.keys(fields).length === 0) {
    return { applied: false, code: "empty_fields" };
  }
  {
    const flow = await store.get(flowId);
    if (!flow) return { applied: false, code: "flow_not_found" };
  }
  for (let attempt = 0; attempt < 5; attempt++) {
    const flow = await store.get(flowId);
    if (!flow) return { applied: false, code: "flow_not_found" };
    if (flow.waitJson?.kind !== "human-gate" && flow.waitJson?.kind !== "gate") return { applied: false, code: "not_a_gate" };
    const items = Array.isArray(flow.waitJson?.items) ? flow.waitJson.items : [];
    const targetItemId = itemId ?? items[0]?.itemId;
    const idx = items.findIndex((it) => it?.itemId === targetItemId);
    if (idx < 0) return { applied: false, code: "item_not_found" };
    // Reject the whole call if any field the pipeline declares numeric
    // (type: "money"/"number" in its own gate-layout.js) is being set to
    // something other than a real number — e.g. a numeric-looking string.
    // Fields the layout doesn't know about (unknown ids)
    // fall through unchecked since there's no declared type to enforce.
    const fieldTypes = collectLayoutFieldTypes(items[idx]?.requestPayload?.layout);
    for (const [key, v] of Object.entries(fields)) {
      const declaredType = fieldTypes[key];
      if (declaredType && NUMERIC_FIELD_TYPES.has(declaredType) && typeof v !== "number") {
        return { applied: false, code: "invalid_field_type", field: key, expectedType: "number" };
      }
    }
    const nextItems = items.map((it, i) => {
      if (i !== idx) return it;
      const rp = it.requestPayload ?? {};
      const isPatchable = (v) => typeof v === "string" || typeof v === "number";
      // gate-projector.js's projectGateItem() serves rp.layout verbatim to
      // clients, so a budget/timeframe/language edit has to land here or the
      // chip never reflects it, regardless of editableMeta getting updated
      // correctly.
      const nextLayout = Array.isArray(rp.layout)
        ? rp.layout.map((entry) => {
            if (!entry) return entry;
            if (entry.kind === "field" && typeof entry.id === "string" && isPatchable(fields[entry.id])) {
              return { ...entry, value: String(fields[entry.id]) };
            }
            if (entry.kind === "field-group" && Array.isArray(entry.fields)) {
              return {
                ...entry,
                fields: entry.fields.map((f) =>
                  f && typeof f.id === "string" && isPatchable(fields[f.id])
                    ? { ...f, value: String(fields[f.id]) }
                    : f
                ),
              };
            }
            return entry;
          })
        : rp.layout;
      return {
        ...it,
        requestPayload: {
          ...rp,
          editableMeta: { ...(rp.editableMeta ?? {}), ...fields },
          layout: nextLayout,
        },
      };
    });
    const result = await store.setWaiting({
      flowId,
      expectedRevision: flow.revision,
      lastStep: flow.lastStep,
      stateJson: flow.stateJson,
      waitJson: { ...flow.waitJson, items: nextItems },
      statusReason: flow.statusReason,
    });
    if (result.applied) return { applied: true };
    if (result.code !== "revision_conflict") return { applied: false, code: result.code };
    await new Promise((r) => setTimeout(r, 25 + attempt * 25));
  }
  return { applied: false, code: "retry_exhausted" };
}
