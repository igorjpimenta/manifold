// routes/discuss-component.js — proposal-artifact fan-out for gate discuss.
//
// componentEmitter persists the component, then emits the canonical
// discuss.component.proposed plus the derived discuss.pending.updated
// through the shared emitter. The pending derivation (draft→text,
// meta→fields) lives here, beside the emit — not duplicated in each
// producer — because it is what cold-reconnect replays.
export function createComponentEmitter({ discussStore, emit }) {
  return function componentEmitter({ flowId, itemId, component }) {
    return Promise.resolve().then(async () => {
      try { await discussStore.setComponent(flowId, itemId, component); } catch {}
      if (!emit) return;
      try {
        await emit("discuss.component.proposed", { flowId, itemId, component });
      } catch {}
      if (component.kind === "proposed_draft") {
        try {
          await emit("discuss.pending.updated", {
            flowId,
            itemId,
            text: component.text ?? null,
            fields: null,
            updatedAt: component.updatedAt ?? Date.now(),
          });
        } catch {}
      } else if (component.kind === "proposed_meta") {
        try {
          await emit("discuss.pending.updated", {
            flowId,
            itemId,
            text: null,
            fields: component.fields ?? null,
            updatedAt: component.updatedAt ?? Date.now(),
          });
        } catch {}
      }
    });
  };
}
