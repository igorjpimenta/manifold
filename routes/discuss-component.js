// Persists the component, then emits discuss.component.proposed plus the
// derived discuss.pending.updated (draft→text, meta→fields) — the
// derivation lives beside the emit because cold-reconnect replays it.
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
