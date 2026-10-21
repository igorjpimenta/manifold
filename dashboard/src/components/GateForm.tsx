// GateForm — operator actuation for blocked runs. When a flow is sitting
// on a human-gate / gate with pending items, this renders an
// approve / reject / edit form for each pending item and POSTs to
// `/api/runs/:flowId/approve` (which mirrors the CLI's `pipeclaw
// approve` semantics). SSE refreshes the parent run state once the
// executor reacts.
//
// Pending entries may be raw strings ("listing-123") or objects with
// `.id` (`{ id: "listing-123", reason: "..." }`); both shapes render.

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { api } from "@/api/client";
import { cn } from "@/lib/utils";

type Decision = "approve" | "reject" | "edit";

interface GateFormProps {
  flowId: string;
  /** raw pending entries from waitJson.pending */
  pending: unknown[];
}

function entryId(entry: unknown): string | null {
  if (typeof entry === "string") return entry;
  if (entry && typeof entry === "object" && typeof (entry as { id?: unknown }).id === "string") {
    return (entry as { id: string }).id;
  }
  return null;
}

function entryHint(entry: unknown): string | null {
  if (entry && typeof entry === "object") {
    const obj = entry as Record<string, unknown>;
    if (typeof obj.reason === "string") return obj.reason;
    if (typeof obj.hint === "string") return obj.hint;
  }
  return null;
}

// Two signals make a pending entry editable:
//   1. entry.editable === true        — explicit free-text edit is offered
//   2. typeof entry.text === "string" — there's existing text to edit;
//                                       used as the textarea's initial value
// Only one need be true. The gate's `pendingSelector` is the source of
// truth; if neither is present, the entry is approve/reject only.
function entryEditable(entry: unknown): { allowed: boolean; initial: string } {
  if (!entry || typeof entry !== "object") return { allowed: false, initial: "" };
  const obj = entry as Record<string, unknown>;
  const hasText = typeof obj.text === "string";
  const hasFlag = obj.editable === true;
  return { allowed: hasText || hasFlag, initial: hasText ? (obj.text as string) : "" };
}

export function GateForm({ flowId, pending }: GateFormProps) {
  const [busy, setBusy] = useState<string | null>(null);
  const [editText, setEditText] = useState<Record<string, string>>({});
  const [editOpen, setEditOpen] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  if (!pending || pending.length === 0) return null;

  async function submit(itemId: string, decision: Decision, text?: string) {
    setBusy(`${itemId}:${decision}`);
    setError(null);
    try {
      await api.approve(flowId, { itemId, decision, text });
      // No state mutation here — SSE (run.updated) will refresh RunDetail
      // and the form will disappear once the item is no longer pending.
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="mt-3 border-t border-border pt-3">
      <div className="text-fg-dim text-xs uppercase tracking-[0.5px] mb-2">act on pending items</div>
      {error && <div className="text-status-red text-sm mb-2">{error}</div>}
      <div className="grid gap-2">
        {pending.map((entry, idx) => {
          const id = entryId(entry);
          if (!id) {
            return (
              <div key={idx} className="text-fg-dim text-xs italic">
                (skipped: pending entry without `id` — {JSON.stringify(entry).slice(0, 80)})
              </div>
            );
          }
          const hint = entryHint(entry);
          const isEditing = editOpen === id;
          const editable = entryEditable(entry);
          // First time the operator opens the edit panel, seed the textarea
          // with the entry's existing text (if any). After that, keep what
          // they've typed even if they collapse and re-open.
          const currentText = id in editText ? editText[id] : editable.initial;
          return (
            <div key={id} className="border border-border rounded p-2 bg-bg-elev2/30">
              <div className="flex items-center gap-3 flex-wrap min-w-0">
                <span className="font-mono text-sm flex-1 min-w-0 truncate">{id}</span>
                <Button
                  variant="chip"
                  onClick={() => submit(id, "approve")}
                  disabled={busy !== null}
                  className={cn("text-status-green border-status-green/40 hover:bg-status-green/10",
                                busy === `${id}:approve` && "opacity-60")}
                >
                  {busy === `${id}:approve` ? "approving…" : "approve"}
                </Button>
                <Button
                  variant="chip"
                  onClick={() => submit(id, "reject")}
                  disabled={busy !== null}
                  className={cn("text-status-red border-status-red/40 hover:bg-status-red/10",
                                busy === `${id}:reject` && "opacity-60")}
                >
                  {busy === `${id}:reject` ? "rejecting…" : "reject"}
                </Button>
                {editable.allowed && (
                  <Button
                    variant="chip"
                    onClick={() => setEditOpen(isEditing ? null : id)}
                    disabled={busy !== null}
                  >
                    {isEditing ? "cancel edit" : "edit…"}
                  </Button>
                )}
              </div>
              {hint && <div className="text-fg-dim text-xs mt-1">{hint}</div>}
              {editable.allowed && isEditing && (
                <div className="mt-2 grid gap-2">
                  <textarea
                    value={currentText}
                    onChange={(e) => setEditText({ ...editText, [id]: e.target.value })}
                    placeholder="Edited text (replaces the agent's output for this item)"
                    rows={Math.min(12, Math.max(4, currentText.split("\n").length + 1))}
                    className="bg-bg-elev border border-border rounded px-2 py-1 text-sm font-mono w-full min-w-0"
                  />
                  <div>
                    <Button
                      variant="chip"
                      onClick={() => submit(id, "edit", currentText)}
                      disabled={busy !== null || !currentText.trim()}
                      className="text-accent border-accent/40 hover:bg-accent/10"
                    >
                      {busy === `${id}:edit` ? "submitting…" : "submit edit"}
                    </Button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
