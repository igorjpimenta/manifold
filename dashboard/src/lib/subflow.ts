// Subflow-classification helpers used by the run-detail subflows card +
// the subflow parent's outcome indicator.
//
// `SubflowData` = an entry from `state.subflows[<groupId>][<subflowKey>]`.

import type { SubflowData, PipelineNode } from "@/api/types";

export interface SubflowOutcomeCounts {
  total: number;
  succeeded: number;
  failed: number;
  awaiting: number;
  running: number;
  /** Terminal-without-business-success: GATE rejected, SUBMIT_CODE
   *  blocked (infra-unavailable, already-sent dedup, etc.), or other
   *  silent skips. Used to be just `rejected` (GATE-only). */
  skipped: number;
}

/** Sub-node data marker for "this step was a non-business skip" — used
 *  by subflowStatus + classifySubflows to distinguish silent terminals
 *  (GATE-rejected, infra-skip) from real successes. */
function subflowEndedAsSkip(data: Record<string, any>): boolean {
  // GATE rejected → STAMP_REJECT sub-step ran
  if (data.STAMP_REJECT) return true;
  // GATE rejected (older shape without STAMP_REJECT capture)
  if (data.GATE?.decision && data.GATE.decision !== "approve") return true;
  // SUBMIT_CODE blocked: infra-unavailable, already-sent dedup, etc.
  // (Operator-actionable blocks add `_error` which routes through
  // failedAt above; only silent blocks reach this check.)
  if (data.SUBMIT_CODE?.ok === false && data.SUBMIT_CODE?.error_type === "blocked") return true;
  return false;
}

export function classifySubflows(parallelGroup?: Record<string, SubflowData>): SubflowOutcomeCounts {
  const out: SubflowOutcomeCounts = { total: 0, succeeded: 0, failed: 0, awaiting: 0, running: 0, skipped: 0 };
  if (!parallelGroup || typeof parallelGroup !== "object") return out;
  for (const b of Object.values(parallelGroup)) {
    if (!b) continue;
    out.total++;
    if (b.failedAt) { out.failed++; continue; }
    if (b.awaiting) { out.awaiting++; continue; }
    if (b.step !== "__end__") { out.running++; continue; }
    const data = b.data ?? {};
    let businessFail = false;
    for (const d of Object.values(data)) {
      if (!d || typeof d !== "object") continue;
      if (d._error) { businessFail = true; break; }
      if (d.ok === false) { businessFail = true; break; }
    }
    if (businessFail) { out.failed++; continue; }
    if (subflowEndedAsSkip(data)) { out.skipped++; continue; }
    out.succeeded++;
  }
  return out;
}

/** Map outcome counts to a CSS class on the cytoscape compound parent. */
export function subgraphOutcomeClass(c: SubflowOutcomeCounts): string {
  if (!c || c.total === 0) return "group-untouched";
  if (c.failed > 0)   return "group-failed";
  if (c.awaiting > 0) return "group-engaged";
  if (c.running > 0)  return "group-running";
  return "group-primary-ok";
}

/** Comparable signature of a subflow's trajectory; same outcome at each step → same sig. */
export function pathSignature(subflow: SubflowData | null | undefined, subSteps: string[]): string {
  if (!subflow) return "";
  const parts: string[] = [];
  for (const step of subSteps) {
    const d = subflow.data?.[step];
    if (d) {
      if (d._error) parts.push(`${step}!`);
      else if (step === "GATE" && d.decision && d.decision !== "approve") parts.push(`${step}✗`);
      else if (step === "SUBMIT" && d.ok === false) parts.push(`${step}!`);
      // SUBMIT_CODE blocked without _error = silent skip (infra,
      // already-sent dedup). Mark with ⊘ to distinguish from real
      // ok submissions on path histograms.
      else if (step === "SUBMIT_CODE" && d.ok === false && d.error_type === "blocked") parts.push(`${step}⊘`);
      else parts.push(step);
    } else if (subflow.awaiting?.subNodeId === step) {
      parts.push(`${step}⏳`);
    }
  }
  return parts.join("|");
}

/** Subflow status label used in the table column + sort order.
 *  `skipped` = terminal-without-business-success (GATE rejected,
 *  SUBMIT_CODE blocked for infra-unavailable / already-sent / etc.). */
export type SubflowStatus = "failed" | "awaiting" | "running" | "done" | "skipped" | "unknown";

export function subflowStatus(b: SubflowData | null | undefined): SubflowStatus {
  if (!b) return "unknown";
  if (b.failedAt) return "failed";
  if (b.awaiting) return "awaiting";
  if (b.step !== "__end__") return "running";
  // Terminal — distinguish business outcome from silent skip
  if (subflowEndedAsSkip(b.data ?? {})) return "skipped";
  return "done";
}

export function statusRank(b: SubflowData): number {
  switch (subflowStatus(b)) {
    case "failed":   return 0;
    case "awaiting": return 1;
    case "running":  return 2;
    case "skipped":  return 3;
    case "done":     return 4;
    default:         return 5;
  }
}

/** Variant name to pass to <Badge variant=...>. */
export function badgeVariantForSubflow(status: SubflowStatus): "failed" | "waiting" | "running" | "succeeded" | "cancelled" {
  if (status === "failed") return "failed";
  if (status === "awaiting") return "waiting";
  if (status === "running") return "running";
  if (status === "done") return "succeeded";
  if (status === "skipped") return "cancelled";
  return "cancelled";
}

/**
 * Infer sub-step topological order from a subflow node's flow
 * definition, with a fallback to observed `subflow.step` values when the
 * pipeline def isn't loaded (historical pipelines).
 */
export function inferSubStepOrder(
  pipelineNode: PipelineNode | undefined,
  observedSubflows: SubflowData[]
): string[] {
  const edges = pipelineNode?.flow?.edges;
  if (edges && edges.length) {
    const order: string[] = [];
    const seen = new Set<string>();
    let cur = "__start__";
    for (let i = 0; i < 20; i++) {
      const outs = edges.filter((e) => e.from === cur).map((e) => e.to);
      const next = outs.find((t) => t !== "__end__" && !seen.has(t));
      if (!next) break;
      seen.add(next);
      order.push(next);
      cur = next;
    }
    if (order.length) return order;
  }
  const seen = new Set<string>();
  for (const b of observedSubflows) {
    if (b?.step && b.step !== "__end__" && !seen.has(b.step)) seen.add(b.step);
  }
  return [...seen];
}
