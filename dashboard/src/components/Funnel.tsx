import { Fragment, useState } from "react";
import type { FunnelStage } from "@/api/types";
import { formatNum } from "@/lib/format";

export function Funnel({ stages }: { stages: FunnelStage[] | null }) {
  // Tap-to-expand reveals p95 + min per stage, plus a per-subflow
  // sub-funnel for subflow parents. We default to compact (p50
  // only) because most days the distribution is well-behaved — the
  // expanded view exists for "this stage looks healthy, prove it" inspection.
  const [expanded, setExpanded] = useState<string | null>(null);
  if (!stages || stages.length === 0) {
    return <div className="italic text-fg-dim py-4">no funnel definition for this pipeline</div>;
  }
  const max = Math.max(1, ...stages.map((s) => s.total));
  const overall = stages[stages.length - 1].conversionFromStart;
  return (
    <>
      {/*
        Single grid container so all rows share one column-template — the
        same pattern as NodeCostBreakdown in PipelineDetail. Each per-row
        button uses `display: contents` (Tailwind `contents`) so its four
        cells participate directly in the parent grid; the `auto` first
        column sizes to the widest stage label across all rows. The
        expanded detail block uses `col-span-4`.
      */}
      {/*
        5 columns: label | bar | total count | (p50 …) | conv%. Splitting
        the count and the parenthetical onto separate auto-sized columns
        keeps each one right-aligned to its own boundary so numbers
        stack cleanly across stages (e.g. 3-digit and 1-digit totals
        line up by their right edges; the parens column lines up by its
        right edge too).
      */}
      <div className="grid grid-cols-[auto_1fr_auto_auto_auto] items-center gap-x-3 gap-y-2 font-mono text-xs">
        {stages.map((s, i) => {
          const conv = s.conversionFromPrev;
          const convTxt = i === 0 ? "" : conv == null ? "—" : `${(conv * 100).toFixed(0)}%`;
          const drop = conv != null && conv < 0.5;
          const isOpen = expanded === s.id;
          // "Ran dry" amber: at least one run yielded 0 at this stage even
          // though the typical run filled it. Only flag the FIRST stage
          // where min drops to 0 — once a run is empty upstream, the
          // emptiness propagates downstream by construction (empty array
          // → empty filter → empty shortlist), so amber-ing every
          // downstream stage is noise. The first drop is the actionable
          // signal.
          const prevMin = i > 0 ? stages[i - 1].min : null;
          const ranDry = s.min === 0 && (s.p50 ?? 0) > 0 && (prevMin == null || prevMin > 0);
          return (
            <Fragment key={s.id}>
              <button
                type="button"
                onClick={() => setExpanded(isOpen ? null : s.id)}
                aria-expanded={isOpen}
                className="contents text-left group"
              >
                <span className="text-fg-dim group-hover:bg-bg-elev2/40 rounded-l-[2px] pl-0.5 whitespace-nowrap inline-flex items-center gap-1">
                  <span>{s.label}</span>
                  <span className="text-[10px] text-fg-dim/80">{isOpen ? "▾" : "▸"}</span>
                </span>
                <span className="relative h-[18px] bg-bg-elev2 rounded-[3px] overflow-hidden">
                  <span
                    className="absolute top-0 left-0 bottom-0"
                    style={{
                      width: `${(s.total / max * 100).toFixed(1)}%`,
                      background: "linear-gradient(90deg, #1f6feb, #58a6ff)",
                    }}
                  />
                </span>
                <span className="text-right whitespace-nowrap tabular-nums group-hover:bg-bg-elev2/40">{s.total}</span>
                <span className={`text-right whitespace-nowrap tabular-nums group-hover:bg-bg-elev2/40 ${ranDry ? "text-status-amber" : "text-fg-dim"}`}>
                  (p50 {formatNum(s.p50)}{ranDry ? " · min 0" : ""})
                </span>
                <span className={`text-right whitespace-nowrap tabular-nums group-hover:bg-bg-elev2/40 rounded-r-[2px] pr-0.5 ${drop ? "text-status-amber" : "text-fg-dim"}`}>{convTxt}</span>
              </button>
              {isOpen && (
                <div className="col-span-5 pl-0.5 pr-1 pt-0.5 pb-1.5">
                  <div className="text-[10px] text-fg-dim grid grid-cols-2 sm:grid-cols-4 gap-x-3 gap-y-0.5">
                    <span><span className="text-fg-dim">p50:</span> {formatNum(s.p50)}</span>
                    <span><span className="text-fg-dim">p95:</span> {formatNum(s.p95)}</span>
                    <span><span className="text-fg-dim">min:</span> {formatNum(s.min)}</span>
                    <span><span className="text-fg-dim">mean:</span> {formatNum(s.mean)}</span>
                    <span className="col-span-2 sm:col-span-4"><span className="text-fg-dim">n:</span> {s.n} runs</span>
                  </div>
                  {s.subStages && s.subStages.length > 0 && (
                    <SubFunnel stages={s.subStages} />
                  )}
                </div>
              )}
            </Fragment>
          );
        })}
      </div>
      {overall != null && (
        <div className="mt-2.5 font-mono text-fg-dim text-xs">
          overall: {(overall * 100).toFixed(1)}% ({stages[0].label} → {stages[stages.length - 1].label}) across {stages[0].n} run(s)
        </div>
      )}
    </>
  );
}

// Per-subflow attrition funnel rendered inside an expanded
// loop stage. The unit is *subflows* (one per item the
// parent's selector emitted) rather than runs, so e.g. POLL with 175
// runs may have 7000+ subflows across them. Visually muted to read as
// a child of the parent row.
function SubFunnel({ stages }: { stages: FunnelStage[] }) {
  if (!stages.length) return null;
  const max = Math.max(1, ...stages.map((s) => s.total));
  return (
    <div className="mt-2 ml-3 pl-2 border-l border-border/60">
      <div className="text-[10px] text-fg-dim/80 mb-1 italic">per-subflow sub-funnel</div>
      <div className="grid grid-cols-[auto_1fr_auto_auto] items-center gap-x-3 gap-y-1 font-mono text-[11px]">
        {stages.map((s, i) => {
          const conv = s.conversionFromPrev;
          // Avoid the "11/8106 = 0%" trap: show <1% (and one decimal in
          // the 0–10 band) so tiny-but-nonzero conversions read as
          // *signal*, not zero.
          const convTxt = i === 0
            ? ""
            : conv == null
              ? "—"
              : conv === 0
                ? "0%"
                : conv < 0.01
                  ? "<1%"
                  : conv < 0.1
                    ? `${(conv * 100).toFixed(1)}%`
                    : `${(conv * 100).toFixed(0)}%`;
          const drop = conv != null && conv < 0.5;
          return (
            <Fragment key={s.id}>
              <span className="text-fg-dim whitespace-nowrap">{s.label}</span>
              <span className="relative h-[12px] bg-bg-elev2 rounded-[2px] overflow-hidden">
                <span
                  className="absolute top-0 left-0 bottom-0 bg-status-blue/70"
                  style={{ width: `${(s.total / max * 100).toFixed(1)}%` }}
                />
              </span>
              <span className="text-right whitespace-nowrap tabular-nums text-fg-dim">{s.total}</span>
              <span className={`text-right whitespace-nowrap tabular-nums ${drop ? "text-status-amber" : "text-fg-dim"}`}>{convTxt}</span>
            </Fragment>
          );
        })}
      </div>
    </div>
  );
}
