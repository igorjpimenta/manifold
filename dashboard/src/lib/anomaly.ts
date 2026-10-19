// Per-day anomaly detection for sparkline charts. A bar is flagged
// when its value exceeds the mean of the other bars by a configurable
// factor (default 1.5×). Conservative: skips bars where the value is
// zero (no work that day shouldn't count as anomalous), and bails out
// entirely when fewer than 3 non-zero bars exist (no meaningful
// baseline to compare against).
//
// Returns a Set<day-key> for O(1) lookup in the bar render loop, plus
// a `ratios` map so the chart can show "2.4× mean" in tooltips and
// pinned labels.

export interface AnomalyResult {
  flagged: Set<string>;
  ratios: Map<string, number>;
}

export function computeAnomalies<T extends { day: string }>(
  perDay: T[],
  getValue: (d: T) => number,
  threshold = 1.5,
): AnomalyResult {
  const flagged = new Set<string>();
  const ratios = new Map<string, number>();
  if (!perDay || perDay.length < 3) return { flagged, ratios };
  const nonZeroCount = perDay.reduce((n, d) => n + (getValue(d) > 0 ? 1 : 0), 0);
  if (nonZeroCount < 3) return { flagged, ratios };

  for (const d of perDay) {
    const v = getValue(d);
    if (v <= 0) continue;
    const others = perDay.filter((x) => x.day !== d.day).map(getValue).filter((x) => x > 0);
    if (others.length < 2) continue;
    const mean = others.reduce((s, x) => s + x, 0) / others.length;
    if (mean <= 0) continue;
    const ratio = v / mean;
    if (ratio > threshold) {
      flagged.add(d.day);
      ratios.set(d.day, ratio);
    }
  }
  return { flagged, ratios };
}
