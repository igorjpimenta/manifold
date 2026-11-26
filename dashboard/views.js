// Pipeline-declared views: a pipeline exports `views: [...]`, rendered
// generically — no per-pipeline dashboard code. Shape:
//   { id, kind: "funnel"|"metric"|"table", label, sinceDays?,
//     source: { kind: "frontmatter-dir", path, timeField? },
//     stages: [{ id, label, outcomeIn? | outcomeEq? }]   // funnel
//     compute: "count" | "sum:field" | "avg:field"       // metric
//     columns?, limit?, sortBy? }                        // table
// New kinds: extend computeView plus a record loader; the frontend's
// <DeclaredView> mirrors the same kind switch.

import { readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";

export async function computeViews(pipeline, { sinceMs = null } = {}) {
  if (!Array.isArray(pipeline?.views) || pipeline.views.length === 0) return [];
  return Promise.all(pipeline.views.map((v) => computeView(v, { sinceMs })));
}

async function computeView(view, { sinceMs }) {
  const effectiveSinceMs = typeof view.sinceDays === "number" && view.sinceDays > 0
    ? Date.now() - view.sinceDays * 86_400_000
    : sinceMs;
  const base = { id: view.id, kind: view.kind, label: view.label, sinceMs: effectiveSinceMs };
  try {
    if (view.kind === "funnel") return { ...base, ...(await computeFunnelView(view, { sinceMs: effectiveSinceMs })) };
    if (view.kind === "metric") return { ...base, ...(await computeMetricView(view, { sinceMs: effectiveSinceMs })) };
    if (view.kind === "table")  return { ...base, ...(await computeTableView(view, { sinceMs: effectiveSinceMs })) };
    return { ...base, error: `unknown view kind: ${view.kind}` };
  } catch (err) {
    return { ...base, error: err?.message ?? String(err) };
  }
}

async function computeFunnelView(view, { sinceMs }) {
  const records = await loadRecords(view.source, { sinceMs });
  if (!records) return { total: 0, funnel: [] };
  const stageDefs = Array.isArray(view.stages) ? view.stages : [];

  // Stages are independent filters, not required to be nested supersets —
  // keeping them nested for an honest funnel is the pipeline author's call.
  const stages = stageDefs.map((s) => ({
    ...s,
    count: records.filter((r) => matchStage(r, s)).length,
  }));

  const out = stages.map((s, i) => ({
    id: s.id,
    label: s.label ?? s.id,
    count: s.count,
    conversionFromPrev: i === 0
      ? null
      : (stages[i - 1].count > 0 ? s.count / stages[i - 1].count : null),
  }));
  if (out.length > 1 && stages[0].count > 0) {
    out[out.length - 1].conversionFromStart = out[out.length - 1].count / stages[0].count;
  }

  // Per-`outcome` distribution; omitted when the source has no such field.
  const byOutcome = {};
  for (const r of records) {
    if (typeof r.outcome === "string") {
      byOutcome[r.outcome] = (byOutcome[r.outcome] ?? 0) + 1;
    }
  }

  return {
    total: records.length,
    funnel: out,
    byOutcome: Object.keys(byOutcome).length > 0 ? byOutcome : undefined,
  };
}

function matchStage(record, stage) {
  if (Array.isArray(stage.outcomeIn) && stage.outcomeIn.length > 0) {
    return stage.outcomeIn.includes(record.outcome);
  }
  if (typeof stage.outcomeEq === "string") {
    return record.outcome === stage.outcomeEq;
  }
  return true;
}

async function computeMetricView(view, { sinceMs }) {
  const records = await loadRecords(view.source, { sinceMs });
  if (!records) return { value: null };
  const compute = view.compute ?? "count";
  if (compute === "count") return { value: records.length, compute };
  const [op, field] = compute.split(":");
  if (!field) return { value: null, error: `metric compute "${compute}" must be "count" or "op:field"` };
  const nums = records.map((r) => parseFloat(r[field])).filter((n) => Number.isFinite(n));
  if (nums.length === 0) return { value: null, compute };
  if (op === "sum") return { value: nums.reduce((a, b) => a + b, 0), compute };
  if (op === "avg") return { value: nums.reduce((a, b) => a + b, 0) / nums.length, compute };
  return { value: null, error: `unknown metric op "${op}"` };
}

async function computeTableView(view, { sinceMs }) {
  const records = await loadRecords(view.source, { sinceMs });
  if (!records) return { columns: view.columns ?? [], rows: [], total: 0 };
  const columns = Array.isArray(view.columns) && view.columns.length > 0
    ? view.columns
    : Object.keys(records[0] ?? {}).filter((k) => !k.startsWith("__"));
  const sortField = view.sortBy ?? view.source?.timeField ?? "sent-at";
  const sorted = [...records].sort((a, b) => {
    const at = a[sortField] ?? a.__ts ?? 0;
    const bt = b[sortField] ?? b.__ts ?? 0;
    return (typeof bt === "number" ? bt : 0) - (typeof at === "number" ? at : 0);
  });
  const limit = typeof view.limit === "number" && view.limit > 0 ? view.limit : 50;
  const rows = sorted.slice(0, limit).map((r) => {
    const row = {};
    for (const col of columns) row[col] = r[col] ?? null;
    return row;
  });
  return { columns, rows, total: records.length };
}

async function loadRecords(source, { sinceMs }) {
  if (!source || typeof source !== "object") return null;
  if (source.kind === "frontmatter-dir") return loadFrontmatterDirRecords(source, { sinceMs });
  return null;
}

const FRONTMATTER_RE = /^---\r?\n([\s\S]*?)\r?\n---/;

function parseFrontmatter(text) {
  const m = text.match(FRONTMATTER_RE);
  if (!m) return null;
  const out = {};
  for (const line of m[1].split(/\r?\n/)) {
    const kv = line.match(/^([^:]+):\s*(.*)$/);
    if (!kv) continue;
    let v = kv[2].trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) v = v.slice(1, -1);
    out[kv[1].trim()] = v;
  }
  return out;
}

function parseDate(raw) {
  if (!raw) return null;
  const t = Date.parse(raw);
  return Number.isFinite(t) ? t : null;
}

async function loadFrontmatterDirRecords({ path: dir, timeField = "sent-at" }, { sinceMs }) {
  if (!dir || typeof dir !== "string") return null;
  try {
    const s = await stat(dir);
    if (!s.isDirectory()) return null;
  } catch { return null; }

  let files;
  try { files = await readdir(dir); }
  catch { return null; }

  const records = [];
  await Promise.all(files.map(async (f) => {
    if (!f.endsWith(".md")) return;
    let raw;
    try { raw = await readFile(path.join(dir, f), "utf-8"); }
    catch { return; }
    const fm = parseFrontmatter(raw);
    if (!fm) return;
    const ts = parseDate(fm[timeField]);
    if (sinceMs != null) {
      // Undated records are excluded from windowed queries: an honest cohort
      // beats inflating it with unknown dates.
      if (ts == null || ts < sinceMs) return;
    }
    records.push({
      id: f.replace(/\.md$/, ""),
      ...fm,
      __ts: ts,
    });
  }));
  return records;
}
