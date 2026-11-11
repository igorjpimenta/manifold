// In-memory substring search across the dashboard's flow_runs view.
//
// Why in-memory and not SQLite FTS5: the dashboard reads flows through
// run-store's listRunsPage and is not the schema owner. Adding an FTS5
// mirror table requires writes + triggers managed by whoever owns the
// schema, which the dashboard explicitly does not.
// An in-memory walk avoids the cross-process coordination problem
// entirely. Upgrade to FTS5 if (a) row count grows past ~10k or
// (b) query latency on `top-level state stringify` becomes a problem —
// both checkable on the search endpoint's response.
//
// Scope:
//   ✓ Structured fields: flowId, pipelineId, status,
//     lastStep, statusReason, waitJson.hint, waitJson.reason
//   ✓ errorEntries[*].{nodeId,message} — sourced from failure_events,
//     which unifies root-level and subflow-level errors in one table, so
//     subflow failures are searchable too.
//   ✗ Top-level state keys — not searchable: dashboard/server.js's
//     fetchAllRuns() reads through run-store's listRunsPage, which never
//     selects the raw state_json column (only flowId/pipelineId/status/
//     lastStep/statusReason/timestamps + _display-derived fields);
//     adaptRun() synthesizes stateJson as {pipelineId} for every row.
//     Reintroducing per-node output search would mean a dedicated
//     gateway-side route backed by assembleState() (mirroring
//     routes/runs.js's single-run detail path), not a loop over a field
//     this read never populates.
//   ✗ Transcripts — would require walking session JSONL files per
//     query × per run. Deferred with an offline indexer.

const DEFAULT_LIMIT = 50;
const DEFAULT_CONTEXT_CHARS = 40;

export function searchRuns(runs, query, opts = {}) {
  const limit = opts.limit ?? DEFAULT_LIMIT;
  const contextChars = opts.contextChars ?? DEFAULT_CONTEXT_CHARS;
  // Map<flowId, ErrorEntry[]> — caller fetches once via the gateway's
  // GET /manifold/errors (failureStore.queryErrorsByFlowIds)
  // so this stays a pure function, DB-free.
  const errorsByFlow = opts.errorsByFlow ?? new Map();
  // Structured status filter applied BEFORE the free-text match —
  // unlike the textual `q=… succeeded` shape, this is exact-equality
  // against `r.status` and won't false-positive on the string "failed"
  // appearing in an error message. The StatStrip sub-count links use
  // this so "click fail = show failed runs" reads strictly.
  const statusFilter = typeof opts.status === "string" ? opts.status.trim().toLowerCase() : "";
  if (statusFilter) runs = runs.filter((r) => (r.status ?? "").toLowerCase() === statusFilter);
  const trimmed = String(query ?? "").trim();
  if (!trimmed) {
    // status-only mode: return every status-filtered run as a hit with
    // no per-field snippets. Hit shape stays compatible (matches: []).
    if (statusFilter) {
      const hits = runs.slice(0, limit).map((r) => ({
        flowId: r.flowId,
        pipelineId: r.stateJson?.pipelineId ?? null,
        status: r.status,
        lastStep: r.lastStep,
        updatedAt: r.updatedAt,
        revision: r.revision,
        matches: [],
      }));
      return {
        query: "",
        status: statusFilter,
        totalHits: runs.length,
        hits,
        truncated: runs.length > limit,
      };
    }
    return { query: "", totalHits: 0, hits: [], truncated: false };
  }

  // Multi-token: all whitespace-separated tokens must appear (AND). Quoted
  // phrases not supported in v1 — operator can use a single token in URL
  // for now; quoting would mean a real tokenizer.
  const tokens = trimmed.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return { query: trimmed, totalHits: 0, hits: [], truncated: false };

  const hits = [];
  for (const r of runs) {
    const matches = matchesForRun(r, tokens, contextChars, errorsByFlow.get(r.flowId) ?? []);
    if (matches.length === 0) continue;
    hits.push({
      flowId: r.flowId,
      pipelineId: r.stateJson?.pipelineId ?? null,
      status: r.status,
      lastStep: r.lastStep,
      updatedAt: r.updatedAt,
      revision: r.revision,
      matches,
    });
  }

  // Most-recent first — same as the runs table ordering.
  hits.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));

  return {
    query: trimmed,
    totalHits: hits.length,
    hits: hits.slice(0, limit),
    truncated: hits.length > limit,
  };
}

function matchesForRun(r, tokens, contextChars, runErrors = []) {
  const fields = [];

  // Structured fields. Order matters for snippet presentation — we want
  // the most identity-bearing ones first.
  pushField(fields, "flowId",         r.flowId);
  pushField(fields, "pipelineId",     r.pipelineId ?? r.stateJson?.pipelineId);
  pushField(fields, "status",         r.status);
  pushField(fields, "lastStep",    r.lastStep);
  pushField(fields, "statusReason", r.statusReason);
  pushField(fields, "waitJson.reason", r.waitJson?.reason);
  pushField(fields, "waitJson.hint",   r.waitJson?.hint);

  // errorEntries — iterate so we get a precise field path back. Covers
  // both root-level and subflow-level errors (unified in flow_node_executions).
  runErrors.forEach((err, i) => {
    pushField(fields, `errorEntries[${i}].nodeId`,   err?.nodeId);
    pushField(fields, `errorEntries[${i}].message`, err?.message);
  });

  // AND across fields: a run matches when EVERY token appears in at
  // least one of its fields (the token doesn't have to land in the
  // same field as the others). Earlier behavior required every token
  // in the SAME field's text, which broke combinations like
  // `<pipelineId> failed` — pipelineId text and status text are
  // disjoint, so the conjunction always returned zero.
  //
  // We compute, per token, the set of fields it touches. A run is a
  // hit when every token has at least one such field. The returned
  // matches[] surfaces only fields that contained at least one
  // token, so the snippets remain meaningful.
  const tokenFieldHits = tokens.map((t) => {
    const hitsForToken = [];
    for (const f of fields) {
      const pos = f.text.toLowerCase().indexOf(t);
      if (pos < 0) continue;
      hitsForToken.push({ field: f, token: t, pos });
    }
    return hitsForToken;
  });
  if (tokenFieldHits.some((arr) => arr.length === 0)) return [];

  // Dedupe field → first hit so we don't emit one match entry per
  // (token, field) pair when several tokens land in the same field.
  const byField = new Map();
  for (const arr of tokenFieldHits) {
    for (const h of arr) {
      const cur = byField.get(h.field.name);
      if (!cur || h.pos < cur.pos) byField.set(h.field.name, h);
    }
  }
  const out = [];
  for (const h of byField.values()) {
    out.push({
      field: h.field.name,
      snippet: snippetAround(h.field.text, h.pos, h.token.length, contextChars),
      matchedAt: h.pos,
    });
  }
  return out;
}

function pushField(fields, name, value) {
  if (value === null || value === undefined) return;
  const text = String(value);
  if (!text) return;
  fields.push({ name, text });
}

function snippetAround(text, pos, matchLen, contextChars) {
  const start = Math.max(0, pos - contextChars);
  const end   = Math.min(text.length, pos + matchLen + contextChars);
  const prefix = start > 0 ? "…" : "";
  const suffix = end < text.length ? "…" : "";
  // Collapse newlines/tabs so the snippet stays one line.
  const body = text.slice(start, end).replace(/\s+/g, " ");
  return `${prefix}${body}${suffix}`;
}
