// In-memory substring search over the dashboard's flow_runs view.
//
// Not FTS5: the dashboard opens registry.sqlite read-only and does not own
// the schema, so an FTS mirror would need writes + triggers owned by someone
// else; the in-memory walk sidesteps that coordination. Revisit past ~10k
// rows or once query latency shows on the endpoint.
//
// Scope: structured fields plus errorEntries (flow_node_executions unifies
// root and subflow errors). Top-level state keys are NOT searchable — the
// runs LIST route never selects state_json — and transcripts are deferred
// (walking session JSONLs per query).

const DEFAULT_LIMIT = 50;
const DEFAULT_CONTEXT_CHARS = 40;

export function searchRuns(runs, query, opts = {}) {
  const limit = opts.limit ?? DEFAULT_LIMIT;
  const contextChars = opts.contextChars ?? DEFAULT_CONTEXT_CHARS;
  const errorsByFlow = opts.errorsByFlow ?? new Map();
  // Exact-equality status filter, applied before the free-text match, so
  // "failed" inside an error message can't false-positive; the StatStrip
  // sub-count links rely on this reading strictly.
  const statusFilter = typeof opts.status === "string" ? opts.status.trim().toLowerCase() : "";
  if (statusFilter) runs = runs.filter((r) => (r.status ?? "").toLowerCase() === statusFilter);
  const trimmed = String(query ?? "").trim();
  if (!trimmed) {
    // status-only: every filtered run as a hit with no snippets (matches: []).
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

  // All whitespace-separated tokens must appear (AND). Quoted phrases are
  // unsupported — that would need a real tokenizer.
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

  // Field order drives snippet presentation: identity-bearing fields first.
  pushField(fields, "flowId",         r.flowId);
  pushField(fields, "pipelineId",     r.pipelineId ?? r.stateJson?.pipelineId);
  pushField(fields, "status",         r.status);
  pushField(fields, "lastStep",    r.lastStep);
  pushField(fields, "statusReason", r.statusReason);
  pushField(fields, "waitJson.reason", r.waitJson?.reason);
  pushField(fields, "waitJson.hint",   r.waitJson?.hint);

  runErrors.forEach((err, i) => {
    pushField(fields, `errorEntries[${i}].nodeId`,   err?.nodeId);
    pushField(fields, `errorEntries[${i}].message`, err?.message);
  });

  // A run matches when EVERY token appears in at least one field — tokens may
  // land in different fields. Requiring all of them in the same field broke
  // combinations like `<pipelineId> failed`, whose texts are disjoint.
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

  // One match entry per field, keeping each field's earliest hit.
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
  const body = text.slice(start, end).replace(/\s+/g, " ");
  return `${prefix}${body}${suffix}`;
}
