// Pure helpers for discuss handlers — no I/O, no closure deps, fully unit-testable.

export const DRAFT_META_PREFIXES = [
  /^draft\b.{0,80}(written|saved|done|updated|complete)/i,
  /^here'?s\s+/i,
  /^below\s+is/i,
  /^above\s+is/i,
  /^the\s+(new|revised|updated)\s+(version|draft|reply|text)/i,
  /^i\s+(have\s+)?revised/i,
  /^revised\s+(version|draft)/i,
  /^changes?:/i,
  /^(before|after)\s*(\(|:)/i,
];

export function looksLikeMetaNarration(text) {
  const trimmed = text.trim();
  if (trimmed.length < 40) return true;
  const firstLine = trimmed.split("\n", 1)[0].trim();
  return DRAFT_META_PREFIXES.some((re) => re.test(firstLine));
}

// Find the draftPath written by the pipeline when it saved the item's draft
// to disk. Returns the first draftPath found in any branch node result for
// this item, or null when none exists. Pure — caller supplies the flow's
// branch rows (branch data lives exclusively in flow_branches, not
// stateJson) rather than this function reaching for a service itself.
export function findDraftPathForItem(branches, itemId) {
  if (!Array.isArray(branches) || !itemId) return null;
  const branch = branches.find((b) => b.branchKey === itemId);
  if (!branch?.data) return null;
  for (const nodeResult of Object.values(branch.data)) {
    if (nodeResult && typeof nodeResult === "object" && typeof nodeResult.draftPath === "string" && nodeResult.draftPath.length > 0) {
      return nodeResult.draftPath;
    }
  }
  return null;
}

// Parse a YAML-style frontmatter block, preserving key order.
// Returns [entries, body] where entries = [[key, value], ...].
export function parseFrontmatterOrdered(raw) {
  const m = raw.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
  if (!m) return [[], raw];
  const lines = m[1].split("\n");
  const entries = [];
  for (const line of lines) {
    const kv = line.match(/^([a-zA-Z][\w-]*)\s*:\s*(.*)$/);
    if (!kv) continue;
    let v = kv[2].trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    entries.push([kv[1], v]);
  }
  return [entries, m[2]];
}

export function serializeFrontmatter(entries, body) {
  const fmText = entries.map(([k, v]) => {
    const needsQuote = /[:#\[\]{}]|^[!*&|>%@`]/.test(v);
    const out = needsQuote ? `"${v.replace(/"/g, '\\"')}"` : v;
    return `${k}: ${out}`;
  }).join("\n");
  return `---\n${fmText}\n---\n\n${body.replace(/^\n+/, "")}`;
}

// Normalize form-submitted field values before persisting to the discuss-pending
// sidecar (e.g. budget strings → integers). Keeps other values unchanged.
export function normalizePendingFields(fields) {
  const out = { ...fields };
  if (typeof out.budget === "string") {
    const n = parseInt(out.budget, 10);
    if (!isNaN(n)) out.budget = n;
  }
  return out;
}
