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
