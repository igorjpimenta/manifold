// pricer — when the runner fails to stamp usage.cost in a JSONL message
// (model not registered in config, fallback chain breakage, etc.), compute
// dollar cost from token counts × per-million rates. Used by the
// before_message_write hook so hook-written rows converge on one value.
//
// Rates sourced from openclaw.json's per-model `cost` blocks where present.
// Adding a new (provider, model): just append to RATES.

const RATES = {
  // openrouter
  "openrouter|deepseek/deepseek-v4-pro":   { in: 0.435, out: 0.870, cR: 0.003625, cW: 0       },
  "openrouter|deepseek/deepseek-v4-flash": { in: 0.0983, out: 0.1966, cR: 0.0197,  cW: 0      },
  "openrouter|anthropic/claude-sonnet-4.6":{ in: 3.0,   out: 15.0,  cR: 0.3,     cW: 3.75    },

  // zai (general endpoint)
  "zai|glm-4.5-flash": { in: 0,    out: 0,   cR: 0,    cW: 0 },

  // zai-coding (GLM Coding Plan dedicated endpoint — required for glm-5.1
  // on this account; the general endpoint rejects it with "insufficient
  // balance", the plan's resource package only being visible from here)
  "zai-coding|glm-5.1": { in: 1.2, out: 4.0, cR: 0.24, cW: 0 },

  // evolink-anthropic
  "evolink-anthropic|claude-sonnet-4-6": { in: 2.7,  out: 13.5, cR: 0.271, cW: 3.375 },
  "evolink-anthropic|deepseek-v4-pro":   { in: 1.74, out: 3.48, cR: 0,     cW: 0     },

  // evolink (Google/DeepSeek aggregator)
  "evolink|gemini-2.5-flash-lite": { in: 0.094, out: 0.374, cR: 0.009, cW: 0 },
  "evolink|gemini-3-pro-preview":  { in: 1.865, out: 11.182, cR: 0.187, cW: 0 },
  "evolink|deepseek-reasoner":     { in: 0.294, out: 0.441, cR: 0.029, cW: 0 },
};

/**
 * Returns { input, output, cacheRead, cacheWrite, total } in dollars,
 * or null if (provider, model) isn't in the rate table.
 */
export function computeCost(provider, model, usage) {
  if (!provider || !model || !usage) return null;
  const rate = RATES[`${provider}|${model}`];
  if (!rate) return null;
  const ci = ((usage.input        ?? 0) / 1_000_000) * rate.in;
  const co = ((usage.output       ?? 0) / 1_000_000) * rate.out;
  const cR = ((usage.cacheRead    ?? 0) / 1_000_000) * rate.cR;
  const cW = ((usage.cacheWrite   ?? 0) / 1_000_000) * rate.cW;
  return { input: ci, output: co, cacheRead: cR, cacheWrite: cW, total: ci + co + cR + cW };
}

/**
 * Take what the source has (usage.cost) and, if it's all zeros despite
 * real tokens, fall back to computed cost. Annotates the returned row
 * with the source so callers can filter.
 */
export function resolveCost({ provider, model, usage }) {
  const srcCost = usage?.cost;
  const hasTokens =
    (usage?.input ?? 0) > 0 ||
    (usage?.output ?? 0) > 0 ||
    (usage?.cacheRead ?? 0) > 0 ||
    (usage?.cacheWrite ?? 0) > 0;
  const srcTotal = typeof srcCost?.total === "number" ? srcCost.total : 0;

  if (srcTotal > 0 || !hasTokens) {
    return { source: "runner", ...srcCost, total: srcTotal };
  }
  // Source has zero cost but real tokens — try to compute
  const computed = computeCost(provider, model, usage);
  if (computed) return { source: "computed", ...computed };
  // Unknown rate — record zero but mark unknown
  return { source: "unknown", input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 };
}

export function knownModels() {
  return Object.keys(RATES).map((k) => {
    const [provider, model] = k.split("|");
    return { provider, model };
  });
}
