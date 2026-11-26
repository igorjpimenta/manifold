// pricer — dollar cost from token counts × per-million rates, for when the
// runner fails to stamp usage.cost (model not registered, fallback breakage),
// so hook-written rows converge on one value. Rates come from openclaw.json's
// per-model `cost` blocks where present; add a new (provider, model) by
// appending to RATES.

const RATES = {
  "openrouter|deepseek/deepseek-v4-pro":   { in: 0.435, out: 0.870, cR: 0.003625, cW: 0       },
  "openrouter|deepseek/deepseek-v4-flash": { in: 0.0983, out: 0.1966, cR: 0.0197,  cW: 0      },
  "openrouter|anthropic/claude-sonnet-4.6":{ in: 3.0,   out: 15.0,  cR: 0.3,     cW: 3.75    },

  "zai|glm-4.5-flash": { in: 0,    out: 0,   cR: 0,    cW: 0 },

  // Dedicated coding-plan endpoint: the general zai endpoint rejects glm-5.1
  // with "insufficient balance" — the plan's package is only visible here.
  "zai-coding|glm-5.1": { in: 1.2, out: 4.0, cR: 0.24, cW: 0 },

  "evolink-anthropic|claude-sonnet-4-6": { in: 2.7,  out: 13.5, cR: 0.271, cW: 3.375 },
  "evolink-anthropic|deepseek-v4-pro":   { in: 1.74, out: 3.48, cR: 0,     cW: 0     },

  "evolink|gemini-2.5-flash-lite": { in: 0.094, out: 0.374, cR: 0.009, cW: 0 },
  "evolink|gemini-3-pro-preview":  { in: 1.865, out: 11.182, cR: 0.187, cW: 0 },
  "evolink|deepseek-reasoner":     { in: 0.294, out: 0.441, cR: 0.029, cW: 0 },
};

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
  const computed = computeCost(provider, model, usage);
  if (computed) return { source: "computed", ...computed };
  return { source: "unknown", input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 };
}

export function knownModels() {
  return Object.keys(RATES).map((k) => {
    const [provider, model] = k.split("|");
    return { provider, model };
  });
}
