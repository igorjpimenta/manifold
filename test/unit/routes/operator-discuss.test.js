import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import path from "node:path";
import {
  stringifyMessageContent,
  filterDiscussHistoryMessages,
  selectDiscussProvider,
  createDiscussHandler,
} from "../../../routes/operator-discuss.js";
import { createMockApi } from "../../helpers/mock-api.js";

// No mocking required — the pure helpers carry no external static imports.

// Filesystem-backed stub for the injected `sessions` gateway dep, rooted at
// process.env.HOME so the tmp-HOME fixtures below keep working. Mirrors the
// production adapter: the file is resolved through the agent's sessions.json
// index by the manifold session KEY, never by treating the key as a filename.
function makeSessionsStub() {
  const calls = { resolve: [] };
  return {
    calls,
    resolveSessionFileForKey: (agentId, session) => {
      calls.resolve.push({ agentId, session });
      const dir = path.join(process.env.HOME, ".openclaw", "agents", agentId, "sessions");
      try {
        const index = JSON.parse(readFileSync(path.join(dir, "sessions.json"), "utf8"));
        const entry = index?.[session];
        if (entry?.sessionFile) return entry.sessionFile;
        if (typeof entry?.sessionId === "string" && entry.sessionId) {
          return path.join(dir, `${entry.sessionId}.jsonl`);
        }
      } catch {}
      return path.join(dir, `${session}.jsonl`);
    },
    sessionFileExists: (file) => {
      try { return existsSync(file); } catch { return false; }
    },
    ensureSessionFile: (file) => {
      mkdirSync(path.dirname(file), { recursive: true });
      try { writeFileSync(file, "", { flag: "wx" }); } catch {}
    },
  };
}

// ── stringifyMessageContent ───────────────────────────────────────────────────

describe("stringifyMessageContent", () => {
  it("returns string content unchanged", () => {
    assert.equal(stringifyMessageContent("hello world"), "hello world");
  });

  it("returns empty string for empty string", () => {
    assert.equal(stringifyMessageContent(""), "");
  });

  it("joins text blocks from an array", () => {
    const result = stringifyMessageContent([
      { type: "text", text: "part one" },
      { type: "text", text: "part two" },
    ]);
    assert.ok(result.includes("part one") && result.includes("part two"), result);
  });

  it("skips non-text blocks in array", () => {
    const result = stringifyMessageContent([
      { type: "toolCall", name: "foo" },
      { type: "text", text: "actual text" },
    ]);
    assert.equal(result, "actual text");
  });

  it("returns empty string for empty array", () => {
    assert.equal(stringifyMessageContent([]), "");
  });

  it("returns empty string for null/undefined", () => {
    assert.equal(stringifyMessageContent(null), "");
    assert.equal(stringifyMessageContent(undefined), "");
  });

  it("handles array entries with text property directly", () => {
    const result = stringifyMessageContent([{ text: "direct text" }]);
    assert.ok(result.includes("direct text"));
  });
});

// ── filterDiscussHistoryMessages ──────────────────────────────────────────────

describe("filterDiscussHistoryMessages", () => {
  it("returns empty array for empty input", () => {
    assert.deepEqual(filterDiscussHistoryMessages([]), []);
  });

  it("skips non-user/non-assistant messages", () => {
    const result = filterDiscussHistoryMessages([{ role: "system", content: "sys", timestamp: 1000 }]);
    assert.deepEqual(result, []);
  });

  it("marks user messages as isOperator=true", () => {
    const msgs = [{ role: "user", content: "hello", timestamp: 1000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result.length, 1);
    assert.equal(result[0].isOperator, true);
    assert.equal(result[0].text, "hello");
  });

  it("marks assistant messages as isOperator=false", () => {
    const msgs = [{ role: "assistant", content: "I can help", timestamp: 2000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result.length, 1);
    assert.equal(result[0].isOperator, false);
    assert.equal(result[0].text, "I can help");
  });

  it("strips [discuss-protocol] suffix from user messages", () => {
    const msgs = [{ role: "user", content: "user question\n\n[discuss-protocol]\nsome internal stuff", timestamp: 1000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result[0].text, "user question");
  });

  it("strips content before --- separator from user messages", () => {
    const msgs = [{ role: "user", content: "preamble\n\n---\n\nactual question", timestamp: 1000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result[0].text, "actual question");
  });

  it("strips <draft>...</draft> from assistant messages", () => {
    const msgs = [{ role: "assistant", content: "Here is my plan.\n<draft>secret draft text</draft>\nDone.", timestamp: 2000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.ok(!result[0].text.includes("<draft>"));
    assert.ok(result[0].text.includes("Here is my plan."));
  });

  it("drops empty user messages after stripping", () => {
    const msgs = [{ role: "user", content: "\n\n---\n\n", timestamp: 1000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.deepEqual(result, []);
  });

  it("drops empty assistant messages after draft stripping", () => {
    const msgs = [{ role: "assistant", content: "<draft>all draft</draft>", timestamp: 2000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.deepEqual(result, []);
  });

  it("preserves ISO timestamp strings as epoch ms", () => {
    const iso = "2026-06-01T12:00:00.000Z";
    const msgs = [{ role: "assistant", content: "hi", timestamp: iso }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result[0].ts, Date.parse(iso));
  });

  it("preserves numeric timestamps unchanged", () => {
    const msgs = [{ role: "assistant", content: "hi", timestamp: 1717200000000 }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result[0].ts, 1717200000000);
  });

  it("returns null for null/invalid timestamps", () => {
    const msgs = [{ role: "assistant", content: "hi", timestamp: "not-a-date" }];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result[0].ts, null);
  });

  it("handles mixed user and assistant turns in order", () => {
    const msgs = [
      { role: "user", content: "question", timestamp: 1 },
      { role: "assistant", content: "answer", timestamp: 2 },
      { role: "user", content: "follow-up", timestamp: 3 },
    ];
    const result = filterDiscussHistoryMessages(msgs);
    assert.equal(result.length, 3);
    assert.equal(result[0].isOperator, true);
    assert.equal(result[1].isOperator, false);
    assert.equal(result[2].isOperator, true);
  });
});

// ── selectDiscussProvider ─────────────────────────────────────────────────────

describe("selectDiscussProvider", () => {
  it("returns primary when chain is empty", () => {
    const result = selectDiscussProvider({ primarySpec: "anthropic/claude-haiku-4-5", chain: [], isCooldown: () => false });
    assert.equal(result.provider, "anthropic");
    assert.equal(result.model, "claude-haiku-4-5");
  });

  it("returns first chain entry when not in cooldown", () => {
    const chain = ["anthropic/claude-haiku-4-5", "claude-cli/claude-sonnet-4-6"];
    const result = selectDiscussProvider({ primarySpec: "", chain, isCooldown: () => false });
    assert.equal(result.provider, "anthropic");
  });

  it("returns fallback when primary is in cooldown", () => {
    const chain = ["anthropic/claude-haiku-4-5", "claude-cli/claude-sonnet-4-6"];
    const result = selectDiscussProvider({
      primarySpec: "",
      chain,
      isCooldown: (p) => p === "anthropic",
    });
    assert.equal(result.provider, "claude-cli");
    assert.equal(result.model, "claude-sonnet-4-6");
  });

  it("returns primary when cooldown but no healthy fallback exists", () => {
    const chain = ["anthropic/claude-haiku-4-5", "claude-cli/claude-sonnet-4-6"];
    const result = selectDiscussProvider({ primarySpec: "", chain, isCooldown: () => true });
    // All in cooldown — returns primary anyway (no healthy alternative)
    assert.equal(result.provider, "anthropic");
  });

  it("skips cooldown-only check when chain has one entry", () => {
    const chain = ["anthropic/claude-haiku-4-5"];
    const result = selectDiscussProvider({ primarySpec: "", chain, isCooldown: () => true });
    assert.equal(result.provider, "anthropic");
  });

  it("handles provider with no model part", () => {
    const chain = ["anthropic"];
    const result = selectDiscussProvider({ primarySpec: "", chain, isCooldown: () => false });
    assert.equal(result.provider, "anthropic");
    assert.equal(result.model, "");
  });

  it("returns null provider for empty primarySpec and empty chain", () => {
    const result = selectDiscussProvider({ primarySpec: "", chain: [], isCooldown: () => false });
    assert.equal(result.provider, null);
  });
});

// ── stamped caller identity — kept guards ───────────────────────────────────
// The elevated discuss route refuses unstamped calls before touching the
// agent runtime or the session file: missing/empty identity → 400.

describe("elev discuss handlers — stamped caller identity", () => {
  it("handleDiscuss: missing identity → 400 invalid_body", async () => {
    const { api } = createMockApi();
    const { handleDiscuss } = createDiscussHandler({ api, sessions: makeSessionsStub() });
    const { status, body } = await handleDiscuss({ flowId: "flow-1", text: "hi" });
    assert.equal(status, 400);
    assert.equal(body.code, "invalid_body");
    assert.match(body.error, /session, agentId/);
  });
});

