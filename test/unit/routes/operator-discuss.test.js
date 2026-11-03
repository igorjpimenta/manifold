import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  stringifyMessageContent,
  filterDiscussHistoryMessages,
  selectDiscussProvider,
  createDiscussHandler,
  createDiscussHistoryHandler,
} from "../../../routes/operator-discuss.js";
import { createMockApi } from "../../helpers/mock-api.js";

// No mocking required — the pure helpers carry no external static imports.

// Filesystem-backed stub for the injected `sessions` gateway dep, rooted at
// process.env.HOME so the tmp-HOME fixtures below keep working.
function makeSessionsStub() {
  return {
    resolveSessionFile: (agentId, sessionId) =>
      path.join(process.env.HOME, ".openclaw", "agents", agentId, "sessions", `${sessionId}.jsonl`),
    sessionFileExists: (file) => {
      try { return existsSync(file); } catch { return false; }
    },
    readSessionFileText: (file) => readFile(file, "utf-8"),
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
// The elevated discuss routes refuse unstamped calls before touching the
// agent runtime or the session file: missing/empty identity → 400.

describe("elev discuss handlers — stamped caller identity", () => {
  it("handleDiscuss: missing identity → 400 invalid_body", async () => {
    const { api } = createMockApi();
    const { handleDiscuss } = createDiscussHandler({ api, sessions: makeSessionsStub() });
    const { status, body } = await handleDiscuss({ flowId: "flow-1", text: "hi" });
    assert.equal(status, 400);
    assert.equal(body.code, "invalid_body");
    assert.match(body.error, /sessionKey, sessionId, agentId/);
  });

  it("handleDiscussHistory: missing identity → 400 invalid_body", async () => {
    const { api } = createMockApi();
    const { handleDiscussHistory } = createDiscussHistoryHandler({ api, sessions: makeSessionsStub() });
    const { status, body } = await handleDiscussHistory({ flowId: "flow-1" });
    assert.equal(status, 400);
    assert.equal(body.code, "invalid_body");
    assert.match(body.error, /sessionKey, sessionId/);
  });
});

// ── createDiscussHistoryHandler — bounded JSONL read ─────────────────────────
// The handler must tail the session file to MAX_HISTORY_LINES before parsing:
// an unbounded synchronous parse on the gateway's single event loop can stall
// past the client's (much tighter) history-load timeout.

describe("createDiscussHistoryHandler — bounded JSONL read", () => {
  const writeSession = (homeDir, agentId, sessionId, lines) => {
    const dir = path.join(homeDir, ".openclaw", "agents", agentId, "sessions");
    mkdirSync(dir, { recursive: true });
    writeFileSync(path.join(dir, `${sessionId}.jsonl`), lines.join("\n"));
  };
  const entryLine = (role, text, ts) =>
    JSON.stringify({ timestamp: ts, message: { role, content: text } });

  it("returns all messages when the session is under the line cap", async () => {
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-hist-"));
    const prevHome = process.env.HOME;
    try {
      process.env.HOME = tmpHome;
      const lines = [
        entryLine("user", "hi", 1),
        entryLine("assistant", "hello back", 2),
      ];
      writeSession(tmpHome, "main", "sess-small", lines);
      const { api } = createMockApi();
      const { handleDiscussHistory } = createDiscussHistoryHandler({ api, sessions: makeSessionsStub() });
      const { status, body } = await handleDiscussHistory({
        flowId: "flow-1", sessionKey: "sess-key-1", agentId: "main", sessionId: "sess-small",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.equal(body.messages.length, 2);
      assert.equal(body.hasMore, false);
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });

  it("tails to the last MAX_HISTORY_LINES entries for an oversized session", async () => {
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-hist-"));
    const prevHome = process.env.HOME;
    try {
      process.env.HOME = tmpHome;
      // 600 lines > the 500-line cap. Distinct text per line so we can
      // assert the *oldest* turns were dropped, not just that the count
      // is bounded.
      const lines = [];
      for (let i = 0; i < 600; i++) {
        lines.push(entryLine(i % 2 === 0 ? "user" : "assistant", `turn-${i}`, i));
      }
      writeSession(tmpHome, "main", "sess-huge", lines);
      const { api } = createMockApi();
      const { handleDiscussHistory } = createDiscussHistoryHandler({ api, sessions: makeSessionsStub() });
      const { status, body } = await handleDiscussHistory({
        flowId: "flow-1", sessionKey: "sess-key-1", agentId: "main", sessionId: "sess-huge",
      });
      assert.equal(status, 200);
      assert.equal(body.messages.length, 500);
      // Oldest surviving turn is turn-100 (600 - 500), the first 100 dropped.
      assert.equal(body.messages[0].text, "turn-100");
      assert.equal(body.messages[body.messages.length - 1].text, "turn-599");
      assert.equal(body.hasMore, true);
      assert.equal(body.nextOffset, 500);
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });

  it("pages further back when the caller passes the prior response's nextOffset", async () => {
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-hist-"));
    const prevHome = process.env.HOME;
    try {
      process.env.HOME = tmpHome;
      const lines = [];
      for (let i = 0; i < 600; i++) {
        lines.push(entryLine(i % 2 === 0 ? "user" : "assistant", `turn-${i}`, i));
      }
      writeSession(tmpHome, "main", "sess-huge-2", lines);
      const { api } = createMockApi();
      const { handleDiscussHistory } = createDiscussHistoryHandler({ api, sessions: makeSessionsStub() });
      const first = await handleDiscussHistory({
        flowId: "flow-1", sessionKey: "sess-key-1", agentId: "main", sessionId: "sess-huge-2",
      });
      assert.equal(first.body.hasMore, true);
      const second = await handleDiscussHistory({
        flowId: "flow-1", sessionKey: "sess-key-1", agentId: "main", sessionId: "sess-huge-2",
        offset: first.body.nextOffset,
      });
      assert.equal(second.status, 200);
      // Remaining 100 turns (0..99) all fit in one more page — no more after this.
      assert.equal(second.body.messages.length, 100);
      assert.equal(second.body.messages[0].text, "turn-0");
      assert.equal(second.body.messages[second.body.messages.length - 1].text, "turn-99");
      assert.equal(second.body.hasMore, false);
      // The two pages together cover the whole session with no gap/overlap.
      assert.equal(second.body.messages[second.body.messages.length - 1].text, "turn-99");
      assert.equal(first.body.messages[0].text, "turn-100");
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });

  it("returns empty messages when the session file doesn't exist", async () => {
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-hist-"));
    const prevHome = process.env.HOME;
    try {
      process.env.HOME = tmpHome;
      const { api } = createMockApi();
      const { handleDiscussHistory } = createDiscussHistoryHandler({ api, sessions: makeSessionsStub() });
      const { status, body } = await handleDiscussHistory({
        flowId: "flow-1", sessionKey: "sess-key-1", agentId: "main", sessionId: "sess-missing",
      });
      assert.equal(status, 200);
      assert.deepEqual(body.messages, []);
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });
});
