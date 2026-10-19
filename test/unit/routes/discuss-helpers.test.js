import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  looksLikeMetaNarration,
  findDraftPathForItem,
  parseFrontmatterOrdered,
  serializeFrontmatter,
  normalizePendingFields,
  DRAFT_META_PREFIXES,
} from "../../../routes/discuss-helpers.js";

// ── looksLikeMetaNarration ────────────────────────────────────────────────────

describe("looksLikeMetaNarration", () => {
  it("returns true for short text (< 40 chars)", () => {
    assert.equal(looksLikeMetaNarration("Short."), true);
  });

  it("returns false for a plain substantive draft reply", () => {
    const draft = "Obrigado pelo contato! Tenho experiência com projetos similares e posso entregar dentro do prazo solicitado.";
    assert.equal(looksLikeMetaNarration(draft), false);
  });

  it("returns true for 'Here's the revised version'", () => {
    assert.equal(looksLikeMetaNarration("Here's the revised version of the proposal that includes all the requested changes."), true);
  });

  it("returns true for 'Draft written and saved'", () => {
    assert.equal(looksLikeMetaNarration("Draft written and saved successfully. The updated text follows the template."), true);
  });

  it("returns true for 'Revised version:'", () => {
    assert.equal(looksLikeMetaNarration("Revised version:\n\nDear client, I would be happy to work on this project."), true);
  });

  it("returns false for a multi-line substantive draft", () => {
    const draft =
      "Tenho 5 anos de experiência com desenvolvimento Python e Django. " +
      "Já entregei projetos similares para clientes do setor financeiro. " +
      "Posso iniciar imediatamente e garantir a entrega no prazo acordado.";
    assert.equal(looksLikeMetaNarration(draft), false);
  });

  it("DRAFT_META_PREFIXES is an array of RegExps", () => {
    assert.ok(Array.isArray(DRAFT_META_PREFIXES));
    assert.ok(DRAFT_META_PREFIXES.every((r) => r instanceof RegExp));
  });
});

// ── findDraftPathForItem ──────────────────────────────────────────────────────

describe("findDraftPathForItem", () => {
  it("returns null for no branches", () => {
    assert.equal(findDraftPathForItem([], "item-1"), null);
  });

  it("returns null when itemId not present in any branch", () => {
    const branches = [
      { groupId: "PROPOSE", branchKey: "other-item", data: { DRAFT: { draftPath: "/tmp/draft.md" } } },
    ];
    assert.equal(findDraftPathForItem(branches, "item-1"), null);
  });

  it("returns draftPath for matching item", () => {
    const branches = [
      { groupId: "PROPOSE", branchKey: "item-1", data: { DRAFT: { draftPath: "/tmp/item1-draft.md" } } },
    ];
    assert.equal(findDraftPathForItem(branches, "item-1"), "/tmp/item1-draft.md");
  });

  it("ignores nodes without draftPath", () => {
    const branches = [
      {
        groupId: "PROPOSE",
        branchKey: "item-1",
        data: {
          NODE_A: { someOtherField: "x" },
          NODE_B: { draftPath: "/tmp/item1.md" },
        },
      },
    ];
    assert.equal(findDraftPathForItem(branches, "item-1"), "/tmp/item1.md");
  });
});

// ── parseFrontmatterOrdered / serializeFrontmatter ────────────────────────────

describe("parseFrontmatterOrdered", () => {
  it("returns [[], raw] for plain text without frontmatter", () => {
    const [entries, body] = parseFrontmatterOrdered("Hello world");
    assert.deepEqual(entries, []);
    assert.equal(body, "Hello world");
  });

  it("parses a valid frontmatter block preserving order", () => {
    const raw = `---\ntitle: My Draft\nbudget: 500\n---\n\nBody text here`;
    const [entries, body] = parseFrontmatterOrdered(raw);
    assert.deepEqual(entries, [["title", "My Draft"], ["budget", "500"]]);
    assert.ok(body.includes("Body text here"));
  });

  it("strips surrounding quotes from values", () => {
    const raw = `---\ntitle: "Quoted value"\n---\n\nBody`;
    const [entries] = parseFrontmatterOrdered(raw);
    assert.equal(entries[0][1], "Quoted value");
  });
});

describe("serializeFrontmatter", () => {
  it("round-trips plain entries", () => {
    const entries = [["title", "Hello"], ["budget", "500"]];
    const out = serializeFrontmatter(entries, "Body text");
    const [back, body] = parseFrontmatterOrdered(out);
    assert.deepEqual(back, entries);
    assert.ok(body.includes("Body text"));
  });

  it("quotes values containing colons", () => {
    const entries = [["url", "https://example.com"]];
    const out = serializeFrontmatter(entries, "body");
    assert.ok(out.includes('"https://example.com"'), `expected quoted url in: ${out}`);
  });
});

// ── normalizePendingFields ────────────────────────────────────────────────────

describe("normalizePendingFields", () => {
  it("converts numeric budget string to integer", () => {
    assert.deepEqual(normalizePendingFields({ budget: "150" }), { budget: 150 });
  });

  it("ignores non-numeric budget strings", () => {
    assert.deepEqual(normalizePendingFields({ budget: "negotiable" }), { budget: "negotiable" });
  });

  it("leaves non-budget fields unchanged", () => {
    assert.deepEqual(normalizePendingFields({ title: "My project", deadline: "2026-07-01" }), {
      title: "My project", deadline: "2026-07-01",
    });
  });

  it("does not mutate the input object", () => {
    const input = { budget: "200" };
    normalizePendingFields(input);
    assert.equal(input.budget, "200");
  });
});
