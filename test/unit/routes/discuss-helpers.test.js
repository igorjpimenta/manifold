import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  looksLikeMetaNarration,
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
      "Já entreguei projetos similares para clientes do setor financeiro. " +
      "Posso iniciar imediatamente e garantir a entrega no prazo acordado.";
    assert.equal(looksLikeMetaNarration(draft), false);
  });

  it("DRAFT_META_PREFIXES is an array of RegExps", () => {
    assert.ok(Array.isArray(DRAFT_META_PREFIXES));
    assert.ok(DRAFT_META_PREFIXES.every((r) => r instanceof RegExp));
  });
});
