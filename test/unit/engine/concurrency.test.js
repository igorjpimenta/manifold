// Unit tests for engine/concurrency.js — the single shared
// concurrency ceiling state/admission-queue-store.js (kind:"child-dispatch"
// and kind:"flow-start") reads on every admit(). See that module's own
// header for why these share one ceiling.

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { configureConcurrency, getConcurrencyLimit, _resetForTests } from "../../../engine/concurrency.js";

describe("concurrency", () => {
  beforeEach(() => { _resetForTests(); });

  test("defaults to 8", () => {
    assert.equal(getConcurrencyLimit(), 8);
  });

  test("configureConcurrency sets a custom max, ignores invalid values", () => {
    configureConcurrency({ maxConcurrent: 5 });
    assert.equal(getConcurrencyLimit(), 5);

    configureConcurrency({ maxConcurrent: 0 });
    assert.equal(getConcurrencyLimit(), 5, "non-positive value ignored");

    configureConcurrency({ maxConcurrent: -1 });
    assert.equal(getConcurrencyLimit(), 5, "negative value ignored");

    configureConcurrency({ maxConcurrent: "not a number" });
    assert.equal(getConcurrencyLimit(), 5, "non-integer value ignored");

    configureConcurrency({ maxConcurrent: 3.5 });
    assert.equal(getConcurrencyLimit(), 5, "non-integer numeric value ignored");

    configureConcurrency({});
    assert.equal(getConcurrencyLimit(), 5, "missing maxConcurrent leaves the current value");

    configureConcurrency();
    assert.equal(getConcurrencyLimit(), 5, "no args at all leaves the current value");
  });

  test("_resetForTests without an argument restores the default", () => {
    configureConcurrency({ maxConcurrent: 5 });
    _resetForTests();
    assert.equal(getConcurrencyLimit(), 8);
  });

  test("_resetForTests with an argument sets that exact value", () => {
    _resetForTests(1);
    assert.equal(getConcurrencyLimit(), 1);
  });
});
