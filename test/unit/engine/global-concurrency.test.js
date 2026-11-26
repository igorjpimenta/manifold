import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { configureGlobalConcurrency, getGlobalConcurrencyLimit, _resetForTests } from "../../../engine/global-concurrency.js";

describe("global-concurrency", () => {
  beforeEach(() => { _resetForTests(); });

  test("defaults to 8", () => {
    assert.equal(getGlobalConcurrencyLimit(), 8);
  });

  test("configureGlobalConcurrency sets a custom max, ignores invalid values", () => {
    configureGlobalConcurrency({ maxConcurrent: 5 });
    assert.equal(getGlobalConcurrencyLimit(), 5);

    configureGlobalConcurrency({ maxConcurrent: 0 });
    assert.equal(getGlobalConcurrencyLimit(), 5, "non-positive value ignored");

    configureGlobalConcurrency({ maxConcurrent: -1 });
    assert.equal(getGlobalConcurrencyLimit(), 5, "negative value ignored");

    configureGlobalConcurrency({ maxConcurrent: "not a number" });
    assert.equal(getGlobalConcurrencyLimit(), 5, "non-integer value ignored");

    configureGlobalConcurrency({ maxConcurrent: 3.5 });
    assert.equal(getGlobalConcurrencyLimit(), 5, "non-integer numeric value ignored");

    configureGlobalConcurrency({});
    assert.equal(getGlobalConcurrencyLimit(), 5, "missing maxConcurrent leaves the current value");

    configureGlobalConcurrency();
    assert.equal(getGlobalConcurrencyLimit(), 5, "no args at all leaves the current value");
  });

  test("_resetForTests without an argument restores the default", () => {
    configureGlobalConcurrency({ maxConcurrent: 5 });
    _resetForTests();
    assert.equal(getGlobalConcurrencyLimit(), 8);
  });

  test("_resetForTests with an argument sets that exact value", () => {
    _resetForTests(1);
    assert.equal(getGlobalConcurrencyLimit(), 1);
  });
});
