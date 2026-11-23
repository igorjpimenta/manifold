// paths.test.js — env store paths are taken verbatim, except that a value
// which arrived wrapped in matching quotes is refused instead of used. The
// quotes would become part of the filename, so the process would open (and
// create) a second store whose name only looks like the configured one, and
// every other reader of the real path would see an empty history.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { resolveStorePaths, resolveDataDir } from "../../../state/paths.js";

describe("env store paths", () => {
  test("an unquoted env path is used verbatim", () => {
    const paths = resolveStorePaths({ env: { MANIFOLD_RUNTIME_DB: "/srv/manifold/runtime.sqlite" } });
    assert.equal(paths.runtimeDb, "/srv/manifold/runtime.sqlite");
  });

  for (const name of ["MANIFOLD_RUNTIME_DB", "MANIFOLD_OPS_DB", "MANIFOLD_PIPELINES_DIR", "MANIFOLD_MEDIA_AUDIO_DIR", "MANIFOLD_DATA_DIR"]) {
    test(`${name} as a double-quoted literal is refused by name`, () => {
      assert.throws(
        () => resolveStorePaths({ env: { [name]: '"/srv/manifold/x"' } }),
        new RegExp(`${name} arrived as a quoted literal`),
      );
    });
  }

  test("single quotes are refused too, and surrounding whitespace does not hide them", () => {
    assert.throws(
      () => resolveStorePaths({ env: { MANIFOLD_RUNTIME_DB: "  '/srv/x'  " } }),
      /MANIFOLD_RUNTIME_DB arrived as a quoted literal/,
    );
  });

  test("resolveDataDir refuses a quoted MANIFOLD_DATA_DIR", () => {
    assert.throws(() => resolveDataDir({ env: { MANIFOLD_DATA_DIR: '"/srv/data"' } }), /MANIFOLD_DATA_DIR/);
  });

  test("an unbalanced or internal quote is a normal path character, not a quoted literal", () => {
    const paths = resolveStorePaths({ env: { MANIFOLD_RUNTIME_DB: '/srv/x"y.sqlite' } });
    assert.equal(paths.runtimeDb, '/srv/x"y.sqlite');
  });

  test("explicit paths win over the environment, quoted or not", () => {
    const paths = resolveStorePaths({
      paths: { runtimeDb: "/explicit/runtime.sqlite" },
      env: { MANIFOLD_RUNTIME_DB: '"/quoted/runtime.sqlite"' },
    });
    assert.equal(paths.runtimeDb, "/explicit/runtime.sqlite");
  });
});
