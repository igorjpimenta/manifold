import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createEventStream } from "../../../harness/events.js";

describe("harness event stream", () => {
  it("delivers appended rows to subscribers verbatim (same reference)", () => {
    const stream = createEventStream();
    const seen = [];
    stream.subscribe((row) => seen.push(row));
    const row = { kind: "flow.started", flowId: "abc", nested: { n: [1, 2] } };
    const returned = stream.append(row);
    assert.equal(seen.length, 1);
    assert.equal(seen[0] === row, true);
    assert.equal(returned === row, true);
  });

  it("passes producer-declared kinds through untouched, no validation", () => {
    const stream = createEventStream();
    const seen = [];
    stream.subscribe((row) => seen.push(row));
    const weird = { kind: "totally.unknown-kind!!", v: 1 };
    const kindless = { noKindAtAll: true };
    assert.doesNotThrow(() => {
      stream.append(weird);
      stream.append(kindless);
      stream.append("a string row");
      stream.append(null);
    });
    assert.deepEqual(seen, [weird, kindless, "a string row", null]);
  });

  it("replays history in append order, honoring limit", () => {
    const stream = createEventStream();
    const a = { kind: "a" };
    const b = { kind: "b" };
    const c = { kind: "c" };
    stream.append(a);
    stream.append(b);
    stream.append(c);
    assert.deepEqual(stream.history(), [a, b, c]);
    assert.deepEqual(stream.history(2), [b, c]);
    assert.deepEqual(stream.history(10), [a, b, c]);
    assert.deepEqual(stream.history(0), []);
    assert.equal(stream.history()[0] === a, true);
  });

  it("only delivers rows appended after subscribing; unsubscribe stops delivery", () => {
    const stream = createEventStream();
    stream.append({ kind: "before" });
    const seen = [];
    const unsub = stream.subscribe((row) => seen.push(row));
    stream.append({ kind: "after" });
    assert.deepEqual(seen, [{ kind: "after" }]);
    unsub();
    stream.append({ kind: "late" });
    assert.deepEqual(seen, [{ kind: "after" }]);
    assert.equal(stream.history().length, 3);
  });

  it("a throwing subscriber does not break the producer or other subscribers", () => {
    const stream = createEventStream();
    const seen = [];
    stream.subscribe(() => { throw new Error("boom"); });
    stream.subscribe((row) => seen.push(row));
    assert.doesNotThrow(() => stream.append({ kind: "k" }));
    assert.deepEqual(seen, [{ kind: "k" }]);
  });
});
