// harness/events.js — verbatim event stream for harness producers.
//
// Append-only log: producers declare the row shape (including any `kind`)
// at write time; the stream never validates, stamps, or interprets rows.
// Subscribers receive the identical reference that was appended. A throwing
// subscriber is skipped so one consumer cannot break the others or the
// producer. No harness imports — this module is dependency-free by design.
export function createEventStream() {
  const rows = [];
  const subscribers = new Set();

  return {
    append(row) {
      rows.push(row);
      for (const fn of [...subscribers]) {
        try {
          fn(row);
        } catch {}
      }
      return row;
    },

    subscribe(fn) {
      subscribers.add(fn);
      return () => {
        subscribers.delete(fn);
      };
    },

    history(limit) {
      if (limit === undefined) return rows.slice();
      if (limit <= 0) return [];
      return rows.slice(-limit);
    },
  };
}
