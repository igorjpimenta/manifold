// In-process push-event stream: append-only history plus fan-out subscribe.
// Delivery faults never throw into the producer — a dead subscriber must not
// fail the dispatch that produced the event.
export function createEventStream({ maxHistory = 200, logger = null } = {}) {
  const history = [];
  const subscribers = new Set();

  function append(event) {
    if (!event || typeof event !== "object") return null;
    const entry = { ...event, at: event.at ?? Date.now() };
    history.push(entry);
    while (history.length > maxHistory) history.shift();
    for (const fn of [...subscribers]) {
      try { fn(entry); } catch (err) {
        try { logger?.warn?.(`events/stream: subscriber failed: ${err?.message ?? err}`); } catch {}
      }
    }
    return entry;
  }

  function subscribe(fn) {
    if (typeof fn !== "function") throw new Error("events/stream: subscribe requires a function");
    subscribers.add(fn);
    return () => subscribers.delete(fn);
  }

  function getHistory({ event = null, limit = maxHistory } = {}) {
    const rows = event ? history.filter((e) => e.event === event) : [...history];
    return rows.slice(-Math.max(0, limit));
  }

  return { append, subscribe, history: getHistory };
}
