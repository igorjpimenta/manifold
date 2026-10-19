// Live event subscription for the dashboard. The server emits Server-Sent
// Events on /api/events; this module wraps EventSource into a React-friendly
// surface so views can opt in via `useSse(callback)` without each one
// managing reconnects.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { SseEvent } from "./types";

type Listener = (event: SseEvent) => void;
type ConnState = "connecting" | "live" | "dead";

interface SseContextValue {
  state: ConnState;
  subscribe: (cb: Listener) => () => void;
}

const SseContext = createContext<SseContextValue | null>(null);

export function SseProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<ConnState>("connecting");
  const listenersRef = useRef<Set<Listener>>(new Set());
  // Hold the latest EventSource so manual reconnects don't leak.
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let cancelled = false;

    function connect() {
      if (cancelled) return;
      setState("connecting");
      const es = new EventSource(`${import.meta.env.BASE_URL}api/events`);
      esRef.current = es;
      es.onopen = () => { if (!cancelled) setState("live"); };
      es.onerror = () => {
        if (cancelled) return;
        setState("dead");
        try { es.close(); } catch { /* swallow */ }
        // Brief backoff, then reconnect. The connection is local-loopback so
        // any "error" usually just means the gateway restarted.
        setTimeout(connect, 1500);
      };
      es.onmessage = (e) => {
        let evt: SseEvent | null = null;
        try { evt = JSON.parse(e.data) as SseEvent; } catch { return; }
        if (!evt) return;
        for (const cb of listenersRef.current) {
          try { cb(evt); } catch (err) { console.warn("sse listener error", err); }
        }
      };
    }

    connect();
    return () => {
      cancelled = true;
      try { esRef.current?.close(); } catch { /* swallow */ }
    };
  }, []);

  const subscribe = useCallback((cb: Listener) => {
    listenersRef.current.add(cb);
    return () => { listenersRef.current.delete(cb); };
  }, []);

  return (
    <SseContext.Provider value={{ state, subscribe }}>
      {children}
    </SseContext.Provider>
  );
}

export function useSseState(): ConnState {
  const ctx = useContext(SseContext);
  return ctx?.state ?? "connecting";
}

/** Subscribe to live events for the duration of the component's lifetime. */
export function useSse(cb: Listener) {
  const ctx = useContext(SseContext);
  // Latest-ref pattern so callers can update their handler each render
  // without re-subscribing on every keystroke-level state change.
  const cbRef = useRef(cb);
  useEffect(() => { cbRef.current = cb; }, [cb]);
  useEffect(() => {
    if (!ctx) return;
    return ctx.subscribe((evt) => cbRef.current(evt));
  }, [ctx]);
}
