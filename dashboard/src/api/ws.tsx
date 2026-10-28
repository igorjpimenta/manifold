// Live event subscription for the dashboard. The server pushes events over
// the /api/events/ws WebSocket; this module wraps it into a React-friendly
// surface so views can opt in via `useWs(callback)` without each one
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
import type { LiveEvent } from "./types";

type Listener = (event: LiveEvent) => void;
type ConnState = "connecting" | "live" | "dead";

interface WsContextValue {
  state: ConnState;
  subscribe: (cb: Listener) => () => void;
}

const WsContext = createContext<WsContextValue | null>(null);

function wsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${import.meta.env.BASE_URL}api/events/ws`;
}

export function WsProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<ConnState>("connecting");
  const listenersRef = useRef<Set<Listener>>(new Set());
  // Hold the latest socket so manual reconnects don't leak.
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let cancelled = false;
    let backoff: ReturnType<typeof setTimeout> | null = null;

    function connect() {
      if (cancelled) return;
      setState("connecting");
      let ws: WebSocket;
      try {
        ws = new WebSocket(wsUrl());
      } catch {
        if (cancelled) return;
        setState("dead");
        backoff = setTimeout(connect, 1500);
        return;
      }
      wsRef.current = ws;
      ws.onopen = () => { if (!cancelled) setState("live"); };
      const reconnect = () => {
        if (cancelled) return;
        setState("dead");
        try { ws.close(); } catch { /* swallow */ }
        // Brief backoff, then reconnect. The connection is local-loopback so
        // any drop usually just means the dashboard server restarted.
        backoff = setTimeout(connect, 1500);
      };
      ws.onerror = reconnect;
      ws.onclose = reconnect;
      ws.onmessage = (e) => {
        let evt: LiveEvent | null = null;
        try { evt = JSON.parse(e.data) as LiveEvent; } catch { return; }
        if (!evt) return;
        for (const cb of listenersRef.current) {
          try { cb(evt); } catch (err) { console.warn("ws listener error", err); }
        }
      };
    }

    connect();
    return () => {
      cancelled = true;
      if (backoff) clearTimeout(backoff);
      try { wsRef.current?.close(); } catch { /* swallow */ }
    };
  }, []);

  const subscribe = useCallback((cb: Listener) => {
    listenersRef.current.add(cb);
    return () => { listenersRef.current.delete(cb); };
  }, []);

  return (
    <WsContext.Provider value={{ state, subscribe }}>
      {children}
    </WsContext.Provider>
  );
}

export function useWsState(): ConnState {
  const ctx = useContext(WsContext);
  return ctx?.state ?? "connecting";
}

/** Subscribe to live events for the duration of the component's lifetime. */
export function useWs(cb: Listener) {
  const ctx = useContext(WsContext);
  // Latest-ref pattern so callers can update their handler each render
  // without re-subscribing on every keystroke-level state change.
  const cbRef = useRef(cb);
  useEffect(() => { cbRef.current = cb; }, [cb]);
  useEffect(() => {
    if (!ctx) return;
    return ctx.subscribe((evt) => cbRef.current(evt));
  }, [ctx]);
}
