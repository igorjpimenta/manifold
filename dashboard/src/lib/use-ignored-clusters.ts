// localStorage-backed suppression state for failure clusters.
//
// Cluster key = `${pipelineId}::${step}`. Per-signature suppression
// would be more precise but adds UI surface area; start coarse and
// only refine if the operator hits noise on novel signatures within
// a step they've already silenced. Per-browser by design — the
// dashboard is a personal tool and a Mac-vs-phone split between
// "what I've triaged" is fine.

import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "gf-ignored-clusters-v1";

export type IgnoredMap = Record<string, { at: number }>;

function read(): IgnoredMap {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function write(next: IgnoredMap) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)); } catch {}
}

export function clusterKey(pipelineId: string | undefined | null, step: string): string {
  return `${pipelineId ?? "?"}::${step}`;
}

export function useIgnoredClusters() {
  const [ignored, setIgnored] = useState<IgnoredMap>(() => read());

  // Cross-tab sync: if the same browser has the dashboard open in two
  // tabs, ignoring in one should reflect in the other.
  useEffect(() => {
    function onStorage(e: StorageEvent) {
      if (e.key !== STORAGE_KEY) return;
      setIgnored(read());
    }
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const ignore = useCallback((key: string) => {
    setIgnored((prev) => {
      const next = { ...prev, [key]: { at: Date.now() } };
      write(next);
      return next;
    });
  }, []);

  const unignore = useCallback((key: string) => {
    setIgnored((prev) => {
      const next = { ...prev };
      delete next[key];
      write(next);
      return next;
    });
  }, []);

  return { ignored, ignore, unignore };
}
