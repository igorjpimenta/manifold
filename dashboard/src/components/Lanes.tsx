// Lanes — the admission pool at a glance: lanes held, lanes free, work queued,
// and the two ways a lane goes wrong. A lane is one running flow; a "suspect"
// lane is a running flow that has not moved for longer than the stale window
// (its dispatch will free the lane only when its own timeout fires), and an
// "orphan" lane belongs to a flow that is terminal, blocked or gone — nothing
// will release it. Both appear here because the alternative is watching the
// free count drop with no way to see who is holding what.

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, THead, TBody, TR, TH, TD } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { formatDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import { api } from "@/api/client";
import type { LanesSnapshot } from "@/api/types";

export function Lanes() {
  const [snap, setSnap] = useState<LanesSnapshot | null>(null);

  useEffect(() => {
    let live = true;
    const load = () => api.lanes().then((s) => { if (live) setSnap(s); }).catch(() => {});
    load();
    const t = setInterval(load, 15_000);
    return () => { live = false; clearInterval(t); };
  }, []);

  if (snap === null) {
    return (
      <Card>
        <CardHeader><CardTitle>lanes</CardTitle></CardHeader>
        <Skeleton className="h-3 w-40" />
      </Card>
    );
  }

  const { limit, held, free, queued, suspect, orphan, lanes } = snap;
  const used = limit > 0 ? Math.min(100, (held / limit) * 100) : 0;
  const bad = suspect + orphan;

  return (
    <Card>
      <CardHeader>
        <CardTitle>lanes</CardTitle>
        <span className={cn("font-mono text-xs", bad > 0 ? "text-status-amber" : "text-fg-dim")}>
          <span className="tabular-nums">{held}</span>/<span className="tabular-nums">{limit}</span> held
          {" · "}
          <span className="tabular-nums">{free}</span> free
          {" · "}
          <span className={queued > 0 ? "text-status-amber" : "text-fg-dim"}>
            <span className="tabular-nums">{queued}</span> queued
          </span>
          {suspect > 0 && <> · <span className="text-status-amber tabular-nums">{suspect}</span> stalled</>}
          {orphan > 0 && <> · <span className="text-status-red tabular-nums">{orphan}</span> leaked</>}
        </span>
      </CardHeader>

      <div className="h-2 rounded bg-bg border border-border overflow-hidden mb-3" aria-hidden>
        <div className="h-full bg-status-green" style={{ width: `${used}%` }} />
      </div>

      {lanes.length === 0 ? (
        <p className="text-fg-dim text-xs m-0">no lanes held — the pool is idle</p>
      ) : (
        <Table>
          <THead>
            <TR>
              <TH>pipeline</TH>
              <TH>node</TH>
              <TH>status</TH>
              <TH>held</TH>
              <TH>idle</TH>
            </TR>
          </THead>
          <TBody>
            {lanes.map((l) => (
              <TR key={l.flowId}>
                <TD className="font-mono text-xs">{l.pipelineId ?? "—"}</TD>
                <TD className="font-mono text-xs">{l.lastStep ?? l.nodeId ?? "—"}</TD>
                <TD>
                  <span
                    className={cn(
                      "font-mono text-xs",
                      l.holder === "orphan" ? "text-status-red" : l.holder === "suspect" ? "text-status-amber" : "text-fg-dim",
                    )}
                    title={l.holder === "orphan"
                      ? "flow is terminal, blocked or gone — nothing will release this lane"
                      : l.holder === "suspect"
                        ? "no progress past the stale window — likely a dispatch that will only free the lane on timeout"
                        : "running and recently active"}
                  >
                    {l.holder === "ok" ? l.flowStatus ?? "running" : l.holder}
                  </span>
                </TD>
                <TD className="font-mono text-xs tabular-nums">{l.ageMs != null ? formatDuration(l.ageMs) : "—"}</TD>
                <TD className="font-mono text-xs tabular-nums">{l.idleMs != null ? formatDuration(l.idleMs) : "—"}</TD>
              </TR>
            ))}
          </TBody>
        </Table>
      )}
    </Card>
  );
}
