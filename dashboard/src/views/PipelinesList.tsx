import { useEffect, useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, THead, TBody, TR, TH, TD } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/api/client";
import { useSse } from "@/api/sse";
import { cn } from "@/lib/utils";
import type { Pipeline } from "@/api/types";

export function PipelinesList() {
  // Nullable until first fetch lands — distinguishes "still loading"
  // (skeleton) from "loaded, empty" (the no-pipelines-loaded copy).
  const [pipelines, setPipelines] = useState<Pipeline[] | null>(null);

  const load = () => { api.pipelines().then(setPipelines); };
  useEffect(load, []);
  useSse((evt) => { if (evt.type === "pipelines_changed") load(); });

  return (
    <>
      <h2 className="text-[15px] font-semibold uppercase tracking-[0.5px] text-fg-dim mt-6 mb-2.5">pipelines</h2>
      <Card>
        {pipelines === null
          ? (
            <div className="space-y-2">
              {[0, 1, 2, 3, 4].map((i) => (
                <div key={i} className="flex items-center gap-3 py-1">
                  <Skeleton className="h-3 w-60" />
                  <Skeleton className="h-3 w-8" />
                  <Skeleton className="h-3 w-72" />
                  <Skeleton className="h-3 w-24 ml-auto" />
                  <Skeleton className="h-4 w-20" />
                </div>
              ))}
            </div>
          )
          : pipelines.length === 0
          ? <div className="text-fg-dim italic">no pipelines loaded</div>
          : (
            <>
              {/*
                Mobile-first card list: <sm operators get a stack of
                tap-targets, each with the load-bearing info (id, goal,
                status) and no horizontal scroll. The desktop table
                below is hidden at this breakpoint.
              */}
              <div className="sm:hidden flex flex-col gap-2">
                {pipelines.map((p) => (
                  <a
                    key={p.id}
                    href={`#/pipelines/${encodeURIComponent(p.id)}`}
                    className={cn(
                      "block rounded border border-border bg-bg-elev2/30 hover:bg-bg-elev2 px-3 py-2",
                      p.historical && "opacity-65 italic",
                    )}
                  >
                    <div className="flex items-baseline gap-2">
                      <span className="font-mono text-sm truncate flex-1">{p.id}</span>
                      {p.historical
                        ? <Badge variant="historical">historical</Badge>
                        : <span className="text-[10px] font-mono text-fg-dim shrink-0">{p.nodes.length - 2} nodes</span>}
                    </div>
                    <div className="text-xs text-fg-dim mt-0.5 line-clamp-2">
                      {p.historical
                        ? <>no longer on disk · {p.runs} run{p.runs === 1 ? "" : "s"} in DB</>
                        : (p.goal ?? "")}
                    </div>
                  </a>
                ))}
              </div>
              {/* Desktop / tablet table — unchanged. */}
              <div className="hidden sm:block">
                <Table>
                  <THead>
                    <TR><TH>id</TH><TH>nodes</TH><TH>goal</TH><TH>file</TH><TH>status</TH></TR>
                  </THead>
                  <TBody>
                    {pipelines.map((p) => (
                      <TR
                        key={p.id}
                        className={cn("cursor-pointer hover:bg-bg-elev2", p.historical && "opacity-65 italic")}
                        onClick={() => { window.location.hash = `#/pipelines/${encodeURIComponent(p.id)}`; }}
                      >
                        <TD className="font-mono"><a href={`#/pipelines/${encodeURIComponent(p.id)}`}>{p.id}</a></TD>
                        <TD className="text-fg-dim">{p.historical ? "—" : p.nodes.length - 2}</TD>
                        <TD className="text-fg-dim">
                          {p.historical
                            ? <span className="text-fg-dim">no longer on disk · {p.runs} run(s) in DB</span>
                            : (p.goal ?? "").slice(0, 100)}
                        </TD>
                        <TD className="font-mono text-fg-dim text-xs">
                          {p.historical ? "—" : (p.file ?? "").replace(/^.*\//, "")}
                        </TD>
                        <TD>
                          {p.historical
                            ? <Badge variant="historical">historical</Badge>
                            : <Badge variant="succeeded">implemented</Badge>}
                        </TD>
                      </TR>
                    ))}
                  </TBody>
                </Table>
              </div>
            </>
          )
        }
      </Card>
    </>
  );
}
