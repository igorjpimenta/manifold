import { displayRootKind } from "./labels.js";

const ALL_ROOT_KINDS = [
  "cron-owner", "cron-tick",
  "telegram-dm",
  "operator-trigger", "explicit", "agent_tool", "cli",
];

export function peersForKind(kind) {
  const label = displayRootKind(kind);
  if (label === "unknown") return [kind];
  return ALL_ROOT_KINDS.filter((k) => displayRootKind(k) === label);
}

// Trigger-edge re-attribution: re-root pipeline cost to its originating
// session (cron / operator / telegram) instead of the flow's owner session.
export const ATTR_JOIN = `
  LEFT JOIN session_lineage sl
    ON cbc.flow_id IS NOT NULL
   AND sl.child_session_key = 'flow:' || cbc.flow_id
   AND sl.source IN ('trigger_hook', 'trigger_backfill')
   AND sl.root_kind != 'unknown'`;
export const ATTR_ROOT_KIND  = `COALESCE(sl.root_kind,  cbc.root_kind)`;
export const ATTR_ROOT_LABEL = `COALESCE(sl.root_label, cbc.root_label)`;

export function formatSessionLabel(session, tsFirstMs) {
  if (tsFirstMs != null) {
    const d = new Date(tsFirstMs);
    const mon = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getMonth()];
    const day = d.getDate();
    const hh  = String(d.getHours()).padStart(2, "0");
    const mm  = String(d.getMinutes()).padStart(2, "0");
    return `${mon} ${day} ${hh}:${mm}`;
  }
  const tail = (session ?? "").split(":").pop() ?? session;
  return (tail || session || "").slice(0, 24);
}

export function sessionKindForRoot(rootKind, session) {
  if (typeof session === "string") {
    if (session.startsWith("agent:operator:cli:"))     return "cli-trigger";
    if (session.startsWith("agent:operator:"))         return "operator-trigger";
  }
  switch (rootKind) {
    case "cron-owner":
      return (typeof session === "string" && session.includes(":run:"))
        ? "cron-tick"
        : "cron-job-session";
    case "explicit":           return "operator-run";
    case "telegram-dm":        return "conversation";
    case "memory-synth-chunk": return "memory-chunk";
    case "bare-subagent":      return "subagent-run";
    case "agent-default":      return "agent-run";
    default:                   return "session";
  }
}

export function formatSessionChildLabel(row) {
  const { child_key, kind, root_label, flow_id } = row;
  if (child_key.startsWith("flow:")) return `flow ${flow_id?.slice(0,8) ?? child_key.slice(5,13)}`;
  if (kind === "cron-tick") {
    const parts = child_key.split(":");
    const idx = parts.indexOf("run");
    if (idx >= 0 && parts[idx+1]) return `run ${parts[idx+1].slice(0,8)}`;
  }
  if (kind === "flow-node") {
    const parts = child_key.split(":");
    const agent = parts[1] ?? "?";
    const tail  = parts[parts.length - 1] ?? "";
    return `${agent} · ${tail.slice(0, 40)}`;
  }
  const parts = child_key.split(":");
  return `${parts[1] ?? "?"} · ${(parts[parts.length - 1] ?? "").slice(0, 30)}`;
}

export function bootstrapForecast(byDay, currentHour, todayRows, numSamples = 200) {
  const horizon = 23 - currentHour;
  if (horizon <= 0 || byDay.size === 0) return null;
  const hourDists = [];
  for (let h = currentHour + 1; h <= 23; h++) {
    const vals = [];
    for (const [, hourMap] of byDay) vals.push(hourMap.get(h) ?? 0);
    hourDists.push(vals);
  }
  const base = todayRows.reduce((s, r) => s + r.cost, 0);
  const paths = Array.from({ length: numSamples }, () => {
    let cumul = base;
    return hourDists.map((vals) => {
      cumul += vals[Math.floor(Math.random() * vals.length)];
      return cumul;
    });
  });
  return hourDists.map((_, i) => {
    const col = paths.map((p) => p[i]).sort((a, b) => a - b);
    const at  = (q) => col[Math.floor((col.length - 1) * q)];
    return {
      hour: currentHour + 1 + i,
      p10:  Number(at(0.10).toFixed(6)),
      p50:  Number(at(0.50).toFixed(6)),
      p90:  Number(at(0.90).toFixed(6)),
    };
  });
}

// libsql rows are array-like Proxies: spreading one directly also picks up
// numeric index keys. Flatten to a plain object once, at the query boundary.
export async function execAll(client, sql, args = []) {
  const { rows, columns } = await client.execute({ sql, args });
  return rows.map((r) => {
    const o = {};
    for (let i = 0; i < columns.length; i++) o[columns[i]] = r[i];
    return o;
  });
}

export async function execGet(client, sql, args = []) {
  const rows = await execAll(client, sql, args);
  return rows[0] ?? null;
}

export async function hasFlowRuns(client) {
  try {
    const rows = await execAll(client,
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_runs' LIMIT 1"
    );
    return rows.length > 0;
  } catch { return false; }
}
