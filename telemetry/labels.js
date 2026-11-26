// labels — internal scope names → operator-friendly display strings, the
// single translation table HTTP responses use. Deliberately lossy: uuids and
// internal scope names are hidden.

// Shape rule, not a prefix list: a UUID means a hand-typed/scripted
// `--session-id`, whereas a real pipelineId never contains one. New ad hoc
// labels then collapse into the same bucket automatically.
const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

// Bucket for cost whose lineage walk landed on a root that isn't a flow
// (cron-owned turns, ad-hoc sessions, spawned subagents, Telegram DMs).
// A real string, not SQL NULL: CostBreakdown.Row.label is non-optional on
// strict clients, and a null fails their decode, blanking the Budget card
// instead of crashing. Must stay in step with UNATTRIBUTED_FILTER so the
// projection, the transform, and clients agree on one value.
export const UNATTRIBUTED_LABEL = "(unattributed)";

// Round-trips UNATTRIBUTED_LABEL back to the NULL rows it came from (same
// "__null__" sentinel /cost/top-calls accepts): a literal
// `pipeline_id = '(unattributed)'` would match zero rows, since no row
// stores that string.
export const UNATTRIBUTED_FILTER = { pipeline_id: "__null__" };

// Pipeline ids are `<team>_<name>`; stripping up to the first underscore is
// a shape rule, so no team name is hardcoded here.
export function stripTeamPrefix(pipelineId) {
  return typeof pipelineId === "string" ? pipelineId.replace(/^[^_]+_/, "") : pipelineId;
}

export function displayRootLabel(rootKind, rootLabel) {
  if (!rootLabel || rootKind === "unknown") return null;
  switch (rootKind) {
    case "cron-owner":
      // Cron job's free-text name — opaque, shown as-is (no prefix boundary
      // to guess at).
      return rootLabel;
    case "telegram-dm":
      return "Chat (Telegram)";
    case "operator-trigger":
      // The triggering pipelineId; stripTeamPrefix is a no-op if unprefixed.
      return stripTeamPrefix(rootLabel);
    case "explicit":
      if (rootLabel.startsWith("skill-")) return `Skill test: ${rootLabel.replace(/^skill-/, "")}`;
      // Ad hoc `--session-id` labels collapse by shape (UUID_RE) instead of
      // passing raw uuid-suffixed strings through; http-tree.js merges rows
      // that share a display label, so they land in one bucket.
      if (UUID_RE.test(rootLabel)) return "Ad-hoc sessions";
      // Manually-triggered runs: index.js recordTrigger sets rootLabel to the
      // pipelineId when the root would otherwise be a bare ad hoc session.
      return stripTeamPrefix(rootLabel);
    case "agent-default":
      return `${rootLabel} (idle agent)`;
    case "bare-subagent":
      return "Spawned subagent";
    case "memory-synth-chunk":
      return "Memory synthesis";
    case "flow-triggered":
      return rootLabel || "Pipeline run";
    case "flow-prime":
      return "Pipeline prime";
    case "flow-node":
    case "flow-gate-discuss":
      // Mid-chain frames; as root they mean the parent flow was pruned.
      return `Pipeline (parent expired)`;
    case "cron-tick":
      return `Cron run`;
    default:
      return rootLabel;
  }
}

export function displayRootKind(rootKind) {
  switch (rootKind) {
    case "cron-owner":
    case "cron-tick": return "Cron";
    case "telegram-dm": return "Telegram";
    case "operator-trigger":
    case "explicit":
    case "agent_tool":
    case "cli": return "Operator";
    default: return "unknown";
  }
}
