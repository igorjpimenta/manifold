// labels — translate internal scope names to operator-friendly display
// strings. Used by HTTP responses so clients don't need their own
// translation table (single source of truth).
//
// Mapping is intentionally lossy in the friendly direction — we hide
// uuids and internal scope names because the operator doesn't read those.

// Matches any label carrying a UUID — the shape of a hand-typed or scripted
// `--session-id` (test/forge scaffolding, one-off debugging labels), as
// opposed to a real pipelineId (which never contains one). Detecting by
// shape instead of enumerating known prefixes ("test-", "verify-",
// "scorer-creation-", ...) means new ad hoc labels collapse into the same
// bucket automatically instead of adding fresh noise every time someone
// types a new one.
const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

// Display label for cost that exists but belongs to no pipeline — the
// "Where it's going" breakdown's bucket for every cost row whose lineage
// walk landed on a root that isn't a flow (cron-owned agent turns,
// operator ad-hoc sessions, spawned subagents, Telegram DMs).
//
// Deliberately a real string rather than SQL NULL: CostBreakdown.Row.label
// is a non-optional String on clients with strict decoding, and
// stripTeamPrefix passes non-strings through unchanged, so a null label
// fails the whole decode and silently blanks the Budget card rather than
// crashing. Kept in step with UNATTRIBUTED_FILTER below so the projection,
// the transform, and any future client agree on one value.
export const UNATTRIBUTED_LABEL = "(unattributed)";

// The `filter` payload that round-trips UNATTRIBUTED_LABEL back to the NULL
// rows it was computed from. Mirrors the "__null__" sentinel /cost/top-calls
// already accepts (see http-cost.js's top-calls filter loop): a literal
// `pipeline_id = '(unattributed)'` would match zero rows, because no row
// stores that string — only the projection synthesizes it.
export const UNATTRIBUTED_FILTER = { pipeline_id: "__null__" };

// Pipeline ids are always shaped `<team>_<name>` (workspace/.pipelines/<team>_<name>.js —
// the same convention every team's pipelines follow). Stripping the leading
// segment up to the first underscore is therefore a shape-based rule, not a
// specific-team rule: it applies identically regardless of which team owns
// the pipeline, so no team name is ever hardcoded here.
export function stripTeamPrefix(pipelineId) {
  return typeof pipelineId === "string" ? pipelineId.replace(/^[^_]+_/, "") : pipelineId;
}

export function displayRootLabel(rootKind, rootLabel) {
  if (!rootLabel || rootKind === "unknown") return null;
  switch (rootKind) {
    case "cron-owner":
      // rootLabel here is the cron job's own free-text name (grammar.js's
      // CRON_OWN capture) — an opaque string we don't generate and have no
      // shape contract for, so it's shown as-is rather than guessing at a
      // team-prefix boundary that may not exist or may differ per job.
      return rootLabel;
    case "telegram-dm":
      // rootLabel is the chat id — opaque, so the label is a constant
      // rather than a pass-through.
      return "Chat (Telegram)";
    case "operator-trigger":
      // rootLabel is the triggering pipelineId (routes/operator.js's
      // recordTrigger call) — strip the team prefix for terseness. Legacy
      // rows still literally "Operator runs" have no underscore, so
      // stripTeamPrefix is a no-op on them.
      return stripTeamPrefix(rootLabel);
    case "explicit":
      // The most common explicit purpose is "graph-flow-owner" — used by
      // any flow launched from the CLI or operator-initiated tool call.
      if (rootLabel === "graph-flow-owner") return "Operator runs";
      // Skill-test / probe / forge sessions
      if (rootLabel.startsWith("skill-")) return `Skill test: ${rootLabel.replace(/^skill-/, "")}`;
      // Hand-typed or scripted --session-id values (test/forge scaffolding,
      // one-off debugging) — collapse by shape (see UUID_RE above) rather
      // than passing through raw, uuid-suffixed strings that clutter the
      // instance list. http-tree.js merges same-display-label instance rows,
      // so every ad hoc label lands in one consolidated bucket.
      if (UUID_RE.test(rootLabel)) return "Ad-hoc sessions";
      // Manually-triggered pipeline runs (index.js's recordTrigger overrides
      // rootLabel to the pipelineId when the resolved root is otherwise a bare
      // ad hoc session) — same shape-based stripping as operator-trigger.
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
      // These are mid-chain frames; if they appear as root it's because
      // the parent flow was pruned. Surface gently.
      return `Pipeline (parent expired)`;
    case "cron-tick":
      // Mid-chain frame surfacing as root — rare.
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
