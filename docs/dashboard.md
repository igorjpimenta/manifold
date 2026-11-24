# Dashboard

The read-only web UI, its API surface, and the pipeline-declared views pattern.

> **Known issue (2026-11-10):** Dashboard not rendering on some
> installs — the SPA shell loads but the React app does not mount. Under
> investigation; tracked as a follow-up to the session-file retirement
> work. The backend (`/api/*`) responds normally.

Read-only web UI with a small set of actuation endpoints (approve, cancel,
rerun, pause, resume). Listens on `dashboardHost:dashboardPort` (default
`127.0.0.1:44300`).
Behind tailscale serve at `https://<tailnet>/graph/` when configured
(`tailscale serve --set-path=/graph`).

API endpoints:

```
GET  /api/health
GET  /api/pipelines
GET  /api/pipelines/:id
GET  /api/runs?status=&pipelineId=&limit=
GET  /api/runs/:flowId
GET  /api/runs/:flowId/events
GET  /api/runs/:flowId/artifacts
GET  /api/runs/:flowId/artifacts/:name
GET  /api/runs/:flowId/transcripts
GET  /api/runs/:flowId/compare
GET  /api/stats
GET  /api/stats/pipelines/:id
GET  /api/stats/pipelines/:id/views?sinceDays=N    (pipeline-declared view payloads — see below)
GET  /api/events    (SSE: flow_created | flow_changed | flow_terminated | pipelines_changed)

POST /api/runs/:flowId/approve     body: { itemId, decision: "approve"|"reject"|"edit", text? }
                                   → 200 { decision, itemId, stillPending, status: "blocked"|"resumed" }
                                   → 400 invalid body | 404 flow not found
                                   → 409 not awaiting / itemId not pending / store write race
POST /api/runs/:flowId/cancel      cancel a non-terminal flow
POST /api/runs/:flowId/rerun       fresh run from __start__ (new flow id)
POST /api/runs/:flowId/pause       pause the flow
POST /api/runs/:flowId/resume      resume a paused flow
POST /api/runs/bulk-cancel         cancel many flows in one call
```

Source: `dashboard/server.js`. Frontend uses hash routing (`#/`,
`#/pipelines`, `#/pipelines/:id`, `#/runs/:flowId`), filter state in
URL query (`#/?status=succeeded&range=7d`). See `dashboard/src/App.tsx`.


## Declared views (embed-view pattern)

A pipeline can attach extra cards to its dashboard detail page by
exporting a `views: [...]` array. The dashboard discovers them via
`GET /api/stats/pipelines/:id/views` and dispatches on `kind` to a
generic renderer — **no dashboard code changes are required** to ship
a new view, as long as the kind + source pair is already supported.

```ts
type ViewDeclaration = {
  id: string;              // pipeline-unique, stable across reloads
  kind: "funnel";          // currently the only supported kind
  label: string;           // card title
  source: SourceDeclaration;
  stages: StageDeclaration[];
};

type SourceDeclaration = {
  kind: "frontmatter-dir"; // walks <dir>/*.md, parses YAML frontmatter
  path: string;            // absolute path
  timeField?: string;      // default "sent-at" — used for sinceDays window
};

type StageDeclaration = {
  id: string;              // stage-unique
  label?: string;          // default = id
  outcomeIn?: string[];    // record's `outcome` field must be in this set
  outcomeEq?: string;      // record's `outcome` field must equal this
  // No filter declared ⇒ stage matches every record (cohort base).
};
```

Server computer: `dashboard/views.js` (top-level `computeViews`,
per-kind `computeFunnelView`, per-source `loadFrontmatterDirRecords`).
Frontend dispatcher: `dashboard/src/views/PipelineDetail.tsx`
`DeclaredViewCard` / `FunnelViewCard`.

### How the funnel computes

For `kind: "funnel"`:

1. Load records via the source loader (frontmatter-dir → one record
   per `*.md` file, with all frontmatter keys plus `__ts` from the
   `timeField`).
2. Apply the time window: records whose `timeField` is missing or
   earlier than `Date.now() - sinceDays * 86_400_000` are excluded.
3. For each stage, count records matching the stage filter. Stages
   are independent filters over the full record set — they don't
   *enforce* superset semantics, so an honest funnel relies on the
   pipeline author declaring later stages as subsets of earlier ones.
4. Compute `conversionFromPrev = count[i] / count[i-1]`, and
   `conversionFromStart` on the last stage.
5. Build `byOutcome` (per-outcome distribution) when the source's
   records carry an `outcome` field — surfaces under the funnel as a
   "breakdown" strip.

### Adding a new view kind

When a pipeline needs a view that isn't a funnel (e.g. a leaderboard,
a per-day timeline, a single-number KPI):

1. **Server:** add a `kind: "<name>"` branch to `computeView()` in
   `dashboard/views.js` and a per-kind computer that returns the
   payload shape (decide the shape — there's no required envelope
   beyond `{ id, kind, label, sinceMs }`).
2. **API types:** add a `DeclaredView<Name>` variant to the union in
   `dashboard/src/api/types.ts`.
3. **Frontend:** add a `view.kind === "<name>"` branch in
   `DeclaredViewCard` (in `PipelineDetail.tsx`) and the matching
   `<Name>ViewCard` renderer.
4. **Pipeline:** declare the view in `views: [...]`.

If the new view uses a new *source* (not `frontmatter-dir`), also add
a `source.kind === "<name>"` branch in `loadRecords()`.

### Anti-patterns — don't

- **Don't hardcode a view in the dashboard for a single pipeline.**
  The whole point of `views: [...]` is to keep per-pipeline surfaces
  out of `PipelineDetail.tsx`. If you're tempted to add an `if
  (pipelineId === "foo")` branch, declare a view instead.
- **Don't expose secrets in `source.path`.** The dashboard reads
  every `.md` file in the dir and ships parsed frontmatter to the
  client. Sensitive fields belong elsewhere.
- **Don't declare stages whose filters aren't subset-ordered.** The
  funnel won't reject it, but conversions will read >100% and
  confuse operators. Pattern: stage 0 = no filter (cohort base),
  later stages add filters that progressively narrow.

### Recipe — declare an outcome funnel

When you need: a "did the pipeline's output actually work" surface
backed by per-record outcomes stored as YAML frontmatter in a flat
directory.

```js
// In your pipeline's index.js:
const RECORDS_DIR = path.join(SHARED, "<domain>", "outcomes");

export default {
  id: "<pipelineId>",
  // ...nodes, edges...
  views: [
    {
      id: "outcomes",
      kind: "funnel",
      label: "outcomes (last 30d)",
      source: {
        kind: "frontmatter-dir",
        path: RECORDS_DIR,
        timeField: "created-at",
      },
      stages: [
        { id: "created",  label: "created" },                                 // cohort base
        { id: "engaged",  label: "engaged",  outcomeIn: ["engaged","won"] },  // strict superset of `won`
        { id: "won",      label: "won",      outcomeEq: "won" },
      ],
    },
  ],
};
```

Each `*.md` in `RECORDS_DIR` needs at minimum:

```markdown
---
created-at: 2026-05-20T10:00:00Z
outcome: engaged
---
<body>
```

The dashboard renders the card below the in-run funnel/histogram row
on `PipelineDetail`, showing absolute counts, per-stage conversion,
the `byOutcome` breakdown, and overall `created → won` %.
