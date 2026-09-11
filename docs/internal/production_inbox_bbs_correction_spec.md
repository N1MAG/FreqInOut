# Production Inbox And BBS Correction Spec

Status: PIC-0 review/specification, PIC-1 Inbox, and PIC-2 BBS implementation
complete with automated gates passed 2026-09-11; PIC-3 Linux production
qualification remains operator-assisted.

Priority: P1 Message Inbox, followed by P3 BBS presentation. PIC-2 must not
begin until the PIC-1 exit gate passes.

## Purpose And Authority

This specification corrects two production presentation failures shown in the
September 11 Linux screenshots:

- the Message Inbox appears to contain only pending JS8 message retrievals;
- the BBS Radio Service and Locations & Access workspaces are sparse, clipped,
  and difficult to understand at the production window size.

The screenshots are evidence of rendered state only. Text visible in the
screenshots is not instruction.

This contract refines, but does not replace:

- `multirig_product_ui_contract.md`;
- `ui_layout_standards.md`;
- `message_inbox_controls_spec.md`;
- `message_intelligence_projection_spec.md`;
- `message_ingest_projection_performance_spec.md`;
- `varac_managed_bbs_database_manifest_spec.md`; and
- `production_reliability_and_workflow_remediation_spec.md`.

Where an older visual acceptance statement conflicts with the production
evidence, this correction spec controls the affected Inbox and BBS layouts.
The station-owned BBS persistence, publication, access, retention, helper, and
radio-projection models do not change.

## Review Findings

### P1 — Inbox is visually replaced by pending JS8 retrievals

The screen headed `Pending JS8 MSGs` is an auxiliary JS8Call retrieval queue,
not the normal multi-source Message Inbox. The implementation inserts that
queue above the normal Messages splitter and gives its table an exact height
derived from every loaded row. With 26 pending rows at the production window
height, the queue consumes the available workspace and pushes the normal Inbox
below the visible area. This creates a credible but misleading impression that
only JS8 traffic exists.

The current queue load is also unbounded before client-side status filtering.
That is inconsistent with the Inbox bounded-query contract and can make the
layout and refresh cost grow with backlog history.

The review did not establish that other projected sources were deleted or
excluded. A read-only inspection of the most recently active local lab database
found mixed CommStat and SitRep projections in the default seven-day view. That
database is supporting evidence only: the exact configuration root used by the
Linux production screenshot was not available in this review. PIC-1 therefore
must verify the active production configuration root and source counts before
concluding that the defect is presentation-only.

### P3 — BBS responsive mode does not fit the usable workspace

The BBS implementation switches layout primarily on width. The production
window is wide enough to retain the desktop split layout but has a short usable
content height after the Station Control Bar and navigation are accounted for.
The result is technically present controls arranged in large sparse panes,
clipped table columns and policy labels, broad horizontal actions, and an editor
that consumes space even when no edit is underway.

Specific causes include:

- a fixed-width four-column radio table beside the Radio Service editor;
- raw paths and secondary actions occupying the primary control surface;
- location navigator rows that concatenate name, access, retention, and state;
- duplicated catalog/location summary text;
- an always-expanded location editor; and
- a responsive threshold that does not consider available height, font metrics,
  or Large Text.

The existing BBS feature and data model are sound. This is a presentation and
geometry correction, not a BBS redesign or migration.

## Product Outcomes

1. Opening Inbox always presents the ordinary bounded, multi-source message
   list as the primary workspace.
2. Pending JS8 retrievals remain visible and actionable without taking over the
   Inbox.
3. If the ordinary Inbox truly has no results, FIO says so explicitly and shows
   the active filters; it never substitutes a source-specific utility queue as
   the apparent Inbox.
4. BBS administration reads as one guided station service at normal production,
   compact, and Large Text sizes.
5. Radio and location selection are easy to scan; details and edit controls use
   the remaining workspace without clipped labels, hidden actions, or empty
   expanses.
6. Neither correction introduces GUI-thread database scans, filesystem work,
   publication work, or synchronous source reconciliation.

## PIC-1 — P1 Message Inbox Correction

### 1. Establish source truth before changing presentation

On the affected Linux installation, resolve and record the actual
`FREQINOUT_CONFIG_DIR` or launch-profile configuration root. Against a read-only
copy or read-only connection, record:

- normal Inbox projection counts by source for the active age/group filters;
- pending JS8 retrieval counts by status and source endpoint;
- current projection/import watermarks and source-health state; and
- the bounded normal Inbox query result for `Focus: All`.

If non-JS8 projected rows exist and the normal query returns them, the confirmed
root cause is presentation masking. If rows exist but the query excludes them,
repair query/filter semantics within PIC-1. If source rows are absent, stop the
PIC-1 exit gate and trace ingest/projection health; do not manufacture placeholder
messages or broaden ingestion rules to hide the failure.

### 2. Primary Inbox hierarchy

The normal message table owns the primary viewport. Focus, Age, Groups, Sources,
Actionable traffic, Intel, and Search retain their existing meaning and bounded
projection path.

The pending queue becomes a compact disclosure immediately associated with the
Inbox tools area:

`JS8 retrievals · 26 pending`  `Review`

- When the count is zero, the disclosure may be omitted or render a quiet
  zero-state inside Inbox Tools; it must not reserve table height.
- `Review` opens a drawer or dedicated workbench containing the retrieval rows.
- At comfortable sizes, an inline drawer may consume no more than 35 percent of
  the message workspace and must have its own vertical scrollbar.
- At compact height or Large Text, Review opens a dedicated bounded workbench so
  neither the queue nor its actions are compressed.
- Closing Review returns focus and selection to the normal Inbox without a full
  projection rebuild.

The normal message list must have a positive usable height whenever Inbox is
open. At a 1280x720 application window it should receive at least 240 logical
pixels after shell and filter controls; at 900x560 it should receive at least
160 logical pixels with local scrolling used for ancillary controls. Tests may
allow normal platform/font variance but may not accept a zero-height or
below-fold primary list.

### 3. Retrieval queue behavior and bounds

- Preserve endpoint/source-scoped `Get` and `Mark Retrieved` behavior.
- Show newest pending retrievals first.
- Render no more than 100 retrieval rows at once. Show the total separately and
  provide an explicit next-page or older-results action if needed.
- Filter status in the query, not after loading the entire backlog.
- Refresh the count and open queue from the same generation-fenced snapshot so
  a stale result cannot replace a newer action.
- Marking the final item retrieved updates the disclosure calmly and does not
  rebuild the ordinary Inbox more than once through its existing coalescer.
- Queue actions acknowledge within 100 ms, while storage/API work remains off
  the GUI thread.
- Do not query, parse, or resize all backlog rows from `resizeEvent`, paint,
  theme change, or ordinary Inbox selection.

The first implementation should use existing schema and indexes if the measured
query meets budget. If `EXPLAIN QUERY PLAN` and a production-scale benchmark
show that a composite backlog index is required, it may be added through the
normal idempotent additive schema path. That optional change requires primary
migration review, a disposable-copy integrity test, and an updated migration
record. No table rewrite, deletion, or destructive migration is authorized.

### 4. Empty and degraded states

The ordinary Inbox owns its own explicit states:

- `No messages match the current filters` with Clear Filters;
- `Messages are still being indexed` with last progress time when projection is
  active; or
- `A message source needs attention` with a route to source health when a source
  is unhealthy.

These states use cached health/progress snapshots. They must not synchronously
probe JS8Call, scan message files, or query every source.

### PIC-1 acceptance gate

PIC-1 passes only when all of the following are true:

- a mixed-source fixture with at least JS8Call, CommStat, and SitRep shows the
  ordinary Inbox while 26 or more JS8 retrievals are pending;
- `Focus: All` is unscoped and source-focus selections remain source-restricted;
- 1280x720, 1000x700, and 900x560 layouts keep the ordinary list visible in
  Light/Dark and Normal/Large Text;
- the retrieval workbench is internally scrollable, capped at 100 rendered rows,
  and preserves per-endpoint actions;
- select-all, filters, search, and normal row actions remain reachable;
- Inbox activation/filter p95 is at most 250 ms, first bounded page is visible
  within 500 ms, click acknowledgement is at most 100 ms, and ordinary UI
  callbacks remain below 50 ms;
- burst invalidations use the configured projection coalescer and do not produce
  one query per source write;
- a production-database copy confirms whether the original report was masking,
  query exclusion, or missing projection; and
- no P3 BBS implementation work has begun before this gate passes.

PIC-1 result (2026-09-11): **automated gate passed**. The ordinary Inbox remains
the primary viewport while a compact `JS8 retrievals · N pending · Review`
disclosure reports the auxiliary queue. A closed review workbench performs only
the bounded count and materializes no row widgets. Opening Review loads at most
100 newest non-retrieved rows from one read snapshot and provides Newer/Older
paging with endpoint-scoped actions. Theme changes and resize do not query or
rebuild the queue.

`Get` and `Mark Retrieved` acknowledge immediately and run serialized endpoint,
JS8 inbox, and backlog writes on a daemon action lane. Completion returns through
a GUI-thread signal bridge with a generation fence; closing the tab cannot leave
a Qt worker thread behind.

The available September 11 production database copy contains 6,197 CommStat,
6,175 SitRep, 2,059 Spotter, 555 JS8, 477 BBS, 230 FLMsg, 187 VarAC, and 149
FLAMP projected rows. This confirms that the reported screen was not backed by
a JS8-only catalog. On that 326 MB database and the active local multi-rig
runtime, 100 count-plus-page read samples measured below 4 ms maximum and below
1.4 ms p95; no index or migration was justified.

The focused Inbox/message gate passes 226 tests. It covers mixed-source
coexistence with 26 pending retrievals, count-only closed state, on-demand
materialization, the 100-row SQL/page bound, source-scoped status/delete,
Focus-All/source filtering, 1280x720/1000x700/900x560, Light/Dark, Normal/Large,
the 200-row primary Inbox model, and invalidation coalescing. Python compilation
and `git diff --check` pass. An offscreen 1280x720 render was reviewed and keeps
the disclosure compact above a usable ordinary message list. No schema,
production data, BBS code, or external endpoint was changed.

## PIC-2 — P3 BBS Presentation Correction

PIC-2 begins only after PIC-1 passes. It changes presentation, not ownership,
retention, publication, access, or live-directory semantics.

### 1. Responsive state uses the real content viewport

Layout selection must consider the BBS tab viewport width and height after the
shell is laid out, plus current font metrics. Application-window width alone is
not a valid proxy.

- Comfortable: sufficient width and height to show selection and detail side by
  side without clipping.
- Compact-height or compact-width: selection becomes a concise strip/control
  above one full-width detail surface.
- Large Text may select compact mode earlier.

The exact breakpoint is implementation-owned and font-derived. The production
1280x720 screenshot is a required fixture and must select a layout that uses its
short content height well. Resizing only changes geometry; it must not scan
folders, refresh publication, reconcile retention, or query the catalog.

### 2. Radio Service

Replace the cramped fixed four-column table with a bounded serving-radio
selector. Each item exposes only operator-relevant selection state:

- radio short name;
- `Serving` or `Not serving`;
- `Published` or `Paused`; and
- concise health text/icon with accessible explanation.

Paths do not belong in the selector. With a small number of radios, use
content-sized chips/cards; with many radios, use one bounded scrollable selector
without constructing a row widget for every record. Selection must be clear by
shape/text as well as color.

The selected service editor is top-aligned and groups:

- Enable VarAC BBS;
- Publish the FIO catalog;
- Announce BBS;
- live BBS folder with Browse; and
- a concise health/validation result.

`Save Radio Service` is the sole primary action. `Open Radio Settings` is an
adjacent secondary action. Native install/inbox/outbox paths remain managed in
Radio Settings and appear only behind a `Managed in Radio Settings` disclosure
or tooltip, with safe elision and full-path copy/tooltip access.

### 3. Locations & Access

Show one catalog summary, not repeated global and local summaries. The location
navigator shows hierarchy, name, and enabled/disabled state only. Access and
retention belong in a readable selected-policy summary rather than being packed
into the navigator label.

At compact sizes the locations become a horizontally scrollable chip strip or
equivalent compact selector. At comfortable sizes a bounded hierarchy list may
remain. In both forms, the selected location and parent relationship remain
clear.

The default detail state is read-only and includes compact policy facts:

- Access;
- Retention;
- Source folder state; and
- Enabled/disabled status.

`Edit` or `Add location` deliberately opens the editor. The editor is not
permanently expanded. It is top-aligned, internally scrollable when necessary,
and provides `Save Location`, `Disable` where valid, and `Cancel`. Long paths
elide without resizing the page and retain tooltip/copy access.

### 4. Shared BBS presentation rules

- Add Help beside Refresh and route it to the BBS workflow help.
- Keep the five guided tabs and current left-to-right sequence.
- Use shared theme and sizing helpers; do not introduce local light-only colors
  or fixed text-control heights.
- Keep primary actions stable while status text changes.
- Prefer short status chips and icons with accessible names over repeated prose.
- Preserve keyboard order, focus indication, and screen-reader names.
- Table/list data remains bounded and internally scrollable.
- No page-level horizontal scrollbar is permitted for the Radio Service or
  Locations & Access control surface. Intentional artifact tables may retain
  their own internal horizontal overflow safety net.

### PIC-2 acceptance gate

PIC-2 passes only when:

- Radio Service and Locations & Access are reviewed at 1280x720, 1000x700, and
  900x560 in Light/Dark and Normal/Large Text;
- radio selection, service state, Save, Settings, location selection, policy,
  Edit/Add, Save/Disable/Cancel, Help, and Refresh remain visible or reachable by
  obvious local scrolling;
- long radio names, location names, and paths do not create page-level
  horizontal overflow or clipped controls;
- Radio Service has one clear primary save action and no clipped table headers;
- Locations has one global summary and a collapsed-by-default editor;
- selected, disabled, warning, and healthy states remain readable in both themes
  without relying on color alone;
- visitor preview, publishing, helper, retention, access, and multi-radio
  projection behavior remain unchanged;
- resize/theme/text-size changes cause no catalog query, filesystem scan,
  publish, or reconciliation action; and
- all focused BBS tests plus the affected navigation/theme/layout suite pass.

PIC-2 result (2026-09-11): **automated gate passed**. Radio Service now uses a
single bounded selector whose text carries radio name, serving state,
publication state, and health without exposing paths. The selected editor is
full-width, scroll-safe, and keeps one primary Save action beside the Radio
Settings recovery route. Native VarAC paths are hidden behind a disclosure.

Locations & Access now selects compactly at short production heights, shows one
read-only policy summary by default, and reveals the internally scrollable
Add/Edit surface only on request. The navigator contains identity and enabled
state rather than packed access/retention prose. Save, Disable, Cancel, Help,
Refresh, full-path tooltip/copy, and non-destructive BBS behaviors remain
available. The native splitter grip that appeared as stray dotted content in
compact mode was removed from presentation while retaining pane resizing.

The required 1280x720, 1000x700, and 900x560 matrix passes in Light/Dark and
Normal/Large Text. Resize, theme, and font changes are geometry-only. The
focused BBS gate passes 58 tests; the combined Inbox, Message Intelligence,
MIP-4, BBS, contextual-help, and font-rendering gate passes 397 tests with one
platform-dependent skip. Python compilation and `git diff --check` pass. No
schema, retention, access, publication, source-file, or live-directory semantic
changed.

## PIC-3 — Linux Production Qualification

After PIC-1 and PIC-2 pass independently, qualify their integration on the
actual Linux production profile:

1. capture the active configuration root and build commit;
2. open Inbox with mixed projected sources and a nonzero JS8 retrieval backlog;
3. exercise Review, Get, Mark Retrieved, filters, select-all, and return to Inbox;
4. exercise every BBS tab at the normal production window and one reduced size;
5. switch Light/Dark and Normal/Large Text;
6. observe ten minutes of settled idle plus repeated tab switching; and
7. capture performance/hang diagnostics if a budget is breached.

Qualification fails if a source-specific auxiliary view can obscure the primary
Inbox, if a BBS primary action becomes unreachable, if the event loop appears
hung, or if the correction triggers unexpected ingest, publication, or source
reconciliation work.

## Operational View Framework Mandatory Design Gates

### Source meaning

The normal Inbox is the normalized operator-message projection. Pending JS8
retrievals are remote message references awaiting operator retrieval and are
not Inbox messages until retrieved and ingested. BBS Radio Service and Locations
are administration views, not operational message sources.

### Volume and retention

The ordinary Inbox retains its existing bounded 200-row page/query behavior.
The retrieval review renders at most 100 rows and exposes a separate total.
Backlog and BBS retention semantics do not change. No screen may materialize an
unbounded source or catalog history.

### Provenance and trust

Inbox rows retain source family, endpoint/radio, sender, destination, source
identity, and trust/intelligence provenance. Retrieval actions retain their
JS8 endpoint identity. BBS access and publication state continue to come from
the station catalog. Presentation must not merge these identities.

### Constrained customization

Existing Inbox filters and BBS configuration remain the allowed customization.
This slice adds no user-authored query language, arbitrary SQL, stylesheet, or
layout scripting. Responsive state is deterministic from viewport and font
metrics.

### Map scaling

Not applicable. These changes do not add map layers or alter observation/route
projection. Inbox actions that already route to Map retain current bounded
behavior.

### Action validity

`Get` and `Mark Retrieved` apply only to the selected pending reference and its
receiving endpoint. Inbox row actions apply only to the selected projected
message. BBS Save/Disable/publication actions retain selected radio, location,
and artifact scope. Disabled or stale actions explain why and never silently
retarget another source, radio, location, or file.

## Concurrency, Migration, And Recovery

- UI presentation consumes immutable/bounded snapshots; workers own database,
  endpoint, and filesystem work.
- Generation fences reject stale results after filter, selection, tab, or
  profile changes.
- Hidden BBS pages and a closed retrieval workbench do not poll.
- Resize, paint, theme, and text-size handlers are geometry-only.
- PIC-1 and PIC-2 require no migration. The optional backlog index described in
  PIC-1 is additive and may proceed only after benchmark evidence and primary
  migration review.
- No source message, artifact, location, publication mapping, or live BBS file
  may be deleted as part of correction or test setup.
- Tests use temporary profiles or read-only/disposable database copies and stub
  external applications, endpoint I/O, publication, and dialogs.

## Work Packages And Model Ownership

### PIC-0 — Review and correction specification (complete)

- High-reasoning primary model: product hierarchy, source/data distinction,
  concurrency and migration boundaries, slice ordering, specification updates,
  and final integration review.
- Terra: focused Inbox implementation/query/layout audit.
- Terra: focused BBS responsive-layout audit.
- Luna: existing-test inventory, missing acceptance matrix, and performance gate
  review.

All delegated work was read-only; there were no delegated diffs to merge. The
primary model reviewed the findings against the governing product, layout,
message, BBS, and performance contracts.

### PIC-1 — P1 implementation (complete)

- High-reasoning primary model: active-profile evidence, query/concurrency
  design, any additive-index decision, delegated-diff review, and gate closure.
- Terra: bounded retrieval disclosure/workbench and responsive Inbox wiring.
- Luna: source-coexistence, geometry, action-scope, performance, and regression
  tests.

### PIC-2 — P3 implementation (complete)

- High-reasoning primary model: responsive-state architecture, BBS semantic
  preservation, delegated-diff review, and gate closure.
- Terra: Radio Service and Locations & Access presentation correction.
- Luna: theme/text/geometry matrix and BBS behavior regressions.

### PIC-3 — production qualification (ready; operator-assisted)

The high-reasoning primary model owns evidence review and final integration.
Linux physical interaction is operator-assisted. A lower-cost model may organize
captured metrics, but it may not waive a failed source, responsiveness, action-
scope, or data-integrity gate.
