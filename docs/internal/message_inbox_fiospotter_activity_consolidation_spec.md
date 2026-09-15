# Message Inbox And FIOSpotter Activity Consolidation Specification

Status: implementation complete; automated gate passed; native-platform visual qualification remains operator-assisted
Date: 2026-09-15

## Purpose

Message Inbox is FIO's primary operational traffic workspace. FIOSpotter owns
configuration for shared Watches, Expect automation, access policies, forms,
and imports. FIOSpotter does not own a second inbox-style traffic browser.

This specification consolidates FIOSpotter Activity into Message Inbox while
preserving the strong Activity interaction model: a bounded traffic table,
message-intelligence summary, clear decoded meaning, and selection-aware Map,
Operator, Reply, and Add to Watch actions.

## Governing Contracts

This specification is additive to and must be implemented under:

- `project_delivery_rules.md`
- `multirig_product_ui_contract.md`
- `task_oriented_workspace_design_guideline.md`
- `message_ingest_projection_performance_spec.md`
- `message_intelligence_projection_spec.md`
- `message_inbox_controls_spec.md`
- `message_inbox_reader_experience_spec.md`
- `fio_spotter_operator_workflow_refinement_spec.md`
- `ui_layout_standards.md`

Where an older FIOSpotter Activity requirement conflicts with this document,
this consolidation specification controls the placement of operational traffic.
It does not weaken source, deletion, trust, Expect, or persistence contracts.

## Redesign Brief

### Operator task

The operator needs to notice important traffic, understand what happened and
why it matters, inspect the source evidence, and take the next valid action
without switching between two inbox-like workspaces.

### What and Why hierarchy

- **What:** one bounded, multi-source Inbox list with clear Source, Kind,
  sender, destination, age, status, and summary semantics.
- **Why:** message intelligence, decoded meaning, trust/provenance, and action
  eligibility shown from the already-projected row snapshot.
- **Where/When:** remain in the Station Control Bar except for message-specific
  destination, group, radio, frequency, and event-time evidence required to
  understand or act on the selected message.

### Workspace ownership

- Message Inbox owns operational traffic review and message actions.
- FIOSpotter owns Watches, Expect, Access Policies, Forms, and Imports.
- The former FIOSpotter Activity tab is removed. Inbox Spotter focus is the one
  operational traffic route; FIOSpotter opens directly on Watches.

## Canonical Vocabulary

The UI must not mix source identity with content kind.

- **Source** is the originating application/transport family aligned with Inbox
  Focus choices: JS8Call, Spotter, CommStat, FLMsg/FLAmp, VarAC, BBS, or Mesh.
- **Kind** is the semantic content: stored message, directed traffic, MCF form,
  SitRep, status receipt, query, bulletin, file, or another normalized subtype.
- **Status** is workflow state: new, read, alert, acknowledged, delivered,
  needs attention, or the applicable operational status.

The default Inbox column previously labeled `Type` becomes `Source`. Kind stays
separately visible, either as its own focus-profile column or as the leading
label in `Kind / Message`. Merely relabeling mixed data is non-conforming.

JS8 `RRSR CALLSIGN,SRID` traffic is a CommStat status-report acknowledgment.
It is presented as `Status receipt`, not as a stored `MSG`. The operator-facing
summary identifies the station and report ID being acknowledged. Raw command
text remains available as evidence.

## Inbox Presentation

### Default All view

The normal wide presentation is:

`Select | Source | Status | From | To / Group | Age | Kind / Message | Actions`

The narrative column receives useful surplus width. Identity and categorical
columns do not become large empty regions on a maximized window.

Focused profiles may substitute domain-specific columns when this materially
improves scanning, but Source, Kind, and Status meanings remain canonical.

### Message intelligence

The existing bounded Message Intelligence summary remains above the table.
Its filters operate only on the current immutable projected page and never
initiate source, filesystem, network, or database work.

### Reader

The reader leads with human meaning:

- `From -> To / Group`
- Source, Kind, Status, and relative Age
- decoded summary or message body
- applicable routing, location, trust, and operational context

Internal projection IDs, hashes, source paths, delete capabilities, and raw
transport references belong in a secondary `Technical provenance` section.
They must not be the title or the first message body presented to an operator.

### Selection-aware actions

The reader provides Map, Operator, Reply, and Add to Watch in stable positions.

- Hide an action when the source family can never support it.
- Disable it with a concise reason when the source supports the action but the
  selected message lacks a prerequisite.
- Map requires cached location evidence.
- Operator requires a usable callsign.
- Reply requires a supported reply route and destination.
- Add to Watch requires at least one canonical matchable field.
- Action-state computation is cache-only.

Source-management actions such as View, Delete, Archive, BBS, or Relay retain
their existing safety and confirmation contracts. Consolidation does not
silently remove or broaden them.

## Adaptive Column Sizing Contract

Column sizing must optimize for rapid scanning rather than equal distribution.

1. Measure headers and the current bounded model snapshot only. Never query a
   source, open a file, parse a message, or load reader detail to size columns.
2. Categorical columns (`Source`, `Kind`, `Status`, `Age`) fit the widest useful
   visible value plus font-derived padding, subject to readable minimums and
   bounded maximums.
3. Callsign and destination columns fit typical visible content and are capped
   so one malformed or unusually long value cannot starve the message summary.
4. Selection and action columns use font-derived fixed widths.
5. Narrative columns (`Message`, `Summary`, or `Kind / Message`) receive the
   remaining width and are the only columns normally allowed to stretch.
6. A profile with no narrative column must leave surplus viewport space empty
   or distribute a bounded amount to a meaningful field. It must never stretch
   a constant-value column such as CommStat `Kind` across the screen.
7. At constrained widths, preserve readable identity fields and allow the
   narrative column to elide. Horizontal scrolling is a last resort, not the
   default response to a maximized or normal desktop window.
8. Refit is coalesced after a model/profile/font/viewport change and skipped
   when the profile, viewport bucket, font metrics, and bounded content
   signature are unchanged. Resizing must not create a geometry feedback loop.
9. Operator-adjusted interactive widths may be preserved for the current
   session until profile, font, or major viewport context changes.
10. Normal and Large text plus light and dark themes must retain readable
    headers, cells, actions, and elision tooltips.

## Shared Watches

Watches are station-owned and source-neutral. Matching runs against canonical
projected fields and may include source, kind, callsign, group, topic, status,
location, keyword, and radio constraints.

- A blank source constraint means all supported Inbox sources.
- Existing nonblank source constraints retain their exact semantics.
- Saving or editing a watch never scans historical data on the GUI thread.
- The projection coordinator loads a bounded compiled watch snapshot and
  matches committed messages off the UI thread.
- `(watch_id, message_id)` remains the idempotency key; re-projection must not
  increment a watch twice.
- Watch administration explicitly says that rules apply to Message Inbox
  traffic across supported sources.
- Add to Watch stages an unsaved draft. No watch is persisted until Save.

## Performance And Reliability Requirements

Messages is expected to be FIO's heaviest-used workspace. Therefore:

- Inbox reads one bounded projection; FIOSpotter must not perform a parallel
  Activity query for the same operator task.
- The selected Inbox page, final column resize modes, and bounded initial
  widths must be installed before the table is first painted. First activation
  must not visibly replace a placeholder page or publish a second geometry
  profile that makes the workspace sweep, swipe, or vanish.
- Render, paint, selection, action-state, sorting, filter changes, resize,
  theme changes, and age repaint are cache-only.
- Semantic classification, including RRSR recognition, occurs during bounded
  background projection or from already-cached row text. It is never a reader
  filesystem/database lookup.
- Raw and richer decoded copies of one RF event use stable correlation rules so
  the unified Inbox does not introduce duplicates.
- Projection and filter results are generation-fenced; stale results cannot
  clear or overwrite a newer view.
- Existing 200-row UI bounds and background transaction bounds remain in force.
- No `QApplication.processEvents`, blocking queued connection, synchronous
  subprocess, unbounded wait, or render-time source access may be introduced.
- Reader navigation remains snapshot-based and must not refresh the Inbox.
- Column fitting is O(rows * visible columns) over the bounded in-memory model,
  is coalesced, and emits no model reset or source refresh.

## Compatibility And Migration

1. Implement canonical Source/Kind presentation, status-receipt classification,
   human-first reader content, and adaptive sizing in the existing Inbox model.
2. Add selection-aware Inbox reader actions using existing navigation and
   Compose contracts.
3. Confirm universal watch matching through the existing projection coordinator
   and update administration wording/source choices without changing stored
   watch identities or nonblank scopes.
4. Remove FIOSpotter Activity after Inbox action/display parity exists and make
   Watches the first/default FIOSpotter page.
5. Normalize the legacy `sitrep` projection/storage source to operator-facing
   `Spotter`. Keep `sitrep` only as an internal query family so historical data
   remains available; semantic CommStat evidence must win and prevent one row
   from appearing under both sources.

No destructive schema migration is authorized or required by this slice.

## Acceptance Criteria

- Inbox All shows Source values aligned with Focus families and no longer labels
  `MSG`, form names, and source families as the same concept.
- RRSR rows display as CommStat `Status receipt` with a human summary.
- Age is relative in all Inbox/Spotter-derived list views; exact time is
  available in the tooltip or reader.
- CommStat Kind fits `CommStat`/its actual values and does not consume the
  maximized-window surplus width.
- Narrative content receives surplus width; fixed/categorical columns remain
  content-derived under Normal and Large text.
- Map, Operator, Reply, and Add to Watch correctly reflect cached row
  capabilities and never block the GUI.
- Add to Watch opens an unsaved universal watch draft with useful canonical
  criteria and the selected source prefilled; nothing is saved automatically.
- Existing source-scoped watches retain their scope; blank-scope watches match
  all committed supported projection families idempotently.
- FIOSpotter has no Activity tab or duplicate activity-store read and opens on
  Watches.
- Inbox All and its Source filter show `Spotter`, never the internal `sitrep`
  family; projected CommStat rows show only `CommStat`.
- The initial visible Inbox table frame already owns its final resize modes and
  content-fit widths, without a visible post-publication geometry rewrite.
- Reader primary content does not expose projection hashes/paths as its title.
- Focus, search, intelligence filters, reader navigation, resize, font changes,
  and theme changes perform no source/file/database work.
- Existing Inbox delete/archive/read/flag/BBS/relay and Compose tests pass.
- Focused projection, watch, source presentation, reader, responsive layout,
  and performance guardrail tests pass before the implementation gate closes.

## Implementation Record

### Work packages and ownership

- **Architecture, vocabulary, and migration safety:** primary Codex GPT-5,
  high reasoning. Defined Source/Kind/Status semantics, the non-destructive
  compatibility handoff, and the cache-only action/read-model boundary.
- **Inbox reader, adaptive table geometry, and navigation:** primary Codex
  GPT-5, high reasoning. Implemented source-neutral reader actions and bounded,
  font-measured column fitting through the existing shared Qt theme and table
  components.
- **Projection semantics and universal Watches:** primary Codex GPT-5, high
  reasoning. Added RRSR recognition, normalized projected match fields, and
  source-neutral watch staging without a schema migration.
- **Focused tests, integration review, specifications, and exit gate:** primary
  Codex GPT-5, high reasoning. The primary model reviewed the complete diff and
  corrected stale fixed-width expectations in the legacy shell regression.

The 2026-09-15 follow-up removed the obsolete Activity tab, made Watches the
stable prebuilt/default FIOSpotter page, changed the public source label from
legacy `SitRep`/`FIOSpotter` wording to `Spotter`, retained bounded historical
`sitrep` projection reads behind semantic filtering, and made initial Inbox
header/width publication atomic before first paint. The Watch editor retains a
font-derived width floor without allowing long placeholder copy to consume the
table's dominant wide-screen allocation.

The active execution contract prohibited spawning new subagents for this turn,
so no delegated diff was produced or integrated. Existing unrelated work and
untracked document-rendering artifacts were left untouched.

### Delivered behavior

- Message Inbox All now separates `Source` from `Kind / Message`; Age is
  relative and exact receipt time remains available as detail/tooltip evidence.
- `RRSR CALLSIGN,REPORT-ID` is classified during projection as CommStat
  `Status receipt` and receives a human-readable acknowledgment summary.
- Reader content leads with the route and decoded human meaning. Projection
  identifiers and source references are retained under `Technical provenance`.
- Map, Operator, Reply, and Add to Watch are stable reader actions whose state
  is calculated from the retained row snapshot. Add to Watch stages an unsaved
  FIOSpotter rule; it never persists implicitly.
- FIOSpotter has no Activity tab or duplicate traffic catalog. It opens on
  Watches; Expect, policies, Forms, and Imports remain owned by FIOSpotter.
- Watch matching now accepts canonical Source and Kind criteria across supported
  projected messages while preserving blank-source meaning (`all`) and the
  existing idempotent match-record path.
- Table widths are measured from active font metrics and the bounded current
  model. Categorical columns are capped, narrative columns receive genuine
  surplus, and CommStat Kind remains Interactive/content-fit at full screen.
  Fit publication is single-shot/coalesced and does not run source work.

### Automated acceptance evidence

- `441 passed` for the integrated Message Inbox, FIOSpotter, CommStat,
  projection, watch, reader, intelligence, responsive-shell, and legacy UI
  contract partition.
- `49 passed` for the focused GUI-soak, production-hotpath, asynchronous UI,
  projection-store, projector, and read-model performance partition.
- `94 passed` for the process-isolated Compose acceptance, GUI soak,
  production-hotpath, projection-store/projector, MIP4/MIP5, and qualification
  partition.
- The updated legacy responsive-layout regression passes independently after
  replacing obsolete fixed/minimum-width assumptions with the content-aware
  contract.
- Changed Python modules pass `compileall`; `git diff --check` passes.
- A diff-only blocking-pattern review found no new database/filesystem/network
  access, blocking queued connection, `processEvents`, synchronous subprocess,
  unbounded thread wait, or future wait in the new render/resize/action paths.

A monolithic repository run was also attempted. It progressed into the suite
but the process aborted in a live JS8/native Qt worker during an otherwise
passing Compose test. The same Compose/performance partition passes when run in
the project's established process-isolated form, so this is recorded as test
harness/native lifecycle instability rather than waived product evidence.

### Exit gate

The implementation and automated regression gate is **passed**. No application
restart, production database/configuration write, endpoint command, commit, or
push was performed. Native Linux and macOS visual checks at 1920x1080, compact
sizes, Light/Dark, and Normal/Large Text remain an operator-assisted release
qualification because this turn did not attach to the operator's active FIO
runtime. The automated widget tests cover the corresponding sizing, action,
theme-refresh, and cache-only contracts.
