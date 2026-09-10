# Local Nets And Tools & Resources Implementation Plan

Status: implementation complete through LN-6; focused automated qualification
passed; the LN-6 exit gate awaits the full soak and Linux/operator validation

Date: 2026-09-09

Governing specification:
`docs/internal/local_nets_tools_resources_spec.md`

LN-0 architecture decisions:
`docs/internal/local_nets_resources_ln0_architecture_decisions.md`

## Delivery Objective

Deliver a station-owned Resources catalog and non-commandable Local Nets
calendar without destabilizing the existing HF scheduler, Plan Builder, SOP,
Operating Group, Ops Center, or navigation work.

The implementation must progress through gated packages. A later package does
not begin until its predecessor's exit gate passes. Hardware, regulatory-data,
or product decisions that cannot be safely inferred stop only the affected
package; they do not authorize a workaround that weakens the governing contract.

| Package | Status |
| --- | --- |
| LN-0 Audit, fixtures, and contract lock | Passed 2026-09-09 |
| LN-1 Canonical resource store and migration | Passed 2026-09-09 |
| LN-2 Resources navigation and catalog | Passed 2026-09-09 |
| LN-3 HF directory subscription | Passed 2026-09-09 |
| LN-4 Local Nets core and workspace | Passed 2026-09-09 |
| LN-5 Ops Center and SOP integration | Passed 2026-09-09 |
| LN-6 Release qualification | Implementation/focused checks complete; full soak and Linux/operator gate pending |

## Existing Implementation Baseline

The current system already provides useful foundations, but their ownership is
not yet suitable for the new feature:

- `freqinout/gui/net_schedule_tab.py` owns the HF Net editor and directly
  manages much of the current resource library.
- `net_resources` stores one row per reusable session and combines net identity,
  recurrence, frequency, mode, source, and descriptive information.
- `net_schedule_tab.resource_id` can connect an HF schedule row to that legacy
  resource row.
- `freqinout/gui/daily_schedule_tab.py` consumes resource identifiers in Daily
  Schedule editing, while `freqinout/gui/freq_planner_tab.py` reads and may
  update linked `net_resources` rows from planning workflows.
- `freqinout/core/known_operating_groups.py` derives known group suggestions
  from `net_resources`; this reader must move to a canonical compatibility API,
  not a second resource store.
- `freqinout/core/db_initializer.py` and `freqinout/gui/net_schedule_tab.py`
  currently both contain schema-assurance seams for the legacy tables. LN-0
  must resolve startup ownership before LN-1 adds schema.
- `freqinout/core/schedule_source_sets.py` owns named HF Net schedule sources and
  dependent plan reprojection.
- `freqinout/core/operational_projection.py` and
  `freqinout/core/schedule_projection.py` already preserve resource IDs and
  source provenance in HF projections.
- `freqinout/core/scheduler_engine.py` reads HF Net schedules and must remain
  unaware of Local Nets.
- `freqinout/gui/controlfreq_tab.py` owns Ops Center Schedule Outlook rendering.
- `freqinout/gui/sop_tab.py` and `freqinout/core/sop_manager.py` own SOP profiles,
  actions, conflicts, and preview.
- Settings currently stores the configured Operating Group list; the new APIs
  must treat that identity as transport-neutral even while some UI labels remain
  HF-specific.
- Settings `local_net_profiles` already supplies label-keyed local
  group/resource metadata to SOP and message-actionability consumers. It is a
  compatibility source, not a recurrence or Local Nets calendar model.
- `freqinout/gui/main_window.py` owns lazy screen factories and the full/compact
  navigation hierarchy.

The first coding action is an exact call-site and schema audit. File names below
are expected seams, not permission to bypass discoveries made by that audit.

## Architecture Rules For Every Package

- Core stores, recurrence, migration, projection, and validation remain Qt-free.
- UI reads immutable/bounded models and does not perform schema creation,
  recurrence expansion, broad scanning, or import parsing during paint.
- One canonical repository owns writes after resource cutover.
- Local Net rows never share the HF scheduler tables and are never loaded by
  `SchedulerEngine`.
- Stable IDs and accepted snapshots cross screens; widget text does not become a
  relational key.
- Existing production data is preserved through additive, backup-first,
  transactional, idempotent migration.
- New primary screens are lazy-loaded and register shutdown/cancellation for any
  worker they own.
- No new screen may synchronously enumerate processes, scan files, access the
  network, or expand an unbounded recurrence set.
- Specs, help, release checks, performance evidence, and the UI work log are
  updated in the package that changes their behavior.

## Package LN-0 — Audit, Fixtures, And Contract Lock

### Goal

Turn the conceptual storage and interaction contract into an exact repository
map before a schema or UI change.

### Work

1. Enumerate every read/write call site for `net_resources`,
   `net_schedule_tab.resource_id`, bundled resource JSON, and Settings resource
   keys.
2. Capture anonymized structural fixtures for:
   - empty installation;
   - current bundled resources;
   - station-created resource rows;
   - imported read-only sets;
   - duplicate and malformed records;
   - HF schedules linked and unlinked to resources; and
   - multiple named HF Net source schedules.
3. Inventory Operating Group APIs and determine the stable group reference
   strategy. Do not use a mutable display name as the only future key.
4. Inventory SOP reference fields and identify the smallest additive Local Net
   context reference.
5. Document the current Schedule Outlook query/update path and scheduler input
   boundary.
6. Produce wireframes or executable geometry fixtures for:
   - Resources / Frequency Catalog tab;
   - Resources / Net Directory tab;
   - Add to HF Nets;
   - Local Nets default and editor states; and
   - Ops Center Local Nets summary.
7. Define the initial bundled reference manifest, citations, version, and
   verification process. No network updater is included.
8. Record baseline timings for current HF Nets construction, resource filtering,
   Plan reprojection, Ops Center schedule refresh, startup, idle, and shutdown.

### Decision checkpoint

Architecture review must confirm:

- canonical table and stable-key names;
- group identity strategy;
- legacy projection/cutover ownership;
- how directory session changes are versioned and compared;
- whether the initial catalog includes only national reference records or any
  bundled local examples; and
- exact SOP reference payload.

### Exit gate

- Every current reader/writer is mapped.
- Migration fixtures reproduce their expected row counts and relationships.
- Scheduler isolation has an executable characterization test.
- Wireframes satisfy the product UI contract at 1920x1080, 1000x700, and
  900x560 with Normal/Large Text.
- No production/configuration data is changed.

## Package LN-1 — Canonical Resource Store And Migration

### Goal

Create the Qt-free source, frequency, net identity, session, version, usage, and
migration foundations without changing the visible UI.

### Expected implementation seams

- `freqinout/core/resource_catalog_store.py` — bounded CRUD/query and usage.
- `freqinout/core/resource_catalog_models.py` — immutable validated models.
- `freqinout/core/resource_catalog_migration.py` — dry-run, backup, migration,
  mapping, and recovery evidence.
- `freqinout/core/resource_reference_validation.py` — advisory catalog checks.
- `freqinout/core/db_initializer.py` — startup-owned schema assurance only.
- focused tests under `tests/test_resource_catalog_*.py`.

Names may change after LN-0, but responsibilities may not be folded into a Qt
tab or the scheduler.

### Work

1. Add source, frequency, directory-entry, session, group-relation, and
   legacy-ID mapping tables.
2. Add/preserve the shared transport-neutral `operating_group_key` on configured
   group rows through the Qt-free identity adapter.
3. Store frequency values as integer Hz and keep display formatting in adapters.
4. Add stable source/resource/session keys and content versions/hashes.
5. Implement bounded search/filter/query and computed usage APIs.
6. Implement create/update/retire/replace behavior with read-only source guards.
7. Implement accepted-version comparison and field-level diff models.
8. Build a dry-run classifier/importer for existing `net_resources` rows.
   General digital/frequency standards become Frequency Catalog records only;
   credible net/session rows may additionally become directory entries and
   sessions; ambiguous rows remain review-required.
9. Classify existing `local_net_profiles` separately: parseable targets may seed
   reviewed station resources; every row remains available to compatibility
   readers and no recurrence is invented.
10. Back up the affected database/configuration before cutover.
11. Migrate in one transaction and checkpoint the schema/ownership version as
    `shadow_ready`; legacy writers remain authoritative until LN-2.
12. Preserve legacy IDs and every source field through a mapping/audit record.
13. Make repeated migration idempotent and verify rollback after an injected
    mid-migration failure.
14. Provide a compatibility reader/projection for current HF code while visible
    ownership remains unchanged.

### Tests

- empty, current, duplicate, malformed, and mixed-source fixtures;
- exact preservation counts and field mappings;
- ambiguous service retained as review-required;
- Operating Group keys are shared across same-name configurations and survive a
  rename;
- `local_net_profiles` remains non-scheduled and lossless through classification;
- built-in record cannot be overwritten;
- station override and replacement behavior;
- retirement with and without usage;
- version diff and accepted snapshot;
- backup failure, transaction failure, retry, and idempotence;
- bounded 10,000-frequency/2,000-net/5,000-session query corpus;
- no schema write from query-only APIs.

### Exit gate

- Migration dry-run and applied reports reconcile exactly.
- Backup-first failure paths leave the legacy database unchanged.
- Canonical and compatibility queries reproduce current HF resource behavior.
- Warm filtered catalog query is below 100 ms p95 on the Linux baseline.
- Existing HF schedule, Plan, SOP, and scheduler suites pass unchanged.
- No Resources or Local Nets navigation is exposed yet.

Risk: **high** because this package changes data ownership.

## Package LN-2 — Resources Navigation And Catalog Workspaces

### Goal

Expose Frequency Catalog and Net Directory as a first-class, performant Resources
workspace, then move existing FIO resource writes to the canonical repository.

### Expected implementation seams

- `freqinout/gui/resources_tab.py` — lazy Tools & Resources host.
- `freqinout/gui/frequency_catalog_view.py` — bounded catalog browser/editor.
- `freqinout/gui/net_directory_view.py` — net/session browser/editor.
- `freqinout/gui/resource_picker.py` — reusable contextual picker.
- `freqinout/gui/main_window.py` — full and compact Resources master navigation.
- `freqinout/gui/net_schedule_tab.py` — replace direct resource writes with the
  canonical service and deep links.
- `freqinout/gui/help_registry.py` and operator help.

### Work

1. Add one direct Resources destination with implemented browser tabs inside it.
2. Use lazy factories and preserve full/compact direct-navigation behavior.
3. Implement shared search, friendly catalog-source/listing filters, paged
   results, detail/editor, advisory reference state, and `Used by` impact.
4. Implement station resource creation, clone-from-reference, update, retire,
   and safe unreferenced deletion.
5. Implement Net Directory identities and multi-session editing.
6. Implement import/export preview and diagnostics.
7. Route every existing FIO resource mutation through the canonical repository.
8. Disable independent legacy resource writers after parity is proven. If a
   legacy materialized projection remains, update it only through the canonical
   transaction owner.
9. Re-run the legacy delta import, switch catalog authority from `shadow_ready`
   to `canonical`, and expose navigation only in the successful cutover.
10. Provide the compatibility resource-choice adapter for existing SOP
    `local_net_profiles` consumers.
11. Keep current HF schedule selection/runtime unchanged in this package.

### Tests

- full and compact direct-navigation mapping;
- no empty Forms/Templates tab;
- query-only opening does not migrate or write;
- filters, search, paging, keyboard access, and selection stability;
- create/edit/clone/retire/delete rules;
- `Used by` across HF schedule and directory relationships;
- import cancel/nonmutation and conflict handling;
- reference source/version/verification display;
- Light/Dark, Normal/Large Text at required viewport matrix;
- tab acknowledgement and construction timing;
- legacy writer prohibition/static ownership check.

### Exit gate

- Resources is understandable and usable without opening HF Nets.
- Existing HF resource CRUD produces the same canonical state as Resources.
- No duplicate writer remains.
- Required UI/performance/platform tests pass.

Risk: **medium-high** because the visible cutover follows LN-1 ownership work.

## Package LN-3 — HF Nets Directory Subscription Workflow

### Goal

Replace row-library copying with the agreed resource/session subscription model
while preserving HF scheduler, Plan, RF Guard, and SOP-conflict behavior.

### Work

1. Add `Add HF Net` to the source-first HF Nets workflow.
2. Embed the shared resource picker with My Groups, Known Nets, and Custom
   filters.
3. Allow selection of one or more published HF sessions.
4. Require a named HF Net source schedule destination.
5. Review recurrence, time, target/radio, early check-in, mode, frequency, and
   conflict policy before save.
6. Persist canonical session ID, accepted version, and local snapshot/overrides
   on the HF schedule row.
7. Starting in Net Directory, route `Add to HF Nets` to the identical review.
8. Show `Scheduled`/`Open Schedule` for an existing subscription.
9. Add `Create New Net` and station-private one-time resource behavior.
10. Show directory updates as reviewable diffs; apply only through the normal HF
    save, reprojection, RF Guard, conflict, and scheduler refresh pipeline.
11. Replace or retire the large embedded Net Row Library only after feature
    parity. A compatibility view may remain temporarily under explicit
    deprecation copy, but two primary resource editors are prohibited.

### Tests

- both entry directions produce the same draft and saved relation;
- multiple sessions can be added without duplicate schedule rows;
- named source schedule selection and usage are preserved;
- plan reprojection and assigned-radio refresh remain correct;
- Net/SOP priority and RF Guard still block/warn appropriately;
- directory update does not silently change an active schedule;
- keep/apply/local-override behavior;
- retired source preserves an actionable schedule warning;
- full HF scheduler regression and restart persistence;
- responsive workflow and unsaved-return-context preservation.

### Exit gate

- A user can understand directory versus active HF schedule without help text.
- HF behavior is functionally unchanged after subscription save.
- Existing schedules remain valid even when their resource is retired or newer.
- No scheduler performance regression exceeds the recorded LN-0 baseline budget.

Risk: **high** because HF schedules are commandable.

## Package LN-4 — Local Nets Core And Workspace

### Goal

Deliver Local Net storage, deterministic bounded recurrence, occurrence state,
and a responsive Plans > Local Nets workspace without Ops/SOP integration yet.

### Expected implementation seams

- `freqinout/core/local_net_store.py` — schedules and occurrence state.
- `freqinout/core/local_net_recurrence.py` — bounded timezone-aware projection.
- `freqinout/core/local_net_models.py` — immutable draft/view models.
- `freqinout/gui/local_nets_tab.py` — lazy list/editor workspace.
- shared resource picker from LN-2.

### Work

1. Add Local Net schedule and occurrence-state tables.
2. Implement Daily, Weekly, Periodic, Bi-weekly, and one-time recurrence.
3. Persist IANA timezone and wall-clock input; project bounded UTC occurrences.
4. Calculate/cache next occurrence on write and rollover.
5. Implement per-occurrence dismiss without disabling recurrence.
6. Add Local Nets navigation under Plans in full and compact modes.
7. Implement summary, ordered cards/rows, filters, add/edit workflow, pause,
   resource update state, and exact next-occurrence review.
8. Make `Reminder only — FIO will not tune a radio` visible in final review and
   appropriate help/empty states.
9. Add contextual Operating Group and resource creation handoffs with draft
   restoration.

### Tests

- recurrence across DST gaps/folds, midnight, leap day, exception/effective
  dates, and ISO bi-weekly boundaries;
- bounded-horizon behavior and deterministic next occurrence;
- dismiss-one-occurrence semantics;
- create known/custom, group/unassigned, Amateur/GMRS, simplex/repeater;
- update/retired-resource states;
- restart persistence and clock rollover;
- explicit SchedulerEngine isolation: no Local Net read, action, QSY, launch, or
  radio mutation;
- 1,000-schedule performance corpus;
- responsive/theme/text/navigation matrix.

### Exit gate

- Local Nets is complete as a standalone reminder calendar.
- Scheduler isolation tests prove it cannot command a radio.
- Recurrence and performance budgets pass.
- No Ops Center or automatic SOP behavior is implied before LN-5.

Risk: **medium**, primarily recurrence/timezone correctness.

## Package LN-5 — Ops Center And SOP Integration

### Goal

Project bounded Local Net reminders into Ops Center and make linked SOP guidance
available without creating a new activation path.

### Expected implementation seams

- `freqinout/core/local_net_projection.py` — immutable Schedule Outlook items.
- `freqinout/gui/controlfreq_tab.py` — Local Nets Outlook renderer/actions.
- `freqinout/core/sop_manager.py` and `freqinout/gui/sop_tab.py` — additive
  context references and handoff.
- operational view/source registries as required.

### Work

1. Project active, next, and bounded later occurrences with
   `source_type=LOCAL_NET` and `commandable=false`.
2. Add a collapsible Local Nets section within Schedule Outlook.
3. Show What/Why, countdown, group, service, frequency/channel, source/update
   health, and SOP availability.
4. Implement Details, Dismiss, and Open SOP actions.
5. Apply 30/15-minute accessible urgency without layout shift.
6. Roll missed occurrences forward after a bounded review period.
7. Add Local Net/session context references to SOP entries and previews.
8. Keep SOP activation manual. Prove Local Nets bypass Net/SOP scheduler conflict
   policies and RF Guard.

### Tests

- active/next/later ordering and caps;
- local/UTC display and countdown boundaries;
- reminder urgency uses non-color cues;
- dismiss updates Ops without disabling future occurrences;
- exact SOP opens from Local Net context;
- no automatic SOP activation;
- no QSY or scheduler command when an occurrence becomes active;
- hidden/collapsed section stops unnecessary UI refresh work;
- Ops render/refresh performance with production-sized traffic and Local Net
  corpus;
- required viewport/theme/text matrix.

### Exit gate

- Ops Center answers What/Why for local activity at a glance.
- Local reminders remain distinct from message traffic and commandable schedule
  rows.
- SOP handoff is correct and manually controlled.
- Ops performance and layout remain inside baseline budgets.

Risk: **medium** because this touches a high-use dashboard.

## Package LN-6 — Release Qualification And Documentation

### Goal

Close cross-feature, migration, accessibility, performance, platform, recovery,
and operator-understanding gates.

### Work

1. Update user Help for Resources, HF subscription, Local Nets, reference
   guidance, Operating Groups, resource updates, retirement, and SOP handoff.
2. Update migration/recovery documentation and release checklist.
3. Run import/export round trips and backup/restore rehearsal on an isolated
   production-sized configuration clone.
4. Run complete repository tests in fresh Qt processes.
5. Run the full UI soak and collect startup/idle/tab/refresh/shutdown telemetry.
6. Run manual Linux production validation at 1920x1080 Normal Text plus compact
   and Large Text checks.
7. Perform operator acceptance for both entry directions:
   - directory to HF Nets;
   - HF Nets to directory;
   - directory to Local Nets;
   - Local Nets custom creation;
   - Ops reminder to SOP.
8. Reconcile governing specs, help, operational view inventory, and UI work log.

### Exit gate

- Every acceptance criterion in the governing spec has evidence.
- Migration/recovery can be performed without changing the production source
  during rehearsal.
- Full regression, corpus performance, UI soak, and Linux platform gates pass.
- No known duplicate writer, silent schedule update, unbounded recurrence,
  automatic Local QSY, or misleading legal language remains.
- Only then is the feature eligible for the release branch.

Risk: **medium** for final integration; no feature expansion is authorized here.

## Test Matrix Summary

### Core

- catalog CRUD, versioning, usage, retirement, import/export;
- migration dry-run, backup, idempotence, rollback, and legacy mapping;
- recurrence/timezone/exception boundaries;
- accepted snapshot and update comparison;
- Operating Group and SOP reference integrity;
- Schedule Outlook projection bounds;
- scheduler isolation.

### GUI

- empty, populated, invalid, update-available, retired, and conflict states;
- create/select/edit/return/deep-link workflows;
- keyboard navigation, accessible names, focus, and non-color status;
- Light/Dark, Normal/Large at 1920x1080, 1000x700, and 900x560;
- one and multiple active radios plus Mesh when shell/navigation is affected;
- no page-level horizontal scroll or clipped primary action.

### Performance

- 10,000 frequency resources;
- 2,000 net identities;
- 5,000 published sessions;
- 1,000 Local Net subscriptions;
- 90-day recurrence horizon cap;
- 50-item Ops later cap;
- warm catalog query below 100 ms p95;
- warm Ops occurrence query below 50 ms p95;
- first visible acknowledgement below 100 ms;
- no sustained idle work from an unopened/unchanged feature.

### Platform And lifecycle

- macOS and Linux filesystem/config roots;
- timezone database availability and DST behavior;
- lazy construction;
- cancellation during import/rebuild;
- resize/navigation during background work;
- close during work within the shared shutdown deadline;
- no Qt cross-thread timer warnings or surviving workers.

## Recommended Cost-Efficient Work Allocation

Use the high-reasoning primary model for:

- LN-0 architecture and ownership review;
- LN-1 schema, migration, transaction, and compatibility design;
- LN-3 commandable HF integration review;
- concurrency/shutdown ownership; and
- each package's final integration review.

Use Terra-class implementation for:

- bounded catalog/query services after interfaces are fixed;
- Resources, HF subscription, Local Nets, and Ops/SOP UI wiring;
- responsive model/view work; and
- integration tests spanning established services.

Use Luna/Mini-class implementation for:

- immutable models and mechanical adapters with exact contracts;
- import/export diagnostics;
- recurrence fixtures after the algorithm is fixed;
- navigation/help copy and accessibility metadata;
- screenshot/geometry fixtures; and
- documentation synchronization.

Every delegated package must state allowed files, prohibited ownership changes,
exact tests, performance budget, migration rule, and required diff/test report.
The primary model reviews every delegated diff before a gate can pass.

## 2026-09-10 Post-LN-6 Resources Usability Correction

Status: implementation and focused automated checks complete. This correction
does not replace the outstanding LN-6 30-minute soak and Linux/operator gate.

Operator review identified that the first Resources browser exposed persistence
concepts instead of task language. The correction keeps the canonical schema and
deep-link routes unchanged while making Resources one direct main-navigation
destination. Frequency Catalog, Net Directory, and Import / Export remain tabs
inside the workspace. Frequencies display as decimal MHz; catalog source keys,
resource keys, and version hashes remain internal; `Source region` replaces the
ambiguous outward `Scope`; and catalog lifecycle uses `Listed`/`Retired` rather
than the operationally ambiguous `Active`. Net Directory calls its child records
published net meetings while the database and service APIs retain `session`.

Source labels and meeting-frequency labels are resolved with bounded batch reads,
so the presentation correction does not introduce per-row database access. New
and cloned records expose only mutable station-owned catalog sources, while edit
flows retain the selected source identity as hidden data. No schema migration or
catalog rewrite is required.

Delegation: Terra/high implemented the bounded responsive UI; Luna/high authored
the focused operator-surface, compact Dark/Large Text, and navigation tests;
Terra/medium audited terminology and lifecycle semantics. The high-reasoning
primary model owned the read API, compatibility review, specification/help/work
log reconciliation, delegated-diff review, and final acceptance run.

Acceptance evidence: 253 focused catalog, transfer, HF subscription, Local Nets,
shell/navigation, and startup tests pass in fresh Qt processes. The dedicated
five-test operator-surface suite includes the 900x560 Dark/Large Text geometry
gate. Release preflight, Python compilation, `git diff --check`, and the isolated
22-screen GUI smoke pass with zero failed screens.

## Work Log Template

Each completed package appends:

- observed problem and intended operator outcome;
- Where/When/What/Why impact;
- data owner and migration result;
- files/services changed;
- delegated model assignment and primary review corrections;
- focused and repository test totals;
- performance and platform evidence;
- manual/hardware evidence still required;
- commit SHA and push status; and
- explicit statement that the next package did or did not begin.
