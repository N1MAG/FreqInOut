# UI Tab Design-Control Conformance Audit

Status: repository-wide audit and UIA-0 through UIA-5 implementation exit gates
complete. Native Linux visual qualification remains operator-assisted.

Date: 2026-09-13

## Purpose and authority

This audit reviews every navigable FIO workspace, its principal internal tabs,
and the shared application shell against:

- `task_oriented_workspace_design_guideline.md`;
- `ui_layout_standards.md`, including the Font-Derived Vertical Geometry
  Contract;
- `ln0_ui_geometry_contract.md`, whose viewport fixtures and geometry rules
  remain the project's concrete design-control baseline; and
- `multirig_product_ui_contract.md`.

This is an evidence and implementation-planning record. It does not claim that
a source inspection proves a screen visually correct. A finding is marked
**confirmed** only when the source directly violates a governing rule or places
synchronous work on an interaction/render path. A **runtime risk** requires the
specified widget-tree, interaction, or visual test before it may be closed.

## Required font-derived-height invariant

The governing invariant is now explicit in `ui_layout_standards.md` and is
cross-referenced by `task_oriented_workspace_design_guideline.md`:

> Every text-bearing control, item-view header/row, tab, chip, banner, card,
> wrapped label, and editor derives its vertical floor from the active font,
> content, indicators/icons, semantic padding, and hit-target treatment.

A literal pixel threshold is not conformance evidence. This rule includes lazy
pages and controls created after initial theme application. Geometry
recalculation after construction or a font/theme change must be coalesced,
idempotent, and cache-only. A bounded multiline preview may scroll locally, but
its cap must remain font/content aware and must not hide its scrollbar, focus
ring, or required action.

The present global guard in `freqinout/gui/theme.py` is only a safety net. Its
widget-family coverage does not include every tab bar, check/radio control,
spin/date/time editor, group title, table/tree header, or item row, and a lazy
page is not automatically proven compliant merely because the application
theme ran at startup.

## Complete surface inventory and disposition

| Navigation/workspace | Included surfaces | Audit disposition |
| --- | --- | --- |
| Application shell | full/compact navigation, feedback banner, Station Control Bar, command palette and contextual popovers | Confirmed local styling and geometry-feedback risks in the control surface; transient-banner repair exists but shell-wide runtime matrix remains required. |
| Ops Center | activity, intersections, inbox, frequency control, schedule/local-net/Shortwave outlook and propagation | Confirmed fixed table-row height and extensive local min/max geometry; local typography/theme exceptions; runtime stress required. |
| Map | map canvas, top filters, control drawer, recency popover, selected-location Overview/Status/Paths/Inbox | Confirmed nonresponsive top filter grid and local visual overrides; main splitters and coalesced refresh align. |
| Messages — Inbox | message-type filters/counters, result table, reader, pending/BBS summaries, delete/index/hidden-message tools | Recent workflow redesign aligns materially; fixed/capped detail surfaces, local styling, and counter/selection stress still require the full matrix. |
| Messages — Compose | FLMsg/FLAmp, JS8Call, FIO Spotter, CommStat RF, form fields, preview, stage/send output and full workbench | Confirmed compact horizontal form scrolling and remaining literal/capped text geometry; recent cache-only and responsive work remains a strong baseline. |
| Managed BBS | Radio Service, Locations & Access, Publishing, Automation, Visitor Preview, Visitor Helpers | Post-audit remediation uses bounded background catalog/initialization work, shared splitter styling, font-derived controls, and locally scrolling chip/editor surfaces. |
| FIO Spotter | Activity, Watches, Expect, Access Policies, Forms, Imports | Recent task-oriented redesign aligns; splitter seeds and bounded previews remain runtime risks at short/Large viewports. |
| NCS — FLDigi/SSB | net-control roster, controls, compare/reference/review and macro mapping | Confirmed local theme imitations, fixed/capped panes, dense action rows, and a 680 px dialog minimum. |
| NCS — JS8Call | setup, roster and operational controls | Confirmed dense fixed grid and no outer compact reflow/scroll owner. |
| NCS — VHF/UHF | local net setup, topics, roster and message editors | Confirmed fixed text-editor caps, three-column topic grid and disabled page horizontal safety without sufficient compact reflow. |
| HF Callsigns | roster/history, filters, import preview, details and group views | Confirmed hardcoded semantic colors and local muted styles; import/detail geometry requires Large/compact tests. |
| Local Callsigns | local operator list/editor and notes | Generally bounded, but notes and action layout require runtime font/compact proof. |
| Local Reports | result table and report detail | Generally bounded, but the detail minimum and selection/scroll preservation require runtime proof. |
| Resources — Frequencies | Frequency Catalog search, filters, results and selected detail | Confirmed filter/selection-time SQLite work, nonresponsive horizontal list/detail arrangement and 13-field schema-first detail wall. |
| Resources — Net Directory | entry list, sessions, selected detail and schedule handoffs | Confirmed filter/selection/action-time SQLite work and permanently narrow multi-pane inspector. |
| Resources — Import/Export | preview, diagnostics and import/export actions | No direct P0 found; validate preview scroll ownership, long diagnostics and all font/theme fixtures. |
| Shortwave | Explore, Listening, Data Sources | Local fixed typography is confirmed; result/detail/data-source loading, long provenance and compact scrolling need runtime proof. |
| Plan Builder | Effective Windows, Pattern Summary, Radio Windows and Week Grid | Confirmed local fixed chip/hint/toolbar geometry; dense planning panes require font/compact proof. |
| SOP Builder | editor, lanes, preview and export/hand-off surfaces | Confirmed fixed workbench floors/caps and only partial responsive band reflow. |
| HF Daily | schedule filters, entries, edit/action controls | Confirmed eight-column action grid at compact sizes. |
| HF Nets | net schedules, sessions and resource handoff | Confirmed eight-column action grid at compact sizes; legacy dense library remains a design-control concern. |
| Local Nets | schedule list/editor and resource/SOP flows | Local fixed title typography and 680x500 editor minimum are confirmed; constructor/store reads and compact editor flow require correction. |
| HF Peer Schedules | peer list, detail/status and actions | Confirmed screen-local error color; otherwise runtime font/compact proof remains. |
| Station Control Center | Overview plus dynamically created per-radio/SDR source pages | Confirmed synchronous SQLite reads during refresh/theme and local/fixed title/table geometry. |
| Station Health | Issues, Runtime Sources and Scheduler Log | Confirmed ten-second UI-thread summary/database refresh and local fixed typography. |
| Settings | Main, Radios, Software; all operator, preference, profile, model, assignment, cluster, group, mesh, software, auth, condition, launch, SOP, logging sections | Confirmed exact min=max stack-page locks, fixed navigation geometry, multiple capped text areas, and UI-thread GPG discovery on Message Auth navigation. |
| Help | topic list/content, context help and related dialogs | Confirmed fixed 220 px rail with no compact alternative and fixed dialog sizing. |

The inventory is derived from the current `MainWindow._screens` registry and
its full navigation routes. Optional Shortwave visibility does not remove it
from the acceptance inventory. Dynamically created radio/SDR pages, settings
sections, and internal tab pages are part of the owning workspace's gate.

## Confirmed gap register

### P0 — interaction, lifecycle, or geometry authority

#### UIA-001 — Station Overview performs database work from presentation paths

`station_overview_tab.py:386-432,459-487` resolves busy/manual state through
services whose implementations synchronously read SQLite
(`busy_state_service.py:46-62`,
`scheduler_manual_control_service.py:122-140`). `apply_theme()` also forces the
refresh (`station_overview_tab.py:152-197`). Paint/theme/refresh publication
must consume an immutable cached snapshot; discovery and database work belong
in a bounded worker or explicit background refresh.

#### UIA-002 — Resources filters and selections perform synchronous SQLite work

Frequency Catalog filters and selection route directly into store queries
(`frequency_catalog_view.py:78-98,195-255`). Net Directory filter, entry
selection, session selection, and actions similarly query synchronously
(`net_directory_view.py:78-92,238-357`); the store opens SQLite in those paths
(`resource_catalog_store.py:605-647,716-726`). Typing and selection must be
cache-only over a generation-keyed snapshot. Explicit mutations may validate
in a worker and publish one coherent result.

#### UIA-003 — Settings locks dynamic pages to transient child size hints

`settings_tab.py:11220-11236` copies a selected child's transient size hint into
matching minimum and maximum heights. This directly violates the font-derived
contract and recreates the geometry feedback mechanism behind prior
swipe/vanish regressions. Shared pages and stacks must remain resizable; a
coalesced cache-only layout publication may set a floor, never an exact
transient clamp.

### P1 — high-use usability and responsiveness

#### UIA-004 — UI-thread periodic/activation work remains in Health, BBS and Settings

- Station Health performs its summary and recent-scheduler database work on a
  ten-second GUI timer (`station_health_tab.py:253-295`,
  `station_health_summary.py:960-1014`).
- Managed BBS refreshes the catalog synchronously on activation/refresh
  (`station_bbs_tab.py:1304-1306,1377-1445`).
- Navigating to Settings Message Auth may probe GPG on the GUI thread
  (`settings_tab.py:10839-10867`).

Each must preserve a last coherent snapshot while bounded work runs elsewhere.
Switching tabs, resizing, or applying a theme may not trigger that work.

#### UIA-005 — Ops tables use a fixed 24 px row floor

`controlfreq_tab.py:1513-1528` sets table rows to 24 px. The global guard does
not prove table/header geometry. Every Ops item view must derive row and header
heights from its current font, icons/indicators, padding and per-row content.

#### UIA-006 — Map's principal filter bar does not reflow

`stations_map_tab.py:2673-2790` uses a fixed ten-column grid and fixed-width
fields (`:2762-2778`) outside the scrollable controls drawer. It must wrap or
stack at measured font/content breakpoints while keeping Search/Clear reachable
and the map canvas dominant.

#### UIA-007 — Compose permits horizontal scrolling in a normal setup form

`message_viewer_tab.py:7484-7491` enables a horizontal scrollbar for the compact
Compose setup pane. Setup controls must stack vertically; local horizontal
overflow is reserved for intentionally wide data/previews/logs. Remaining
literal caps/floors at `:6386-6401,6605,6653,7002-7005,7076-7117,12745` need
font/content-derived replacements or documented bounded-surface proof.

#### UIA-008 — Net-control screens do not protect compact/Large task flow

- JS8 NCS uses dense fixed groups and a ten-column setup without an outer
  compact scroll/reflow path (`js8call_net_control_tab.py:377-529`).
- Local NCS caps multiline editors and uses a three-column topic grid
  (`local_ncs_tab.py:139-148,179-234,282-343`).
- FLDigi/SSB retains dense action rows, fixed table/compare floors, and a macro
  dialog whose minimum height is 680 px (`fldigi_net_control_tab.py:711-918`,
  `fldigi_macro_mapping_dialog.py:62`).

These operational screens need task-order stacking and one clear local scroll
owner before any control becomes compressed.

#### UIA-009 — Schedule, SOP and Resource workbenches retain wide/schema-first layouts

HF Daily and HF Nets keep eight-column compact action grids
(`daily_schedule_tab.py:852-875`, `net_schedule_tab.py:613-639`). SOP retains
fixed workbench floors/caps and only limited three-band reflow
(`sop_tab.py:4753-4875,4931-4943`). Frequency Catalog presents 13 fields in the
normal detail wall (`frequency_catalog_view.py:150-179`), while Net Directory
keeps multiple narrow inspectors visible (`net_directory_view.py:94-236`).
These must follow list-to-detail or stepper archetypes from the design-control
guideline.

#### UIA-010 — Shared theme/component authority is bypassed broadly

Confirmed screen-local font sizes, colors, or theme imitations remain in:

- Ops Center (`controlfreq_tab.py:776,812,888-962,1064,1126,1213-1226,1480`);
- FLDigi/SSB NCS (`fldigi_net_control_tab.py:551-611,1300-1315,1565-1580,2247-2353,5112-5133`);
- HF Operators (`operator_history_tab.py:261,777-815,1588-1605,2072,2218`);
- Managed BBS (`station_bbs_tab.py:202,244,369,418,760-766,842-849`);
- Messages (`message_viewer_tab.py:5291,5458,5860-5865,9695`);
- Map (`stations_map_tab.py:1956,1982`);
- Station Overview/Health (`station_overview_tab.py:28-42,519-540`,
  `station_health_tab.py:58,92,150,206`);
- Resources, Shortwave, Local Nets and Peer Schedules
  (`resources_tab.py:50,330`, `shortwave_tab.py:1317`,
  `local_nets_tab.py:322`, `peer_sched_tab.py:385`).

Add missing semantics to `theme.py` or a shared component first, then remove
local imitations. Literal colors are never accepted merely because they happen
to resemble the current Dark theme.

#### UIA-011 — Several primary splitters and navigation rails lack compliant affordance

Managed BBS makes its splitter handle two pixels and transparent
(`station_bbs_tab.py:446-453`). Help uses a fixed 220 px topic rail without a
compact alternative (`help_tab.py:42-78`). Settings fixes its navigation list
height while disabling its own vertical scroll (`settings_tab.py:3886-3893,
10793-10825`). All must use the shared splitter/navigation treatment and
measured responsive mode changes.

### P2 — bounded surfaces and risks requiring runtime proof

#### UIA-012 — Fixed/capped text surfaces need classification, not threshold scanning

Examples include the Spotter history and Forms preview
(`fio_spotter_tab.py:620,1879,3030`), BBS chip strip
(`station_bbs_tab.py:797-805`), Station Overview result table
(`station_overview_tab.py:102-105`), Plan Builder chips/hints
(`freq_planner_tab.py:351-375,442-464`), Settings text areas, and Local
Operator/Local Reports detail panes. Some may be legitimate bounded scroll
surfaces. None is closed until the font/content formula or documented exception
and runtime test exist.

#### UIA-013 — Dynamic splitters may starve a task surface

Spotter uses the correct page scroll owner and shared splitter treatment, but
its seed sizes (`fio_spotter_tab.py:935-986,1291-1408,1874-1879`) need short and
Large Text proof. Control Center, Ops, Compose, operator import, and NCS screens
likewise need repeated resize/theme/selection tests to exclude oscillation,
stale paint, nested-scroll traps, and selection loss.

#### UIA-014 — Current automated source audit has incomplete semantic coverage

`tools/audit_ui_text_size_heights.py` presently finds only eight values at or
below a 48 px heuristic. It misses larger but still invalid caps, table/header
rows, lazily created controls, multiline content and transient exact-height
locks. Its two `setMinimumHeight(0)` Spotter results are reflow resets, showing
why classification is required. It must become a semantic candidate audit with
an explicit exception mechanism, backed by runtime widget-tree checks.

## Existing alignment to preserve

- FIO Spotter has one vertical page scroll owner, responsive splitter
  orientation, shared splitter styling, cache-only selection improvements, and
  the recent editor-first task sequence.
- Messages Inbox keeps horizontal overflow local to its intentionally dense data
  table. Compose already uses shared font helpers for many chips and normal
  controls and has generation-aware background work.
- Map's principal splitters use the shared handle helper and its refresh path is
  substantially coalesced/generation-aware.
- Ops Center has an outer vertical scroll owner and an explicit compact filter
  arrangement.
- Settings has a correct outer scroll owner; remediation must remove the exact
  child-height lock without reintroducing the previous full-window overlay or
  swipe/vanish failure.
- Recent CommStat and feedback-banner fixes are the reference pattern:
  font-derived content floors plus one coalesced cache-only shell geometry
  publication.

## Remediation slices and exit gates

No successor slice begins until the current slice passes its exit gate.

### UIA-0 — Conformance harness and shared primitives

- Expand the source audit to classify all text-bearing fixed/min/max geometry,
  item-view rows/headers, raw typography/colors, invisible splitter handles and
  known UI-thread I/O hooks.
- Add reusable runtime widget-tree assertions for font-derived vertical
  geometry and lazy-page/theme lifecycle.
- Add missing theme/component roles before feature tabs use them.

**Exit gate:** the harness enumerates every registered/lazy main screen and
nested tab, supports explicit documented exceptions, fails seeded violations,
and passes Light/Dark × Normal/Medium/Large without doing I/O from geometry
checks.

### UIA-1 — P0 snapshot and geometry authority

Correct Station Overview and Resources interaction-time database work, Settings
exact-height locking, Health/BBS activation work, and Message Auth discovery.

**Exit gate:** instrumented filter, selection, resize, paint, theme, lazy-load
and tab-switch paths perform no SQLite/filesystem/process/endpoint I/O; rapid
navigation retains a coherent prior snapshot; no geometry oscillation occurs.

#### UIA-1 implementation record — complete

- Frequency Catalog and Net Directory now load bounded, generation-keyed
  snapshots in workers. Typing, filters, row/session selection, detail
  rendering, resize, and theme paths project only from the last coherent
  snapshot.
- Station Overview and Station Health publish immutable worker snapshots;
  periodic and activation refreshes no longer query services or SQLite on the
  GUI thread. Theme/render paths reuse coherent evidence.
- Managed BBS activation and explicit refresh use a bounded snapshot worker.
  Location, visitor, artifact, filter, and session projections are cache-only;
  explicit mutations retain their confirmation while the refreshed snapshot
  is published.
- Settings expanded stack pages use a content-derived minimum instead of a
  transient exact min/max clamp. Message Auth GPG discovery is coalesced in a
  background worker and preserves the current key presentation until the new
  result is complete.
- Every worker has generation fencing and an explicit bounded shutdown path.

**Exit gate evidence:** the combined UIA-1 Resources, Station, BBS, Settings,
snapshot lifecycle, compact-layout, and regression suite passes. Runtime
instrumentation confirms ordinary interaction and theme paths are cache-only;
slow providers return control to the GUI immediately and stale generations do
not replace coherent results.

### UIA-2 — shell, Ops, Messages, Map, BBS and Spotter

Correct fixed table rows, compact form overflow, map filter reflow, shared
splitters and remaining theme exceptions. Preserve the recent high-use workflow
behavior and cache-only generation boundaries.

**Exit gate:** every included internal tab passes the full fixture matrix; rapid
message next/previous, counters, map filters, Spotter selection and ten repeated
shell feedback/theme cycles preserve selection/scroll and do not swipe, vanish,
clip, or produce stale paint.

#### UIA-2 implementation record — complete

- Ops item-view rows and headers now derive their floors from active font and
  indicator metrics. Its labels, schedule urgency, and frequency-state badges
  use shared semantic theme roles rather than local colors or font sizes.
- Map's principal filter bar reflows at measured live control widths while
  keeping Search and both clear actions reachable. Its detail and operational
  marker HTML now derives semantic colors and text scale from the shared theme.
- Messages Compose stacks setup controls without normal-form horizontal
  scrolling, wraps the compose-mode selector from font/content metrics, and
  preserves draft and scroll state. Repeated geometry publication is
  idempotent and can shrink after an authored floor is released.
- Managed BBS uses the shared visible splitter treatment and font-derived chip
  geometry. BBS and Spotter preserve the UIA-1 cache-only snapshot boundary;
  Spotter splitter seeds no longer overwrite operator adjustments.
- Shell feedback/theme cycles and message reader navigation remain bounded and
  generation-aware. Shared theme contrast selection is used for filled chips
  and generated Map marker badges.

Work packages and models:

- high-reasoning primary GPT-5: slice architecture, shell and shared-theme
  changes, Map breakpoint and contrast corrections, delegated-diff review,
  compatibility correction, integration, documentation, and exit gate;
- `gpt-5.6-terra` (high): Ops and Map responsive/font/theme work and focused
  tests;
- `gpt-5.6-luna` (high): Messages/Compose responsive geometry, rendering
  typography, repeated-cycle protection, and focused tests; and
- `gpt-5.6-terra` (high): Managed BBS, Spotter, Map semantic HTML review, and
  focused stress tests.

**Exit gate evidence:** the final combined UIA-2 suite passes **207 tests**.
The delegated Map regression suite passes **243 tests**, the BBS/Spotter suite
passes **39 tests**, and the narrow Expect geometry case passes ten consecutive
runs. The semantic scanner reports no finding in any UIA-2 owning file; its
repository-wide backlog is reduced from 46 hard findings/141 candidates to 22
hard findings/48 candidates for later slices. Changed modules compile and
`git diff --check` passes. UIA-2 authorizes UIA-3.

### UIA-3 — NCS and operator workspaces

Reflow JS8, FLDigi/SSB and VHF/UHF net-control tasks; migrate local semantic
styles; correct operator import/detail geometry.

**Exit gate:** empty, active, blocked and long-content states pass the fixture
matrix; roster/list remains dominant; operational actions stay visible; no
nested same-axis scroll trap or fixed-size dialog exceeds the viewport.

#### UIA-3 implementation record — complete

- JS8 and FLDigi/SSB net-control workspaces use one vertical page-scroll owner,
  content-measured responsive rows, font-derived roster geometry, and stable
  semantic task order. Draft/session state remains intact through repeated
  compact, wide, font, and theme transitions.
- The VHF/UHF NCS workspace retains its outer scroll owner while its report,
  notes, rows, and topics derive their usable geometry from the active font and
  content. Resize paths perform layout work only.
- HF Callsigns, Local Callsigns, and Local Reports now use shared semantic theme
  roles, font-derived controls/table rows/detail surfaces, and responsive action
  bands. Operator import/profile dialogs fit the compact audit viewport and keep
  their primary footer actions reachable.
- FLDigi macro mapping no longer imposes an oversized dialog floor. Its dense
  action row stacks from measured live control widths, while its genuinely wide
  mapping table retains a bounded local horizontal scrollbar.
- Primary review replaced delegated magic breakpoints and local contrast guesses
  with shared content-measured and filled-surface contrast helpers, restored
  theme refresh for count chips, and made static responsiveness checks aware of
  multiline bounded calls and current callback-only worker completions.

Work packages and models:

- high-reasoning primary GPT-5: slice architecture, shared layout/contrast
  primitives, review and correction of every delegated diff, compatibility-test
  reconciliation, integration, documentation, and exit gate;
- `gpt-5.6-terra` (high): JS8 NCS and VHF/UHF NCS implementation and focused
  tests;
- `gpt-5.6-luna` (high): FLDigi/SSB NCS and macro-mapping implementation and
  focused tests; and
- `gpt-5.6-terra` (high): HF Callsigns, Local Callsigns, Local Reports, operator
  dialog implementation, and focused tests.

**Exit gate evidence:** the combined UIA-3 and owning-workspace regression suite
passes **247 tests with 1 skipped**. The conformance harness/scanner tests add
**16 passing tests**. The scanner reports no finding in a UIA-3 owning file; the
repository backlog is **22 hard findings and 34 candidates**, all routed to
UIA-4/UIA-5. Changed Python files compile and `git diff --check` passes. UIA-3
authorizes UIA-4.

### UIA-4 — Resources, planning, SOP, schedules and Settings

Adopt list/detail or stepper patterns, remove schema-order presentation, correct
navigation/text caps, and validate every Settings section—including all Software
products and dynamically added radio instances.

**Exit gate:** every editor and nested section has one readable task path,
preserves its draft/selection on responsive transitions, and passes font/theme,
scroll, cache-only and long-content checks.

#### UIA-4 implementation record — complete

- Resources now use cache-backed catalog snapshots for filter, selection,
  resize and theme interactions. Frequency Catalog and Net Directory use a
  responsive list/detail pattern with one vertical form-scroll owner; Resource
  Import/Export and the picker stack controls at measured compact widths.
  Snapshot requests discard stale generations and coalesce work that has not
  started, preserving the last coherent result while a refresh completes.
- Plan Builder, Daily and Net schedules, Local Nets, peer schedules and SOP use
  shared semantic theme roles, font-derived rows/headers/bounded surfaces and
  compact reflow. Wide data surfaces remain explicitly local; ordinary forms
  do not acquire a horizontal page scrollbar. SOP application cards follow the
  live theme, while its offline print/export HTML has documented white-paper
  palette and typography exceptions.
- Settings and Software Administration replace pixel-era text/table/list bounds
  with shared line-count and visible-row helpers. Filled selection states use
  contrast-derived foregrounds. Theme application paints only the last coherent
  dependency-status snapshot and cannot initiate application, process or
  endpoint probing.
- Primary review corrected remaining schedule-table clamps, a dialog theme that
  had defaulted to Light instead of the active application theme, Software chip
  contrast, stale Resources request accumulation, and all remaining UIA-4
  scanner candidates.

Work packages and models:

- high-reasoning primary GPT-5: slice architecture, concurrency and cache
  boundaries, shared font-derived helpers, delegated-diff review/correction,
  integration, documentation and exit gate;
- `gpt-5.6-terra` (high): Resources, Frequency Catalog, Net Directory and
  Resource Picker implementation and focused tests;
- `gpt-5.6-luna` (high): Plan Builder, SOP, schedules, Local Nets and peer
  schedule implementation and focused tests; and
- `gpt-5.6-terra` (high): Settings and Software Administration mechanical UI
  conformance and focused tests.

**Exit gate evidence:** the integrated UIA-4 and owning-workspace suite passes
**462 tests with 19 platform skips**. An additional corrected planning gate
passes **30 tests**, the previously timing-sensitive Local Nets projection
budget passes independently, changed Python modules compile, and
`git diff --check` passes. The scanner reports no finding in a UIA-4 owning
file; the complete repository backlog is **7 hard findings and 0 candidates**,
all assigned to UIA-5. UIA-4 authorizes UIA-5.

### UIA-5 — Shortwave, Help, secondary dialogs and final integration

Close all remaining P2 classifications and exercise optional/lazy surfaces,
dialogs, popovers and error/empty states.

**Exit gate:** the complete coverage manifest has no unclassified fixed/capped
text geometry, local visual authority, untested lazy page, or unresolved P0/P1;
all automated suites pass and Linux visual qualification records any genuine Qt
platform exception.

#### UIA-5 implementation record — complete

- Shortwave and HF subscription surfaces now use font-derived control, row and
  preview geometry; shared theme roles; readable task-order reflow; visible
  splitter/scroll ownership; and compact dialogs whose actions remain reachable
  at the 900x560 audit viewport.
- Help loads its document through a bounded, generation-keyed worker. Topic
  navigation, theme changes and resize operate on the last coherent cached
  document and do not reread the filesystem. Its table of contents reflows above
  the guide when measured content width no longer fits beside it.
- Logs reads a bounded tail (at most 2 MiB/800 lines) outside the GUI thread,
  coalesces refresh requests and preserves the last coherent snapshot on error.
  Search, filtering, font/theme changes and responsive reflow are cache-only.
- Startup splash typography, artwork scale and colors now derive from the active
  application font and shared theme. Main-shell and secondary-dialog size floors
  no longer prevent the compact audit viewport.
- The common snapshot worker stops before Qt destroys its child thread, including
  the `deleteLater()` lifecycle used by lazy tabs. Primary integration review
  additionally removed delegated GUI-thread Log I/O, Help document reads during
  interaction, and accidental double scaling in the splash.
- Full-suite reconciliation retained source-scoped JS8 provenance, made
  age-window ingest fixtures relative rather than calendar-expiring, primed the
  scheduler's published asynchronous projection in cache-reader tests, and gave
  the VarAC persistence fixture the required assigned instance.

Work packages and models:

- high-reasoning primary GPT-5: architecture, concurrency/lifecycle boundaries,
  shared primitives, all delegated-diff review and correction, full integration,
  specification/work-log reconciliation and exit gate;
- `gpt-5.6-terra` (high): Shortwave and HF subscription conformance plus focused
  tests;
- `gpt-5.6-luna` (high): Help, Logs and startup splash implementation plus
  focused tests;
- `gpt-5.6-terra` (high): optional/lazy-surface manifest review and scheduler
  cache-fixture reconciliation; and
- `gpt-5.6-luna` (high): final full-suite stale-fixture reconciliation for JS8
  source identity, ingest age, scheduler projections and VarAC assignment.

**Exit gate evidence:** the UIA-5 changed-surface and integration gate passes
**291 tests**; the scanner/harness gate passes **24 tests**; the Phase 7/native
construction stress gate passes **132 tests**; and the complete test inventory
passes in bounded process-isolated shards: **1,090 passed/2 skipped**, **5
passed** (the Local Nets release file, including its unchanged 50 ms p95
benchmark), **1,107 passed**, **814 passed/7 skipped**, **461 passed/27
skipped**, and **220 passed/1 skipped**. The semantic scanner reports **0 hard
findings and 0 candidates**. Changed Python modules compile and `git diff
--check` passes.

The monolithic macOS offscreen PySide process is not used as the release gate:
legacy tests accumulate native Qt/application state across thousands of cases
and can abort inside Qt despite the same files passing in clean shards. No test
assertion is skipped or weakened by sharding. Linux still requires the manual
Light/Dark, Normal/Medium/Large Text and compact/full-screen visual matrix for
platform-native subcontrols and window-manager behavior.

### UIA-6 — first-render and Map activation stability

Status: implementation and automated exit gate complete. Native macOS, Linux,
and Windows multi-monitor confirmation remains operator-assisted.

Production feedback identified two related first-visible failures after UIA-5:
Map could flash and reposition on Linux, a Windows single-rig report observed
the top-level FIO window moving to another monitor during Map load, and ordinary
tabs could initially paint with incorrect geometry until a minimize/restore
cycle. Runtime measurement found that the primary stack and the nested Messages
mode stack allowed a tall hidden Compose page to inflate the visible Inbox and
top-level size hint. Map additionally rewrote its filter grid and two splitters
synchronously for every resize while its native WebEngine view was being
created, then initialized Leaflet without a post-layout viewport invalidation.

The correction uses the existing `CurrentPageStack` for both affected stacks,
replaces lazy placeholders atomically, and adds one navigation-generation-
fenced first-visible layout settlement. Map geometry is coalesced and
idempotent: unchanged filter columns and drawer state do not rebuild layouts or
rewrite splitters, and Leaflet is invalidated once per real viewport/page
generation without reloading data. macOS and Windows WebEngine warm-up now uses
a page-only `QWebEnginePage`; it cannot show or position a native child view or
alter the main window's monitor assignment.

A continued-production report reopened this gate. A focused Qt reproduction
then proved the remaining top-level path: `CurrentPageStack` correctly ignored
hidden pages but still published the active Map's transient native minimum-size
hint, and the main stack's `Expanding/Expanding` policy allowed that hint to
replace a shown 900x560 window with a 2418x1818 window. The main stack now uses
`Ignored/Ignored`; layout stretch still fills available space while tab-owned
scrolling handles overflow. The live log also proved a WebEngine cold-load
`ApplicationInactive`/`ApplicationActive` blip caused the global lifecycle to
pause Map during page load and render it again on resume. Inactive transitions
now receive a 1.5-second grace period, while explicit hidden/suspended states
still pause immediately. A transient blip therefore performs no child pause,
resume, or duplicate visible refresh.

A macOS screen recording then isolated the remaining cold path: starting the
WebEngine helper on the first Map click briefly deactivated the full-screen FIO
Space and exposed the Terminal desktop before macOS returned to FIO. Page-only
prewarm is therefore enabled by default on macOS as well as Windows, and an
early Map click is held on the current page until that one-time prewarm
finishes. The same review found that every warm Map re-entry manufactured a
dirty render and changed the support card from expanded warming/loading to a
compact ready strip even when the render input was unchanged. Clean re-entry
now reuses the live page, inactivity alone does not dirty Map, and routine live
refreshes retain one font-derived compact strip.

Work packages and models:

- `gpt-5.6-sol` (high reasoning), primary: lifecycle/geometry architecture,
  cross-platform WebEngine safety, production implementation, delegated-diff
  review and correction, integration, specification/work-log reconciliation,
  and exit gate;
- `gpt-5.6-terra` (high): Map activation, resize-feedback and Leaflet lifecycle
  audit plus focused Map tests;
- `gpt-5.6-luna` (high): first-render/minimize-restore runtime audit plus focused
  active-page/lazy-activation tests; and
- `gpt-5.6-terra` (high): independent responsive re-entry/static lifecycle
  audit across Map, Messages, Settings and the main shell.

Primary review accepted the delegated test-only diffs, updated one stale Map
test that required the old synchronous resize mutation, corrected a delegated
top-level regression so it exercises the required shell policy, and added
explicit coverage for unchanged drawer state and transient application-state
fencing. The focused first-render/Map/shell gate passes **342 tests**; the related UI
lifecycle/design-control gate passes **52 tests**; and the multi-rig main-shell
gate passes **36 tests**. A separate high-use Messages/Compose/Spotter/Map
regression gate passes **307 tests**. Changed Python files compile and `git diff --check`
passes. No database migration, production-data mutation, RF/device command,
external endpoint action, top-level geometry command, commit, or push occurred.

The external qualification gate is intentionally still open until the operator
confirms first Map activation and repeated Map/Inbox switching on production
macOS, Linux, and Windows, including full-screen/maximized use and multi-monitor
placement.

## Acceptance matrix

Every owning screen and internal tab must be exercised at:

- Light and Dark themes;
- Normal, Medium and Large Text;
- 1920x1080, approximately 1000x700 and approximately 900x560;
- representative empty, populated, long-label/path, warning, disabled, loading
  and failure states; and
- one radio, two radios plus Mesh, and three-radio stress where the station
  shell or radio-derived content is present.

Automated checks must assert:

1. every text/indicator/focus ring is complete and controls meet their
   font-derived floor;
2. item-view headers and rows derive from font/content metrics;
3. one page-level vertical scroll owner and no ordinary form/page horizontal
   scroll;
4. shared splitter/theme/components are used;
5. task order, selected context, draft, scroll offset and primary action survive
   reflow and refresh;
6. geometry settles after bounded event processing and does not alternate or
   grow across repeated cycles; and
7. typing, hover, resize, paint, selection, theme, tab switch, chip layout and
   validation perform no filesystem, database, process, endpoint, device or
   network I/O.

Manual Linux visual qualification remains necessary for native control
subcontrols, window-manager sizing, high-DPI behavior and actual focus/hover
paint, but it supplements rather than replaces the automated contract.

## Work-package ownership for this audit

- high-reasoning primary GPT-5: governing rubric, font-derived-height contract,
  whole-application inventory, concurrency/performance assessment,
  reconciliation, specification and final integration review;
- `gpt-5.6-terra` (high): Settings, Station, Plans, Resources, Shortwave and
  schedule audit;
- `gpt-5.6-luna` (high): Messages, Compose, FIO Spotter, Managed BBS, Map and
  Ops audit; and
- `gpt-5.6-terra` (medium): NCS, Operators, Help, remaining dialogs and the
  mechanical fixed-height/style scan.

All delegated findings were reviewed against source by the primary model. The
agents made no code or documentation edits.

## Implementation progress

### UIA-0 — complete

The former 48 px/name-heuristic scanner is now a semantic static classifier. It
reports hard violations separately from review candidates; covers text-bearing
literal geometry at any value, matching min/max locks, table/header row values,
raw typography/colors and narrow/invisible splitter handles; ignores reset
values; and requires a rule-specific, reason-bearing exception. The current
baseline is 46 hard findings and 141 candidates. That count is an implementation
backlog, not a test failure or a claim that every candidate is wrong.

The reusable runtime harness covers font/style-derived control floors,
table/tree headers and rows, tab bars, explicit page-versus-local horizontal
scroll ownership, bounded geometry settling, and lazy-page theme/text-scale
lifecycle. Its static manifest matches all 23 `MainWindow` screen keys and the
principal dynamic/static nested tabs and selector workspaces without
constructing runtime services or opening configuration data.

The shared theme safety guard now includes platform-native line/combo/spin/date
input families, buttons/checks/radios, tab bars, titled group boxes, and
table/tree header and row floors. It is idempotent, honors the explicit opt-out,
and can be reapplied after lazy construction. Item-view row height deliberately
excludes the entire view's size hint, and `QHeaderView.minimumSizeHint()` is
excluded because Qt reports its generic scroll-area floor rather than the
painted horizontal header thickness.

Work packages and models:

- high-reasoning primary GPT-5: architecture, shared theme implementation,
  delegated-diff review, correction of item-view/header hint semantics,
  expansion of nested-surface manifest coverage, integration and gate;
- `gpt-5.6-terra` (high): semantic static classifier and seeded tests; and
- `gpt-5.6-luna` (high): runtime widget-tree harness and manifest tests.

Acceptance evidence: 27 focused UIA-0/theme tests pass; changed modules compile;
`git diff --check` passes. The primary review caught and corrected an initial
runtime-harness comparison of horizontal header section widths as heights, and
prevented a whole item-view size hint from becoming each row's height. No
database, filesystem, endpoint, device or network work was added to geometry
paths. UIA-0 passes and authorizes UIA-1.
