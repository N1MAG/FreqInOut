# Production Reliability And Workflow Remediation Spec

Status: Slices 0–1 implemented; Slice 1 automated gate passed 2026-09-06;
physical macOS reconnect and Linux/live-device gates pending; Slices 2–6 not
started

Date: 2026-09-06

Scope: Mesh administration/runtime, FIO Managed BBS, Messages and FIOSpotter,
Launch Control, SOP Builder, Plan Builder, cross-application responsiveness,
and roster-import reporting

## Purpose And Authority

This document converts the September 2026 production observations into one
dependency-aware implementation contract. It is the delivery authority for the
findings enumerated here. The deeper domain contracts remain authoritative for
behavior not changed by this remediation:

- `multirig_product_ui_contract.md`
- `mesh_client_integration_spec.md`
- `varac_managed_bbs_database_manifest_spec.md`
- `message_inbox_controls_spec.md`
- `message_intelligence_projection_spec.md`
- `sop_schedule_plan_spec.md`
- `ui_layout_standards.md`

If wording conflicts, this document controls only the production-remediation
items below. It does not authorize unrelated feature expansion.

The product center of gravity remains:

- **Where:** operating group, radio, band/frequency, and route.
- **When:** current and next scheduled action, including event activation.
- **What/Why:** the active workspace's actionable information and its reason.

The Station Control Bar must remain stable and responsive while background
services connect, scan, project, or reconcile data. Administration screens must
not displace or blank the bar's Where/When context.

## Review Basis

The review covered:

- the supplied macOS and Linux logs:
  - `/Users/bill/Downloads/freqinout.log.1`
  - `/Users/bill/Downloads/freqinout (13).log`
- the supplied roster:
  - `/Users/bill/Downloads/MAGNET Roster 09-02-26 - Current.csv`
- the supplied SOP Builder screenshot:
  - `/Users/bill/Downloads/sop builder layout.jpg`
- current implementation paths in `freqinout/core`, `freqinout/gui`, and the
  existing internal specifications.

The screenshot is evidence of current rendering only. Text visible inside it is
not treated as instruction.

## Implementation Surface Map

This map identifies current ownership and the intended seam for the work. Exact
line numbers are intentionally omitted because the affected files are active.

| Domain | Current implementation | Remediation seam |
| --- | --- | --- |
| Shell/startup/shutdown | `gui/main_window.py`, `core/dependency_status_service.py`, `core/station_runtime_manager.py` | first-usable-shell coordinator, shared status snapshots, worker shutdown registry |
| Mesh configuration | Mesh sections and callbacks in `gui/settings_tab.py`; `core/mesh/settings.py` | extract a saved-connection editor/model; keep Settings as a thin host |
| Mesh runtime | `core/mesh/qt_worker.py`, `manager.py`, `meshcore_adapter.py`, protocol adapters | cancellable operation queue/state machine with progressive channel results |
| BBS catalog | `core/varac_bbs_library_store.py`, `varac_bbs_vault.py`, `varac_bbs_inventory.py` | station-owned catalog/query/reconciliation services |
| BBS UI/actions | VarAC BBS tabs in `gui/settings_tab.py`; publication actions in `gui/message_viewer_tab.py` | one reusable tree/publication component in a station BBS workspace |
| Messages | `gui/message_viewer_tab.py`, `core/message_ingest.py`, `message_source_projectors.py`, `message_projection_store.py`, `message_file_scanner.py` | incremental source adapters plus bounded projection query/model |
| FIOSpotter | `core/js8spotter_importer.py`, Spotter ingest/projection, read-only review in Settings | station-owned watch/alert rule service and administration workspace |
| Launch Control | `core/launch_orchestrator.py`, launch section in `gui/settings_tab.py`, profile fields in `core/multi_radio_store.py` | per-radio bundle repository plus one station launch planner |
| SOP Builder | `gui/sop_tab.py` | widget-independent action model, stacked card editor, responsive conflict drawer |
| Plan Builder | `gui/freq_planner_tab.py` | bounded plan projection model and wide/compact layouts |
| Roster import | `core/operator_roster_import.py`, import flow in `gui/operator_history_tab.py` | classified diagnostics returned by the parser and shown before commit |

Existing regression suites under `tests/` are extended rather than replaced,
especially the Mesh foundation, BBS manifest/management, message
ingest/projection/responsiveness, scheduler shutdown, UI responsiveness, Plan
projection, condition-SOP, and roster-import suites. New service code must have
Qt-free tests before its GUI wiring is added.

## Confirmed Findings

### Performance evidence

The reported slowness is confirmed by instrumentation and is not primarily a
theme or paint problem.

- Startup completion reached 111.2 seconds in `freqinout.log.1` and 236.5
  seconds in `freqinout (13).log`.
- Main-window construction reached 93.6 and 204.3 seconds. The constructor still
  eagerly creates Settings, schedules, NCS, SOP, operator, map, station, health,
  and other heavy screens; only Messages and Plan Builder are lazy.
- Database initialization reached 22.4 seconds.
- Native message-source projection reached 83.1 seconds while projecting 5,000
  CommStat plus 5,000 SitRep rows.
- Projection-to-view conversion reached 29.9 seconds for 3,605 rows.
- A full 520-file scan reached 14.7 seconds, and an unchanged 270-file
  incremental scan reached 9.9 seconds on Linux.
- The two logs contain 389 slow UI refresh warnings, 841 slow
  dependency-process snapshots, and 32 event-loop stall reports.
- Plan Builder reconstruction reached 13.2 seconds and ControlFreq heavy
  refresh reached 7.8 seconds.

The dominant architectural problem is synchronous, overlapping work: process
inspection, database projection, filesystem discovery, widget construction,
and table population can all execute in or block the GUI event loop. Timer-based
debouncing reduces call count but does not make a blocking callback safe.

### Data ownership findings

- Mesh edit signals trigger health queries and full channel-table reconstruction
  on nearly every field change.
- MeshCore channel discovery requests up to 32 channels sequentially, with a
  per-request timeout. It runs as one worker turn, so stop/reconnect commands
  cannot be serviced promptly while it is blocked.
- The Mesh runtime shutdown path waits only 200 ms before retaining a still-live
  thread. This cannot guarantee cancellation of a blocking BLE/channel call.
- The Managed BBS schema already models one shared artifact library and
  many-to-many location membership, but most administration is presented under
  a selected radio's VarAC settings.
- BBS preview is generated as plain text, and source-file existence is not part
  of the normal manifest query. Folder resync can recreate a disabled mapping.
- JS8 message ingestion reads the JS8 inbox and specialized Spotter traffic; it
  does not yet define a general incremental ingestion path for all relevant
  directed traffic in `DIRECTED.TXT`.
- CommStat operational severity is currently projected into the same visible
  status concept used for read/unread state.
- Launch-app selection is displayed as radio-scoped, but the persisted app list
  and automatic-start path still use global `launch_control_items`. Manual start
  constructs a selected-radio list, so manual and startup behavior use different
  planners.
- The SOP Builder has a scroll area, but its wide temporary action table,
  minimum-height panels, and multi-column workbench make the compact layout
  behave like a compressed spreadsheet rather than a vertically readable
  builder.

## Shared Architecture Contract

### 1. Keep the GUI thread presentation-only

The GUI thread may read an already-built in-memory snapshot and paint bounded
rows. It must not perform device I/O, process enumeration, recursive filesystem
walks, large database projections, schema migration, or unbounded widget
creation.

Every background operation must publish a small immutable result carrying:

- operation id and source id;
- lifecycle state (`queued`, `running`, `complete`, `cancelled`, `error`);
- progress when knowable;
- started/completed timestamps and elapsed time;
- whether the result supersedes an older request.

Only the newest result for the same source/request class may update the UI.

### 2. Separate ingest, projection, query, and presentation

Source adapters append or upsert normalized records incrementally. Projection
updates only records whose source version changed. Views query a bounded page or
summary through indexed SQL and paint it with a model/view widget. Opening a
view must never trigger an all-history reprojection.

Required checkpoints are persisted per source using a stable source identity,
file identity where applicable, offset/high-water mark, parser version, and last
successful timestamp. Reparse is explicit, resumable, and backgrounded.

### 3. Use one refresh coordinator

Station Control Bar, Station Health, Settings, and source services must consume
shared cached snapshots. A screen must not independently force a process scan
or device refresh while another request is active. Invalidations coalesce by
scope, and background work uses backoff after failure.

Editing a setting updates local form state immediately and schedules validation.
It does not query unrelated tables or rebuild inventory widgets on each
keystroke.

### 4. Bound all large views

Messages, operators, BBS artifacts, mesh channels, and builder workbenches use
bounded item models rather than creating one widget hierarchy per retained
record. Filter before render. Large histories are paged; summary counts are
separate indexed queries.

### 5. Preserve explicit scope

- Mesh connection/channel state is scoped to a saved mesh connection.
- VarAC runtime paths are scoped to a radio.
- The FIO Managed BBS catalog, locations, publication, retention, and access
  rules are station-scoped and shared across radios/cluster nodes.
- Launch app selection and command/path overrides are radio-scoped; the startup
  run is a station plan composed from active radios.
- Message evidence retains its source family and radio/instance identity.

## Performance And Lifecycle Budgets

Budgets are measured at p95 on the production Linux baseline (1920x1080,
Normal Text) with the production-sized database. macOS and Windows must meet the
same interaction budgets unless an OS-controlled prompt is active.

- First usable shell: <= 5 seconds warm and <= 10 seconds cold. Deferred source
  indexing may continue with visible, non-blocking status.
- Visual acknowledgement of a click/edit: <= 100 ms.
- Cached tab activation or filter update: <= 250 ms.
- First Messages page: <= 500 ms after tab activation; no more than 200 rows in
  one UI model fetch.
- Routine GUI callback: target <= 16 ms, hard budget 50 ms. Work above 50 ms is
  moved off-thread or split into yielding chunks.
- Station Control Bar render from a cached snapshot: <= 50 ms and never hidden
  by source work.
- Graceful shutdown: <= 3 seconds normally, with explicit cancellation and a
  bounded error path; no `QObject::killTimer`, cross-thread timer, or live
  `QThread` destruction warning.
- Background CPU when idle: source timers must coalesce; no repeated full process
  snapshots or unchanged-file traversal.

Performance spans must distinguish queue wait, source I/O, database work,
projection, model fetch, and paint. Each span includes row/file/device counts so
regressions are actionable.

## Mesh Service Remediation

### Configuration identity

- A new connection name is generated from the selected protocol:
  `meshcore-1`, `meshtastic-1`, and so on.
- Changing protocol updates an untouched auto-generated name. Once the operator
  edits the name, later protocol changes never overwrite it.
- Protocol, connection type, user-facing name, stable device id, advertised
  name, and source radio/role remain separate fields.
- The protocol/transport/physical-endpoint key identifies a saved device.
  `adapter_id` is an internal runtime identity and must be unique even when
  legacy records reused it for two physical endpoints.
- Identity normalization considers the complete saved-device library,
  including disabled siblings, before choosing the one active runtime. The
  protocol-prefixed active endpoint is authoritative; stale enabled flags do
  not launch another device in the same protocol/transport family.
- Raw BLE identifiers appear as secondary detail, not the primary identity.
- Station control chips and saved-connect actions lead with the saved
  connection label; the advertised device name stays in secondary detail and
  tooltips.
- Health and action state match one physical saved endpoint. A contradictory
  advertised device name cannot inherit Connected or Needs attention through
  a duplicated legacy adapter id.

### Responsive connection editor

- The BLE device id/advertised name, scan action, scan timeout, and connection
  status receive separate responsive rows.
- Normal/wide mode may use two columns. Compact/Large Text mode stacks label and
  control pairs vertically.
- Content height is recalculated after protocol/transport changes; no outer
  panel uses fixed height for dynamic content.
- The scan result list is visible whenever a scan has results or is active. It
  must not depend on toggling the scan checkbox a second time.
- Saved-device selection and exact connection state appear first, discovery
  and `Use Device` second, and transport/device identifiers in a collapsed
  Advanced disclosure. Channel administration uses the saved connection label;
  the internal adapter id is diagnostic tooltip content only.

### Scan, connect, and reconnect lifecycle

- Device scan is an explicit cancellable background request. The first visible
  `Scanning…` state appears within 100 ms and includes elapsed/remaining time.
- Saving a new connection updates the local list immediately. Runtime restart is
  queued after the form transaction and cannot blank or rebuild the Station
  Control Bar.
- A saved device reconnects by stable id first and advertised name second. A
  missing device moves to capped exponential backoff with `Retry now`; it does
  not continuously scan.
- Resize, navigation, and close remain responsive during scan/connect/channel
  work.
- The MeshCore scan/results workflow stays name-clear: scan rows show the
  advertised device as the selectable item, while the saved connection label is
  the primary identity in the control bar and connect menu.
- OS-neutral primary guidance is used: `Bluetooth settings for this computer`.
  Platform-specific detail is shown only after runtime platform detection.

### Channels

- `Refresh channels` sends a real device request; it does not merely reread the
  FIO channel-policy table.
- Channel discovery is operator-requested by default. Idle runtime polling must
  not issue a recurring full channel-slot sweep; an explicitly injected poll
  cadence is reserved for tests or a future protocol capability that requires
  it.
- MeshCore contact discovery is bounded to a conservative five-minute default
  cadence and does not run on the first timer tick after connection. Passive
  event/message receipt and health monitoring remain active between contact
  refreshes.
- Channel discovery is incremental and cancellable. Each result may be staged as
  it arrives; a nonresponding index cannot monopolize the worker.
- The adapter uses a protocol-aware stop condition or bounded count supplied by
  device capability. A sequence of 32 full timeouts is prohibited.
- The channel surface shows device state and FIO policy separately:
  channel/index, privacy/key readiness, device membership, accept/ignore,
  category, retention, last seen, and sync status.
- `Configure` edits supported device channel fields only after an explicit
  review. Secret material is never displayed or logged.
- `Remove from FIO` removes/archives the FIO policy and retained publication
  mapping without silently modifying the radio device.
- `Remove from device` is a separate confirmed action, visible only when the
  adapter and protocol safely support it. Unsupported devices explain that the
  change must be made with the device's companion tool.

### Mesh acceptance

- Selecting MeshCore never produces a Meshtastic-prefixed untouched name.
- BLE fields are readable at 900x560 Normal and Large Text on Linux/macOS.
- Add/save/scan/reconnect/channel refresh never hides the top control bar or
  blocks resize for more than 100 ms.
- Saved mesh chips, connect actions, and connection indicators use the saved
  connection label first so device names do not appear mixed or duplicated in
  the station rail.
- A slow/nonresponsive BLE device can be cancelled and FIO exits cleanly.
- Reconnecting does not require a discovery-checkbox workaround.
- Channel additions, FIO removal, supported device configuration/removal, and
  restart persistence are covered by adapter-contract and UI tests.

## FIO Managed BBS Remediation

### Product model and navigation

There is one station-owned **FIO Managed BBS**. Its catalog, logical locations,
access rules, retention, and publication mappings are shared. In cluster mode,
the same logical BBS model is available to every BBS node according to synced
state and permissions.

The administration surface moves out of selected-radio settings and becomes a
first-class station workspace reachable from Station and from Messages `+BBS`.
Selected-radio VarAC settings retain only:

- VarAC installation/runtime paths and connection settings;
- that radio's live materialization directory;
- radio/instance enablement and health;
- a link to `Manage FIO BBS`.

### Graphical publication workspace

The default view is a tree/list hybrid:

- left: logical BBS location tree with access and retention badges;
- center: bounded artifact list for the selected location;
- each artifact has a visible publication checkbox;
- right/detail drawer: origin, exact path, size, age, modified time, access,
  retention/expiry, and publication health.

Checking a box adds or enables a location-artifact mapping. Unchecking disables
the mapping and ends publication; it does not delete the source file. The same
component is used by BBS administration, preview, and the Messages `+BBS`
workflow. Visitor Preview shows the effective permission-filtered tree, not a
plain-text dump.

Age is shown as whole days (`0d`, `1d`, `24d`) with the exact local/UTC modified
time in detail/tooltip.

### Retention and source reconciliation

- Each location has `manual`, `global default`, or `expire after N days`
  retention. The resolved retention policy and expiry timestamp are queryable,
  not hidden only in opaque metadata.
- At expiry, the mapping is disabled with reason `retention_expired`. The source
  artifact is not deleted unless a separate explicit purge policy authorizes it.
- Manifest reads exclude disabled, expired, deleted, and missing artifacts.
- A lightweight reconciler stats only currently publishable file-backed rows in
  bounded batches. A missing source becomes `missing` and is unpublished
  without throwing repeated errors.
- Reappearance does not silently republish an artifact the operator unchecked.
  The system distinguishes `operator_disabled`, `retention_expired`, and
  `source_missing`.
- Folder discovery may add new artifacts but may not delete and recreate all
  mappings or reset `publish_enabled` to true.

### Visitor helper contract

- Helper content is one screen and uses short imperative language.
- Every location advertises the exact command needed to request/source it.
- The obsolete `wait 10 seconds` wording is removed. Runtime state instead says
  `Request sent—refresh when the updated listing is ready` when asynchronous.
- Visitor-facing labels omit `.txt` for logical commands/notices.
- Before changing generated filenames, an adapter test verifies what VarAC
  accepts. If an extension is technically required, FIO keeps it only in the
  generated compatibility filename while the UI presents the clean logical
  label.

### BBS acceptance

- One edit controls publication across all configured radio BBS instances.
- Tree, access, publication checkbox, file age, retention, and missing-source
  state are visible without opening radio settings.
- Unchecking immediately removes the item from the next effective manifest;
  source content remains intact.
- Expired or deleted-on-disk files stop publishing without error loops.
- A location rescan never re-enables an operator-disabled mapping.
- Messages `+BBS` and BBS administration show identical checked locations.

## Messages And FIOSpotter Remediation

### Relevant JS8 traffic

FIO incrementally ingests JS8 directed traffic when either condition is true:

- destination is the user's current callsign or an effective historical alias;
- destination is a JS8 group configured by the user or an operating group with
  which the user is explicitly associated.

This includes conversational or free-form directed traffic even when it is not
stored as a literal JS8 inbox message. Heartbeats, heartbeat acknowledgements,
and pure SNR/signal reports are excluded. Source radio/instance, received time,
event time, sender, destination, path, and original text are retained.

The implementation uses a byte/record checkpoint over the directed log and
indexed upserts. Dedupe identity prevents one transmission appearing twice when
it is also available from the JS8 inbox or FIOSpotter. Rotated/truncated logs
are detected without forcing a startup-wide historical parse.

### Inbox selection and sorting

- The checkbox header is always visibly recognizable as select-all.
- Its scope is the currently filtered result set or current loaded page; the
  tooltip and bulk-action summary name that scope before a destructive action.
- Empty/unsafe scope renders a disabled checkbox rather than removing the
  affordance.
- Initial/default order is newest first using received time, then event time,
  then stable id. Opening another focus or changing filters restores this order
  unless the operator explicitly chose another sort during the current session.
- The active sort indicator remains visible.

### CommStat semantics

CommStat uses distinct fields and columns:

- `Read`: New/Read, owned by FIO inbox state;
- `Report`: Green/Yellow/Red/unknown, owned by StatRep content;
- `Summary`: concise message content and exceptional condition;
- `From`, `To/Group`, `Age`, `Source`, and actions.

The redundant generic `Kind = CommStat` column is removed from CommStat focus.
`Source` distinguishes direct CommStat RF evidence from a SitRep-derived or
other aggregation. Operational report severity never changes when a message is
marked read.

### FIOSpotter administration

Add a station-scoped **FIOSpotter Watches & Alerts** workspace reachable from
Messages tools and Settings. It owns:

- callsign, group, topic/keyword, status, and location watch rules;
- enablement, priority/severity, notification behavior, source/radio scope, and
  optional expiry;
- concise last-match, match-count, and health state;
- add/edit/disable/delete/test actions;
- staged import/review from SuperSpotter/original JS8Spotter data where
  compatible.

Known operators and their explicitly associated groups populate autocomplete
and bulk-selection choices. They do not silently create enabled watches. The
same rule service supplies ingestion alerts, Inbox focus, Ops Center attention,
and Map pins so different screens cannot disagree.

### Message performance acceptance

- A 100,000-row retained corpus does not load into Python/UI memory on tab open.
- Current-window counts and first page use indexed queries; historical detail is
  paged.
- Incremental source projection handles only changed rows and does not project a
  fixed 5,000 rows per source during startup.
- Relevant directed JS8 fixtures are ingested once; heartbeat/SNR fixtures are
  excluded; rotation and delayed traffic are covered.
- CommStat read state, report severity, summary, and source remain independent
  through mark-read, filter, restart, and reprojection.

## Launch Control Remediation

### Persistence model

Each radio profile owns a versioned launch bundle containing ordered app rows:

- app identity;
- configured/enabled;
- launch at FIO startup;
- monitor health;
- command/path override;
- dependency/readiness policy;
- update timestamp.

Station custom tools remain reusable definitions; a radio bundle references or
overrides them. Existing global `launch_control_items` migrate once to the
default/selected legacy radio with a migration audit. They remain read-only
fallback until migration is confirmed, then are no longer a write target.

### One planner for startup and manual launch

One `StationLaunchPlanner` composes launch requests from active radio bundles.
Automatic startup and `Start Startup Apps` call the same planner:

- startup selects startup-enabled apps from every active, launch-enabled radio;
- manual start may scope to one selected radio;
- identical executable/process identities are deduplicated;
- radio-specific command/path/API-port differences remain separate instances;
- dependencies and configured order are honored;
- result status names the radio(s) and app instance.

Save writes the radio bundle transactionally before the UI reloads it. Switching
radios or restarting FIO must show the saved checkbox/order state.

### Launch performance acceptance

- Toggling or saving a launch checkbox never performs a forced synchronous
  process snapshot.
- Runtime status comes from the shared cached dependency-status service and
  refreshes asynchronously.
- Automatic startup launches the same app set the review UI previews.
- Manual and automatic paths pass the same planner-contract tests.
- Two radios with a shared app launch it once; two distinct configured instances
  launch separately.

## SOP Builder Remediation

SOP Builder is a first-class builder, not a hidden utility or a spreadsheet.
The default workflow is vertically readable and answers what action is being
built, under which conditions, and what Ops Center will tell the operator.

### Layout contract

- Entire workspace is vertically scrollable at all supported window/text sizes.
- Top band: SOP selector, status, primary Save/New actions, compact context.
- Main column: stacked action cards. Each card exposes, without horizontal
  scrolling:
  - group and condition levels;
  - trigger/source;
  - action and resource/tool;
  - band/frequency or route;
  - start, duration, interval;
  - contact type/target;
  - description;
  - conflict state and remove/duplicate actions.
- Wide mode may place labels/fields in multiple columns within a card. Compact
  mode stacks them; it never compresses controls below usable widths.
- Conflict summary is concise by default. The workbench opens as a responsive
  drawer/panel with its own scroll and bounded table/model.
- ControlFreq/Ops preview remains visible as a compact summary and expands for
  detail.

### Advanced mode

The existing wide table is not required for the primary workflow. If retained,
it is explicitly labeled `Advanced bulk editor`, hidden behind an Advanced
action, and is not needed to create, edit, validate, or save any action. The
card editor and advanced editor share one action model; neither scrapes state
from the other's widgets.

### SOP acceptance

- Every action field and validation/conflict is available in the stacked card.
- At 900x560 and 1000x700, every primary control is reachable by vertical scroll
  and no primary workflow requires horizontal scroll.
- Adding actions does not grow an unbounded widget tree in one paint; card
  virtualization/collapsing is used for large SOPs.
- Save/validation does not rebuild every card unless its model changed.
- Light/Dark and Normal/Large Text visual tests cover empty, populated, invalid,
  and conflict-heavy SOPs.

## Plan Builder Remediation

Plan Builder follows the existing source-first contract but receives a complete
responsive pass:

- compact top band for plan identity and ingredient chips;
- effective timeline/schedule remains the dominant surface;
- source controls stay with the surface they control;
- selected-window details and RF Guard use drawers below or beside the timeline;
- wide mode uses a split layout; compact mode stacks sections in task order;
- toolbars may internally scroll, but the main task does not depend on a clipped
  horizontal table;
- table/model sizes are bounded and rebuilds occur only when plan projection
  input changes.

Acceptance covers empty, one-plan, multi-source, RF-conflict, and long-label
states at 1920x1080, 1000x700, and 900x560 with Normal and Large Text in Light
and Dark themes.

## Roster Import Remediation

### Supplied roster result

The current parser reports `166 imported, 22 skipped`. Review of the supplied
CSV confirms that no valid operator row was skipped.

- Blank/separator rows ignored: CSV lines 18, 21, 30, 65, 86, 109, 125, 139,
  155, and 177–185 (18 rows).
- Section/legend rows ignored: line 186 `New additions`, line 187 `* = Signal
  only`, line 188 `C.S. Change`, and line 189 `Limbo` (4 rows).
- Invalid operator rows: 0.
- Imported operators: 166.
- Detected child groups: MR01 through MR10.

### New result contract

Import preview and completion distinguish:

- operator rows imported;
- existing operators updated;
- blank rows ignored;
- section/legend rows ignored;
- invalid operator rows skipped;
- warnings by CSV line, callsign text, field, and reason.

Only a row that appears intended to be an operator record but fails validation
is called `skipped`. Blank and recognizable section/legend rows are `ignored`.
The preview offers a copy/export diagnostics action before commit. Parser tests
use this exact roster shape plus invalid callsign, duplicate, missing-header,
and mixed-delimiter fixtures.

## Delivery Slices And Gates

Implementation must proceed in this order so UI work is not built on blocking
or incorrectly scoped services.

### Slice 0 — Measurement, refresh coordination, and clean lifecycle

Implementation result (2026-09-06): **automated exit gate passed**. The detailed
baseline and acceptance evidence is recorded in
`slice0_performance_baseline_2026-09-06.md`.

- Startup now emits first-usable-shell and per-component construction spans.
- The initial shell keeps Ops Center, Settings, the Station Control Bar, and its
  SOP context eager; secondary schedules, NCS, Messages, Plan, Map, operator,
  station-detail, peer, and Help workspaces retain stable lazy slots.
- Routine Settings status rendering consumes cached immutable dependency
  snapshots. Endpoint scopes coalesce on one worker, forced process refreshes
  obey single-flight, and routine global process state no longer waits on a JS8
  endpoint capability probe.
- Process inventory inspects cheap process names first and only requests
  executable/command detail for direct targets or known wrappers.
- Shared cancellation and shutdown-registration contracts now cover dependency
  refresh, background ingest, the UI watchdog, and owned Qt worker threads.
- The read-only performance report and isolated Qt soak harness are automated
  and covered by focused tests.
- No schema or data migration was introduced in Slice 0.

On a SQLite-consistent clone of the production-sized databases, measured on the
macOS development host with external radio/Mesh/launch/ingest side effects
suppressed, first usable shell was 4.219 seconds, main-window construction was
2.565 seconds, and shutdown was 16 ms. Seven forced uncached process inventories
ranged from 11.1 to 19.4 ms. The 30-minute offscreen soak completed 17,932 event
loop samples and 871 operator-paced interactions with 33.7 ms maximum lag and
156 ms shutdown, without Qt timer/thread warnings. Physical Linux production
confirmation remains part of release/platform validation; it is not represented
as having been run on this macOS host.

Deliverables:

- startup construction trace by screen/service;
- shared cached dependency/process snapshot;
- GUI-thread callback guard/telemetry;
- cancellable worker operation contract and shutdown registry;
- first-usable-shell gating with heavy screens/services deferred;
- automated performance corpus and baseline report.
- bounded offscreen Qt soak/acceptance harness for safe navigation/layout
  interactions, event-loop lag sampling, and shutdown timing.

Exit gate: shell and shutdown budgets pass; no event-loop stall in the
`tools/gui_slice0_soak.py` 30-minute idle/interaction soak (configurable
shorter for CI). No domain behavior changes beyond scheduling/caching.

Risk: **high**, because it changes lifecycle and refresh ownership.

### Slice 1 — Mesh lifecycle and administration

Deliverables: responsive editor, protocol-derived name, explicit scan lifecycle,
cancellable channel sync, saved-device reconnect, channel configure/remove, and
platform-neutral guidance.

Implementation record (2026-09-06):

- Saved connection identity now separates protocol, operator-facing name,
  stable adapter/device id, advertised BLE name, and optional source
  radio/role. New untouched names follow the selected protocol and manual names
  are preserved.
- The connection editor uses responsive rows for BLE id, advertised name,
  timeout, scan state, and results. Scan begins visibly, runs off the GUI
  thread, can be cancelled directly, and keeps its protocol/transport context
  fixed until completion.
- Mesh operations publish immutable lifecycle snapshots. Connection retries use
  capped exponential backoff with a manual retry path; runtime replacement waits
  for the old worker to finish and explicit Disconnect cannot inherit a queued
  restart.
- MeshCore channel discovery yields/stages channels incrementally, stops after
  the protocol's bounded eight slots, skips unused capacity without staging
  phantom feeds, and supports cross-thread cancellation. Sparse configured
  slots remain discoverable. The legacy 32-full-timeout behavior is removed.
- The channel administration surface separates device facts from FIO policy,
  including review, category, retention, mapped groups, and Inbox/Ops/Map/topic
  scope. Remove from FIO archives the policy without touching the device.
  Device configuration/removal is capability-gated; removal requires host
  confirmation and unsupported adapters direct the user to their companion
  application. Secret material is not rendered.
- The Station Control Bar remains mounted during scan, save, worker restart,
  and cancellation. Settings edits no longer query and rebuild the channel
  inventory on each keystroke.
- No database or destructive data migration was required. The saved-connection
  JSON fields are additive and legacy records remain readable; the existing
  channel-policy table stores the expanded policy behavior.

Automated acceptance passed with 2,418 tests and 37 environment-dependent
skips. This includes protocol/name compatibility, 900x560 Normal/Large Text
geometry, a simulated five-second BLE stall with sub-100-ms acknowledgement and
resize assertions, cancellation, retry/backoff, restart persistence, channel
capability/configuration/removal contracts, FIO-only archival, incremental
results, top-bar integration, clean worker shutdown, and all repository
regressions.

Live macOS follow-up found the advertising MeshCore Companion
`MeshCore-N1MAG MOBL1` in 10,234.2 ms, with stable CoreBluetooth id
`97C92879-047E-FEA8-7A11-8A2EE82B381D`, RSSI -66, and the Nordic UART service.
This validates real-hardware discovery and stable identity capture. Connection
then reached the macOS encrypted-pairing boundary and failed with CoreBluetooth
error 15 (encryption timed out). The host Bluetooth trace reported `lePaired 1`
and `isPairing=0`, proving that macOS was reusing a stored bond rather than
presenting a new PIN prompt; the device rejected those stored keys. FIO now
recognizes this error signature and gives explicit stale-bond recovery guidance.
Reconnect and channel administration remain unvalidated until the host/device
bond is reset. The FIO retry remained backgrounded and the Station Control Bar
stayed visible. The physical exit gate remains open until the remaining matrix
passes on macOS and the 1920x1080 Linux production host:

A subsequent successful PIN re-pair reached Companion receive and returned the
device's configured channels. That run exposed four additional integration
defects which are now covered by the implementation and automated regressions:

- empty MeshCore capacity slots were being normalized as pending `Channel N`
  feeds; refresh now scans the bounded eight-slot range, skips empty capacity
  while preserving sparse configured slots, retained legacy phantom rows are
  hidden without a destructive migration, and known device channels sort ahead
  of FIO-only staged channels;
- the idle one-second worker tick incorrectly sent `SYNC_NEXT_MESSAGE` even
  without a firmware waiting-message notification; receive now performs zero
  command writes while idle and drains only after the push signal;
- an older connected health row could outrank a newer failure for the same
  physical device after its adapter identity changed; both the Station Control
  Bar and source-control read model now use the newest matching observation;
- Disconnect discarded runtime ownership before its worker thread finished,
  allowing an immediate Reconnect to become a no-op or overlap shutdown;
  ownership now remains until the matching thread's finished signal and a
  pending reconnect starts only afterward. BLE service/notification setup must
  complete before health may report connected.

An empty, cancelled, or failed repeat scan now retains its last discovered
device and the device action. This preserves recovery access after a
connection failure rather than forcing the operator through another successful
scan merely to reveal the action. The host again reported CoreBluetooth error
15 after the successful session, so live reconnect remains open; the next test
must keep any phone/tablet MeshCore companion client disconnected while
forgetting and re-pairing the computer bond.

A second forget/re-pair run presented the PIN prompt (twice; the operator may
have mistyped the first entry) and ultimately showed the saved device connected
in the Station Control Bar. The Mesh Settings status nevertheless remained at
Needs attention after a refresh, and Disconnect followed by the explicit
Connect action did nothing. Runtime review found two independent UI/lifecycle
faults: worker health updated the control bar but was not delivered to the Mesh
Settings status, and explicit Connect used the configuration-change restart
guard even though Disconnect intentionally preserves the unchanged saved
configuration. Mesh Settings now consumes deduplicated live health transitions,
and explicit Connect forces an ordered runtime restart. The channel model was
also corrected: MeshCore administration lists only its actual Public/hashtag/
private channels; contact/direct-route facts remain message metadata and do not
create a synthetic Direct channel. These corrections require one more physical
reconnect/status retest before the exit gate can close.

Post-follow-up automated verification totals 2,429 passed assertions and 37
environment-dependent skips across fresh-process repository partitions, plus a
dedicated 138-test Mesh/lifecycle set and 157-test Station Control Bar/shell
set. The monolithic process still reproduces the separately documented macOS
Qt teardown crash after 68 percent; the only partition that returned 139 did so
after reporting all 802 assertions passed. This does not close the physical
Mesh gate.

The reconnect/status/channel-model correction passes 141 focused Mesh,
Settings, lifecycle, and source-control tests. The expanded Mesh plus adaptive
shell/control-bar regression set passes 298 tests. New assertions cover the
real station-command Connect path with an unchanged saved signature, live
Settings health transition, absence of a synthetic MeshCore Direct channel,
preservation of direct-route metadata, and sparse channels after an unused
slot.

The following live restart exposed a separate saved-device recovery gap. FIO
was not losing MOBL1: an isolated scan found the exact configured
`97C92879-047E-FEA8-7A11-8A2EE82B381D` / `MeshCore-N1MAG MOBL1` identity at
approximately -52 dBm with the Nordic UART service. Both a direct saved-id
connection and a connection using the freshly scanned BLE device object failed
before GATT setup with `CBErrorDomain Code=14 "Peer removed pairing
information"`. FIO must retain that raw diagnostic in logs and must not
misclassify it as device absence. Live macOS Bluetooth tracing independently
confirmed `lePaired 1`, successful LE/GATT establishment, encryption status
706 (`peer removed keys`), and the resulting security disconnect on repeated
attempts.

The corrected recovery contract is:

- direct Disconnect followed by Connect is the preferred healthy path;
- a device-specific failure that recovers after one clearly requested card
  restart and one explicit Retry/Connect is an acceptable degraded path, but
  FIO must explain the action and must not retry indefinitely;
- normal disconnect, card restart, app restart, and manual reconnect must not
  require the computer to forget/re-pair the device; requiring Forget/Pair or
  firmware recovery fails the ordinary operator workflow;
- a non-terminal saved-id open failure gets one bounded rediscovery by exact
  saved id/name and one retry using the live discovered device object, then
  returns to capped background backoff without rescanning on every retry;
- the MeshCore BLE Found row and Scan/Use Device controls remain visible even
  with no results or a failed connection; `Use Device` persists that selection
  and requests connection immediately;
- `Connect Saved Device` and `Disconnect` remain available independently of
  scan results, and Disconnect also stops the failed connection's retry loop;
- explicit CoreBluetooth Code 14 / peer-removed-pairing state is terminal for
  automatic rediscovery because the card has rejected the host keys. Standard
  macOS CoreBluetooth has no application unpair API, so FIO explains re-pairing
  only as last-resort recovery for that explicit state, never as normal
  reconnect behavior.

This follows mesh-client's direct-then-bounded-scan lifecycle where it can help;
mesh-client's Noble backend ultimately uses the same macOS CoreBluetooth stack
and cannot silently repair Code 14 either. Automated correction verification
passes 144 focused Mesh/Settings/lifecycle tests and 301 expanded Mesh,
adaptive-shell, and Station Control Bar tests. The full monolithic suite again
hit the separately documented Qt teardown crash at 68 percent without a prior
assertion failure. The physical gate remains open until the external Code 14
state is repaired once and the normal no-repair restart/reconnect matrix passes.

Live `Use Device` integration also established that action payloads must carry
the stable `mesh_connection_config_key` rather than the friendly adapter id.
The initial implementation persisted the selected endpoint but then failed
activation lookup with `saved mesh connection meshcore-mobl1 was not found`.
Both Use Device and Connect Saved Device now emit the stable
protocol/transport/endpoint key; regression coverage validates that boundary.
This lookup failure is independent of the later CoreBluetooth Code 14 result.

The next physical run established a sharper lifecycle boundary. A fresh
forget/pair/PIN sequence connected successfully and delivered sustained GATT
indications. The operator then used FIO Disconnect and immediately used
Connect: the control changed from gray to yellow, CoreBluetooth reached BLE and
GATT connection, then encryption failed with status 706 (`peer removed keys`).
macOS still reported `lePaired 1`, so it correctly did not prompt for a PIN.
This confirms that routine reconnect must never depend on repeated PIN entry,
while the current host/card bond is independently inconsistent after the first
session.

FIO now adds a process-wide MeshCore BLE session barrier modeled on the useful
ownership boundary in mesh-client. Disconnect must finish notification cleanup,
the raw BLE disconnect, and BLE event-loop thread teardown before a replacement
client may open. If teardown outlives its bounded foreground wait, ownership is
retained by a background completion guard and Connect reports `Bluetooth is
still disconnecting` instead of overlapping clients. A passively dropped link
also retires its old Companion wrapper and loop before retry. Lifecycle logging
records session acquisition, Companion readiness, disconnect request, teardown
completion, and elapsed time. This hardening prevents an FIO overlap from
contributing to bond instability; it does not claim to repair a card which has
already discarded its keys. The post-hardening automated gate passes 148
focused Mesh/Settings/lifecycle tests and 305 expanded Mesh, adaptive-shell,
and Station Control Bar tests.

The first physical run with that barrier proved teardown was not causal. The
initial paired session reached Companion-ready state; FIO then completed
notification, native disconnect, loop shutdown, and ownership release in 82.4
ms. Eight seconds later a new worker acquired ownership and opened a fresh
CoreBluetooth/GATT session. Encryption alone then failed with Code 14/status
706 because the peer no longer held the stored key. No second FIO or
mesh-client process was active, and the cleanup order matches mesh-client's
unsubscribe-then-disconnect path.

This signature is a terminal operator-attention state, not a transient outage.
FIO therefore suppresses automatic retries after Code 14; an explicit Retry
Now/Connect permits one new attempt and re-blocks if the terminal result
returns. This prevents misleading yellow churn and repeated futile BLE opens.
The terminal-retry correction raises the focused Slice 1 gate to 150 tests and
the expanded Mesh/shell/control-bar gate to 307 tests.
The physical device is a Seeed Studio SenseCAP T1000-E running an operator-
reported Companion 1.17.0 or 1.17.1 build. MeshCore issue #3183 documents a
T1000-E Bluetooth failure introduced or exposed on 1.17.0; its reporter
recovered only after a full nRF52 SoftDevice erase, firmware reload, and key
restore. The official 1.17.0-to-1.17.1 source diff contains no T1000-E,
nRF52 BLE, bonding, or framework change, so 1.17.1 is not a documented repair
for this failure. The 1.17.1 nRF52 implementation explicitly uses bonded MITM
security and its normal disconnect callback clears only live connection state
before restarting advertising; it does not delete the saved bond. A later
unmerged pull request (#3263) times out a connection that never becomes secured,
but does not address a previously completed bond later rejected with Code 14.
These upstream boundaries support treating the observed failure as card-side
bond/storage state while retaining FIO's serialized teardown and terminal-
retry protections. A firmware erase/reload is a destructive recovery option,
not normal operation, and requires explicit operator approval plus a device
configuration/key backup.

A subsequent Forget Device operation prompted immediately for a PIN. macOS
then reported successful pairing, encryption, and paired-device-cache storage;
FIO reached Companion-ready state and received sustained GATT indications.
This reconfirms that discovery and first-pair connection work. The physical
gate still requires a controlled FIO Disconnect followed by one Connect, with
no scan, card restart, or Forget Device between them.

That controlled test failed at the device-security boundary on 2026-09-06.
FIO completed its disconnect and BLE event-loop teardown in 41.9 ms. Twelve
seconds later the single explicit Connect acquired a new session and reached
the saved T1000-E, but the card again rejected the stored bond with
CoreBluetooth Code 14. FIO immediately published `needs-attention`; no scan,
device restart, Forget Device, second client, overlapping worker, or automatic
retry participated. The terminal retry suppression remained quiet beyond the
former five-minute retry interval. The macOS physical gate therefore does not pass: FIO lifecycle
and operator-attention behavior meet their contract, but ordinary reconnect is
blocked by the T1000-E 1.17.x bond state. Slice 2 must not begin until an
operator-approved device backup/recovery path restores normal bonded reconnect
and the matrix below is rerun.

1. Start Scan and confirm visible acknowledgement, resize/navigation response,
   and continuous Station Control Bar visibility.
2. Cancel one scan, then scan again, choose Use Device, and confirm the device
   is persisted, connection begins, and the generated name is MeshCore-prefixed.
3. Restart FIO and reconnect by the saved id/name without rescanning; exercise
   missing-device backoff and Retry Now once. After one successful initial PIN
   pairing, first test direct Disconnect then Connect. If that fails, test one
   card restart followed by one explicit Connect. App/card recovery must not
   require another PIN prompt or a system-level Forget Device action; record
   direct reconnect as healthy, restart-assisted reconnect as acceptable
   device recovery, and Forget/Pair as failure.
4. Refresh channels, confirm progressive results and policy persistence, then
   verify Remove from FIO leaves the device unchanged. Capability-gated device
   actions must either work after confirmation or remain disabled with companion
   guidance.
5. Close FIO during scan or channel refresh and confirm exit within three
   seconds with no Qt timer/thread warnings.

A two-device macOS follow-up with MOBL1 and a MOKO SMART LW010-R advertising as
`MeshCore-N1MAG MOBL2` exposed a saved/runtime identity collision: both physical
endpoints had been retained with `meshcore-mobl1` as their connection and
adapter identity. The Settings tab persisted through its own settings manager,
while MainWindow attempted immediate activation from a stale in-memory cache;
this explains why a scan could show `Use Device` yet the following Connect was
a no-op until restart. Health matching by the shared adapter id also allowed
the selected MOBL1 editor and connected MOBL2 runtime to appear together.

The correction keeps both physical endpoints, assigns a unique internal adapter
id to the second endpoint during non-destructive normalization, reloads the
MainWindow settings cache before resolving the emitted endpoint key, and starts
only the protocol-prefixed active endpoint. No database/schema migration or
record deletion is performed; corrected identities persist on the next normal
activation/save. The Local Mesh screen now leads with a saved-device selector,
exact status and Connect/Disconnect actions; Scan/Use Device is immediately
below; Advanced connection details are collapsed; and the channel header uses
the friendly saved label. The control-bar menu distinguishes `Connected:` from
available `Connect:` rows and names the device in Disconnect.

Primary integration verification passes 156 focused Mesh/Settings/lifecycle
tests and 313 expanded Mesh, adaptive-shell, and Station Control Bar tests.
Offscreen visual review covered two colliding saved identities at 1200x900 and
compact 900x650 Large Text. The physical exit gate remains open for a restart
into this build and the five-step hardware matrix above; Slice 2 has not begun.

MOKO SMART LW010-R / `MeshCore-N1MAG MOBL2` now supplies the healthy macOS
reconnect baseline. After the initial PIN completed, Companion became ready at
18:49:28. A later direct Disconnect completed its notification/native BLE
teardown in 97.7 ms; Connect acquired the saved endpoint six seconds later and
reached Companion-ready in approximately one second. No scan, Use Device, card
restart, or Forget Device was required for that reconnect. This passes the
direct bonded reconnect behavior for MOBL2 and confirms that the T1000-E Code
14 result is device/firmware-specific rather than the general FIO BLE lifecycle.
The overall Slice 1 exit gate still requires the remaining channel, close, and
Linux hardware checks.

Final automated hardening removed unsolicited device work from the idle path.
Production workers no longer perform a recurring channel-slot sweep; channels
are read only after the operator chooses Refresh. Contact discovery now waits
five minutes after connection and repeats at that conservative cadence, while
passive indications, messages, health, and reconnect handling continue normally.
An explicit test-only channel cadence remains injectable. A cancelled channel
refresh now preserves its discovered count, emits a normal cancelled terminal
snapshot, and cannot publish a false capability/complete result or expected-error
noise after a slow adapter returns.

The channel UI acknowledges Refresh immediately, exposes Cancel before the
device responds, disables duplicate cancellation, and prevents incremental
policy totals from replacing live refresh/cancel feedback. Tooltips distinguish
FIO-only Accept/Ignore/Remove actions from capability-gated device changes.

Primary integration verification passes 46 focused Slice 1 tests and 317
expanded Mesh/Settings/adaptive-shell/Station-Control-Bar tests. The full
repository assertion gate passes in fresh-process partitions: 2,451 passed and
37 environment-dependent skips. The known monolithic macOS Qt teardown crash
reproduced at 68 percent in `log_viewer.py` after no assertion failure; all
partitions, including the complete 824-test L-M partition, exited cleanly.
Compilation and `git diff --check` pass. Offscreen visual review covers
900x560 Normal and Large Text plus 1200x900, including the scrolled channel
actions. This completes implementation and automated acceptance only; current-
build macOS channel/close checks and the Linux hardware matrix are still required
before the Slice 1 exit gate can close or Slice 2 can begin.

Exit gate: Linux and macOS hardware test plus simulated timeout/cancel/restart
tests pass; Station Control Bar remains continuously visible.

Risk: **high**, due BLE/platform concurrency and shutdown behavior.

### Slice 2 — Station-owned BBS

Deliverables: schema migration for explicit retention/state, station navigation,
tree/checkbox publication component, missing-source reconciler, helper rewrite,
and thin per-radio VarAC adapter settings.

Implementation status (2026-09-06): complete. The operator explicitly
authorized Slice 2 to proceed while the independent Slice 1 Mesh hardware gate
is deferred until the next production-hardware session. This exception changes
delivery sequencing only; it does not close or waive the Slice 1 exit gate.

The station catalog is schema version 2. Startup performs verified,
backup-first and idempotent schema/legacy-ownership migrations. Existing
radio-owned locations are copied as a union with station rows taking precedence;
legacy profile values remain for rollback. Source presence, operator intent,
retention expiry, and location enablement are modeled independently. The
background service reconciles missing sources and expiries in bounded batches.

`Station > Managed BBS` now owns the graphical location tree, access and
retention policy, Add/Edit/Disable workflow, bounded newest-first artifact view,
publication checkboxes, and artifact health/detail. Compact layout stacks the
tree and list and makes detail opt-in so 900x560 Large Text preserves the main
workspace. Visitor Preview filters the effective tree by caller visibility and
associated callsigns and makes publication controls read-only. Location access
supports public, callsign, access-code, and combined rules; new codes are salted
and hashed. Radio Settings retain only radio paths, the live BBS adapter, inbound
guard, and a link to the station workspace. Messages `+BBS` uses the identical
station locations and atomically replaces memberships, including uncheck-all,
without copying or deleting the received source file.

Each radio live directory has a distinct manifest keyed to its resolved path,
so one catalog projects independently through one or multiple VarAC instances.
Compatibility callers without an explicit database identity remain on the
folder-backed path and cannot accidentally read the process-global catalog.
Visitor helper labels are extensionless in the UI while on-disk compatibility
files retain `.txt`; fixed-delay instructions were replaced by asynchronous
refresh guidance.

Exit gate: migration is backup-safe/idempotent; one catalog publishes correctly
through one and multiple radio instances; expiration and missing-file tests pass.

Exit-gate result: passed in automated acceptance. The focused BBS contract set
passes 130 tests with one environment skip; BBS, navigation, Settings, and
background-service integration adds 42 passes with five environment skips.
Version migration, ownership import, rollback-on-backup-failure, two-radio
projection, operator-disabled preservation, expiry, missing/restored source,
atomic checkbox membership, and compact UI tests all pass. A synthetic 10,000-
mapping/200-row bounded administration query measured p50 2.59 ms, p95 2.77 ms,
and max 2.82 ms on the development Mac. Light/Normal 1200x800 and Dark/Large
Text 900x560 were visually reviewed. The full fresh-process regression partitions
are recorded in the work log; the final BBS/Settings/navigation refinement set
passes 282 tests with one environment skip.

Risk: **high**, due persistence migration and external file publication.

### Slice 3 — Message ingestion, semantics, and FIOSpotter

Deliverables: incremental relevant-directed JS8 ingest, unified dedupe, bounded
query/model path, always-visible select-all, newest-first stability, separated
CommStat fields, and FIOSpotter watch administration.

Exit gate: production-scale corpus meets budgets and all source/read/severity
contracts pass across restart/reprojection.

Risk: **high** for ingest/dedupe and **medium** for presentation/admin UI.

### Slice 4 — Radio launch bundles

Deliverables: radio-owned persistence/migration, shared startup/manual planner,
dedupe and instance rules, cached status, and restart tests.

Exit gate: checkbox/order state persists per radio and startup matches preview.

Risk: **medium-high**, due migration and external process control.

### Slice 5 — SOP and Plan responsive builders

Deliverables: action model separated from widgets, stacked SOP cards, optional
advanced editor, responsive conflict/preview panels, and Plan Builder compact
reflow.

Exit gate: supported viewport/theme/text matrix passes visual and interaction
tests; no workflow field is lost.

Risk: **medium** after model separation; **high** if attempted as direct widget
rearrangement without it.

### Slice 6 — Roster diagnostics and final integration

Deliverables: classified import results, row-level preview/export, exact roster
fixture, cross-feature regression, updated help, and migration/recovery guide.

Exit gate: supplied file reports 166 imported, 18 blank ignored, 4 label rows
ignored, 0 invalid; full performance/platform regression passes.

Risk: **low** for roster reporting, **medium** for final integration.

## Test And Release Strategy

Each slice requires:

- Qt-free unit tests for policy, parsing, planning, and migrations;
- adapter-contract tests with simulated slow, absent, interrupted, and malformed
  sources;
- GUI tests for state and interaction, not fragile source-text assertions;
- screenshot/geometry checks at the supported viewport, theme, and text matrix;
- a production-sized synthetic corpus and a before/after performance report;
- manual Linux production validation before merging to the main branch.

Database migrations create a timestamped backup, run in one transaction, are
idempotent, and expose a dry-run/diagnostic summary where data ownership changes.
Feature flags may protect an incomplete new surface, but old and new writers may
not update the same state concurrently.

## Cost-Efficient Model Assignment

A less expensive model can perform substantial implementation after this spec
is converted into file-bounded task cards with exact tests.

- Use a high-reasoning model for Slice 0 architecture/review, Mesh cancellation
  and shutdown, BBS schema/retention migration, JS8 ingest/dedupe, and Launch
  ownership migration. These have cross-thread or data-loss risk and benefit
  from repository-wide context.
- Use a mid-cost coding model such as `gpt-5.6-terra` for most responsive UI,
  model/view wiring, FIOSpotter administration, and integration tests once the
  service interfaces are fixed.
- Use a lower-cost model such as `gpt-5.6-luna` or `gpt-5.4-mini` for bounded
  tasks: copy/platform wording, protocol-derived default name, BBS age display,
  helper labels, select-all rendering, default-sort tests, roster diagnostics,
  and screenshot-matrix fixtures.

Every delegated coding task should include: allowed files, prohibited ownership
changes, acceptance tests, performance budget, migration rule, and a required
diff/test report. Architecture review should occur at each slice gate rather
than paying a high-cost model to produce every mechanical UI edit.

## Definition Of Complete

This remediation is complete only when:

- every numbered observation in this document has a passing acceptance test or
  a documented hardware/manual verification;
- performance budgets pass on the Linux production baseline;
- Mesh work cannot stall resize, navigation, or shutdown;
- FIO Managed BBS is administered once at station scope;
- Messages capture relevant directed JS8 traffic and preserve independent read,
  severity, summary, and source semantics;
- per-radio Launch Control persists and automatic startup matches manual review;
- SOP and Plan primary workflows are usable without horizontal scrolling at the
  compact viewport;
- the supplied roster is reported accurately without implying operator loss;
- the governing specs, help, migration notes, and UI regression work log agree.
