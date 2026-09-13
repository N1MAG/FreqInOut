# Production Reliability And Workflow Remediation Spec

Status: Slices 0–6 behavior implemented with automated gates passed; the
September 10 responsiveness remediation implementation gate passed under
`message_ingest_projection_performance_spec.md` and
`multi_endpoint_scheduler_concurrency_spec.md`; Linux production confirmation is
reopened; the September 11 Inbox/BBS production correction has passed the PIC-1
and PIC-2 automated gates and is ready for PIC-3 Linux qualification; Slice 1 software gate passed
2026-09-06 with the T1000-E reconnect hardware exception documented

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
- `message_ingest_projection_performance_spec.md`
- `production_inbox_bbs_correction_spec.md`
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
| FIO Spotter | `core/js8spotter_importer.py`, `core/js8_expect_*`, Spotter ingest/projection, forms, and legacy administration in Settings | top-level station service with Activity, Watches, Expect, Forms, and Imports browser tabs backed by shared FIO data |
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
- Repeated unchanged scheduler intent must settle: projection completion cannot
  request another projection or turn a data refresh into forced endpoint writes.
- Runtime CPU attribution must not depend on an operator reproducing the fault
  under a profiler. After sustained process CPU above the configured threshold,
  FIO writes a bounded, redacted stack and cached-service report to
  `cpu_hotspots` under its configuration directory. Normal samples produce no
  log traffic and perform no database, network, Qt, endpoint, or process-list
  work.
- High-rate service events are coalesced before presentation. Configuration or
  endpoint problems remain visible as stable status with explicit recovery
  actions; they cannot drive per-event control-bar reconstruction or alternating
  transient messages.

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
direct, top-level **BBS** service reachable from the expanded navigation, the
compact rail, and Messages `+BBS`. BBS is station-owned but is not nested under
Station in the navigation: operators understand it as a distinct service
served by VarAC, not as a radio setting or a station-health screen.

The service guides setup and daily administration from left to right. The
first tab answers **where the BBS is served**; the remaining tabs manage what
the caller can use and why:

1. `Radio Service` — the configured VarAC radio instances that serve the one
   catalog, their live BBS folders, enablement, and publication health;
2. `Locations & Access` — bounded service overview plus logical hierarchy,
   caller access, and retention;
3. `Publishing` — staged, location-scoped membership and artifact lifecycle
   actions;
4. `Visitor Preview` — a dedicated read-only caller simulation;
5. `Visitor Helpers` — generated navigation/command files, clearly separated
   from operator-published content.

Radio Service gives the selected service editor the primary workspace. On a
wide display, the bounded one-to-three-radio selector and editor sit side by
side; in compact mode the selector sits above a scrollable editor. A table must
not consume the remaining height merely because it stretches to fill the page.

Locations & Access absorbs the useful overview/status content. The selected
location's enabled state, access, retention, and source policy wrap in the
right pane, with the complete editor directly below it. The editor must not be
compressed into the narrow location-list column.

Selected-radio VarAC settings retain only native VarAC configuration:

- VarAC installation/runtime paths and connection settings;
- launcher, inbox, and outbox locations;
- radio-specific inbound safety/guard settings;
- a link to the top-level BBS service.

Live BBS materialization directories, service enablement, publication status,
locations, access, retention, helpers, and visitor behavior belong to the BBS
service. Legacy radio-profile fields may remain as adapter persistence for
compatibility, but they must not remain a competing visible administration
surface in VarAC Settings.

### Graphical publication workspace

Publishing and Visitor Preview use a clear location-chip selector instead of a
small drop-down above a dominant table. Chips keep the current location visible
and allow a bounded horizontal scroll or wrap when many locations exist.

The Publishing table is file-first and shows publication state, filename, age,
remaining expiry, and health. Origin, exact path, size, modified time, access,
retention, and exact expiry remain in the detail disclosure. Age is shown as
whole days (`0d`, `1d`, `24d`). Expires shows `Never`, `Expired`, `Today`, or
the number of days remaining; exact Local/UTC timestamps remain available in
detail/tooltip.

Checkbox edits are staged in memory. `Apply Changes` commits all pending
location memberships in one transaction, confirms what changed, and triggers
the normal background projection; `Revert` returns to persisted state. Moving
between location chips must not silently discard staged edits.

Artifact actions have explicit, non-destructive semantics:

- `Remove from BBS` disables all location mappings for the selected artifact,
  removes it from the normal BBS content view, and preserves the catalog record
  and source file. A Removed filter permits recovery.
- `Keep in BBS` applies to the selected location, uses the existing mapping
  retention override, and publishes without expiry until the operator restores
  normal retention or removes it.
- `Republish` applies to the selected location, restores publication, clears a
  Keep override, and starts a fresh resolved retention period from the operator
  action time. It never changes the source file timestamp.

The normal Published view, Expired view, and Removed view must make artifact
state explicit without forcing expired and removed history into the primary
workspace.

Visitor Preview is also file-first. Its caller-visible location chips and a
short wrapping policy summary replace the oversized location tree. The filename
column receives the stretch width automatically so normal file names do not
require manual column resizing.

### Retention and source reconciliation

- Each location has `manual`, `global default`, or `expire after N days`
  retention. The resolved retention policy and expiry timestamp are queryable,
  not hidden only in opaque metadata.
- At expiry, the mapping is disabled with reason `retention_expired`. The source
  artifact is not deleted unless a separate explicit purge policy authorizes it.
- A per-artifact/location `keep` retention-class override suppresses automatic
  expiry without changing the location's default policy. It uses the existing
  mapping column and requires no schema migration.
- Republish calculates a new mapping expiry from receipt of the operator action
  plus the resolved location retention. Initial publication continues to use
  the source modification time unless explicitly republished.
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
- New visitor helper filenames and labels omit `.txt`; VarAC exposes arbitrary
  files from its configured BBS directory, so the generated instruction files
  do not need a text extension. Historical `.txt` helper names remain
  recognized and are removed by managed reconciliation when superseded.
- The first helper is `00 HOW TO USE - Type command then refresh BBS`. Helper
  copy remains short and does not promise a fixed wait interval.
- Generated helper/navigation files are system output, not operator artifacts.
  They never appear in the `Publishing` content list. The `Visitor Helpers`
  view shows their name, purpose, locations, age, and health without offering a
  publication-membership checkbox.

`Return Live BBS Home` is an operational recovery action, not a configuration
reset. It restores the currently served visitor view to the configured home
location and clears transient caller/session overlays; it does not alter radio
configuration, locations, access, retention, catalog membership, or source
files. The ambiguous `Reset To Default` label must not appear in the primary
administration workflow.

### BBS acceptance

- One edit controls publication across all configured radio BBS instances.
- Tree, access, publication checkbox, file age, retention, and missing-source
  state are visible without opening radio settings.
- Applying staged checkbox changes removes unchecked items from the next
  effective manifest; source content remains intact.
- Expired or deleted-on-disk files stop publishing without error loops.
- Keep, republish, remove, and recover-from-Removed behavior pass fixed-clock,
  source-preservation, and manifest tests.
- A location rescan never re-enables an operator-disabled mapping.
- Messages `+BBS` and BBS administration show identical checked locations.

## Messages And FIOSpotter Remediation

### Relevant JS8 traffic

FIO incrementally ingests JS8 directed traffic when either condition is true:

- destination is the user's current callsign or an effective historical alias;
- destination is a JS8 group configured by the user or an operating group with
  which the user is explicitly associated.

This includes conversational or free-form directed traffic, including ordinary
social text addressed to an associated group, even when it is not stored as a
literal JS8 inbox message. Traffic addressed only between other stations is not
an Inbox message. Heartbeats, heartbeat acknowledgements, SNR reports and
queries, pure ACK/NACK frames, JS8 query/control commands, grid/link telemetry,
and FIOSpotter Expect request frames are excluded from Messages. Classification
uses anchored protocol grammar; prose is not excluded merely because it contains
words such as `ack`, `query`, `status`, or `grid`.

The exclusion is a projection boundary, not loss of RF evidence. `DIRECTED.TXT`,
`ALL.TXT`, native JS8 source data, and the independent `js8_links` index retain
station, path, time, and signal evidence for Map and propagation views. The hot
message projection records an independent `inbox_visible` policy state,
suppression reason, and classifier version so a policy-hidden frame is not
confused with operator read/archive/delete state.

Source radio/instance, received time, event time, sender, destination, path, and
original source evidence are retained. If JS8 provides identical raw and decoded
text, intelligence analyzes it once. An exact repeated multi-word payload may be
canonicalized for display while the native source remains unchanged; ordinary
human repetition is preserved.

The implementation uses source-scoped byte/record checkpoints over the directed
log and JS8 inbox plus indexed upserts. A checkpoint advances across suppressed
rows so a noise-only burst is not reparsed on every refresh. Dedupe identity
prevents one transmission appearing twice when it is also available from the
JS8 inbox or FIOSpotter. Rotated/truncated logs are detected without forcing a
startup-wide historical parse. Existing projection rows are reclassified in
bounded, resumable background batches; source rows and external references are
not deleted.

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

### FIO Spotter product boundary and navigation

**FIO Spotter is a top-level, station-owned service**, not a subordinate
Messages tool and not merely a compatibility page for JS8Spotter. It receives a
first-class expanded-navigation row and a dedicated compact-navigation icon.
The label is `FIO Spotter` where space permits and `Spotter` on the compact
rail. Settings may link into the service but must not remain the primary home
for Spotter administration.

FIO Spotter uses a browser-tab workflow familiar to JS8Spotter and
JS8SuperSpotter users without reproducing their monolithic UI or separate data
silos. Tabs proceed left to right:

1. **Activity** — bounded recent/matched traffic with age, source, group,
   callsign, form, topic, status, and radio chips; a selected record exposes the
   decoded content, evidence, and actions to open Inbox, Map, operator history,
   or compose a reply. A first-class Message Intelligence strip reuses the same
   operator/group-duty classifier as Ops Center and Messages. Reply, Relay,
   Review, and Social counts, What/Why guidance, the table Action column, and
   the active action filter must therefore agree. Source and action chips filter
   the already-loaded bounded page in memory; they do not issue another query.
2. **Watches** — station-owned callsign, group, topic/keyword, status, and
   location rules with enablement, priority, source/radio scope, optional
   expiry, last match, count, and health. Add, edit, disable, delete, and test
   are first-class actions.
3. **Expect** — one master rule list plus a readable detail/editor area for
   **E? Token**, reply, allowed callers/groups, blocked callers, radio scope,
   maximum replies, cooldown, enabled state, and separate unattended auto-reply
   permission. `All JS8 radios` is the default and recommended scope; an
   exceptional rule may be restricted by selecting a known FIO radio name.
   JS8 instance IDs and per-rule schedules are routing details derived from the
   receiving radio and are not operator-facing editor fields. Persistent chips
   show whether replies and automatic transmission are active or paused. A
   Requests/Replies history view shows request, decision, reason, source, age,
   and transmitted response.
4. **Forms** — FIO's known MCF/form catalog, categories, mappings, validation,
   import folder, and compose/preview actions. Human names lead; protocol codes
   remain supporting detail.
5. **Imports** — previewed, staged import of compatible JS8Spotter and
   JS8SuperSpotter history, searches/watches, Expect rules, operator/grid
   references, and forms. The preview reports candidate, duplicate, skipped,
   conflict, and applied counts and retains source/provenance.

Tabs use text plus familiar icons, graphical state chips, and a responsive
master/detail layout. Chips wrap or scroll before controls collapse into long
dropdowns. Icon-only controls require accessible names and tooltips. Compact
layouts stack the detail panel below the selected bounded table rather than
compressing columns. The Station Control Bar remains the Where/When surface;
FIO Spotter explains the What and Why of watches, matches, requests, and replies.

Known operators, callsign aliases, associated groups, location evidence,
traffic projections, and watch matches come from existing FIO stores. FIO
Spotter does not create a parallel roster, map, message database, or search
index. Autocomplete queries the bounded operator/group indexes and still allows
an explicit unknown callsign or term. Existing Settings controls migrate to or
deep-link into this workspace; there is one writer for each rule.

Expect caller authorization follows the same operator-centered mental model as
managed BBS access while keeping JS8 destination groups distinct:

- `*` in **Allowed callers** is the familiar JS8Spotter-compatible spelling for
  any caller. It persists as the rule's `allow_any` state, and an explicit
  blocked caller still takes precedence.
- Explicit allowed and blocked callsigns resolve through Operator History. A
  callsign change therefore preserves an intentional access decision across the
  current and former callsigns associated with that operator identity.
- **Allow all trusted operators** authorizes any Operator History identity whose
  roster record has the `trusted` flag.
- **Trusted operators from groups** authorize only trusted Operator History
  identities associated with at least one selected operator group. These
  memberships describe who the caller is; they do not authorize an automatic
  reply to a JS8 group destination.
- **Query groups** separately list JS8 group destinations to which a group
  reply is allowed. Dynamic FLAMP group replies continue to require this
  explicit destination opt-in even when `*` or trusted-operator access permits
  the sender.
- Callsign and group suggestions come from the bounded Operator History identity
  index only when the Expect page is opened. This lookup must not run during
  application startup, UI paint/resize, or the on-air request path.

Allowed-caller, query-group, trusted-operator-group, and blocked-caller editors
show accepted values as removable chips above a separate lookup/custom-value
field. Enter or **Add** accepts the highlighted lookup result or an explicit
custom value and leaves the editor ready for another; canonical uppercase and
query-group `@` normalization prevent duplicates. At narrow widths, the rule
list/editor and audit panes stack vertically inside the page scroll area; no
page-level horizontal scrolling or clipped controls are acceptable in Normal
or Large Text modes.
Every Spotter combo box uses an expanding closed control and a content-sized,
bounded popup so the full choice is readable without forcing a wide minimum
page size. This applies to Watch type/match/priority, Expect rule and policy
selection, radio selection, policy scope, and Forms purpose selection.
The reusable allow policy is explicitly optional and its empty choice explains
that rule-level access fields remain active. Saving a reusable policy keeps it
selected for the rule being edited and confirms that **Save rule** attaches it.
Token completion uses an editor-owned string model and an explicitly installed
completer; accepting a lookup appends a chip without replacing previously
entered values, clears the lookup field after Qt finishes applying the selected
completion, and leaves the next lookup immediately usable. Custom groups remain
supported, and an empty completion set never opens a popup. The radio selector's
choice is self-explanatory; detailed receiving-radio routing guidance belongs in
Help rather than a persistent paragraph in the compact editor. All user-facing
hints follow the product-wide UI Hint Neutrality Contract:
they use semantic labels and never embed a real or plausible callsign.

The same rule service supplies ingestion alerts, Inbox focus, Ops Center
attention, and Map pins so different screens cannot disagree. Messages remains
the complete traffic triage surface; FIO Spotter is the administration and
focused operational browser for Spotter-specific behavior.

#### FIO Spotter RF boundary, summarized intelligence, and form administration

FIO Spotter is intentionally a concise **local RF observation and RF automation
workspace**. It is not another copy of the complete Messages feed. Activity may
show traffic that FIO received through a configured local JS8Call instance,
including a CommStat-shaped report heard through that RF path. An internet-only
CommStat record, aggregated SitRep, manual record, or imported historical row
must not enter Spotter Activity merely because it exists in the shared message
projection. Messages, Map, and broader Message Intelligence may still use that
evidence with its real provenance.

Source-family labels alone are not proof of local RF reception. Runtime
Spotter/JS8 rows retain the receiving radio, JS8 instance, ingest origin, and
source reference. Imported legacy Spotter rows remain visibly imported and are
not relabeled as newly heard RF. When a shared CommStat artifact has both RF and
internet evidence, Spotter eligibility is based on immutable local receive
evidence, not a mutable or merged transport label. Until that provenance is
available, FIO prefers omission over claiming an internet-only record was heard.

Activity stays summary-first. For a CommStat report heard through JS8Call:

- `Form` reads `CommStat`; protocol subtype and raw syntax remain detail.
- the compact `Summary` reads `Green`, `Yellow`, `Red`, or `Unknown` plus only
  meaningful exceptions; it never reuses read/unread state;
- the primary summary names the status and meaningful exception or scope rather
  than repeating the encoded CommStat string;
- `Source` remains a concise human label such as `JS8Call` or the receiving
  radio, with exact application-instance and transport evidence in detail; and
- the existing Message Intelligence action/topic vocabulary controls the
  summary strip and table so Activity, Inbox, Map, and Ops Center do not invent
  competing interpretations.

The operator-facing default remains small: one bounded recent page, a few
plain-language filter chips, a compact status summary, and progressive detail.
Raw wire strings, source keys, projection terms, and redundant status prose do
not become permanent columns. Activity filtering is in-memory over the loaded
page and performs no database, filesystem, network, or parsing work during
filter clicks, selection, paint, theme changes, or resize.

FIO Spotter is the authoritative administration home for saved form responses,
Expect rules, and caller-access policies. Messages Compose remains the home for
an operator composing and sending one message. Its Spotter handoff is phrased as
`Configure automatic response in FIO Spotter`, creates or identifies a disabled
draft, and opens FIO Spotter > Expect with that draft selected. It does not leave
the operator with an invisible external write. An already-built Expect page
uses explicit revision invalidation on activation; it never polls. A duplicate
handoff selects the existing entry without replacing its policy or response.

Forms provides two clear actions for a selected known form:

- `Compose and Send` hands the form to Messages for a deliberate transmission;
- `Make Available by E?` opens or creates its Expect draft and keeps the
  operator in the FIO Spotter policy workflow.

The form catalog remains distinct from routing mappings: availability by E? is
shown as a concise `Not available`, `Draft`, `Active`, or `Paused` state. Drafts
remain visible even when disabled or missing a caller policy. The current rule
editor leads with response and access, while reusable-policy administration and
request/reply history are collapsed until requested. A later editor redesign
may use the guided sequence `Response`, `Access`, `Safety`, and `Review &
Activate`; that larger workflow is not required to make this correction gate
truthful.

#### Bulk refresh of saved Expect form dates

The Expect page provides one deliberate bulk action named **Update Expect Form Dates**.
JS8Spotter MCForm responses carry a compact `#XXXX` month/day/time code so a
recipient can judge when the stored response was refreshed. Rebuilding every
saved response by hand is unnecessary operator work.

The action follows these rules:

- it previews the number and names/tokens of eligible saved form responses and
  asks for confirmation before changing them;
- one current local-time date code is computed for the entire operation, so a
  bulk refresh cannot straddle different minute codes;
- only canonical static `F!nnn`/`F!nnnA` rules whose response begins with the
  same form token, optionally after one valid JS8 destination, are eligible;
  dynamic `Q`, free-text, signed, mismatched, or ambiguous rules are skipped and
  counted without modification;
- a valid trailing MCForm date code is replaced; when an otherwise valid form
  response has no date code, the current code is appended;
- when the rule separately uses a persisted MsgAuth date code, that stored date
  code is refreshed in the same transaction so a later signature covers the
  same advertised date;
- caller policies, radio scope, enabled/paused state, unattended permission,
  reply limits, cooldown, expiry, send/request history, and response content
  other than the date code remain unchanged;
- the operation never transmits, never enables a draft, and never clears reply
  history; and
- every changed rule receives a management-audit record. The UI reports changed
  and skipped counts, refreshes the bounded list once after commit, and retains
  the selected rule when possible.

The core bulk update is one short transaction with deterministic validation and
no Qt dependency. It must not loop through independent connections or refresh
the table per row. The UI performs no background polling; a click issues one
bounded preview read and, after confirmation, one bounded write transaction.

#### Store and Forward direction

SuperSpotter's Store and Forward workflow is valuable and should become a
familiar FIO Spotter tab, but its core remains a protocol-neutral **Message
Relay Queue**, not a Spotter-only message silo. This is a separately gated
package after the Activity and Forms/Expect workflow above pass. Its initial JS8
adapter uses explicit operator-created, callsign-only held messages; finite
expiry; durable dedupe and delivery claims; bounded notification retry and
per-recipient cooldown; visible audit; and the normal endpoint-scoped JS8,
selected-target, RF Guard, PTT/busy, hold, and schedule protections. Automatic
`message waiting` notification defaults off. Remote third-party storage and
group delivery remain out of the first package.

#### FIO Spotter workflow correction exit gate

- A locally received JS8 CommStat report appears in Activity as `CommStat` with
  the same normalized Green/Yellow/Red/Unknown intelligence used by Messages.
- Internet-only CommStat and imported historical rows do not masquerade as
  newly heard RF Activity; provenance remains inspectable.
- A Compose handoff becomes visible in Expect immediately on navigation and is
  selected without a restart, polling timer, or policy overwrite.
- Forms can open both the manual Compose path and the automatic-response policy
  path with human form identity preserved.
- Update Expect Form Dates previews and atomically updates every eligible static form
  using one deterministic date code; invalid/dynamic rules are unchanged and
  reported as skipped; no transmission or enablement occurs.
- Activity and Expect pass compact/wide Normal/Large Text and light/dark layout
  checks without clipped primary actions or page-level horizontal scrolling.
- Focused projection, Expect store, access-policy, FLAMP Q, selected-target,
  message-intelligence, and lazy-activation regressions pass; Activity remains a
  bounded cached query and no new idle timer or render-path I/O is introduced.

The completed follow-on interaction and runtime contract is
`fio_spotter_operator_workflow_refinement_spec.md`. It supersedes the earlier
collapsed reusable-policy editor with a dedicated `Access Policies` tab,
reduces Expect to one visible per-response `Auto reply` state plus one station
service pause, adds guarded saved-response Compose and Send now, and makes
Activity-to-Watch matching real through a cached background projection hook.
Watch match persistence is additive and idempotent by watch/message identity;
failure remains advisory and cannot hold message ingest or the UI.

All unattended FIO Spotter Expect transmissions use one selected-target safety
contract. Immediately before preflight, FIO makes a best-effort compatibility
clear of any stale callsign selected in the receiving JS8Call instance inside
the existing endpoint-serialized send transaction. It then re-reads target
state and applies the normal TX-enabled, queue-empty, TX-text-empty, RF Guard,
access-policy, durable-claim, cooldown, and audit requirements before sending
the explicitly addressed reply. Official
JS8Call releases expose selected-target reading but not writing; if the target
remains selected, FIO holds the reply and tells the operator to deselect it in
JS8Call. FIO must not merely ignore a selected target, and this compatibility
attempt must not change manual or operator-confirmed send behavior. The contract
covers both fixed Expect responses, including form responses, and the dynamic
FLAMP Q service.

### Dynamic FLAMP Expect query service

FIO Spotter may explicitly enable a built-in dynamic Expect service for the exact,
case-insensitive request `E? Q <qid>`, where `<qid>` is exactly four hexadecimal
characters. For compatibility with established JS8Spotter operating practice,
the compact spelling `E? Q<qid>` is also valid; no other surrounding or trailing
text is accepted. The query value is supplied by the received message; operators
do not create one rule per Q ID. For `E? Q 970F`, the only valid response payloads
are:

- `Q 970F YES` when an authoritative current FLAMP transfer has a known total
  block count, every required block is present, and the source artifact still
  exists;
- `Q 970F NO` when no eligible transfer is held by the receiving station; or
- `Q 970F 22,23` when an authoritative partial-transfer record proves those
  exact blocks are missing.

The missing-block response is confidence-gated. FIO must not derive a missing
list from filename fragments, a highest-seen-block fallback, or a generated BBS
block-list helper. A partial transfer whose total or available-block set cannot
be validated is held for review and audited; it must not be described as
complete and must not emit a misleading `NO`. Missing numbers are sorted,
deduplicated, range-checked, and bounded to the JS8 response budget. If the list
cannot fit safely, the request is held with a visible reason.

The FLAMP transfer projection persists canonical digit-leading identifiers such
as `970F`, source radio/JS8 instance, relay path, known total, available/missing
blocks, state (`complete`, `partial`, `unavailable`), source mtime/hash,
observation time, and parser confidence. A companion source-scan record stores
the radio/JS8 identity, relay directory, success, bounded file count, error, and
scan time. The existing background ingest worker refreshes this projection;
on-air request handling performs database-only lookup and never walks or hashes
the relay directory. `NO` is allowed only after a recent successful scan proves
the source has no matching transfer. A missing, failed, or stale scan holds the
request with a visible reason. Missing source files transition the record to
unavailable without an error loop.

The request hot path selects only the normalized `Q` rule through the
Expect-key index and only allow-policy IDs referenced by those candidate rules.
It must not deserialize unrelated rules/policies, reread the same catalogs for
group-reply authorization, or run schema/index setup during evaluation, claim,
completion, or dispatch audit. Startup database initialization is the only
migration owner.

Dynamic-Q observation uses a dedicated three-second incremental tail of each
active radio's `DIRECTED.TXT`. It keeps a source-specific byte checkpoint,
performs no general message projection, directory scan, hash pass, or schema
work, and runs only when the source fingerprint changes. Disabled or paused
service still consumes and visibly holds a query so it cannot be transmitted
later merely because the operator enables or resumes automation. The broader
message/Spotter pass remains on its normal cadence and does not evaluate the
same dynamic record a second time. On first use, the dedicated checkpoint
inherits the existing Spotter checkpoint so an upgrade does not replay
historical RF requests.

Dynamic Q service safety is cumulative:

- the dynamic service defaults off after upgrade; the service, normal Expect
  processing, unattended replies, and the matching
  caller/group allow policy are enabled;
- the message is a complete directed record addressed to this station, or to an
  explicitly enabled group policy; group replies are opt-in to avoid a reply
  storm;
- sender, target, source radio/JS8 instance, event identity, and Q ID validate;
- relayed `*DE*` requests are held unless a separate trusted-relay policy is
  explicitly enabled;
- the receiving radio resolves to one concrete JS8 endpoint and RF Guard passes
  immediately before transmit; there is no primary/global-radio fallback;
- one durable request claim is acquired before transmit, and cooldown/max-reply
  policy is enforced atomically across restart and replay; and
- an endpoint-level transaction lock prevents another worker from interleaving
  preflight, target selection, text setup, send, and claim/audit completion.

A directed request older than the bounded live-request window is held rather
than replayed after an offset reset or first installation. The Expect tab owns
the dynamic-service switch, a `New FLAMP Q rule` guided action, the source-index
health summary, normal pause/unattended controls, and request/reply audit views.

Reply routing is direct to the requesting callsign for a station-addressed
request and to the addressed group only when group-query replies were expressly
enabled. Every accepted, blocked, held, failed, deduplicated, and sent decision
is visible in Expect history with source and reason. A failed send uses bounded
backoff and never immediately loops.

#### FLAMP receive-state freshness and offline catch-up

The relay directory is a **saved snapshot**, not FLAMP's live receive queue.
Review of FLAMP 2.2.14 shows that `FLAMP/relay` is rewritten only when the
operator chooses **Save Relay Files** or when FLAMP exits with **Save Relay Data
On Program Exit** enabled. Receiving fill blocks while FIO is stopped can
therefore make an unchanged relay file stale even though a later FIO directory
scan succeeds. FLAMP's public XML-RPC service does not expose receive-queue
state or a save-relay operation, and FIO must not consume FLDigi's RX stream in
competition with FLAMP. A recent directory-scan timestamp alone is consequently
not proof that an unchanged partial relay snapshot is current.

FIO reconciles three bounded evidence classes in this order:

1. **Validated completed receive.** The configured per-radio `FLAMP/rx` root is
   scanned only at its root and one FLAMP date-directory level. A regular file
   whose exact basename matches the AMP `<FILE>` header and whose filesystem
   time is not older than that transfer's saved relay observation is accepted as
   FLAMP completion evidence. This is authoritative because FLAMP writes the RX
   artifact only after its receive object reports every checksum-validated block
   complete. It upgrades an unchanged partial relay snapshot to `complete` and
   makes `Q <qid> YES` available after FIO starts.
2. **Validated saved relay.** The existing AMP parser remains authoritative for
   total and checksum-accepted block numbers in the saved relay snapshot. It may
   establish `partial` or `complete`; filename prefixes alone never establish
   either state. A missing-block reply is eligible only while the partial relay
   file's own modification time is within the ten-minute confidence window.
   Older partial snapshots remain visible but are held because a later unsaved
   FLAMP receive cannot be ruled out. A checksum-complete snapshot remains
   conclusive regardless of age.
3. **No conclusive evidence.** Missing headers, conflicting identifiers or
   totals, inaccessible roots, an in-progress/failed reconciliation, or an
   uncorrelated completion candidate yield `unavailable`/held behavior. FIO does
   not guess from a decoded filename, highest observed block, helper file, or
   unrelated FLAMP log activity.

The projection persists the AMP transfer filename, expected encoded size,
relay-file size/mtime/hash, completion-evidence path and kind, and the timestamp
at which that Q record was validated by a successful scan generation. The
schema change is additive and idempotent. Existing rows with no new metadata
are reparsed once; no message, rule, policy, operator, or source file is changed.
The source-scan row advances only in the same transaction as all per-Q
reconciliation results. A Q row is eligible for a reply only when it was
validated by the source's latest successful generation. This prevents a fresh
source status from masking an unevaluated stale transfer row.

Reconciliation remains off the GUI path and outside per-request evaluation.
Before the first directed-query tail is consumed in a process, that background
worker completes one startup projection; a dedicated 30-second background job
then maintains it without coupling the work to general message ingestion. Each
source run builds a bounded stat manifest once, reparses/hashes only new or
changed relay files, and checks exact completion basenames only for known
transfers. Unchanged complete rows require no content read. Unchanged partial
rows retain their parsed relay facts but are reevaluated against the lightweight
RX manifest, so a completed output appearing while FIO was stopped is
recognized before a startup query can use yesterday's row. After that startup
gate, the existing three-second directed-query tail is a database-only consumer
and performs no FLAMP filesystem work.

This catch-up contract deliberately does not claim access to unsaved,
still-partial blocks held only in FLAMP memory. If FLAMP has neither written a
new relay snapshot nor completed and saved the RX artifact, FIO cannot prove a
new missing-block list through FLAMP's supported interfaces. Such an ambiguous
record must retain its last-observed timestamp in audit/UI, and a future live
partial-state adapter requires a documented FLAMP API or a captured,
checksum-verifiable producer artifact before it may affect automatic replies.

Acceptance fixtures cover:

- an unchanged partial relay snapshot followed by a matching FLAMP RX output,
  which becomes `YES` after restart without touching the relay file;
- mismatched basename, older completion output, directory, symlink, and
  out-of-scope nested candidates, none of which can upgrade a transfer;
- changed relay content with preserved timestamp but changed size, which is
  reparsed, and an unchanged complete row, which is not reread;
- a recent partial relay snapshot, which may return missing blocks, and an old
  partial snapshot, which is held rather than returning a potentially stale list;
- malformed/conflicting AMP headers, deleted relay/RX artifacts, failed scans,
  and source-radio isolation;
- atomic scan-generation eligibility so no fresh scan record can coexist with
  an unvalidated reply-eligible Q row; and
- a production-shaped 138-file relay/RX fixture and a larger bounded fixture,
  proving startup/background reconciliation stays off the GUI thread and the
  `E? Q` request path remains database-only.

### SuperSpotter familiarity contract

The reviewed JS8SuperSpotter 2.6 concepts retained in the FIO design are its
operational activity/search browser, Expect rule list and reply history, clear
pause/automatic-TX state, MCForms catalog, roster/location context, watch/search
terms, map handoffs, and previewed import. FIO deliberately replaces separate
SuperSpotter roster, map, CommStat, and message stores with its existing
operator, observation, Map, CommStat, Inbox, and Message Intelligence services.
Email/APRS gateways, downloaded map tiles, and SuperSpotter's unguarded direct
socket-send behavior remain out of scope.

### Message performance acceptance

- A 100,000-row retained corpus does not load into Python/UI memory on tab open.
- Current-window counts and first page use indexed queries; historical detail is
  paged.
- Incremental source projection handles only changed rows and does not project a
  fixed 5,000 rows per source during startup.
- Relevant directed JS8 fixtures are ingested once; heartbeat/SNR fixtures are
  excluded; ACK/NACK, query/control, Expect-request, grid/link, duplicate-text,
  other-station, rotation, and delayed-traffic fixtures are covered. Suppressed
  JS8 evidence remains available to the independent link/map index.
- Inbox first paint loads no more than 1,500 projected rows and never loads
  policy-suppressed rows or their external references into the UI model.
- CommStat read state, report severity, summary, and source remain independent
  through mark-read, filter, restart, and reprojection.
- Opening FIO Spotter does not construct or load its history until first use.
  Each browser tab uses bounded indexed queries and shows an explicit result
  scope; changing chips does not rebuild unrelated tabs.
- FIO Spotter list readers use query-only connections and never run schema,
  index, or journal-mode mutations. Startup owns migrations. Screen
  reactivation does not repeat an already-loaded tab query; explicit Refresh
  remains the operator-controlled update path.
- Bounded table replacement is painted as one batch. Live
  `ResizeToContents` sizing is prohibited on populated Spotter tables because
  it can recalculate geometry after every inserted cell. Activity timing
  records query and render components separately in the performance log.
- Dynamic Q lookup is indexed and bounded, request evaluation and transport run
  off the GUI thread, and the Station Control Bar remains responsive during
  import, history refresh, or Expect dispatch.
- With 500 unrelated enabled Expect rules retained, 1,000 audit-disabled
  `Q 970F` policy evaluations complete in no more than 5 seconds on the
  reference development system. The regression also proves one key-filtered
  rule read and one referenced-policy read per evaluation, with no second
  catalog pass. FIO Spotter owns no idle refresh timer; explicit Refresh and
  ingest-owned background projection are the only refresh mechanisms.
- A restart/replay or concurrent duplicate dynamic request produces one durable
  claim and at most one transmitted reply.
- A stale JS8Call selected callsign is subject to a compatibility clear inside
  the serialized unattended Expect send transaction before preflight and before
  `TX.SEND_MESSAGE`. A supporting/custom endpoint proceeds only after the target
  reads back empty; an official endpoint that leaves it selected holds with an
  actionable reason and transmits nothing. Queue, TX-text, and other blocking
  guards remain cumulative, while manual sends retain their existing policy.
- The exact `E? Q 970F` fixture covers absent, complete, authoritative partial,
  unknown-total, malformed, wrong-source, paused, RF-guard-held, send-failed,
  duplicate, cooldown, and restart cases.

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

Implementation result (2026-09-08): **automated Slice 4 exit gate passed**.
Launch Control now persists an ordered, versioned bundle per radio in additive
`radio_launch_bundles` and `radio_launch_bundle_items` tables. The startup-owned
migration checkpoints and backs up the settings database, imports legacy rows,
legacy autostart flags, and station custom tools once, records the selected
target and source digest in `launch_bundle_migration_audit`, leaves the source
KV values unchanged, and disables legacy fallback after confirmation. A failed
backup or write changes no launch ownership state.

`StationLaunchPlanner` is the single pure contract for the Settings preview,
automatic station startup, and selected-radio `Start Startup Apps`. It applies
radio-specific path/command and endpoint values, stable dependency ordering,
exact instance identity and station-wide deduplication. Shared instances retain
all serving-radio provenance; different JS8Call, FLRig, FLDigi, VarAC, or custom
command/path instances remain separate. Executor results name the radio(s) and
instance, a failed prerequisite blocks every dependent radio it did not cover,
and dependency cycles are rejected before execution.

Settings radio switching now stashes unsaved bundle drafts independently and
reloads the selected radio's persisted checkbox/order state. `Monitor Health`
is independent from `Launch at Startup`, the compact Startup Preview is planner
backed, and Save commits the radio bundle before the normal UI reload. Launch
table reads and readiness polling use the shared asynchronous dependency-status
snapshot; checkbox, save, and paint paths no longer force a process walk.

The focused launch, migration, planner, status, multi-rig, and Settings gate
passes 366 tests with 4 environment skips. The repository gate passes in four
fresh-process batches: 2,634 tests passed and 37 environment skips. The single
long-lived macOS Qt run reached 80% without an assertion failure, then hit the
known test-process teardown/worker-pool segmentation fault in an unrelated
ControlFreq construction; fresh-process batching completed every test file.
Python compilation and `git diff --check` pass. No Slice 5 work began.

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

Production follow-up (2026-09-08): deferred-screen factories must treat optional
settings hooks as optional at the call site. HF Callsigns exposed a mismatch
where lazy construction evaluated a missing `on_settings_saved` attribute before
the shared safe signal connector could inspect it, leaving the stable placeholder
visible on every selection. Operator History now implements the presentation-only
settings hook, the factory uses optional lookup defensively, and any future
deferred factory failure retains its placeholder while logging the screen label,
exception message, and traceback. Regression coverage exercises both the real HF
settings callback contract and a deferred tab without that optional callback.

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

Linux production evidence on 2026-09-07 adds a separate failure signature for
the saved T1000-E (`FE:BC:04:8F:50:E3`, advertised as `MeshCore-N1MAG MOBL1`).
Across the captured 10:00–10:09 window, FIO acquired 17 serialized sessions,
completed eight explicit teardown requests in 0.4–3.4 ms, and eventually
reached Companion-ready twice. Twelve saved-target attempts and three
discovered-target fallbacks failed at GATT service discovery; two operations
timed out and five were cancelled by subsequent operator actions. The log has
no PIN, authentication, removed-key, Code 14, or BlueZ bond-failure marker.
This supports a device/BlueZ/GATT bring-up problem, but does not support asking
the operator to forget a still-valid pairing.

FIO must therefore distinguish service discovery from authentication. A GATT
service failure tells the operator to keep the saved pairing, restart the card
if needed, wait for advertising, and choose Connect once. Forget/re-pair
guidance is reserved for explicit authentication, PIN/passkey, encryption-key,
or removed-key evidence. Lifecycle logging records link connect, link ready,
service verification, and Companion initialization as separate timed stages.
A production follow-up now preserves reconnect backoff for 30 seconds across
an immediately replaced in-process worker and gives each saved endpoint one
process-local connection-attempt lease. A replacement worker defers without
counting another failure while the prior attempt still owns that lease, so
repeated refresh/restart paths cannot overlap GATT setup for the same endpoint.
Successful connection clears the handoff, explicit Retry Now remains the
operator-controlled bypass, and failures identify link connect, service
discovery, or Companion initialization as separate stages. This adds no
database/configuration migration and does not change pairing, bonding, PIN,
device-channel, or firmware behavior. The Linux/T1000-E hardware gate remains
required.

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

Top-level `BBS` now owns the graphical service workflow. Its guided tabs cover
Radio Service, Locations & Access, Publishing, Visitor Preview, and Visitor
Helpers. Radio Service is first because it answers where the station BBS is
served; its selector and editor share the available workspace instead of
stacking a tall table over a compressed form. Locations & Access includes the
useful service summary and puts the selected location's wrapped policy and
editor together. Publishing uses location chips, a bounded newest-first file
view, staged membership checkboxes with Apply/Revert confirmation, explicit
Remove from BBS, Keep in BBS, and Republish actions, and Age/Expires/Health
signals. Compact layout stacks where needed and makes publication detail opt-in
so 900x560 Large Text preserves the main workspace. Visitor Preview is a
dedicated read-only, file-first caller view with location chips and a selected
policy summary. Location access supports public, callsign, access-code, and
combined rules; new codes are salted and hashed. Radio Settings retain only
native VarAC paths and inbound guard plus a link to BBS; live BBS folders and
service enablement are managed under Radio Service. Messages `+BBS` uses the
identical station locations and atomically replaces memberships, including
uncheck-all, without copying or deleting the received source file.

Each radio live directory has a distinct manifest keyed to its resolved path,
so one catalog projects independently through one or multiple VarAC instances.
Compatibility callers without an explicit database identity remain on the
folder-backed path and cannot accidentally read the process-global catalog.
New Visitor Helper filenames are extensionless both in the UI and on disk;
historical `.txt` helper names remain recognized for cleanup and transition.
The first helper is `00 HOW TO USE - Type command then refresh BBS`, with no
fixed-delay instruction. Generated helpers are excluded from Publishing and
appear only in Visitor Helpers with purpose, locations, age, and health.

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

Refinement exit-gate result (2026-09-07): passed. The five-tab workflow,
staged Apply/Revert publication model, remove/keep/republish persistence,
extensionless helper transition, file-first Visitor Preview, and responsive
location chips pass 144 focused BBS tests with one environment skip. Related
shell/navigation/Settings coverage passes 289 tests with 19 environment skips.
All 172 repository test files pass in fresh processes; two files contain only
environment-dependent skips. The monolithic Qt run remains unsuitable as a
release gate because its long-lived process reproduces the pre-existing
log-viewer/thread-lifetime segmentation fault.

Risk: **high**, due persistence migration and external file publication.

### Slice 3 — Message ingestion, semantics, and FIOSpotter

Deliverables: incremental relevant-directed JS8 ingest, unified dedupe, bounded
query/model path, always-visible select-all, newest-first stability, separated
CommStat fields, a top-level lazy FIO Spotter service with Activity/Watches/
Expect/Forms/Imports tabs, complete Expect administration and history, and the
guarded dynamic `E? Q <qid>` FLAMP response service. Existing FIO operators,
groups, aliases, observations, messages, map routes, forms, and imports remain
the authoritative data sources.

Exit gate: production-scale corpus meets budgets and all source/read/severity
contracts pass across restart/reprojection; FIO Spotter passes Normal/Large Text
and light/dark compact/wide navigation tests; Expect administration persists;
and the dynamic Q matrix proves exact parsing, source-scoped indexed state,
confidence-gated responses, RF/source guard behavior, durable duplicate and
cooldown enforcement, restart safety, and one non-interleaved same-endpoint
transmission. Missing-block replies remain disabled unless the authoritative
partial-transfer fixture passes.

Risk: **high** for ingest/dedupe and **medium** for presentation/admin UI.

Implementation result (2026-09-07): **exit gate passed**. Relevant free-form
JS8 traffic addressed to the current or historical station callsign or an
associated group is projected once from `DIRECTED.TXT` and API seams with
source identity preserved; heartbeat/SNR traffic remains excluded. FIO Spotter
is a lazy top-level service with the five specified browser tabs. Watches and
legacy searches share the station watch store; Expect rules, allow policies,
runtime state, source routing, schedules, audit history, and the guarded dynamic
FLAMP Q service are administered together. Forms exposes editable routing
mappings, factory classification, bounded preview, and Compose handoff.

The dynamic service is off by default. Its additive tables are initialized
idempotently, its live request path is database-only, and its background
projection reuses unchanged file mtime/hash state rather than reparsing relay
files. Missing-block replies are enabled only for authoritative total/block
fixtures. A 100,050-row retained-message fixture proves the core query cap and
the stricter FIO Spotter cap; compact Dark/Large Text and wide Light/Normal
visual gates pass without page-level horizontal scrolling. The authoritative
fresh-process repository gate passes 2,519 tests with 37 environment skips.
The known monolithic Qt lifetime fault remains reproducible only after many
test modules share one process; the same files pass in isolated processes.

Production hang follow-up (2026-09-07): a captured watchdog dump showed the
main Qt thread blocked during an ordinary Settings save while the legacy
Settings `Spotter Form Mapper` rebuilt a `QTableWidget` with per-row combo
widgets. That mapper duplicated the completed top-level FIO Spotter `Forms`
page and violated the one-writer service boundary above. The legacy Settings
surface, its refresh/auto-classify handlers, and all save/load rebuild calls
are removed. Existing `js8_spotter_form_mappings` data remains untouched and
is still persisted through unrelated Settings saves; FIO Spotter Forms is the
sole administration surface. Ordinary Settings save now refreshes only the
runtime projections affected by the save instead of forcing the entire
multi-radio table projection. No schema or migration changed.

Production Expect/performance follow-up (2026-09-07): the latest supplied log
contains one MeshCore link/service-discovery failure followed by normal
scheduler and shell callbacks, then ends abruptly without a traceback, Qt
fatal, shutdown, Spotter action, or exit marker. It does not establish Mesh or
the Expect editor as the process-exit cause. FIO Spotter now logs rule/policy
administration actions and failures so a future short log can be correlated.
The comma-token completer has an explicit editor/model ownership path, the
optional allow-policy workflow remains visible at all supported sizes, and an
administration read failure is no longer presented as a misleading empty
policy list. Spotter list reads remain query-only; startup remains the only
migration owner.

Activity now reuses the shared operator/group-duty Message Intelligence
classifier and projection evidence. Its Reply/Relay/Review/Social strip,
What/Why guidance, row Action, and action filter agree; all source/action
filtering occurs against the cached 200-row page. Dynamic Q evaluation selects
only the indexed `Q` candidates and their referenced policies, carries the
group authorization result forward, and removes the second catalog pass.
Evaluation/audit/claim/completion/dispatch and source-state lookup no longer
repeat schema or journal-mode setup. The on-air path no longer stats a relay
file; a successful background index records deleted files as database-known
unavailable sources while malformed-present sources remain held.

On the reference Mac, 1,000 Q evaluations with 500 unrelated rules averaged
0.528 ms each. Two hundred evaluation + audit + durable claim + completion
cycles averaged 2.184 ms each. The relevant integrated gate passes 642 tests;
the authoritative fresh-process repository gate passes 2,546 tests with 37
environment skips across all 177 test files. Dark/Large Text 900x560 and
Light/Normal 1400x900 visual gates are stable with no page-level horizontal
overflow. The Linux T1000-E hardware retry gate remains open; no Mesh device
configuration or pairing state changed in this follow-up.

Expect radio-workflow follow-up (2026-09-07): the rule editor now names the
protocol field **E? Token** and defaults new rules, including dynamic FLAMP Q,
to `All JS8 radios — reply on receiving radio`. The optional restriction lists
only configured FIO radios with JS8Call capability and displays the human radio
name in both the selector and rule list. Internal radio IDs are never requested
from the operator. JS8 instance and schedule controls are removed from the
normal editor; new or deliberately retargeted rules use the selected radio's
current routing, while an unchanged legacy rule retains its stored routing
metadata. An unavailable legacy radio remains visible as such rather than
silently changing scope. All Spotter dropdowns now have flexible closed widths,
bounded content-sized popup widths, and item tooltips. This is a UI/persistence
adapter change only and requires no schema migration.

Production dynamic-Q follow-up (2026-09-08): JS8Call recorded both
`E? Q906F` and `E? Q 906F`. The compact form was rejected by the original exact
parser; the spaced form was accepted, matched the enabled trusted-operator
policy, resolved authoritative partial state, and reached send preflight. The
send was blocked after the native client timed out waiting for
`STATION.CONFIG`. Code review found that response correlation required FIO's
private request `_ID`, although released JS8Call builds commonly return standard
response types without echoing that field. Native API request correlation now
prefers `_ID` when returned and otherwise assigns the oldest pending request
whose declared response type matches. Unrelated receive traffic cannot satisfy
a pending request. Preflight failure summaries report the actual blocking issue
ahead of non-blocking capability warnings, and dynamic receive/hold/dispatch
decisions are recorded in the normal log as well as the durable Expect audit.
No schema, rule, policy, radio, or FLAMP data migration is involved.

FLAMP offline-catch-up follow-up (2026-09-08): read-only production database
evidence showed Q `906F` still projected from a partial relay snapshot with
blocks 26 and 27 missing even though a newer, exact-name completed artifact was
already indexed under FLAMP's dated RX output. The implementation now follows
the receive-state freshness contract above: it stores additive evidence and
scan-generation metadata, validates exact completed outputs, reparses legacy
rows once, and reuses unchanged relay parses. A startup projection runs before
the process consumes its first dedicated Expect tail; a separate 30-second
background projection then keeps the state current without rebuilding Messages
or scanning from the three-second request path. Enabling the service queues an
immediate projection rather than waiting for that cadence. Temporarily
inaccessible source roots record a failed generation and retain the last good
row, so automatic reply holds instead of transmitting a stale answer. Valid
source scans still
retire genuinely deleted relay rows.

Missing-block replies additionally require a relay snapshot no more than ten
minutes old, measured from the source file rather than the scan time. This
prevents a current background-scan timestamp from laundering an old partial
snapshot into a confident answer. Completed RX evidence and checksum-complete
relay snapshots remain conclusive.

The production-shaped 138-transfer fixture rereads zero relay payloads on its
second pass. The affected-area gate passes 143 tests with one environment skip;
the authoritative fresh-process repository gate passes all 176 test-bearing
files plus two environment-skip-only files with no failures across 2,608
collected tests. Python compilation and `git diff --check` pass. The schema
migration is additive/idempotent and preserves existing transfer rows; no
message, rule, policy, operator, radio, or FLAMP source file is modified.

Selected-target autoreply follow-up (2026-09-08): production evidence showed
that fixed and dynamic Expect evaluation, routing, and source-state lookup could
all succeed while dispatch was blocked by an unrelated callsign selected in the
receiving JS8Call UI. Every live unattended FIO Spotter autoreply already
converges on one dispatcher. That dispatcher now requests selected-target
clearing from the shared send service, which makes a compatibility attempt
inside the endpoint transaction lock before normal preflight and the explicitly
addressed send. Official JS8Call source review confirmed that released versions
provide only selected-target reading, not writing; therefore stock builds read
back the unchanged target and hold with an actionable deselect reason. The
implementation does not use the less-safe selected-target bypass and
does not change Compose, NCS acknowledgements, pending-message requests, or
other manual/operator-confirmed transmissions. No other live unattended FIO
JS8Call autoreply funnel was found; dormant legacy auto-query code remains out
of scope. Fixed form-Expect and dynamic FLAMP Q end-to-end regressions model an
endpoint that supports the compatibility setter; a separate stock-compatible
regression proves an unchanged target blocks with no `TX.SEND_MESSAGE`. The
affected-area gate passes 219 tests with one environment skip, and the
authoritative fresh-process repository gate passes all 176 test-bearing files
plus two environment-skip-only files with no failures across 2,610 collected
tests. Python compilation and
`git diff --check` pass. No schema, configuration, or persisted data changes.

September 10 production performance follow-up: the semantic and FIO Spotter
behavioral gate remains passed, but the message performance gate is reopened.
Linux evidence recorded 152.752- and 267.269-second native projections that
replayed roughly 10,000-12,000 rows, foreground file completion above five
seconds even when unchanged, SQLite lock errors, and UI watchdog stalls. The
specific remediation authority, batching cadence, writer ownership, migration
constraints, performance budgets, and MIP-0 through MIP-5 gates are defined in
`message_ingest_projection_performance_spec.md`. This follow-up changes the
specification only; implementation has not begun.

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

Implementation result (2026-09-09): **automated Slice 5 exit gate passed**.
SOP actions now use a widget-independent ordered draft collection as the Save
and validation authority. The primary editor is a vertically scrollable set of
guided action cards with group/resource/action choices, timing, contact,
conflict policy, enable/apply state, duplicate, and remove controls. Card
rendering is paged at 12 actions, so a large SOP does not construct an
unbounded widget tree in one paint. The spreadsheet-style editor remains an
optional collapsed bulk adapter; opening it projects from the draft model and
closing it updates the same model without losing card-only fields. Invalid time
text is rejected before normalization, calculated end time is explicit, and
real-time conflict results update the visible card as well as the bulk row.

The SOP workspace stacks fixed-width bands at compact widths and disables page
horizontal scrolling. The Plan Builder reflows plan identity, source controls,
and inline editing between wide and compact grids; detail and RF Guard surfaces
use font-metric-aware bounds. Projection and RF Guard tables have explicit
display caps while summaries retain complete totals. The deterministic Plan
snapshot now includes all schedule/policy fields plus selected plan, source,
radio, time, and display inputs, so polling only rebuilds when an actual
projection input changes.

Acceptance covers SOP empty, populated, invalid, and conflict-heavy states at
900x560 and 1000x700 and Plan default and contextual view states at 1920x1080,
1000x700, and 900x560, with Normal/Large Text in Light/Dark themes. The focused
Slice 5 and affected-area gate passes 321 tests. The complete repository gate
passes in four fresh-process batches: 2,693 tests passed with 37 environment
skips. Warm blank-profile construction measured approximately 38–40 ms for SOP
Builder and 47–55 ms for Plan Builder after first-use font initialization.
Python compilation and `git diff --check` pass. No database or configuration
migration is involved, and no Slice 6 work began.

### Slice 6 — Roster diagnostics and final integration

Deliverables: classified import results, row-level preview/export, exact roster
fixture, cross-feature regression, updated help, and migration/recovery guide.

Exit gate: supplied file reports 166 imported, 18 blank ignored, 4 label rows
ignored, 0 invalid; full performance/platform regression passes.

Risk: **low** for roster reporting, **medium** for final integration.

Implementation result (2026-09-09): **automated exit gate passed**. Roster
parsing now returns explicit `imported`, `updated`, `blank_ignored`,
`legend_ignored`, and `invalid_skipped` totals plus one diagnostic for every
non-header source row. Diagnostics retain CSV line, callsign text, field, and
reason; duplicate callsigns within one CSV are invalid rows rather than silent
last-write-wins updates. A bounded dialect probe supports comma-, tab-, and
semicolon-delimited roster exports without reading the full input twice.

HF Callsigns now performs a read-only current-identity lookup before displaying
the import review. The preview is non-mutating, distinguishes new and updated
operators, shows source line/result in its sample, and bounds the on-screen
diagnostic table to 80 rows while Copy/Export retains the complete report. It
uses a compact scrollable surface without a fixed wide minimum. Closed/former
callsigns are deliberately not treated as implicit roster updates because a
callsign can be reused; reassociation remains the explicit Change Callsign
workflow in Operator History.

Accepted rows are written only after confirmation. The metadata writer exposes
a strict error mode and a write count, no longer commits from `finally`, and the
roster caller verifies the complete count before its single commit. A failed
multi-row import therefore rolls back rather than reporting partial success.
Successful new and updated rows continue through the shared operator identity,
group membership, HF Callsigns, Map/search, and VarAC trusted-callsign sync
paths; no parallel roster store or schema migration was introduced.

The supplied MAGNET roster reports exactly **166 imported, 18 blank ignored, 4
section/legend ignored, and 0 invalid skipped**, with MR01 through MR10 and 188
row diagnostics. On the macOS development host, 500 parses measured 1.374 ms
median, 1.584 ms p95, and 1.991 ms maximum. Focused Slice 6 and affected
operator/platform coverage passes 97 tests with 25 environment skips. The full
repository passes 2,704 tests with 37 environment skips in fresh-process
batches; one larger combined Qt batch completed all 889 assertions before the
previously documented macOS post-summary exit 139, and its two fresh-process
halves then exited cleanly with 465 and 424 passes.

A 120-second isolated real-window soak completed 117 event-loop samples, 45
interactions, 11 resize cycles, and 23 navigation changes. First usable shell
was 869.5 ms, maximum event-loop lag was 1.5 ms, and shutdown was 36.4 ms with
all Qt worker threads stopped cleanly. Physical Linux production validation
remains a pre-main release check and is not represented as having run on this
macOS host.

Delegation and review: Terra/high implemented the widget-independent result
model and bounded preview/export seam. Luna/high updated help and safe
migration/recovery guidance. Luna/medium implemented the exact-fixture,
edge-case, transaction, and compact Qt acceptance package. The high-reasoning
primary model owned transaction and identity policy, reviewed every delegated
diff, closed the implicit former-callsign association risk, ran performance and
platform integration, and completed this gate. No subsequent slice began.

## September 10 Full-UI Responsiveness Requalification

The production logs and seven hang dumps show that the reported regression was
not cosmetic rendering. The Qt thread was waiting in schedule/assignment schema
and database reads, manual-state reads, and process inventory while a historical
message projection lane repeatedly discovered and prepared large batches. The
same run showed eager startup construction of Settings, SOP, and Ops Center and
an unconditional SitRep rollup rebuild during database initialization.

The remediation keeps the first shell and every recurrent control-bar/timer path
cache-only: Settings and SOP data projections are first-use deferred; Ops Center
constructs a clock-only first frame; index maintenance and initial refresh yield
until after the selected surface paints; the scheduler publishes database-backed
state from its own worker; the Station Control Bar consumes that snapshot; and
native message catch-up is queue-first with global/sliced bounds. Startup group
compatibility repair now selects only values normalization can change and rebuilds
SitRep rollups only when a repair actually changed source identity. Shared and
local operator autocomplete/list lookups are read-only and cannot perform schema
or identity repair during a tab activation.

On a disposable copy of the 311 MB production database containing 6,121 SitRep
source rows, 5,998 SitRep event rows, 325 latest-call rows, and 5,214 CommStat
artifacts, the bounded group repair completed in 85.208 ms. The complete nets
schema/startup pass on a fresh copy completed in 1,891.094 ms, compared with the
79,747.936 ms database-init span in the supplied Linux log. The copied database
was deleted after measurement; no production database was mutated.

The automated exit gate passes 201 scheduler tests (one intentional skip), 134
message ingest/projection tests, 91 startup/SOP/Ops/Settings/UI tests, and the
focused startup-repair tests. Linux first-paint, tab-switch p95, command-bar p95,
backlog-drain CPU, idle CPU, and shutdown remain the required external
requalification; the implementation is not described as production-verified
until those measurements pass.

An isolated 30-second real-window navigation/resize soak passed with a 701.6 ms
first usable shell, 612.4 ms construction, 1.2 ms maximum event-loop lag across
114 samples, 25 interactions, 13 navigation switches, six resize cycles, and
62.8 ms clean shutdown. Against a disposable copy of the 311 MB store, the
deferred Settings surface constructed in 266.907 ms and populated on first
activation in 89.014 ms after startup initialization.

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

This section is historical slice guidance. Current and future work is governed
by the authoritative project-wide contract in
`docs/internal/project_delivery_rules.md`; stricter slice-specific safety and
exit gates below remain binding.

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
