# UI Regression Work Log

This log tracks user-observed UI regressions and contract follow-up items that
must remain visible across implementation passes. Use it for issues that are
easy to lose inside broader specs.

## 2026-09-04

### Messages Performance, File Discovery, And `+BBS`

Status: corrective implementation complete; production file-arrival and
multi-location BBS QA requested.

Observation: Messages felt slow, projection-first FLMsg/FLAmp rows no longer
showed `+BBS`, and a received FLMsg artifact could fail to appear. The periodic
refresh also performed repeated bounded projection queries even when only a
local table filter changed.

Causes: projection payloads were excluded by the legacy file-only BBS action;
managed BBS location identity was dropped while converting compose targets;
the action column was too narrow; the file scanner skipped changed descendants
when the root mtime remained stable; projected file receipt time/status were
incorrectly derived from the report timestamp and hard-coded INFO state; and
one timer pass could load up to 20,000 projection rows more than once.

Implementation: projected external file refs now participate in `+BBS` and the
checkbox destination chooser; managed IDs/names are retained; target/published
state lookups are cached; the action column has stable room for actions; file
roots trigger a debounced incremental scan while periodic traversal detects
nested changes; received time is FIO/file arrival while report time remains
event provenance; read state projects as NEW/READ; source/age scope controls DB
reloads; focus counts use one pass; and unchanged projection workers no longer
trigger full table reloads.

Next check: place a new direct FLMsg file and a file in a nested receive folder,
confirm both appear as New within the selected age window, verify `+BBS` offers
the configured FIO-B managed/live locations, publish to two checked locations,
and confirm removing `-BBS` leaves the received source file intact.

### Shared Actionable Traffic Summary

Status: first implementation slice complete; production-data QA pending.

Observation: Ops Center's source-count table was useful but did not yet behave
like a dashboard, while Messages exposed strong intelligence through a dense
set of controls. Operators need a common concise answer to what traffic needs a
reply, what impactful report needs distribution, and why the duty applies.

Contract: `actionable_traffic_summary_spec.md` defines exact user/group
relevance, event-over-social priority, and role-derived duties. Group hierarchy
is not inferred. Hub, Hub-Alt/Alt-Hub, NCS, and ANCS share distribution duty;
Peer remains a reporter role.

Implementation: added a Qt-free actionability projection and one shared summary
widget used by Ops Center and Messages. Ops Center now collapses legacy source
counts behind `Sources`; its action buckets drill into the corresponding
Messages filter. Reading traffic does not complete its operational action.

Next check: verify counts and the lead What/Why line against N1MAG production
traffic containing direct social messages, MR08 group traffic, and impactful
reports addressed to a Hub/Alt-Hub/NCS operator.

## 2026-09-01

### FLDigi NCS Blank Workspace

Status: fixed, pending user QA.

Observation: opening `NCS > FLDigi / SSB` could show the station command bar and
navigation while the FLDigi NCS workspace was blank.

Cause: the FLDigi NCS scroll content was attached to the scroll area inside a
session-context refresh callback instead of during UI construction. If the tab
opened before that callback remounted the content, the tab had no visible NCS
actions.

Fix: mount `_ncs_scroll_content` once at the end of `_build_ui()` and give the
scroll area stretch in the root layout. Session refresh now updates labels and
state without remounting the scroll widget.

### Qt Shutdown Timer Warning

Status: corrective lifecycle fix implemented; production shutdown QA requested.

Observation: closing FIO can log `QObject::killTimer` and
`QObject::~QObject: Timers cannot be stopped from another thread`.

Contract: QObjects that own timers must stop and delete those timers in their
owning thread. Worker shutdown should be queued, non-blocking, and capped by a
short cleanup grace period if the GUI thread waits at all.

Production confirmed that retaining an unfinished mesh worker in a module-level
guard was insufficient: normal window close still ended the process, so Python
eventually destroyed the guarded live `QThread` and its timer. Final close now
hides the window, performs the existing queued shutdown, and keeps the Qt event
loop alive with a lightweight poll until all child and guarded Qt workers have
stopped. Only then is the close accepted. Interactive reconnect/disconnect
retains the existing 200 ms maximum GUI wait.

Next check: reproduce normal app exit after Mesh, Map, Ops Center, and NCS have
all been visited and confirm the clean-shutdown log line is emitted without Qt
timer or live-thread warnings.

### Operator Identity Compatibility-Index Migration

Status: fixed after production-data startup validation.

Observation: startup could report `UNIQUE constraint failed:
operator_checkins.operator_id` after the identity-history rollout. Production
contained exact portable calls such as `KK4CJO/P` and `W3BFO/P` alongside their
base calls. The new resolver correctly associated each pair with one stable
identity, but a unique compatibility-roster index incorrectly prohibited that
relationship.

Contract: identity uniqueness belongs to `operator_identities` and effective
callsign history. `operator_checkins` is an exact observed-callsign compatibility
roster and may retain multiple rows associated with one identity. Migration may
link those rows but must not delete, merge, or overwrite production roster data.

Implementation: schema ensure now replaces the obsolete unique roster index
with a non-unique lookup index before backfill. Callsign change updates one
primary roster row and retains associated portable/variant evidence rows. The
migration is idempotent and repairs the affected database on the next startup.

### Mesh Device Library And Connection Management

Status: partially implemented, active follow-up.

Observation: MeshCore devices are now discoverable and connectable, but the
operator still needs a clearer saved-device library model in Settings and the
station command bar. Known/configured devices should be selectable; unsaved
discoveries should not appear as primary command chips except through an Add
Device path.

Contract: MeshCore, Meshtastic, APRS, and future local-network sources must use
aligned source-connection contracts: saved device identity, protocol family,
lifecycle state, last-known observation data, and explicit operator actions
such as connect, disconnect, manage channels, and add device.

Remaining risk: duplicate labels or raw BLE identifiers in the control rail can
make one physical device look like multiple sources. The UI must prefer the
saved user-facing device name and show raw ids only as secondary detail.

### Dark Theme Contrast Audit

Status: partially implemented, active follow-up.

Observation: several table-heavy settings/views still have low contrast or
light-theme row fills under dark theme, including Mesh Channels and VarAC BBS
settings tabs.

Contract: table rows, selected rows, accepted/pending/error states, tab bars,
and chip groups must source colors from the app theme instead of hard-coded
light backgrounds. Status color must not be the only indicator.

Next check: sweep Settings, Messages, Map detail panels, Ops Center, Plan
Builder, NCS, and VarAC BBS under dark theme and record each concrete offender
before fixing so regressions are traceable.

### Location Confidence And Operational Pins

Status: first implementation slice complete; UI adoption follow-up remains.

Observation: FIO already harvests grids from JS8Call, CommStat, FIOSpotter, and
map projections, but the update/precedence behavior is not yet expressed as one
shared operator-facing confidence contract. SuperSpotter also has useful RF map
pin behavior that should enrich FIO without creating a second map or activity
subsystem.

Spec updates: added a shared Location Confidence Contract, an Operational Pin
contract, Ops Center pin projection requirements, protocol-neutral location/pin
projection rules, and explicit deferred status for store-and-forward.

Implementation notes: added a reusable location-evidence comparison helper in
core projection code. Spotter/FIOSpotter, CommStat, MeshCore/Meshtastic, RF pin,
and future APRS projection paths can now carry a normalized confidence record in
observation provenance while preserving the legacy short confidence label.
Operational RF/app pin candidates can be built from message intelligence through
a receive-gated helper with bounded pin types, source metadata, expiry, and
action-validity fields.

Deferred: store-and-forward remains later consideration only. JS8Call
query-message behavior is sufficient for the current phase; automatic message
waiting advertisements are out of scope unless explicitly re-spec'd.

### Meshtastic Mirrored Local-Mesh Integration

Status: read-only projection slice implemented; live transport work remains.

Observation: MeshCore is now the first live local-mesh path, but Meshtastic
should not become a separate one-off integration. It needs to reuse the same
view-contract, source-connection, channel-policy, retention, topic, Inbox, Ops
Center, Map, and control-bar patterns so future MeshCore, Meshtastic, APRS,
Reticulum/LXMF, and Mesh MQTT work does not fragment the UI.

Spec updates: added a Meshtastic Mirrored Integration Contract to the mesh
client spec and expanded the protocol-neutral connector phases with
Meshtastic-specific read-only prototype requirements.

Implementation notes: Meshtastic adapter normalization now distinguishes
channel text, direct node-to-node text, position packets, and node-info packets.
Direct messages project to the `Direct` feed; node info and position packets are
kept out of Inbox and available as topology/map events. Channel review now sorts
Public and named feeds ahead of generated `Channel ##` rows and reports whether
a private key is already on-device without exposing secrets. Live TCP/serial/BLE
transport calls remain future work and must still be checked against official
Meshtastic docs before implementation.

### FLDigi NCS Start-Net Slide/Vanish

Status: P1 mitigation implemented, needs user QA on macOS window behavior.

Observation: starting an FLDigi/SSB ad hoc net caused the main FIO window to
slide or vanish behind other windows. The start-net flow emitted an NCS status
change and refreshed operator-history views; the shared refresh path always
scheduled a Stations Map render even when the map was hidden.

Contract: NCS start/end state changes may update snapshots, nav badges, and
top-control status, but they must not force hidden map redraws or heavyweight
cross-tab work in the same button event. Hidden map views should be marked dirty
and rendered only when visible.

Implementation: `MainWindow.refresh_operator_history_views()` now mirrors the
local operator-history fanout: load map data, render only if the map is visible,
and mark the map dirty otherwise. FLDigi start-net defers the operator-history
fanout with `QTimer.singleShot(0, ...)` so the active-net UI settles before
secondary refresh work runs.

### Compose Embedded Splitter Handle Leak

Status: implemented, needs visual QA.

Observation: the standard Message Compose tab exposed a resize handle across
each compose mode. It looked like stray full-workbench preview chrome and made
the embedded compose surface feel broken.

Contract: embedded compose panels are automatic responsive layouts with scroll
areas where needed. Visible manual splitter handles belong only in the full
Compose Workbench, where the user explicitly asked for extra space and manual
control.

Implementation: Message Compose now refreshes compose splitter handle width
based on context. Embedded compose uses hidden handles; the full Compose
Workbench restores visible handles, then hides them again when the workbench is
closed.

### Map Hidden-View Render Boundary

Status: implemented, needs stress QA with Mesh/APRS-sized data.

Observation: the first FLDigi NCS fix guarded one cross-tab caller, but hidden
map redraw safety should live at the map component boundary so future callers
cannot accidentally trigger a hidden heavy render. The refresh fallback also
had a recursive `_request_map_refresh()` -> `_schedule_render()` path if the
timer was unavailable.

Contract: hidden or inactive map views may accept retained data updates and
mark the projection dirty, but they must not render, load WebEngine content, or
push map payloads until the map is visible and active. Health/status-only
updates should remain lightweight and independent of map redraw.

Implementation: `StationsMapTab._schedule_render()` now self-gates on app/map
visibility and records pending dirty refresh metadata instead of rendering when
hidden. `_request_map_refresh()` now falls back directly to a deferred flush if
the refresh timer is unavailable, avoiding recursion through `_schedule_render`.

### Meshtastic And FIOSpotter Contract Completion Slice

Status: implemented and covered by focused tests.

Observation: the Meshtastic read-only foundation needed to tolerate real client
library packet objects, not just dict fixtures, before it can be trusted as a
MeshCore sibling. Mesh health also risked leaking raw BLE UUIDs into daily
operator chips and health summaries. Shared Inbox labels still exposed the old
`JS8Spotter` name.

Contract: local-mesh adapters normalize protocol packets into message/node
events at the connector boundary. UI projections consume saved device names and
protocol families, while raw ids stay in diagnostics/provenance. Built-in
Spotter user labels should say `FIOSpotter`.

Implementation: Meshtastic packet normalization now accepts mapping and object
packets, including nested decoded/position/user objects. Text packets become
message events, direct messages use the `Direct` channel, and position/node-info
packets become node events rather than Inbox messages. Mesh connection snapshots
now use saved display names, and shared Inbox source labels use `FIOSpotter`.

Verification: `tests/test_mesh_client_foundation.py`,
`tests/test_source_connection_snapshot.py`, `tests/test_source_control_rail.py`,
`tests/test_observation_projection.py`, `tests/test_location_confidence.py`, and
`tests/test_rf_pins.py` pass together.

### Mesh Saved-Device Selection And Dark Theme Contrast Slice

Status: implemented and covered by focused tests.

Observation: Local Mesh had moved toward a saved-device library, but the daily
source rail still risked treating the friendly adapter id as the connection
target. In a room with two MeshCore cards, this could make the control bar and
Settings disagree about whether MOBL1 or MOBL2 was active. Mesh channel review
rows also used light-theme row colors in some dark-theme settings screens,
making accepted/pending feed text too low contrast.

Contract: saved local-mesh devices are selected by a stable
protocol/transport/endpoint key. The control rail lists only saved devices for
routine Connect actions, activates the selected endpoint, preserves saved
siblings for later selection, and keeps separately configured protocols such as
Meshtastic enabled. Dark-theme row styling must be determined from the active
theme luminance or semantic theme state, not a single exact background color.

Implementation: Mesh settings now exposes saved-device loading, stable
connection keys, active-settings payload generation, and saved-device
activation. The source rail emits endpoint-stable Connect actions. The main
window applies the selected saved endpoint before restarting mesh runtime and
refreshes Local Mesh settings if that tab is open. Local Mesh row-state brushes
and connection banners now use luminance-based dark-theme detection.

Verification: `tests/test_source_control_rail.py`,
`tests/test_mesh_client_foundation.py`, and
`tests/test_source_connection_snapshot.py` pass together. The edited mesh,
source rail, main window, and settings modules compile.

### Local Mesh Runtime Shutdown Affinity

Status: implemented and verified with full-suite/native teardown coverage.

Observation: full application test assertions passed, but an all-in-one pytest
run could exit with a native `139` after teardown. This aligns with the runtime
warning `QObject::killTimer: Timers cannot be stopped from another thread` seen
when exiting FIO. The local mesh worker owns a `QTimer` after being moved to a
worker `QThread`; shutdown must stop that timer on the worker thread before
references are released.

Contract: source connection workers that own Qt timers must stop and delete
their timers on their owning thread. Application shutdown may coalesce status
updates, but it must not abandon a live worker thread or allow Qt timer cleanup
from the main UI thread.

Implementation: `MainWindow._stop_mesh_runtime()` keeps the non-blocking queued
stop request required by the UI Responsiveness Contract, but retains a guarded
reference to any mesh worker/thread pair that does not finish during the normal
shutdown wait. This prevents Qt from tearing down the timer owner from the wrong
thread while still avoiding a long UI-blocking shutdown call.

Follow-up: shared background services that own `QTimer` instances now also stop,
delete, and clear timer references during shutdown. This covers the JS8 receive
hub singleton and the background ingest controller so deferred QObject cleanup is
not the only mechanism keeping timers from surviving application teardown.

Verification: the full pytest suite now completes with `PYTEST_EXIT:0`
(`2214 passed, 37 skipped`) instead of the previous post-summary native `139`.

### Local Mesh Settings Control Density

Status: implemented and focused verification passed.

Observation: the Local Mesh settings panel could clip the BLE scan timeout
spinbox in dark theme and at larger text sizes. Mesh channel review actions were
also rendered as a single long horizontal row, causing buttons such as
`Refresh Review` and category controls to run past the available panel width.

Contract: local source settings must treat connection identity, scan controls,
and feed review actions as bounded control groups. Rows that are likely to grow
with saved device ids, channel names, or larger accessibility fonts must wrap or
split into additional rows instead of relying on horizontal scrolling.

Implementation: the MeshCore BLE device row now uses a two-row grid: saved BLE
identity fields on the first row, scan timeout and scan action on the second.
Mesh channel actions now render in a compact two-row grid so review, join,
category, mute, and refresh controls remain visible on laptop-width settings
screens.

Verification: `uv run pytest -q tests/test_mesh_client_foundation.py -q` and
`git diff --check` pass.

### Station Health Backoff Noise

Status: implemented and focused verification passed.

Observation: Station Health could show JS8Call/API or ingest cooldown rows as a
red `Backoff` warning even when the underlying dependency was reachable and FIO
was only waiting before the next retry to keep the UI responsive. This made a
normal throttling state look like an operator-facing fault.

Contract: retry backoff/cooldown is informational unless paired with a real
operator-actionable issue such as a current error, repeated failure, refused
connection, missing path, or unreadable source. Health copy should explain the
state in operator language and avoid treating responsiveness protection as an
alarm.

Implementation: station health summary rendering now labels cooldown/backoff as
`Retry waiting`, uses informational severity when no real warning condition is
present, and keeps existing warning behavior when a real error accompanies the
retry wait. Runtime ingest source rows already used this calmer contract.

Verification: `uv run pytest -q tests/test_ingest_health.py -q` and
`uv run pytest -q tests/test_release_1_2_2_followup.py -q` pass.

### Station Health Warning Categorization

Status: implemented and focused verification passed.

Observation: Station Health still surfaced several non-actionable states as
yellow warnings after the cooldown/backoff cleanup. JS8Call API compatibility
mode was shown as a warning even though the API was reachable, JS8 native
shadow checks were shown as warnings even though native JS8 remains diagnostic
only, and generic ingest-source waiting rows could appear as warnings without
any actionable error detail.

Contract: user-facing health warnings must represent something the operator can
or should fix now. Optional compatibility fallbacks, diagnostic-only comparison
checks, and normal waiting-for-next-check states must remain visible as
informational diagnostics without increasing the Health attention count.

Implementation: the station health summary now marks JS8 `api_basic` capability
as `Ready (basic)`, marks diagnostic-only JS8 shadow mismatches as `Diagnostic`,
and keeps generic ingest-source waiting rows informational when there is no
error detail. Runtime ingest aggregation still reports real missing or
unreadable active sources as warnings.

Verification: `uv run pytest -q tests/test_station_health_scheduler_filter_1_2_7.py -q`,
`uv run pytest -q tests/test_release_1_2_2_followup.py -q`, `uv run python -m
py_compile freqinout/core/station_health_summary.py`, and `git diff --check`
pass.

## 2026-09-04

### Adaptive Multi-Rig Shell And Navigation

Status: implemented; focused automated verification passed; production Linux
visual feedback incorporated.

Objective: keep Where and When continuously visible while returning most of the
window to each tab's What/Why workspace. One radio is common, two radios plus Mesh
is a realistic maximum, and three radios must remain usable. Normal Text on the
1920x1080 Linux production display is the density baseline; Large Text remains an
accessibility requirement.

Implementation:

- Added a Qt-free shell presenter with roomy, compact, and condensed densities.
- Added calm, upcoming, urgent, and overdue schedule prominence, with transitions
  at 30 and 15 minutes.
- Replaced permanently expanded per-radio cards with a source-awareness rail,
  one selected-radio context row, QSY, Hold/Resume, primary SOP, and Controls.
- Kept infrequent frequency, timed-QSY, suspend, health, and plan controls in an
  on-demand selected-radio panel using the existing RF-safe command handlers.
- Moved clock and enabled operating-group condition summaries from the navigation
  status area into the command-bar shell.
- Added a manually collapsible workflow rail and automatic compact navigation for
  reduced window widths.

User QA follow-up: the first implementation made the selected-radio relationship
too implicit by labeling the primary row `NOW`, even though the selected radio was
also highlighted in the source header. The revised contract labels the row with
the radio itself, such as `FIO-A · AMRRON 20M`, and removes that selected radio
from the header. Alternate radios remain one-click selection targets. Expanding
navigation also exposed a stale single-row shell height that clipped Next and the
quick actions in condensed mode; navigation changes now trigger an immediate
presentation reflow and condensed height derives from scaled control-row metrics.

Second user QA follow-up: expanding Controls repeated the selected radio, current
destination, Next, plan, Health, and quick actions in a legacy radio card. The
collapsed navigation also used platform-dependent icons and direct child shortcuts
such as JS8Call instead of representing the full master menus.

Refinement implementation:

- Replaced the expanded legacy card with a responsive advanced-control tray. The
  persistent context row remains visible; duplicate QSY and Hold quick actions
  yield to target selection, QSY Now, Timed QSY, scheduler Suspend, Resume, and
  compact Health and Plan actions.
- Preserved the existing target model, RF-safe QSY handlers, duration preferences,
  scheduler suspend/resume handlers, health summary, and plan-assignment route.
- Added roomy one-row, compact two-row, and condensed/Large Text three-row tray
  arrangements. The shell height now derives from density and scaled row height.
- Rebuilt compact navigation around the same master hierarchy as the full menu:
  Messages, Net Control, Operators, Plans, Station, and Settings open complete
  child flyouts; Ops, Map, and Help remain direct destinations.
- Replaced platform stock icons with application-owned SVG icons plus short labels.
  Map now uses a map marker, Messages an envelope, and Net Control a radio/wave
  symbol. The active master group uses a stable accent edge and border.
- Added `adaptive_shell_controls_navigation_spec.md` as the detailed behavior and
  acceptance specification. No database or configuration schema changed.

Verification:

- `162` presenter, adaptive-shell, existing main-shell, and responsiveness tests
  pass (`36` focused presenter/adaptive/responsiveness tests plus `126` existing
  main-shell tests).
- Visual matrix exercised 1920x1080, 1000x700, 900x600, Normal and Large Text,
  one to three radios, condition summaries, compact navigation, and the expanded
  selected-radio Controls panel.
- The shell does not require horizontal scrolling in the tested matrix.
- No database or configuration schema was changed.
- The refined Controls tray measured 122px high at 1920x1080 Normal Text, 156px
  at 1000x700 Normal Text, and 258px at 900x600 Large Text, with zero horizontal
  scroll in each visual run.
- Compact navigation exposed all nine master/direct items; the Net Control flyout
  contained FLDigi / SSB, JS8Call, and VHF/UHF rather than routing directly to one.
- `172` related shell, state, and responsiveness tests pass with the one known
  unrelated prewarm assertion deselected; Python compilation and `git diff --check`
  pass.

Final QSY refinement: target lists now omit every assigned-plan option whose
normalized frequency exactly matches the radio's currently reported frequency.
This applies to the quick QSY menu, advanced Controls tray, and legacy selector;
an assigned plan with no remaining destination reports `No alternate QSY targets`.
Frequency comparison uses the runtime MHz label when available and falls back to
the runtime Hz value. No RF action, persistence, or schema behavior changed.

Compact-navigation clipping follow-up: macOS and Linux screenshots showed every
compact button/icon losing its right edge. The fixed button width subtracted the
outer navigation margins but not the compact widget's inner margins, leaving the
button six logical pixels wider than its paint area. Button width now subtracts
both nested margin pairs (60px inside a 74px Normal rail and 74px inside an 88px
Large Text rail), with a regression test for both densities.

Compact-label follow-up: after the geometry correction, the full `Messages` and
`Operators` labels still exceeded the Normal Text button width on production.
Their visible rail labels are now `Msgs` and `Calls`; full `Messages` and
`Operators` semantics remain in accessible names/tooltips, and both buttons still
open their complete master-group flyouts. `Inbox` was intentionally not used as
the master label because Compose is an equal child of Messages.

Known unrelated test state: `test_phase7_main_window_does_not_prewarm_messages_tab`
expects only FreqPlanner prewarming, while the existing runtime helper currently
returns Messages and FreqPlanner. This shell work did not change that behavior.

### Traffic intelligence production QA follow-up

Production screenshots showed unbounded Ops action counts, weak visibility of
the active scope, fixed-width center columns, no category-level unread counts,
and a Green F!701C report incorrectly presented as Reply work.

Implementation:

- Ops traffic intelligence now defaults to the last 24 hours and offers 1h,
  6h, 24h, 7d, 30d, and all-time receive windows. The visible scope states age,
  group, and source, and action drill-down carries those filters into Messages.
- Added a persistent `Traffic by group` view with unread, total, trend, and
  latest-receipt columns. Trend compares the current receive window with the
  immediately preceding equal window and highlights material spikes.
- Message focus buttons now display age- and group-scoped unread counts, while
  an always-visible scope line makes the Inbox filter state explicit.
- Ops table headers now give the information-bearing center columns elastic
  space at both wide and compact widths.
- Reply detection now requires an explicit question or response/acknowledgment
  phrase. Explicit Green/normal/steady reports remain volume evidence but are
  suppressed from action buckets unless the content asks for a response.
- Projection reads accept a receive-time lower bound and a larger bounded query
  limit so current/prior traffic windows can be compared without loading the
  full retained history.

Verification: `364` focused traffic, projection, Ops, Messages, and shell tests
pass with the existing unrelated Messages-prewarm assertion deselected. Python
compilation and `git diff --check` pass. Offscreen visual smoke covered a 930px
compact Ops content width and confirmed filter reflow, persistent Traffic
Intelligence, elastic center columns, and no horizontal clipping. A Messages
widget smoke confirmed all nine focus controls render their unread count. The
existing unrelated Messages-prewarm assertion remains unchanged.

Dark-theme production follow-up: spike rows in `Traffic by group` were using a
light warning fill with the inherited dark-theme light text. Spike rows now use
the existing theme-aware urgency background and foreground as a pair, are
restyled on theme changes, and force the Traffic Intelligence panel to recompute
its content height after row-count changes. Dark visual verification confirmed
`#5B4420` with `#F2F2F2` text and no Sources-button/table overlap.

Ops/Inbox parity and source-clarity follow-up: production showed a nonzero Ops
`Review` bucket opening an empty Inbox at the same apparent scope. Ops queried a
receive-time-bounded canonical projection of up to 20,000 rows, while Messages
loaded only 1,500 rows before applying age and could clear the requested action
bucket while switching focus. Messages now installs all incoming scope values
first, restores the requested action bucket after focus selection, and reloads
the same canonical projection with the receive-time bound and 20,000-row cap.
The shared classifier also prefers canonical projected payload fields over a
derived presentation summary so Ops and Inbox cannot classify the same row from
different evidence.

`Traffic by group` now includes a compact source-mix column. Its always-visible
header reports total, new, and the number of groups rising or spiking; the detail
table can be collapsed with that aggregate signal preserved. The legacy Sources
disclosure identifies CommStat traffic as its own source and labels SitRep as
`SitRep Summary` with aggregate station-status wording, avoiding the impression
that both rows represent equivalent transports.

Verification: `372` focused traffic, projection, Ops, Messages, and shell tests
pass with the existing unrelated Messages-prewarm assertion deselected. Added
regressions cover canonical-row/wrapper classification parity, receive-window
projection bounds, source-mix rendering, the persistent increasing-groups
aggregate, dark spike-row contrast, collapse-state persistence, and separate
CommStat/SitRep Summary source rows. Python compilation and `git diff --check`
pass.

### Traffic-by-group dashboard chart

The detailed traffic-by-group table has been converted to a compact horizontal
comparison chart. Each row uses a solid current-window bar and a dashed
prior-equal-window marker, while retaining exact current/prior counts, trend,
new count, latest age, and source mix in text and tooltips. The table-backed
rendering is intentionally retained underneath the visual delegate so keyboard
navigation, accessible cell text, scrolling, and Enter/double-click Inbox
drill-down remain native Qt behaviors. The visible table header and grid are
removed so the surface reads as a dashboard visualization rather than a data
grid.

Configured operating groups, configured local groups, and explicit groups on
the user's own operator record are marked as operator groups and sorted into the
first tier. Trend and volume sorting continue within that tier; unrelated groups
follow and remain visible as broader event indicators. The persistent collapsed
header still reports total, new, and increasing-group counts.

Verification: `374` focused traffic, projection, Ops, Messages, and shell tests
pass with the existing unrelated Messages-prewarm assertion deselected. Offscreen
visual checks covered Light/Normal at 980px, Dark/Normal at 650px, and Dark/Large
Text at 900px. The chart retained zero horizontal overflow, readable exact-value
labels, distinct current bars and prior markers, and compact 33–36px rows across
those cases. Regression coverage includes operator-group-first ordering,
current/prior chart roles, theme colors, persistent aggregate/collapse behavior,
and group drill-down from any chart cell.

Production chart-copy follow-up: associated operating and membership groups
remain bold and first, but the repeated `My group` phrase has been removed.
Visible metadata is now one compact sequence:
`trend · new · source counts · latest`. The dashboard/focus-search specification
also defines a reusable application-owned icon language for entity kinds,
evidence, actions, schedule/SOP, and RF-readiness views without relying on color,
emoji, or platform icon themes.

Focus-history clarification: operator and event focus must not appear broken
when the selected Traffic Age window has no matching activity. The focus spec
now separates Current Scope from an age-labeled Last Known summary. Historical
evidence may cross the Age filter for context but never enters current traffic,
action, unread, trend, or incident counts. The performance contract uses compact
incremental latest-evidence rows or bounded indexed latest-record probes, a
chunked background backfill, and paginated History drill-down rather than loading
an entity's retained history into Ops Center.

Callsign-change clarification: `Change callsign` is owned by HF Operator
History management. The identity-history spec defines a stable operator id,
effective-dated current/former callsigns, an atomic audited change workflow,
and collision/reuse safeguards. Ops Search consumes the compact alias resolver
so either call opens one operator focus; it does not mutate identities or load
historical traffic for autocomplete. Source evidence keeps the callsign that
was actually transmitted.

### Ops dashboard focus and visual differentiation

Status: implementation complete; production-data QA requested.

Ops Center now treats the former Search FIO field as an explicit operational
focus. Categorized, icon-led autocomplete resolves callsigns and former
callsigns, groups, topics, historical events, geography, bands, and source
families from compact indexes. Typing is autocomplete-only; selecting a result
or pressing Enter applies a session-only focus. Navigation and application
commands remain separate under `Go…` and `Ctrl+K`.

The focus banner always states the intersecting Traffic Age, Group, and Source
scope and renders Current Scope separately from Last Known. Retained read or
archived messages, observations, Spotter status, SitRep status, and Operator
History last-seen evidence can supply an aged Last Known fact without entering
current traffic, unread, action, incident, or trend counts. Message and
observation projections update compact entity summaries incrementally; initial
backfills are bounded, resumable, and backgrounded. Suggestion and snapshot
caches are explicitly bounded and use stale-while-revalidate with request-id
suppression for superseded results.

Operator History now owns `Change Callsign…` and `Callsign History…`. Changes
are one audited transaction over a stable `operator_id`; effective-dated aliases
allow delayed evidence to resolve by event time while later callsign reuse stays
separate. Operator metadata, explicit/inferred peer schedules, awareness pins,
and VarAC tags follow the new current callsign. Received message and observation
evidence retains the callsign actually transmitted.

The default Operations view now uses distinct visual grammars: ranked situation
cards, source-lane cards, a peer rendezvous timeline, a schedule time rail, the
existing current/prior traffic bars, and a three-band RF-readiness ladder.
Dense awareness, source, peer, schedule, and propagation tables remain behind
Evidence/Details disclosures. Compact focus actions collapse into `More…`, and
theme-aware section fills replace fixed light backgrounds.

Verification: `469` focused message, observation, operator, traffic, Ops, and
shell regressions pass with the pre-existing Messages-prewarm assertion
deselected. Python compilation and `git diff --check` pass. Offscreen visual QA
covered Light/Normal at 1000x700, Dark/Large Text at 900x560, and a Dark wide
dashboard at 1600x900; the focus actions and all new dashboard grammars remained
readable without horizontal clipping. The monolithic suite reached `1475`
passes and `3` skips before stopping on the pre-existing stale
`radio_row.addWidget(QLabel("Radio"))` source assertion; an independent Qt/mesh
thread teardown segfault also remains outside this change. Neither occurs in
the focused regression set.

Synthetic autocomplete timing over a 5,000-entity compact index measured a
0.36 ms warm p95 on the development Mac, comfortably inside the 50 ms warm
target; production telemetry remains the authority for Linux hardware.

## 2026-09-04 — Ops Center peer schedule scale and rendering stability

Production review exposed stale/overpainted peer rows while scrolling and
clicking, duplicate rows for the same operator on different bands, a redundant
peer detail table, and uneven vertical spacing between the left and right
dashboard columns.

Peer Schedule Finder now uses one bounded native item-view viewport with one
row per operator. Every matching band/frequency window is retained in that row,
rendered on up to three distinct timeline lanes, and available in the tooltip.
The repeated peer table and nested row widgets were removed. Callsign, operating
group, region, and role filters are populated from operator identity data and
are applied before rendering; a 150-operator roster remains six visible rows
high and scrolls internally. Actions are available from each row's ellipsis or
context menu.

Operational Awareness and peer/schedule cards now use top alignment and
content-derived fixed geometry inside their splitters. Empty action rows are
removed from layout flow, high-frequency view/filter changes no longer animate
nested card heights, and dynamic card containers declare fixed vertical size
policies. Schedule details remain an explicit, hidden-by-default disclosure.

Verification includes consolidated multi-band projection coverage, combined
group/region/role filtering across a synthetic 150-operator roster, repeated
reverse-order redraws, bounded viewport geometry, internal item scrolling, and
source checks ensuring the former duplicate tables are absent. The focused Ops,
awareness, focus-search, traffic-actionability, and shell regression set passes
188 tests with the pre-existing Messages-prewarm assertion deselected. Offscreen
visual QA covered Light/Normal at 1400x900 and 900x700 plus Dark/Large Text at
1200x800, including mid-list scrolling and concurrent 20m/40m lanes.
