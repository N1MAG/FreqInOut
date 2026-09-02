# UI Regression Work Log

This log tracks user-observed UI regressions and contract follow-up items that
must remain visible across implementation passes. Use it for issues that are
easy to lose inside broader specs.

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

Status: specified, needs runtime verification after the next shutdown QA pass.

Observation: closing FIO can log `QObject::killTimer` and
`QObject::~QObject: Timers cannot be stopped from another thread`.

Contract: QObjects that own timers must stop and delete those timers in their
owning thread. Worker shutdown should be queued, non-blocking, and capped by a
short cleanup grace period if the GUI thread waits at all.

Next check: reproduce normal app exit after Mesh, Map, Ops Center, and NCS have
all been visited. If the warning remains, identify the timer-owning object from
shutdown traces and move its stop/delete path onto the owner thread.

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
