# UI Regression Work Log

This log tracks user-observed UI regressions and contract follow-up items that
must remain visible across implementation passes. Use it for issues that are
easy to lose inside broader specs.

## 2026-09-06

### Slice 1 Mesh Lifecycle And Administration

Status: implementation and automated acceptance complete; physical macOS
reconnect and Linux production exit gates pending.

Slice 1 now separates saved mesh connection name, stable adapter/device id,
advertised name, protocol, and optional source radio/role. Untouched connection
names track the selected protocol (`meshcore-1`, `meshtastic-1`), while manual
names survive later protocol changes. BLE setup uses responsive rows and an
explicit background Scan/Cancel lifecycle with immediate progress; results no
longer depend on toggling a second control.

The station command rail and saved-connect actions now lead with the saved
connection label instead of the advertised BLE device name. Scan results keep
the device identity readable as the selectable item, but the secondary details
and tooltips carry the advertised name and stable device id so the UI no longer
mixes saved configuration identity with physical device identity.

The runtime publishes immutable operation snapshots, uses capped exponential
reconnect backoff, supports direct cancellation before the worker event loop is
free, and sequences worker replacement so old and new device runtimes cannot
overlap. Channel discovery is incremental and stops at the protocol no-response
boundary instead of accumulating 32 full timeouts. Explicit Disconnect clears
any pending automatic restart.

The new channel administration component separates device facts from FIO
policy. Operators can accept/ignore feeds and set category, retention, mapped
groups, and Inbox/Ops/Map/topic scope. `Remove from FIO` archives the local
policy without changing the device. Device configuration/removal is available
only through adapter capabilities; removal requires confirmation, unsupported
adapters show companion guidance, and secrets are never displayed.

Primary-model review corrected cancellation propagation, thread-safe adapter
and operation registries, group-string normalization, stale scan results under
another protocol, explicit-disconnect restart inheritance, compact action
reflow, and compatibility with lightweight Settings test doubles. No schema or
destructive data migration was added.

Acceptance results:

- Full repository: 2,418 passed, 37 skipped in 49.18 seconds.
- Simulated five-second stalled BLE scan: visible/click response and resize
  checks under 100 ms, successful cancellation, no live scan thread afterward.
- macOS BLE stack: an initial five-second scan completed in 5,236.8 ms with no
  device present. With the device advertising, a ten-second scan found
  `MeshCore-N1MAG MOBL1` in 10,234.2 ms at RSSI -66, captured stable id
  `97C92879-047E-FEA8-7A11-8A2EE82B381D`, and identified the Nordic UART
  service. Real discovery and identity capture therefore pass.
- The saved-device connection reached CoreBluetooth but macOS returned error 15
  (encrypted pairing timed out). The Bluetooth daemon recorded `lePaired 1` and
  `isPairing=0`: the host considered MOBL1 paired, did not offer a new PIN
  prompt, and the device rejected the stored key. FIO now maps that signature
  to explicit stale-bond recovery guidance. Ordinary disconnect and restart
  are never defined as requiring re-pairing; host re-pairing is a last-resort
  action only when CoreBluetooth explicitly reports incompatible saved keys.
  FIO kept retry work off the GUI thread and the Station Control Bar remained
  present; reconnect and live channel behavior remained pending successful
  pairing.

Live paired-session follow-up then confirmed Companion initialization and real
channel decoding, but exposed a state/lifecycle cluster. Empty channel-capacity
slots were shown as `Channel 4` through `Channel 31`; the detailed Mesh state
said Needs attention while a retained older health row kept the control-bar chip
green; Disconnect followed by Reconnect could lose runtime ownership; and an
empty/failing repeat scan could hide the device-selection action even though MOBL1 had
already been discovered. The Bluetooth trace also showed FIO writing
`SYNC_NEXT_MESSAGE` every second throughout the otherwise idle connected
session.

The corrected path now scans the bounded protocol slots while skipping empty
capacity, hides only legacy pending/device-generated phantom rows, keeps real device
channels ahead of staged FIO channels, and preserves the last valid scan result
and selection action. Idle receive performs no BLE command write until the
device sends its waiting-message push. Runtime references remain owned until
the exact worker thread finishes, and a queued reconnect begins afterward.
Companion notification initialization is now part of the connection contract;
a bare GATT connection cannot paint green. Health selection is newest-first so
an old success cannot override a current error.

Focused follow-up verification passes 138 Mesh adapter, channel, Settings,
worker, lifecycle, reconnect, and control-bar tests. Added regressions cover the
unused-slot stop condition, idle receive write count, legacy phantom filtering,
device-first natural channel ordering, scan-action persistence, disconnect then
reconnect sequencing, and newer-failure-over-stale-success health selection.
The current macOS bond again returns CoreBluetooth error 15 after the successful
session. Physical retest therefore requires forgetting/re-pairing once more with
other MeshCore clients disconnected; the cross-platform exit gate remains open.

The current continuation added focused regressions for saved-connection label
presentation across the station rail, mesh settings, and reconnect indicator.
Those tests now verify that the connect menu, chip labels, and status tooltip
all keep the saved connection name first and only surface the BLE device name
as secondary detail.

The next live run did prompt for the PIN after the host forgot MOBL1 and then
showed the device connected in the Station Control Bar. It also exposed two
distinct stale-action defects: Mesh Settings retained `Needs attention` because
runtime health was only wired to MainWindow, and the explicit saved-device
Connect action was suppressed by the unchanged-configuration signature after a
manual Disconnect. Runtime health is now delivered to Mesh Settings with
transition deduplication, and explicit Connect uses the ordered forced-restart
path. The last observed shutdown completed cleanly in 18.842 ms with all Qt
worker threads stopped.

MeshCore terminology and administration were corrected at the same boundary.
Public, hashtag, and private are device channel types; a contact message and a
direct route are message/routing facts, not a synthetic device channel. The
adapter and protocol-aware staging no longer invent `Direct`, and legacy
synthetic Direct rows are hidden non-destructively from MeshCore channel
administration. Discovery now scans the protocol's bounded eight slots, skips
empty capacity, and continues across gaps so sparse real channels remain
discoverable.

Verification for this correction passes 141 focused Mesh, Settings, lifecycle,
and source-control tests and 298 tests in the expanded Mesh plus adaptive-shell
and Station Control Bar set. The added `gpt-5.4-mini` work package supplied the
saved-connect, live Settings-health, and channel-model regression tests; the
high-reasoning primary model reviewed and corrected the diff, caught the
adapter-id protocol-prefix edge case, aligned the bounded scan with the current
Companion protocol, integrated the implementation, and ran the acceptance set.

The full repository assertion gate was rerun in fresh-process partitions after
the known monolithic Qt teardown crash reproduced at 68 percent. Results total
2,429 passed and 37 environment-dependent skips: 529/2 (`a-h`), 178/0 (`i-k`),
802/0 (`l-m`), 509/6 (`n-r`), 231/28 (`s`), and 180/1 (`t-z`). The `l-m`
partition completed all 802 assertions before returning 139 during process
teardown; all other partitions exited zero. A separate Station Control Bar and
shell regression set passes 157 tests. Inspection against the active
`/Users/bill/RadioCode/runtime/multi-rig` profile confirms that 28 legacy empty
slots are hidden while the five meaningful feeds remain visible; no stored row
was deleted. The newest saved-device health projection is warning, matching the
current CoreBluetooth error rather than the older success.

The next live restart established that discovery identity was still healthy:
with FIO closed, MOBL1 advertised immediately under the exact saved
CoreBluetooth id and name at approximately -52 dBm with the Nordic UART
service. Two isolated connection probes then failed before GATT setup with the
exact raw error `CBErrorDomain Code=14 "Peer removed pairing information"`:
one used the saved id directly and one used a freshly scanned BLE device
object. This distinguishes an external host/card key mismatch from a missing
device or stale FIO identity. The app now preserves the raw platform error in
the log while showing operator guidance in Settings. The live macOS Bluetooth
trace confirms the sequence: `lePaired 1`, LE/GATT connected, encryption failed
with status 706 because the peer removed keys, and macOS disconnected the link
because the peer was no longer paired. The sequence repeated on later retries.

The recovery UI no longer disappears after this failure. MeshCore BLE always
shows the Found row, an empty/failed-scan explanation, Scan, `Use Device`,
`Connect Saved Device`, and `Disconnect`. `Use Device` persists the selected
BLE identity and immediately requests a connection. A normal saved-id failure
gets one bounded mesh-client-style rediscovery by exact id/name and a retry
with the live discovered device object; automatic backoff does not repeat the
scan indefinitely. Explicit Code 14 skips that ineffective scan retry and
reports that normal reconnect should not require pairing, while explaining the
OS-level last-resort boundary. Standard macOS CoreBluetooth does not expose an
application unpair API, and mesh-client uses the same CoreBluetooth boundary,
so neither implementation can silently replace keys the card has discarded.

Post-correction verification passes 144 focused Mesh/Settings/lifecycle tests
and 301 tests in the expanded Mesh plus adaptive-shell/control-bar set. A full
monolithic repository run again reached 68 percent before the previously
documented macOS Qt teardown segmentation fault, this time while constructing
the log viewer; no assertion failure preceded it. The physical reconnect gate
remains open because the current Code 14 state must first be repaired outside
FIO, after which ordinary restart/disconnect/reconnect must pass without
another forget/re-pair cycle.

The first live `Use Device` exercise caught an integration-key mismatch before
the updated process was restarted: Settings correctly persisted MOBL1 but
emitted the friendly adapter id (`meshcore-mobl1`) to an activation function
that requires the stable protocol/transport/endpoint library key. The log made
the resulting no-op explicit as `saved mesh connection meshcore-mobl1 was not
found`. Settings now emits `mesh_connection_config_key(config)` from both Use
Device and Connect Saved Device. Regression coverage asserts that exact key,
and both the 144-test focused set and 301-test expanded set pass afterward.
This FIO action defect is separate from the subsequent BLE attempt, which
again reached CoreBluetooth and returned Code 14.

The two-device MOBL1/MOBL2 production run then exposed a deeper identity and
presentation fault. Both BLE endpoints existed in the saved library with the
same `meshcore-mobl1` adapter and connection name, while MOBL2 was active. A
Settings-owned save was followed immediately by lookup through MainWindow's
stale settings cache. This combination explains the observed mixed MOBL1/MOBL2
form, false Needs attention state, duplicate Connect choices, and Connect no-op
until restart.

The reviewed correction normalizes internal adapter ids against all saved
siblings before filtering runtime devices, retains both physical endpoints,
honors only the protocol-prefixed active endpoint, reloads MainWindow settings
before activation lookup, and matches health by exact device identity. This is
non-destructive and adds no schema migration; normalized identity persists on
the next ordinary activation/save.

Local Mesh now presents saved devices and their exact status/actions first;
nearby discovery and Use Device second; Advanced connection details collapsed;
and channel administration labeled with the friendly saved connection. Adding
a device displays an explicit new-device row so the selector cannot imply an
existing device is being edited. The control-bar menu disables the connected
row, labels the active disconnect target, and promotes Scan for Device. UUIDs
and internal adapter ids remain available only as tooltips/Advanced diagnostics.

Verification: 156 focused Mesh/Settings/lifecycle tests pass, including six
new multi-device/cache-boundary regressions. The expanded Mesh, adaptive-shell,
and Station Control Bar set passes 313 tests. `py_compile` and `git diff
--check` pass. Visual review covered two intentionally colliding saved records
at 1200x900 and 900x650 Large Text. The currently running FIO process predates
this correction, so physical acceptance still requires restart into the new
build; the Slice 1 gate remains open and Slice 2 has not begun.

Current continuation ownership:

- `gpt-5.6-terra` (high): saved-device/discovery-first Local Mesh UI package.
- `gpt-5.6-luna` (high): source-control menu and friendly device action package.
- `gpt-5.4-mini` (high): focused identity, selector, and cache-boundary tests.
- high-reasoning primary model: live evidence analysis, persistence/runtime
  identity design, delegated diff review/correction, visual QA, integration,
  expanded acceptance, specifications, and work log.

MOBL2 physical follow-up passed the normal macOS reconnect boundary. After its
initial PIN exchange completed, `MeshCore-N1MAG MOBL2` reached Companion-ready.
At 18:52:24, FIO Disconnect completed full teardown in 97.7 ms; a direct Connect
at 18:52:30 reached Companion-ready at 18:52:31 using the saved endpoint. The
operator reports the card continues to reconnect well after disconnect. This
is the healthy reference behavior and isolates the T1000-E pairing-key failure
from FIO's shared BLE teardown/reconnect implementation. Channel and shutdown
checks plus the Linux hardware run remain before the Slice 1 exit gate closes.

The final automated hardening pass makes idle MeshCore behavior intentionally
quiet. Recurring channel polling is disabled by default, so a full eight-slot
read happens only after the operator chooses Refresh. Contact discovery no
longer begins on the first worker tick and uses a five-minute default cadence;
passive indications, message receipt, health checks, and reconnect remain live.
This removes the two unsolicited command bursts most likely to delay an explicit
action or put needless pressure on device firmware.

Slow channel refresh cancellation now remains a normal terminal state even when
an adapter returns after cancellation: already staged progress is retained, no
false capabilities/complete event is sent, and expected cancellation does not
surface as an error. Channel administration paints Refresh/Cancel feedback
immediately and keeps policy-summary refreshes from overwriting the live state.

Verification for this pass: 46 focused Slice 1 tests and 317 expanded Mesh,
Settings, adaptive-shell, and Station Control Bar tests pass. Fresh-process
repository partitions total 2,451 passed and 37 environment-dependent skips.
The single-process run reproduced the documented macOS Qt teardown crash at 68
percent in `log_viewer.py`, with no prior assertion failure; every fresh-process
partition exited cleanly. Compilation and `git diff --check` pass. Visual review
covered 900x560 Normal/Large Text and 1200x900, including the scrolled channel
actions. Implementation and automated acceptance are complete; current-build
macOS channel/close validation and the Linux physical matrix remain mandatory,
so Slice 2 has not started.

Final continuation model ownership:

- `gpt-5.6-terra` (high): bounded channel-administration state and tooltip UI.
- `gpt-5.6-luna` (high): read-only lifecycle audit and bounded worker polling/
  cancellation implementation.
- `gpt-5.4-mini` (high): focused idle-poll, channel-cancel, scan-shutdown, and
  channel-state regression tests.
- high-reasoning primary model: polling/concurrency contract, review and
  correction of every delegated diff, stale-test alignment, visual QA,
  integration/repository acceptance, specifications, and final gate decision.

Model ownership:

- `gpt-5.6-terra` (high): responsive connection editor, scan presentation, and
  focused connection UI tests.
- `gpt-5.6-luna` (medium): reusable channel administration component and
  focused channel UI tests.
- `gpt-5.4-mini` (high): focused configuration, retry, cancellation, adapter
  capability, policy archive, and worker lifecycle tests.
- `gpt-5.6-luna` (high): live-follow-up channel provenance/sorting, retained
  scan results, action visibility, and focused UI regressions.
- `gpt-5.4-mini` (high): live-follow-up reconnect, stale-health, and idle BLE
  receive regression tests.
- `gpt-5.6-terra` (high): read-only comparison of mesh-client's Noble BLE
  discovery, saved-device reconnect, service discovery, and picker lifecycle.
- `gpt-5.6-luna` (high): read-only runtime/settings/log diagnosis and live
  advertisement identity verification.
- `gpt-5.4-mini` (high): Code 14 terminal-path, one-shot discovery fallback,
  persistent recovery-control, and connection-action regression tests.
- high-reasoning primary model: architecture, concurrency and shutdown,
  persistence compatibility, isolated live connection probes, runtime
  integration, review/correction of every delegated diff, expanded acceptance
  tests, whole-repository regression, specifications, and final integration
  review.

To close the remaining exit gate, run the five-step physical matrix in the
Slice 1 section of `production_reliability_and_workflow_remediation_spec.md`
with an awake/advertising MeshCore device on macOS and the 1920x1080 Linux
production host. Slice 2 must not begin before those results pass.

### Slice 0 Qt Soak Acceptance Harness

Status: complete; automated Slice 0 exit gate passed.

Slice 0 now has a dedicated offscreen soak runner at
`tools/gui_slice0_soak.py`. It uses an isolated `FREQINOUT_CONFIG_DIR`, cycles
only safe navigation/layout interactions on the real `MainWindow`, samples
event-loop lag on a practical cadence, and records first-usable-shell plus
shutdown timing. The harness is intentionally bounded so CI can shorten the run
with `--duration-sec`.

The corresponding tests verify safe target selection, interaction sequencing,
first-usable accounting, hard Qt warning recognition, and normal shutdown
accounting. The harness explicitly suppresses scheduler tuning, Mesh startup,
background ingest, and application launch so it cannot operate the live station.
It requires an explicit isolated configuration directory and uses a Qt precise
timer so ordinary coarse-timer coalescing is not misclassified as application
lag.

The required 30-minute offscreen run passed on the macOS development host:

- first usable shell: 855.7 ms;
- 17,932 event-loop samples;
- 871 operator-paced interactions, including 436 screen switches and 218 resize
  cycles;
- maximum event-loop lag: 33.7 ms;
- shutdown: 156.4 ms;
- no `QObject::killTimer`, cross-thread timer, or live-`QThread` warning.

A SQLite-consistent clone of the production-sized databases also passed the
shell budgets with 4,219.0 ms first usable, 2,565.0 ms main-window construction,
380.3 ms database initialization, and 16.3 ms shutdown. Seven forced uncached
process inventories ranged from 11.1 to 19.4 ms.

Regression assertions were run in stable partitions to avoid the repository's
pre-existing monolithic-suite Qt/Mesh teardown crash: 1,595 passed/2 skipped,
127 passed, and 672 passed/35 skipped (2,394 passed/37 skipped total). The crash
is a test-process teardown issue rather than an assertion failure and was not
masked by omitting test files.

Model ownership for this slice:

- `gpt-5.6-luna` (medium): bounded performance-log parser, CLI report, and
  focused parser tests;
- `gpt-5.6-terra` (high): deferred-shell foundation, lazy screen factories, and
  focused shell tests;
- `gpt-5.4-mini` (high): initial isolated soak harness and controller tests;
- high-reasoning primary model: architecture, process/dependency cache and
  concurrency ownership, cancellation/shutdown integration, expanded deferral,
  review and correction of every delegated diff, production-clone measurement,
  final acceptance, specifications, and integration review.

No migration was required or added. Slice 1 had not started when this Slice 0
entry was recorded.

### Production reliability and administration review

Status: governing specification complete; Slices 0–1 implementation complete;
Slice 1 physical platform gate pending; Slices 2–6 not started.

The supplied macOS/Linux logs, MAGNET roster, SOP screenshot, existing domain
specifications, and related implementations were reviewed as one dependency
set. The resulting contract is
`production_reliability_and_workflow_remediation_spec.md`.

The logs confirm that perceived slowness is caused by synchronous and overlapping
process inspection, source projection, file discovery, construction, and widget
population. Recorded startup reached 236.5 seconds, native source projection
83.1 seconds, projected-row conversion 29.9 seconds, and unchanged incremental
file discovery 9.9 seconds. Across both logs, FIO recorded 389 slow UI refresh
warnings, 841 slow dependency snapshots, and 32 event-loop stalls.
Performance/lifecycle remediation is therefore Slice 0 and a prerequisite for
the Mesh, BBS, Messages, Launch, and builder UI work.

The supplied roster produces 166 operator entries. Its 22 reported skips are 18
blank separator/trailing rows plus four section/legend labels (`New additions`,
`* = Signal only`, `C.S. Change`, and `Limbo`); no valid operator row was lost.
The new contract separates ignored layout rows from invalid operator rows and
requires row-level diagnostics.

Implementation is divided into gated slices: performance/lifecycle, Mesh,
station-owned BBS, Messages/FIOSpotter, radio launch bundles, responsive SOP/Plan
builders, and roster/final integration. High-reasoning review remains required
for concurrency and persistence boundaries; bounded UI/copy/test work is
explicitly suitable for lower-cost coding models after interfaces are fixed.

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

## 2026-09-04 — Projected file actions, FLMsg titles, and terminal close

Production review found three linked regressions. Projection-first FLMsg/FLAmp
rows painted only `View` even though their event route already supported BBS and
delete operations; unknown custom forms treated `L05` as Subject and displayed
one-character codes such as `C`; and the main window could disappear while the
Qt event loop remained alive.

Projected file rows now share the standard file-management paint gate, exposing
`+BBS` or `-BBS` and `Delete` alongside `View`. BBS target selection remains the
existing checkbox-based multi-location workflow, and removing a BBS association
does not remove the received source artifact.

Unknown custom-form fallback parsing no longer assigns fixed meanings to every
`Lxx` position. Compact coded values are excluded from subject fallback, dates
are detected across the form, and the longest narrative is retained as the
message body. When no descriptive subject exists, cleaned filename text is used;
`W5TTA_TX_RR_20260904-2357z_SquatchOnTheLoose.k2s` therefore displays
`Squatch On The Loose`. Metadata and native-file projection versions were bumped
so cached production rows are re-enriched.

Terminal close still hides the window immediately and waits asynchronously for
all child/guarded Qt workers. Once they stop, FIO now explicitly quits the Qt
event loop after accepting the final close. An isolated macOS reproduction that
previously remained alive until a 15-second test failsafe now exits normally in
about 1.3 seconds with code 0.

Verification: the focused message intelligence, projection, BBS, lifecycle, and
responsiveness set passes 345 tests. A wider message/BBS/shutdown/mesh selection
passes 557 tests with five skipped. The two stale source-contract assertions for
legacy Compose labels and Messages-prewarm policy were aligned with the already
specified and implemented projection-first behavior and pass independently. The
Messages responsive-layout regression also passes independently. Python
compilation and `git diff --check` pass. An initial full-suite run reached 2,372
passing and 37 skipped before those two stale assertions were aligned; a second
run encountered the known independent Qt/mesh teardown segmentation fault at
68 percent rather than a test assertion failure.

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

## 2026-09-06 — MeshCore physical reconnect and BLE ownership

A fresh macOS forget/pair/PIN run connected MOBL1 and delivered sustained GATT
traffic. FIO Disconnect changed the control gray; the following Connect changed
it yellow but did not restore the session. CoreBluetooth tracing showed that
the reconnect found the saved UUID, reached BLE/GATT, and then failed encryption
with status 706 (`peer removed keys`) while macOS still reported the device as
paired. The absence of a second PIN prompt is expected: PIN entry is an initial
pairing or explicit bond-replacement operation, not a normal reconnect step.

Primary integration added a process-wide MeshCore BLE session owner. A
replacement connection cannot open until the prior Companion notifications,
raw BLE client, and asyncio thread are fully stopped. A teardown that exceeds
the bounded wait retains ownership until a background completion guard observes
the thread exit; Connect reports that Bluetooth is still disconnecting instead
of overlapping native clients. Passive link loss follows the same retirement
path before retry. Session-ready and teardown timing logs were added for the
next physical run. This confines FIO's lifecycle contribution but cannot repair
keys already removed by the card.

Delegation: Luna performed the read-only live log/CoreBluetooth correlation;
Terra compared the lifecycle with mesh-client and identified the missing native
session ownership boundary; Mini supplied focused ordering/timeout regression
tests; the high-reasoning primary model owned the concurrency design,
implementation, diff review, documentation, and integration verification.

Verification after review and adjustment of the delegated tests: four direct
disconnect/order/status regressions pass; the focused Slice 1 set passes 148
tests; the expanded Mesh, adaptive-shell, and Station Control Bar set passes
305 tests. Python compilation and `git diff --check` pass. The physical
Disconnect/Connect gate remains open because the current macOS/card bond still
enters Code 14 after the first clean session.

Physical retest correction: the 16:36 run did include the BLE ownership patch.
It connected after the initial PIN, completed FIO Disconnect teardown in 82.4
ms, waited eight seconds, acquired a fresh session, and then failed only when
the card rejected the stored encryption key. macOS recorded a new GATT handle
and `lePaired 1`, followed by SMP status 706 (`peer removed keys`). This rules
out the FIO gate, Qt worker overlap, stale GATT ownership, and insufficient
disconnect delay.

Code 14 now blocks automatic retries and publishes `needs-attention`; manual
Retry Now/Connect allows one diagnostic attempt. The device is now identified
as a Seeed Studio SenseCAP T1000-E running Companion 1.17.0 or 1.17.1. The
directly relevant upstream MeshCore issue #3183 reports T1000-E Bluetooth
timeouts on 1.17.0 and recovery only after a full nRF52 erase/reflash/restore.
Review of the official 1.17.0-to-1.17.1 diff found no T1000-E, nRF52 BLE,
bonding, or framework change; 1.17.1 is therefore not a documented fix. The
unmerged #3263 secured-connection timeout addresses a different half-paired
stall and is not evidence of a Code 14 correction.

The 17:03 physical recovery supplied another clean baseline: after Forget
Device, macOS requested the PIN immediately, accepted it, enabled encryption,
reported pairing success, and stored the pairing; FIO became Companion-ready
and continued receiving GATT indications. This proves discovery and initial
pairing are healthy. No firmware mutation is authorized or required for the
next test. The remaining macOS gate is one controlled Disconnect then Connect
without Scan, device reboot, or Forget Device. The updated focused gate passes
150 tests and the expanded gate passes 307 tests.

The requested controlled test ran at 17:47. Disconnect teardown completed in
41.9 ms. After a twelve-second pause, one Connect acquired a fresh FIO BLE
session and reached the saved T1000-E, then failed with Code 14 because the
peer had again removed/rejected the saved pairing information. No scan, reboot,
Forget Device, second process, or overlapping session occurred. FIO changed to
the yellow/`needs-attention` terminal state and did not resume background
retries through and beyond the former five-minute retry interval. This
physically validates the new ownership and retry-suppression
behavior, but fails the Slice 1 product exit criterion that a normal
Disconnect/Connect preserve the bond. Slice 1 remains open at the external
T1000-E firmware/storage recovery boundary; Slice 2 has not started.

Operator clarification: requiring one card restart is an acceptable small
nuisance when FIO identifies it and presents the next action clearly. The
physical matrix now distinguishes direct reconnect (healthy), one card restart
plus one explicit Connect with the existing bond (acceptable device recovery),
and any requirement to Forget/Pair or repair firmware (workflow failure). The
next device is a RAK WisMesh-style card marked MOKO SMART LW010-R; FIO discovery
will establish its advertised identity before the exact firmware variant is
assumed.

## 2026-09-06 — Slice 2 station-owned Managed BBS

The operator explicitly authorized Slice 2 while the Slice 1 Mesh production-
hardware gate is deferred until tomorrow. The Mesh gate remains open and was
not waived; Slice 3 did not begin.

Architecture and migration: the high-reasoning primary model reviewed and
integrated schema version 2, station ownership, runtime catalog identity,
concurrency boundaries, and live-publication behavior. Schema and legacy-
ownership mutations are independently backup-first and idempotent. The legacy
radio location union is copied once with station rows winning conflicts and
legacy fields retained for rollback. Source state, operator publication intent,
retention expiry, and location enablement remain independent. Retention changes
and source mtime updates recalculate existing mapping expiry.

Runtime: one station catalog is reconciled in a bounded background job and then
projected through each enabled radio. Per-radio live directories use distinct
manifest identities, preventing one radio's reconciliation from deleting the
other's output. Missing sources are effectively unpublished without generating
errors; a folder refresh does not resurrect an operator-disabled mapping.
Compatibility callers lacking an explicit catalog DB stay folder-backed rather
than opening the operator's global DB.

UI: Terra/high implemented and tested the first-class `Station > Managed BBS`
workspace and station routing. The workspace combines a logical location tree,
progressive Add/Edit/Disable policy controls, bounded newest-first artifacts,
publication checkboxes, and origin/path/age/access/retention/health detail. At
compact widths the tree and artifact surface stack and detail is opt-in. Radio
Settings now exposes Radio Paths, Radio Live BBS, Inbound Guard, and a `Manage
FIO BBS` link; legacy shared controls remain hidden compatibility state for
rollback. Messages `+BBS` now edits the same station memberships atomically and
never copies or deletes a received source file on the UI thread.

Primary final review added the remaining product-contract details: visible
whole-day age with exact Local/UTC modified time, a read-only caller-filtered
Visitor Preview, canonical public/callsign/access-code rules, and salted-hash
credential storage. Station-wide allowed-callsign policy is imported once and
used by every radio projection rather than varying by selected radio.

Helper/catalog identity: Luna/high removed the implicit global-database fallback,
threaded explicit catalog identity through publication paths, and replaced
fixed-ten-second visitor instructions with state-based refresh guidance.
Logical labels no longer display `.txt`; VarAC compatibility filenames retain
the extension on disk and old helper names remain recognized.

Focused tests: Mini/high updated mechanical filename, checkbox, uncheck-all,
source-preservation, and helper-contract tests. The primary model reviewed every
delegated diff, corrected hidden Qt widget ownership, added retention-policy
recalculation, selected a useful default location, and refined compact detail
presentation.

Verification: the focused BBS set passes 130 tests with one environment skip;
the related background, adaptive-shell, and Station readiness set passes 42
tests with five environment skips. Migration tests cover verified backup,
rollback on backup failure, idempotency, retained data, and station precedence.
Two-radio tests prove identical catalog content reaches distinct live folders
with distinct manifests. A 10,000-mapping bounded administration query returned
200 rows at p50 2.59 ms, p95 2.77 ms, and max 2.82 ms. Offscreen visual review
covered Light/Normal at 1200x800 and Dark/Large Text at 900x560. Full fresh-
process partitions covered 2,471 passing tests and 37 environment-dependent
skips; three legacy source-contract assertions were updated for the intentional
Station BBS move and pass on rerun. Python compilation and `git diff --check`
pass.

The final combined BBS, Settings, navigation, helper, and legacy-compatibility
selection passes 282 tests with one environment skip.

## 2026-09-07 — Slice 2 production refinement: Mesh recovery evidence and top-level BBS service

Linux production review covered the supplied `freqinout.log` and the saved
T1000-E identified as `FE:BC:04:8F:50:E3` / `MeshCore-N1MAG MOBL1`. In the
captured 10:00–10:09 interval, FIO acquired 17 serialized BLE sessions, reached
Companion-ready twice, and completed eight requested teardowns in 0.4–3.4 ms.
Twelve saved-target attempts and three discovered-target fallbacks failed during
GATT service discovery; two operations timed out and five were cancelled by
subsequent operator actions. No authentication, PIN, removed-key, Code 14, or
BlueZ bond-failure marker was present. The evidence therefore does not justify
asking the operator to forget a valid saved pairing.

MeshCore service-discovery failures now give a bounded recovery path: preserve
the saved pairing, restart the card if needed, wait for advertising, and choose
Connect once. Re-pair guidance remains reserved for explicit authentication,
PIN/passkey, encryption-key, or removed-key evidence. Timed lifecycle telemetry
now separates link connect, link ready, service verification, and Companion
initialization. Retry/backoff behavior was deliberately not changed; carrying
backoff across worker replacement remains a hardware-gated follow-up so this
review cannot introduce a new reconnect regression.

The BBS information architecture now matches the operator mental model. BBS is
a direct top-level expanded and compact navigation destination, not a child of
Station or VarAC Settings. Its guided tabs are `Overview`, `Radio Service`,
`Locations & Access`, `Publishing`, `Visitor Preview`, and `System Helpers`.
Radio Service manages the BBS-specific live folder, service enablement,
publication, and announcement state for each configured VarAC radio while
preserving that radio's native launcher/inbox/outbox settings. Native VarAC
paths and inbound safety remain in Radio Settings, which links back to BBS.

Publishing retains the location-scoped checkbox and graphical detail workflow
the operator approved. Disabled locations cannot remain publication targets.
Visitor Preview is a dedicated read-only caller simulation. Generated helper
files are excluded from Publishing and shown only under System Helpers with a
clean logical name, purpose, compatibility filename, location, whole-day age,
and health. The underlying compatibility files remain intact; this is a UI and
ownership separation, not a destructive data migration.

Delegation and review: Terra/high implemented the bounded BBS navigation and
six-tab UI package. Luna/high independently reviewed the Mesh log, lifecycle
diff, and focused Mesh tests. Mini/high audited focused UI tests; its initial
over-broad source-contract edits were rejected, and the primary model restored
unrelated coverage before retaining only narrow product-contract assertions.
The high-reasoning primary model owned log interpretation, architecture,
persistence and concurrency boundaries, Mesh guidance/telemetry, integration
corrections, specification updates, visual review, and the final exit gate.

Primary review corrected four integration risks before acceptance: publishing
cannot be enabled while the native VarAC BBS service is disabled; disabled
locations are not writable targets; Open Radio Settings carries the selected
radio identity; and the main window's existing radio store is reused rather
than opening schema work on the UI refresh path. Offscreen visual review covered
the six BBS pages at 1200x800 and the Radio Service page at approximately
900x560. The focused Mesh/BBS/Settings/shell acceptance matrix passes 510 tests
with 20 environment skips. Fresh-process repository partitions pass 2,480 tests
with 37 environment-dependent skips; the two skip-only files return pytest code
5 because they collect no runnable tests, not because an assertion failed.
Python compilation and `git diff --check` pass. The Slice 2 software exit gate
is closed; the T1000-E direct reconnect remains a documented hardware follow-up,
with restart-assisted recovery accepted for the present production review.

## 2026-09-07 — Slice 2 BBS workflow and retention refinement

Production screenshots were reviewed against the BBS ownership and responsive
UI contracts before implementation. The BBS workspace now follows the operator
sequence directly: `Radio Service`, `Locations & Access`, `Publishing`,
`Visitor Preview`, and `Visitor Helpers`. The low-value standalone Overview was
folded into Locations & Access. Radio Service uses a side-by-side selector and
editor at normal width and a compact radio selector at 900x560, preventing the
configured-radio list from crushing the selected service controls.

Locations & Access keeps hierarchy visible while placing the wrapped selected
policy and editor together. Publishing and Visitor Preview use horizontally
scrollable location chips. Publishing is file-first and defaults to `In BBS`,
with separate Expired, Removed, and All views; checkbox edits are staged until
Apply Changes and may be reverted. Explicit Remove from BBS, Keep in BBS / Use
Retention, and Republish operations preserve both source files and catalog
identity. Age, remaining expiry, publication health, and exact expiry details
are distinct. Visitor Preview dedicates the flexible column to the full file
name and moves location/access/health into compact columns.

Retention work reused the existing per-mapping `retention_class` and therefore
required no schema migration. Keep clears effective expiry and remains stable
through retention recalculation and reconciliation. Republish starts a fresh
window from the operator action without modifying the source mtime. Remove from
BBS disables every mapping and clears stale expiry without deleting the source
or catalog record. `Return Live BBS Home` was retained as a narrowly named
runtime recovery action; it is not a configuration reset and does not belong in
the primary publishing workflow.

Visitor-generated helper files are now extensionless on disk as well as in the
UI. The first helper is exactly `00 HOW TO USE - Type command then refresh BBS`.
Historical `.txt` forms remain recognized for safe cleanup and transition.
Visitor Helpers omits the confusing compatibility-filename column and keeps
generated navigation material outside operator Publishing.

Delegation and review: Terra/high implemented the bounded Qt layout package;
Luna/high implemented the retention/action and helper mechanics; Mini/high
updated focused UI, persistence, helper, and source-contract tests. The
high-reasoning primary model owned the information architecture, persistence
semantics, specification changes, integration review, and final corrections.
Primary visual review found and fixed a zero-width chip-content issue, initial
location-editor loading through a hidden parent tab, live-folder path cursor
position, selected-filter leakage into the global catalog count, and stale
expiry presentation after removal.

Verification: the focused BBS matrix passes 144 tests with one environment
skip. Related shell, navigation, and Settings coverage passes 289 tests with 19
environment skips. Offscreen visual review covered all five pages at 1200x800
Light/Normal and the core pages at 900x560 Dark/Large Text. A monolithic run
reproduced the repository's known long-lived Qt test-process segmentation fault
at 65 percent in an unrelated log-viewer construction test; that test passes in
isolation. The authoritative fresh-process gate covered all 172 test files with
zero failing files and two skip-only files. Python compilation and
`git diff --check` pass.
