# UI Regression Work Log

All new entries must follow the authoritative multi-model delivery contract in
`docs/internal/project_delivery_rules.md` and record the required package/model,
primary-review, acceptance, and exit-gate evidence.

## 2026-09-13 — Whole-application UI design-control conformance audit

Status: audit and all UIA-0 through UIA-5 remediation exit gates passed; native
Linux visual qualification remains operator-assisted.

All 23 screens in the current MainWindow registry, their routed navigation
contexts, and principal nested tabs/settings sections were reviewed against the
task-oriented workspace guideline, LN-0 geometry fixtures, shared-theme rule,
performance/lifecycle contract, and the newly explicit Font-Derived Vertical
Geometry Contract. The resulting
`ui_tab_design_control_conformance_audit.md` separates confirmed source-level
violations from runtime risks and defines five gated remediation slices after a
shared conformance-harness slice.

The highest-priority confirmed gaps are synchronous database/process work in
Station Overview, Resources, Station Health, Managed BBS and Settings Message
Auth; transient exact-height locking in Settings; fixed 24 px Ops table rows;
nonresponsive Map/NCS/schedule layouts; horizontal scrolling in a normal Compose
setup form; and broad screen-local typography/color/splitter treatments that
bypass the shared theme. The current global text-size guard and 48 px source
heuristic are explicitly classified as safety nets, not compliance proof.

Work packages and models:

- high-reasoning primary GPT-5: rubric, font-derived-height architecture,
  complete surface inventory, concurrency/performance reconciliation,
  specification/work-log edits, delegated-result review and exit gate;
- `gpt-5.6-terra` (high): Settings, Station, Plans, Resources, Shortwave and
  schedule audit;
- `gpt-5.6-luna` (high): Messages, Compose, Spotter, BBS, Map and Ops audit;
- `gpt-5.6-terra` (medium): NCS, Operators, Help, remaining dialogs and
  mechanical geometry/theme scan.

No application code, schema, migration, configuration, RF/device behavior or
production data changed. Delegates edited no files. Primary review preserved
unrelated worktree artifacts.

### UIA-0 conformance harness completion

Status: UIA-0 exit gate passed; UIA-1 authorized next.

The source audit now classifies hard violations and review candidates across
literal text geometry, exact locks, item-view rows/headers, local typography and
colors, and splitter handles, with rule-specific documented exceptions. The
runtime harness covers font/style floors, tabs, item views, scroll ownership,
lazy theme/text-scale lifecycle, bounded settling, all 23 registered screens,
and principal nested workspaces. The shared theme guard now raises missing
font/style floors for native input families, tabs, buttons/checks/radios,
titled groups and table/tree rows/headers after startup or lazy construction.

Work packages and models:

- high-reasoning primary GPT-5: shared-theme architecture and implementation,
  integration, delegated-diff review/correction and gate;
- `gpt-5.6-terra` (high): static semantic audit and seeded tests;
- `gpt-5.6-luna` (high): runtime geometry harness and coverage manifest.

Primary review corrected horizontal-header width/height interpretation,
whole-view versus row size hints, and incomplete dynamic nested-tab manifest
coverage. Acceptance: 27 focused tests pass, Python compilation and
`git diff --check` pass. No persistence, migration, RF/device behavior or
production data changed.

### UIA-1 snapshot and geometry-authority completion

Status: UIA-1 exit gate passed; UIA-2 authorized next.

Resources Frequency Catalog and Net Directory, Station Overview, Station
Health, and Managed BBS now publish bounded generation-keyed snapshots. Their
typing, selection, filtering, resize, theme, paint, and tab-navigation paths
project from the last coherent cache. Slow/stale refreshes neither block the
GUI nor replace newer results. BBS explicit mutations refresh the cache without
losing the operator-facing outcome message.

Settings no longer freezes expanded stack pages to a transient exact height.
Message Auth GPG executable and key discovery runs in a coalesced worker and
keeps current results visible until a complete replacement arrives. All new
workers have bounded shutdown ownership; no schema or migration changed.

Work packages and models:

- high-reasoning primary GPT-5: snapshot/lifecycle architecture, Settings and
  BBS implementation, delegated diff review/correction, compatibility fixes,
  specifications/work log, integration and exit gate;
- `gpt-5.6-terra` (high): Resources immutable snapshot service, Frequency/Net
  cache-only projections, and focused tests; and
- `gpt-5.6-luna` (high): Station Overview/Health snapshot workers, lifecycle,
  coherent-result behavior, and focused tests.

Primary review corrected cross-page snapshot ownership, BBS catalog-only
database compatibility, BBS post-mutation cache refresh/status preservation,
Settings GPG worker coalescing, and legacy exact-height test expectations.
Acceptance: 212 combined UIA-1 tests pass, all changed Python modules compile,
and `git diff --check` passes. No persistence schema, migration, RF/device
behavior, external endpoint, or production data changed.

## 2026-09-11 — Standing multi-model delivery governance

Status: documentation/governance exit gate passed; no runtime behavior changed.

The maintainer's standing authorization for cost-controlled model selection is
now a mandatory repository rule. `AGENTS.md` requires every project task to read
`docs/internal/project_delivery_rules.md`; the Compose workbench, production
remediation, Shortwave, and SDR implementation authorities reference the same
contract without duplicating it. The contract assigns architecture,
concurrency/lifecycle, migrations, destructive-operation review, delegated-diff
review, and final integration to the high-reasoning primary model; assigns
bounded UI, mechanical, audit, fixture, and focused-test packages to Terra,
Luna, or an available Mini-class model; requires pre-coding package/model
reporting; preserves unrelated work; requires specification and work-log
updates; and prevents successor work from starting before its gate passes.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): contract architecture, precedence, integration, delegated-result
  review, repository edits, and exit-gate judgment;
- `gpt-5.6-luna`, medium reasoning: read-only audit of existing model-assignment
  language and the minimum durable cross-reference pattern;
- `gpt-5.6-terra`, high reasoning: independent ambiguity, enforceability,
  safety-exception, reporting, and model-identifier review.

Primary review incorporated both audits, retained the user's standing
authorization verbatim in the canonical contract, clarified unavailable
Mini-class fallback and exact-model reporting, and preserved stricter
specification gates. Acceptance checks verified every mandatory reference,
heading, authorization clause, model boundary, gate clause, work-log clause,
and clean Markdown whitespace. No application code, database, configuration,
or runtime data was changed.

## 2026-09-11 — Message Compose Workbench completion

Status: CMW-0 through CMW-4 automated exit gates passed; Linux production
qualification remains open for the operator's real GPG, NBEMS, Managed BBS,
VarAC, and JS8Call installations.

The workbench now treats composition as the primary task at embedded and
pop-out sizes. The non-modal full workbench is bounded to the available screen,
resize work is coalesced, internal editors yield before clipping, and returning
to embedded Compose performs a clean reparent/layout pass. NBEMS, JS8Call,
FIOSpotter, and CommStat retain independent in-memory drafts; Reset clears only
the active mode and preserves the selected radio.

The keystroke preview path is now memory-only. Destination readiness is cached
after explicit setup changes and revalidated at Stage time. Form discovery and
parsing, signing-key discovery, target schedule/path guidance, and Spotter
MsgAuth lookup use generation-keyed workers. Stage/sign/verify/BBS work and
guarded JS8 preflight/send also run off the GUI thread. In-flight guards remain
active until each QThread actually finishes, closing the rapid-double-click
reference race, and shutdown gives Compose workers a bounded clean exit.

NBEMS presents `FLMsg`, `FLAmp`, and `Both` as file choices, with an independent
VarAC Outbox copy and station-owned `Add to BBS` workflow. Staging uses
temporary files plus no-overwrite publication. Signed FLAmp output must verify
locally before it becomes visible or eligible for BBS publication; failure
never falls back to unsigned. Managed BBS receives the FLAmp artifact when
FLAmp/Both is selected and FLMsg otherwise, and logical memberships update in
one database transaction without direct writes to location/live projection
folders. Partial results retain the draft and enumerate successes and failures.

FIOSpotter Save-to-Expect now creates a disabled, all-radio review draft and
refuses to replace an existing rule or policy. JS8Call, FIOSpotter, and CommStat
RF actions retain selected-target safety preflight and report API acceptance as
`Queued`, not as confirmed transmission.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): completion spec, BBS/signing/Expect ownership, thread/action
  architecture, cached preview boundary, target-guidance worker, delegated-diff
  review, integration, and exit-gate decision;
- `gpt-5.6-terra`, medium reasoning: bounded responsive workbench geometry,
  stable reparenting, scroll/size behavior, coalesced resize work, and
  mode-scoped draft/reset implementation;
- `gpt-5.6-luna`, medium reasoning: staging/signing/BBS failure matrix,
  responsiveness and action contract tests, and replacement of stale static
  assertions with the worker/catalog ownership contract.

Primary review corrected logical multi-location BBS ownership, result snapshot
reporting, send/stage QThread completion races, clean shutdown waits, duplicate
initial setup, destination filesystem probes during preview, stale target
guidance, and draft preservation for the checklist-based BBS selector.

Acceptance evidence:

- 71 focused Compose, NBEMS, workbench, guidance, signing, BBS, JS8Call,
  FIOSpotter, and CommStat tests pass.
- 145 adjacent Expect, JS8 send, Message responsiveness, reader/BBS, and station
  BBS integration tests pass.
- A 120-edit offscreen JS8 preview probe measured 0.809 ms median, 0.838 ms
  p95, and 1.335 ms maximum on the development Mac, below the 16 ms p95 and
  50 ms bounded acceptance targets.
- Python compile and `git diff --check` pass.
- A full-repository run reached 709 passing and 3 skipped tests before the first
  unrelated failure in `test_js8_inbox_ingest_keeps_same_native_id_from_two_sources`;
  its fixture traffic is now excluded by the existing JS8 inbox policy and no
  Compose-owned file is involved. A later all-tests run was stopped after the
  suite accumulated unrelated scheduler workers; interrupting that Qt process
  produced a harness segmentation fault. The bounded Compose and adjacent
  suites exit cleanly.

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

## 2026-09-07 — Slice 3 FIO Spotter and message-ingestion reliability

FIO Spotter is now a lazy top-level station service in both expanded and
compact navigation. Its browser workflow is `Activity`, `Watches`, `Expect`,
`Forms`, and `Imports`. Activity uses the existing bounded message projection
and provides Inbox, Map, Operator, and Compose handoffs. Watches owns CRUD,
enable/disable, expiry, source/radio scope, test matching, last-match count, and
health in the shared station watch store. Imported SuperSpotter search rows are
converted into these watches with provenance instead of remaining only as an
opaque archive.

Expect administration now combines the runtime enabled/paused state, editable
rules and allow policies, caller/group blocks, allow-any, source scope, radio
and JS8 identity, schedule, reply limits, cooldown, and request/reply history.
Forms provides the bounded MCF catalog, purpose and FIO-routing checkboxes,
factory classification, source preview, persistent mapping save, and Messages
Compose handoff. Imports remains preview-first and reports candidates,
duplicates, skips, conflicts, and applied counts. Every page owns its vertical
overflow; page-level horizontal scrolling is disabled, and compact action rows
reflow rather than expanding the shell.

The optional dynamic FLAMP Expect service recognizes only exact,
case-insensitive `E? Q <four hexadecimal characters>` requests. It is off by
default and shares the normal unattended-enable, pause, caller/group policy,
source resolution, RF Guard, and audit controls. A source-scoped additive
projection preserves digit-leading Q IDs and distinguishes complete,
authoritative partial, unavailable, and absent state. `YES` requires a known
total and complete block set; a missing-block response requires a validated
total/block set; ambiguous partial data is held. `NO` requires a recent
successful source scan. Old/replayed and relayed requests are held rather than
transmitted.

The request path performs indexed database reads only. Background projection
runs only while the dynamic service is enabled, performs one directory index,
and reuses unchanged mtime/hash records without reparsing or rehashing files.
Durable atomic request claims enforce replay dedupe, maximum replies, cooldown,
and bounded failed-send retry. A per-endpoint transaction lock covers selected
target handling, preflight, transmit-text setup, send, and claim/audit outcome.

Relevant free-form JS8 traffic now enters Messages from both `DIRECTED.TXT` and
JS8 API events when addressed to the station's current callsign, a historical
callsign alias, or an associated group. Specialized FIO Spotter forms and
dynamic Q traffic retain their dedicated paths. Heartbeat and SNR-only records
are excluded before link/projection work. Source radio, JS8 instance, source
key, and source path are preserved, while semantic cross-adapter dedupe prevents
the same traffic from appearing twice after refresh, rotation, or replay.

Delegation and review: Terra/high audited JS8SuperSpotter 2.6 and implemented
the primary five-tab UI/navigation package; Luna/high implemented the FLAMP
parser/projection, Expect claim/dispatch, and endpoint serialization package;
Mini/high implemented focused heartbeat, bounded-query, and directed-message
tests plus a mechanical directed-ingest pass. The high-reasoning primary model
owned the product boundary, migration and concurrency review, background-scan
safety, historical identity and cross-adapter integration, Forms/Expect
completion, all delegated-diff review, visual QA, and the final exit gate.

Primary review corrected missing local-table initialization on the new directed
path, end-marker interference with noise filtering, duplicate follow-on queue
work, unsafe live relay-directory scanning, repeated O(N²) FLAMP indexing, an
unisolated settings test, stale fixture dates, and compact page overflow. The
new schemas are additive and idempotent; an explicit migration test preserves
an existing watch row across repeated initialization.

Verification: the focused Message/FIO Spotter/CommStat regression set passed
485 tests with two environment skips before final refinements; the final
integrated Slice 3/core/background/shell selection passed 178 tests with one
environment skip. A 100,050-row retained-message fixture proves the 20,000-row core cap,
500-row Spotter service cap, newest-first ordering, and the UI's separate
200-row request limit. Offscreen visual review covered Expect at 1400x900
Light/Normal and Expect plus Forms at 900x560 Dark/Large Text with zero
page-level horizontal overflow. The final authoritative fresh-process run
passed 2,519 tests with 37 environment-dependent skips and no failing files.
The monolithic process again reached the unrelated long-lived Qt log-viewer
lifecycle fault after 68 percent; the affected tests pass in fresh processes.
Python compilation and `git diff --check` pass. Slice 3 is closed, and Slice 4
has not begun.

## 2026-09-07 — Slice 3 follow-up: identity-aware Expect access

FIO Spotter Expect access now accepts `*` as the JS8Spotter-compatible spelling
for any caller. The UI keeps the explicit “Allow all callers” control synchronized
with that token, while blocked callers retain precedence. Rules and reusable
allow policies can also permit every trusted Operator History identity or only
trusted identities associated with selected roster groups. Addressed JS8 groups
remain a separate field and safety boundary; dynamic FLAMP replies to a group
still require that destination group to be explicitly enabled.

Explicit allowed and blocked callsigns resolve through stable operator identity
and callsign history. The lazy Expect catalog exposes current and former
callsigns plus roster groups through comma-token autocomplete. It is bounded to
2,000 rows, refreshed no more than once per minute while Expect is active, and
is not loaded during startup, paint, resize, or the wildcard-only RF decision
path. The rule list now summarizes its effective access mode rather than showing
only a policy name. Narrow layouts stack rule/editor and audit splitters, cap the
rule-list height, and preserve zero page-level horizontal overflow.

The compatibility review also closed a legacy-editor hazard: saving an Expect
entry from the older Settings surface now preserves trusted-operator and trusted-
group access fields that surface does not expose. A focused regression protects
the richer policy from being silently cleared by checkbox or legacy control
updates.

The supplied Linux log identifies `/home/bill/.freqinout` as the active runtime
root. Its latest recorded launch took 82.4 seconds to startup completion and
82.0 seconds to first usable shell. Named main-thread costs include database
initialization at 11.7 seconds, eager Settings construction at 12.4 seconds,
Ops Center construction at 2.3 seconds, and focus-index backfill at 2.9 seconds.
The log also records an event-loop stall during startup and repeated Station
Control Bar callbacks ranging into seconds. The log contains elapsed-time data,
not process CPU samples, so it supports a startup/work-scheduling diagnosis but
does not by itself quantify CPU utilization. MeshCore service-discovery retries
also overlap the launch, and a subsequent FIO Spotter navigation records an
85.5-second `main_window.set_screen` interval while the watchdog reports another
stall; the Spotter page's own lazy construction accounts for only 0.8 seconds of
that interval. This access follow-up deliberately adds no eager startup work;
database-init/schema repetition, eager Settings, the uninstrumented remainder of
main-window construction, command-bar dependency polling, and main-thread mesh
retry interaction remain the next measured performance targets.

Verification: the final focused access/store regression run passes 17 tests.
The combined Slice 3, background, shell, Operator History/group, BBS
compatibility, condition-alert, and multi-rig integration gate passes 315 tests
with one environment skip.
Offscreen visual review at 900x560 with Large Text and 1400x900 with Normal Text
shows responsive vertical/horizontal split transitions and zero page-level
horizontal overflow. Python compilation and `git diff --check` pass.

## 2026-09-07 — FIO Spotter production activation performance correction

The follow-up Linux log and operator CPU observation exposed a visible-table
regression that the offscreen layout gate did not exercise. FIO Spotter itself
constructed in 534 ms, but the redundant activation refresh held
`main_window.set_screen` for 55–62 seconds on three consecutive attempts and
triggered watchdog stalls. Activity used live `ResizeToContents` headers while
replacing up to 1,400 cells; once visible, those inserts could repeatedly
recalculate table geometry. The screen lifecycle also queried Activity once in
the constructor and again immediately through `set_tab_active(True)`.

Spotter now loads each browser tab once on first visit and updates it thereafter
only through its explicit Refresh or save actions. Activity, Watches, Expect
history, and Forms replace their bounded rows with painting and selection
signals suspended, and populated tables use stable interactive widths instead
of live content measurement. Activity emits separate query and render timings
under `fio_spotter.activity_refresh`.

The database review found another contention source: read helpers were invoking
full idempotent schema/index setup, and each new connection attempted to set WAL
journal mode. Under concurrent Message ingest and MeshCore writes, opening a
read-only tab could therefore contend for write locks. FIO Spotter Activity,
Watches, Expect rules/policies/audits, dispatch audit, FLAMP index status, and
Operator History completion now use short-timeout query-only connections.
Schema creation remains centralized in startup and explicit write paths.

Focused verification passes 57 tests with three environment skips. A visible
200-row/500-character synthetic Activity table refresh completes in 5.7–6.1 ms
on the development Mac, repeat screen activation performs no query, all five
browser tabs switch in 0–16 ms against the local multi-rig profile, and an
isolated two-second Qt event-loop soak consumes 0.2 ms of process CPU. A WAL
contention regression confirms Activity remains below 0.5 seconds while an
ingest writer holds an immediate transaction. Full integration verification is
327 passed with four environment skips. Python compilation and `git diff --check`
pass. The gate also corrected a test-only UTC-midnight fixture whose
one-hour backdating could precede its day-granularity roster assignment; no
production identity behavior changed.

## 2026-09-07 — Production hang and Mesh retry-ownership remediation

The supplied watchdog dump resolves the reported Spotter-rule crash as a GUI
thread hang. The main thread was inside Settings save, then full runtime
projection refresh, then the legacy Settings Spotter mapper's per-cell widget
rebuild. FIO Spotter Forms already owns the same mappings, so the duplicate
Settings mapper, its Refresh/Auto-Classify controls, callbacks, and rebuild
paths were removed. Existing mapping records are preserved byte-for-byte by an
unrelated Settings save. The save path no longer forces a full multi-radio
projection refresh, eliminating the exact captured stack without changing the
top-level Forms workflow or its data contract.

The Linux MeshCore review found repeated replacement workers could lose their
retry delay and begin a new saved-device service-discovery attempt while the
prior attempt was still unwinding. Retry state now has a short process-local
handoff across replacement, and one endpoint-scoped lease prevents overlapping
connect attempts. A deferred replacement does not increment the failure count;
manual Retry Now remains explicit. BLE failure logging now identifies link,
service discovery, and Companion initialization separately. This work does not
forget, pair, reset, write channels, or modify the mesh device, and introduces
no persistent migration. The T1000-E Linux hardware gate remains open.

Delegation and review: Terra/high removed the bounded legacy Settings surface
and added mapping-preservation/save-scope regressions. Luna/high implemented
the retry handoff, attempt lease, stage diagnostics, and focused lifecycle
tests. Mini/high performed a read-only lifecycle/performance audit that kept
the separate Station Control Bar polling concern out of this narrow incident
fix. The high-reasoning primary model owned the incident diagnosis,
concurrency/data-ownership review, delegated-diff integration, and acceptance
gate.

Focused verification passes 232 Settings, FIO Spotter, Mesh lifecycle,
reconnect, settings, channel, and responsiveness tests. The authoritative
fresh-process repository gate passes 2,534 tests with 37
environment-dependent skips across 177 test files; the two skip-only files
return pytest's no-tests-collected status and contain no failure. Python
compilation and `git diff --check` pass.

## 2026-09-07 — Slice 3 follow-up: Expect stability, intelligence, and FLAMP Q hot path

The latest supplied production log is a short 39-line fragment covering
19:15:38–19:16:06. It records one MeshCore BLE `link_connect` failure
(`failed to discover services, device disconnected`), continued scheduler
activity, Station Control Bar callbacks of 340 ms and 1,648 ms, and then ends
on a normal scheduler line. It contains no traceback, Qt fatal, shutdown,
Spotter interaction, or process-exit marker. Mesh is a real source of retry and
status churn, but this artifact does not establish it—or the Expect editor—as
the exit cause. The earlier watchdog dump still resolves the only captured
main-thread hang to the now-removed duplicate Settings Spotter mapper.

The Expect administration workflow now uses an explicitly editor-owned
`QCompleter` and `QStringListModel`, caps its visible results, and suppresses an
empty popup. The reusable policy is visibly optional; saving one selects it for
the current rule and tells the operator to use **Save rule** to attach it. If a
selected policy disappears, stale editor fields are cleared. Query failures are
logged and presented as storage/startup-repair failures rather than as a false
empty policy catalog. Opening the dynamic Q editor and saving or failing to
save a rule/policy now produces a compact diagnostic log event. The primary
integration review removed a delegated page-triggered schema upgrade because
it violated the query-only reader contract; startup remains the only migration
owner.

FIO Spotter Activity now consumes the shared message projection's topics,
confidence, recommended action, and intelligence provenance. A first-class
Message Intelligence strip reuses the same operator/group-duty classifier as
Ops Center and Messages, so Reply/Relay/Review/Social counts, What/Why,
selected-row Action, and bucket filters agree. Source and intelligence chips
filter the already-loaded 200-row page in memory and never issue a second
query. Raw source evidence remains available beneath the assessment.

The dynamic FLAMP Q request path was changed from catalog-wide processing to
one indexed Expect-key read plus one referenced-policy read. Group-reply
authorization is carried in the evaluation result rather than recovered by a
second full rule/policy scan. Latency-sensitive evaluation audit, durable
claim, completion, and dispatch-audit writes use an initialized-runtime SQLite
connection that does not repeat schema or journal-mode setup. FLAMP state
lookup is query-only, and the on-air path no longer stats a relay file. The
background index clears persisted source mtime/hash when a file disappears, so
database state safely distinguishes removed content (`NO` after a recent
successful scan) from a present but non-authoritative transfer (hold).

Reference performance with 500 unrelated rules: 1,000 audit-disabled
`Q 970F` evaluations completed in 527.6 ms (0.528 ms average). Two hundred
evaluation + audit + durable claim + completion cycles completed in 436.9 ms
(2.184 ms average). A regression proves the Expect-key index is selected, only
the `Q` candidates and referenced policy are read, no second catalog pass
occurs, runtime claim/completion do not invoke schema setup, and the live Q path
does not touch relay files.

Delegation and review: Luna/high performed the read-only production log,
Mesh-lifecycle, and incident analysis; Terra/high implemented the bounded
Expect editor/completer and initial Activity projection package; Mini/high
implemented focused optional-policy, responsive-layout, stale-policy, cached-
filter, and projection tests. The high-reasoning primary model owned the
architecture/concurrency boundary, rejected the UI migration, implemented and
benchmarked the Q/runtime SQLite path, integrated shared Message Intelligence,
reviewed every delegated diff, and ran the final gates.

Verification: 76 focused Spotter/Expect/projection tests passed, followed by
642 integrated FIO Spotter, Message Intelligence, directed-ingest, shell, and
Mesh tests. The authoritative fresh-process repository gate passed 2,546 tests
with 37 environment-dependent skips across all 177 test files. Python
compilation and `git diff --check` pass. Offscreen visual review covered
900x560 Dark/Large Text and 1400x900 Light/Normal; the transient first-frame
capture was rerendered after the event queue drained and the stable views show
no clipping or page-level horizontal overflow. No Mesh device setting,
pairing, channel, or firmware state was changed. The physical Linux T1000-E
retry gate remains open.

## 2026-09-07 — Slice 3 follow-up: Expect radio workflow and dropdown legibility

The Expect editor now labels the command field **E? Token**. New rules default
to `All JS8 radios — reply on receiving radio`; a radio restriction is selected
by the configured FIO radio name and is limited to profiles with JS8Call
capability. The rules table uses the same human-readable radio name. The former
operator-facing Source Scope, numeric Radio ID, JS8 Instance, and Schedule rows
are removed. FIO derives routing from the receiving radio. When an existing
rule is saved without changing its radio selection, legacy instance/schedule
metadata is preserved; changing the radio deliberately adopts current FIO
routing with no stale per-rule override. No migration or device state change is
involved.

All QComboBox instances owned by FIO Spotter were audited. Watch type, match,
and priority; Expect policy, radio, policy management, and policy scope; and
per-form purpose selectors now expand within their layout while their popup is
sized to the longest bounded item and every item has a tooltip. This avoids
clipped choices without introducing a page-level minimum width at compact or
Large Text sizes.

Focused verification passes 73 FIO Spotter/Expect runtime, dispatch, store,
dynamic FLAMP Q, and access tests. Compact 900x700 rendering at 125% text scale
was inspected at the top and editor scroll positions: the page has no
horizontal overflow, the radio choice is fully readable, and removed routing
controls do not leave dead space. The authoritative fresh-process repository
gate passes all 175 test-bearing files plus 2 environment-skip-only files with
no failure across the 2,582 collected tests. The known long-lived monolithic Qt
process still reaches the pre-existing LogViewer/thread lifetime segmentation
fault after 66%; the same files pass in isolation.

## 2026-09-07 — Slice 3 follow-up: neutral hints and multi-group Expect access

FIO now has a permanent product contract prohibiting real or plausible amateur
callsigns in placeholders, tooltips, empty-state prompts, and other UI hints.
Spotter Allowed callers uses `Callsign or *`; remaining hard-coded callsign
examples were removed from BBS location access, Peer Schedule, MsgAuth guidance
and bulk import, and the legacy BBS sweeper JSON hint. A repository-wide AST
contract test scans GUI placeholder/tooltip/status-help calls so future examples
cannot silently reintroduce identity-like hints. Live autocomplete remains based
on the operator's configured data and is therefore intentionally not treated as
sample copy.

The Expect access vocabulary now distinguishes **Query groups** (JS8 group
destinations whose group-addressed E? requests may be answered) from **Trusted
operators from groups** (trusted Operator History identities authorized through
their group memberships). The same labels and copy apply to reusable policies
and rule summaries.

Verification passes 79 focused Spotter, Expect, dynamic FLAMP Q, BBS, Peer
Schedule, and MsgAuth tests. The authoritative fresh-process repository gate
passes 176 test-bearing files plus 2 environment-skip-only files with no
failure across 2,584 collected tests. Dark theme at 900x700 and 125% text scale
was visually inspected at the Expect access editor: both group concepts,
neutral hints, radio selection, checkboxes, and actions remain readable without
horizontal page overflow.

## 2026-09-07 — Slice 3 follow-up: visible Expect access lists and Spotter icon

Expect access editors now separate lookup/custom entry from the accepted-value
list. Enter, autocomplete selection, or **Add** creates a removable chip and
immediately leaves the field ready for another value. Callsigns and group names
are normalized to uppercase, query groups receive one canonical `@` prefix, and
duplicates are ignored whether they came from lookup, custom entry, pasted CSV,
or case variants. Custom query and operator-group values remain supported.

The caller-membership field is now labeled **Trusted operators from groups** so
it cannot be mistaken for **Query groups**, which remains a destination rule.
The FIO Spotter navigation asset now uses the same 24-pixel canvas, blue accent,
stroke weight, and line geometry conventions as the other main-navigation icons.

Verification passes 120 focused Spotter, Expect, dynamic FLAMP Q, shell, and
navigation tests. The authoritative fresh-process repository gate passes all
176 test-bearing files plus 2 environment-skip-only files with no failure across
2,585 collected tests. Compact Expect layout tests cover Normal and 125% text at
900x560 with populated chip lists, no page-level horizontal overflow, readable
chip height, custom query groups, removal, and case/prefix duplicate suppression.
The populated editor was also visually inspected in Dark theme at 1400x900 and
125% text: the page scrolls instead of compressing chip, input, or Add controls.

## 2026-09-08 — Slice 3 follow-up: repeat lookup reliability

Qt may write a clicked completer value into its line edit after application
activation handlers return. That platform-dependent ordering left the accepted
value in the lookup field, so the next search appended to stale text and yielded
no useful results. The token editor now performs a next-event cleanup after a
completion is accepted, resets the completer prefix, preserves every accepted
chip, and keeps focus ready for the next value. Chip widgets are reconciled
incrementally so a deferred deletion from the first selection cannot collapse
the replacement chip canvas during the second. A UI regression clicks two
successive live popup results and verifies both chips remain visible with
non-zero geometry while the entry is empty after each selection.

The persistent **All JS8 radios is the normal choice...** paragraph was removed
from the rule editor. The selector already communicates the default and detailed
routing guidance remains appropriate for Spotter Help.

Verification passes 120 focused Spotter, Expect, dynamic FLAMP Q, shell, and
navigation tests. The fresh-process repository gate passes all 176 test-bearing
files plus 2 environment-skip-only files with no failures across 2,585 collected
tests. Dark theme at 1400x900 and 125% text was visually inspected after two
successive popup selections; both chips, the cleared input, and the surrounding
form remain visible without compression.

## 2026-09-08 — Slice 3 follow-up: production dynamic FLAMP Q dispatch

Production `DIRECTED.TXT`, `ALL.TXT`, application log, and read-only Expect
database evidence were correlated. Two requests were received: compact
`E? Q906F` and spaced `E? Q 906F`. The compact spelling exposed a parser
compatibility gap. The spaced request was ingested 16 seconds after reception,
matched the enabled MAGNET policy through trusted Operator History, and resolved
Q 906F as an authoritative 28-block partial transfer missing blocks 26 and 27.
The durable dispatch audit proved that FIO then blocked transmission during
JS8Call preflight after timing out waiting for `STATION.CONFIG`; the displayed
reason named the first capability warning rather than necessarily identifying
the later blocking condition. Code review found that the client required FIO's
private request `_ID`, although released JS8Call builds commonly return standard
response types without echoing that field.

The exact query parser now accepts both spaced and established compact syntax
while continuing to reject extra text. Native JS8 API requests correlate by
`_ID` when available and otherwise by the oldest pending request's explicitly
declared response type, matching released JS8Call behavior without allowing
unrelated RX events to satisfy a request. Preflight summaries prioritize the
actual blocking issue. Dynamic-Q receive, hold, and final dispatch decisions now
produce concise operational log records in addition to durable database audit.

Dynamic-Q observation now tails changed `DIRECTED.TXT` sources on a dedicated
three-second lightweight cadence. It uses a separate incremental checkpoint,
inherits the established Spotter checkpoint on upgrade, avoids all general
message projection and FLAMP filesystem scanning, and leaves the 90-second
message pass responsible for normal projection without reevaluating the same Q
record. Disabled or paused service consumes and audits the query as held so it
cannot transmit later after a state change. The dedicated checkpoint is seeded
before the initial broad message pass so a request received during startup
cannot be skipped.

Focused verification covers both production line shapes, incomplete append
recovery, incremental offset behavior, change-only scheduling, ID-less JS8Call
response correlation, guarded send through ID-less responses, and blocking-
reason presentation. No production database was modified and no schema,
operator, radio, rule, policy, or FLAMP state changed. The focused affected-area
gate passes 97 tests with one environment skip. The authoritative fresh-process
repository gate passes all 176 test-bearing files plus 2 environment-skip-only
files with no failures across 2,591 collected tests. Python compilation and
`git diff --check` pass.

## 2026-09-08 — Slice 3 follow-up: FLAMP offline receive-state catch-up

Read-only review of the supplied production databases resolved the stale
`Q 906F 26,27` answer. The source-scoped projection still represented an older
partial file in `FLAMP/relay`, while Messages had already indexed a newer,
exact-name completed artifact under FLAMP's dated `rx` output. Review of FLAMP
2.2.14 source and its operator documentation confirmed the two artifacts have
different lifecycle semantics: relay files are saved snapshots, while the RX
artifact is written after FLAMP reports checksum-validated completion. Its
public XML-RPC interface does not expose the live receive queue or a save-relay
operation, so FIO cannot safely infer unsaved partial fills or compete for
FLDigi's receive stream.

The dynamic FLAMP projection now performs bounded offline catch-up. It scans
only the configured RX root and one date-directory level, accepts a regular
exact-basename completion whose filesystem time is at least the relay snapshot
time, and upgrades that transfer to complete. Relay parse results persist the
AMP filename, expected size, file size/mtime/hash, evidence kind/path, parser
version, and successful validation generation. Existing rows migrate
additively and reparse once; subsequent unchanged passes stat the bounded
manifest but do not reread relay payloads. Removing completion evidence reverts
to the validated relay facts, while a temporarily inaccessible relay or RX root
records a failed scan, retains the last good row, and forces Expect to hold.
Partial missing-block replies use the relay file's own modification time and
require a snapshot no more than ten minutes old. An old partial file is retained
for operator context but held for automatic reply; a fresh scan timestamp can no
longer make stale missing-block evidence appear current.

The first background Expect worker run completes one projection before
consuming its dedicated checkpoint, preventing a startup query from using the
previous process's row. A separate 30-second worker refresh owns later
reconciliation, and enabling the service requests that projection immediately.
The three-second directed tail stays database-only after the startup gate, and
general message ingestion no longer duplicates the scan.
Profile-specific FLAMP receive roots and the shared fallback relay root are
resolved explicitly.

No subagent was used for this follow-up; the high-reasoning primary model owned
the protocol/source review, state and concurrency design, additive migration,
implementation, regression review, and integration gate. Verification passes
143 focused FLAMP/Expect/background/BBS/UI tests with one environment skip. The
authoritative fresh-process repository gate passes all 176 test-bearing files
plus two environment-skip-only files with no failures across 2,608 collected
tests. Python compilation, a real FLAMP `b2s` parser check, and
`git diff --check` pass. No production database or FLAMP source artifact was
modified.

## 2026-09-08 — Slice 3 follow-up: JS8 selected-target compatibility

The supplied production logs proved that FIO received and evaluated both fixed
and dynamic Expect traffic but blocked dispatch when JS8Call retained an
unrelated selected callsign. The live-transmit audit found one unattended FIO
Spotter funnel: fixed/form Expect and dynamic FLAMP Q both call
`dispatch_expect_auto_reply`. Manual Compose, NCS acknowledgement, pending
message query, group/single Spotter, and End Net transmissions are not automatic
replies and retain their existing operator-confirmation and target safeguards.
Dormant legacy auto-query branches remain unchanged.

Unattended Expect dispatch now asks the shared guarded-send service to make a
best-effort compatibility clear of the selected callsign. The attempt,
target-state verification, remaining preflight, and send stay inside the
existing per-endpoint transaction lock. Official JS8Call source review found no
released selected-target setter; stock builds therefore retain the selection,
and FIO holds with an actionable manual-deselect reason. FIO still
blocks on disabled TX, queued frames, non-empty TX text, RF Guard, access,
claim, cooldown, and audit failures; it does not bypass selected-target
verification. End-to-end tests cover both a fixed FIOSpotter form response and
a dynamic `E? Q` response against a setter-capable compatibility endpoint. A
stock-compatible regression proves that an unchanged selected target blocks and
produces no `TX.SEND_MESSAGE`.

Delegation and review: Terra/high performed the read-only automatic-transmit
inventory and scope audit; Luna/high made the single bounded dispatcher change;
Mini/high added the fixed and dynamic end-to-end regressions. The high-reasoning
primary model defined the concurrency/safety contract, reviewed every delegated
diff, updated both governing specifications and this work log, and ran final
integration. The affected-area gate passes 219 tests with one environment skip.
The authoritative fresh-process repository gate passes all 176 test-bearing
files plus two environment-skip-only files with no failures across 2,610
collected tests. Python compilation and `git diff --check` pass. No database,
configuration, migration, radio, or source artifact was modified.

## 2026-09-08 — HF Callsigns deferred-screen load repair

The Linux production log showed deterministic `AttributeError` failures in
`main_window.create_operator_history_tab` each time **HF Callsigns** was selected;
Local Operators and Local Reports loaded normally. Code review found that the
Slice 0 lazy factory evaluated `tab.on_settings_saved` even though
`OperatorHistoryTab` did not implement that hook. The exception occurred after
the widget was constructed but before it replaced the placeholder, which made the
screen appear not to load.

Operator History now implements a lightweight settings callback that reloads
settings and reapplies presentation without rebuilding its data table. The lazy
factory also looks up that callback defensively. Deferred factory failures now
keep the stable placeholder available for retry and emit the screen label plus a
full exception traceback, replacing the prior class-name-only production clue.
No database, runtime configuration, message, radio, or Mesh state was modified.

Delegation and review: Terra/high correlated the production log, launcher, and
failure timing; Luna/high independently audited the HF factory and identified the
missing callback boundary; Luna/high reviewed the focused regression coverage.
The high-reasoning primary model reconciled the recommendations, implemented the
settings contract and defensive failure boundary, and performed integration
review. The focused deferred-screen, HF/Local operator, and navigation set passes
194 tests. The authoritative fresh-process repository gate passes all 176
test-bearing files plus two environment-skip-only files with no failures across
2,614 collected tests. Python compilation and `git diff --check` pass.

## 2026-09-08 — Messages JS8 noise and duplicate-payload remediation

Production showed the Messages queue dominated by JS8 protocol traffic such as
`SNR?`, `QUERY MSGS`, `QUERY CALL`, grid exchanges, and ACK frames. The same
rows increased projection, filter, focus-count, external-reference, and model
work on every refresh. Source review also found that JS8 native projection fed
identical raw and decoded text to Message Intelligence, explaining doubled
summaries such as `SNR? SNR?` and repeated human text.

Messages now uses one Qt-free, versioned JS8 payload policy at inbox import,
directed/API parsing, compatibility-cache loading, and native projection. Exact
protocol grammar suppresses empty frames, heartbeat/SNR telemetry, ACK/NACK,
JS8 query/control, grid link telemetry, FIOSpotter Expect requests, and
third-party relay frames. Natural-language direct and associated-group traffic
remains visible, including ordinary social text and prose containing words such
as `ack` or `query`. Exact repeated multi-word payloads are canonicalized for
display and intelligence without changing the native JS8 source; simple human
emphasis remains intact.

Suppression is independent of operator lifecycle state. The additive message
projection fields `inbox_visible`, `inbox_suppression_reason`, and
`classification_version` hide existing noise in bounded background batches
without deleting `js8_messages`, external references, JS8 logs, or `js8_links`.
The Ops entity bridge drops policy-hidden message rows while Map continues to
receive station/path/SNR evidence from its independent link index. A dedicated
source-scoped inbox checkpoint advances even when every new row is suppressed,
preventing noise-only bursts from being reparsed indefinitely.

The projected Messages model load is capped at 1,500 rows; the compatibility
JS8 cache is newest-first and capped at 2,000. Visibility-aware and JS8
projection indexes support the bounded reads. Regression coverage proves
anchored classification, meaningful direct/group retention, historical source
preservation, hidden-row reconciliation, duplicate correction, source-scoped
checkpoint progress, blank-slate compatibility, and independent map-link
retention.

Delegation and review: Terra/high mapped the projection contract and data
ownership; Luna/high audited the JS8 leak/duplication path; Luna/high audited
refresh cost; Luna/high implemented the bounded classifier-contract and
integration regressions. The high-reasoning primary model defined the policy and additive
projection design, reviewed every delegated result and diff, implemented the
ingest/projection/UI integration, and ran the integration gate. The affected
JS8 ingest, projection, Messages responsiveness, and Map set passes 329 tests.
The repository-wide gate passes 2,621 tests with 37 environment skips. Python
compilation and `git diff --check` pass. No production database, JS8 source
log, configuration, radio, or Map/link evidence was modified.

## 2026-09-08 — Slice 4 radio launch bundles

Launch Control previously displayed the selected radio's configured software
but stored checkbox/order state in one global `launch_control_items` value.
Automatic startup read that global list while `Start Startup Apps` constructed
a separate selected-radio queue. Monitor Health was an alias for the generic
enabled flag, each refresh forced a process snapshot, and results could not
identify the radio or distinguish application instances.

Slice 4 adds radio-owned bundle and ordered-item tables plus a one-time migration
audit. The startup migration owner checkpoints and creates a timestamped backup,
then imports the legacy list, legacy autostart flags, and reusable station custom
tools in one transaction. Target selection is deterministic (runtime primary,
sole active, legacy default, then first profile); no-radio installs defer without
inventing a profile. Source KV remains unchanged as a read-only fallback only
until confirmation. Backup failure, write failure, repeat startup, and a
pre-existing bundle are non-destructive and idempotent.

A Qt-free `StationLaunchPlanner` now produces both the displayed Startup Preview
and executable queue. It scopes active radios, applies bundle opt-in and ordered
startup rows, hydrates radio-specific software paths and endpoints, topologically
orders dependencies, rejects cycles, deduplicates exact shared identities, and
keeps different endpoint/path/command identities separate. Executor progress and
results carry radio names/IDs plus instance identity. Dependency failure must
cover every radio served by a shared dependent before that dependent can run.
Cancellation immediately stops the pending sequence without terminating external
applications.

Settings keeps unsaved launch drafts separately while switching radios, persists
all staged bundles before reload, and no longer writes the legacy global launch
or autostart keys. Monitor Health and Launch at Startup are independent. The
table, preview, and executor consume shared immutable dependency snapshots;
refresh/save/toggle paths do not synchronously enumerate processes. The Station
Control Bar also filters health with the selected radio's bundle.

Delegation and review: Terra/high performed the runtime, persistence, migration,
and concurrency audit. Luna/high audited and implemented the bounded Settings UI
seam and created the focused Qt-free persistence/planner test package. The
high-reasoning primary model defined and implemented the store/planner/executor
architecture, reviewed every delegated diff, closed multi-instance/dependency
coverage gaps, updated the governing specification and work log, and ran final
integration.

The focused launch/migration/planner/status/multi-rig/Settings gate passes 366
tests with 4 environment skips. The repository gate passes in four fresh-process
batches: 2,634 tests passed and 37 environment skips. A single long-lived macOS
Qt run reached 80% without assertion failure before the test process segfaulted
inside unrelated ControlFreq construction with numerous test-created worker
pools still alive; batching completed every test file. Python compilation and
`git diff --check` pass. The change is additive and does not modify production
settings, launch applications, or begin Slice 5.

## 2026-09-09 — Slice 5 SOP and Plan responsive builders

The SOP Builder's wide action table was still the effective editing authority,
making the normal workflow spreadsheet-like and difficult to use at 900x560.
Its summary cards were read-only, large SOPs could expand without a render
bound, and validation depended on reading Qt cell widgets. Plan Builder kept
plan, source, and inline-edit controls in fixed horizontal rows; secondary
detail surfaces could crowd out the schedule, and its projection snapshot
omitted fields that can materially change the result.

Slice 5 introduces a Qt-independent `SopActionDraftCollection` that preserves
the full persisted action payload and supplies Save/validation. The primary SOP
surface is now editable, vertically scrollable action cards with guided choices
and all workflow fields. The optional Advanced bulk editor projects from and
updates the same model. Card rendering is paged in groups of 12; duplicate,
remove, validation, and conflict state remain model-indexed. Compact layout
stacks management, traffic, and workbench bands without a page-level horizontal
scrollbar. Primary copy no longer describes an internal temporary-table
migration.

Plan Builder now switches between wide and compact grids for plan identity,
source selection, and inline editing. Ingredient/review toolbars retain bounded
internal scrolling, selected-window and RF Guard surfaces use font-aware height
bounds, and the main timeline remains visible. Effective, pattern, and radio
projections are capped at 500 displayed rows and RF Guard at 200 displayed
issues while summaries retain true totals. The projection snapshot is a stable
canonical representation of every source row and all view/source/radio inputs,
preventing missed rebuilds without introducing periodic unconditional work.

Delegation and review: Terra/high audited and implemented the bounded SOP model
and responsive card seam. Luna/high audited and implemented Plan Builder's
responsive UI seam. Luna/medium created and expanded the focused interaction,
viewport, theme, text-scale, state, and field-preservation tests. The
high-reasoning primary model reviewed every delegated diff, replaced unbounded
card expansion with paging, added guided card controls, closed invalid-time and
bulk round-trip field-loss gaps, completed projection bounds and canonical
snapshot coverage, updated the governing documents, and ran final integration.

The focused Slice 5 and affected-area gate passes 321 tests. Four fresh-process
repository batches pass 2,693 tests with 37 environment skips. Warm
blank-profile construction is approximately 38–40 ms for SOP Builder and
47–55 ms for Plan Builder after first-use font initialization. Python
compilation and `git diff --check` pass. This slice makes no schema or production
data changes and does not begin Slice 6.

## 2026-09-09 — Slice 6 roster diagnostics and final integration

The HF Callsigns roster import previously grouped blank separators, roster
labels, and actual invalid operator rows under one `skipped` count. That made the
supplied MAGNET result look as though 22 operator records were lost even though
the parser had accepted every valid operator. The confirmation surface also
showed only a small operator sample and committed through a metadata helper that
could suppress a write exception and commit from `finally`.

Slice 6 introduces a Qt-independent classified result and row-diagnostic model.
The parser reports imported, updated, blank ignored, section/legend ignored, and
invalid skipped separately; diagnostics preserve CSV line, callsign text, field,
and reason. It recognizes the supplied trailing roster labels, rejects duplicate
callsigns in one input deterministically, normalizes mixed group delimiters, and
uses a bounded comma/tab/semicolon dialect probe.

HF Callsigns performs a read-only lookup of current roster/identity callsigns
before review, then presents a compact scrollable preview with result and source
line columns. The displayed diagnostics are capped at 80 rows, while Copy and
Export include the complete report before any write. Cancel remains
non-mutating. Closed/former callsigns are not silently merged during roster
import; the explicit Operator History Change Callsign workflow owns that
association and protects against later callsign reuse.

Confirmed rows use the shared metadata/identity tables in one caller-owned
transaction. Strict write errors propagate, the complete write count is checked,
and failure closes/rolls back without a success message or VarAC sync. Successful
new and updated rows refresh the shared HF Callsigns data and trusted VarAC tag
projection. No database schema or production data migration is involved.

The supplied roster result is 166 imported, 18 blank ignored, 4 section/legend
ignored, 0 invalid skipped, MR01–MR10, and 188 diagnostics. Five hundred parses
measured 1.374 ms median, 1.584 ms p95, and 1.991 ms maximum on the macOS
development host. Focused operator/platform coverage passes 97 tests with 25
environment skips. The repository gate passes 2,704 tests with 37 environment
skips in fresh processes. A combined Qt-heavy batch completed 889 assertions but
hit the already documented macOS post-summary exit 139; its two isolated halves
then exited cleanly with 465 and 424 passes.

The isolated 120-second real-window soak passed with 869.5 ms first usable shell,
1.5 ms maximum event-loop lag, 36.4 ms shutdown, 45 interactions, 11 resize
cycles, and 23 navigation changes. Physical Linux production validation remains
for the pre-main release gate.

Delegation: Terra/high implemented the roster model and responsive preview;
Luna/high handled help and recovery guidance; Luna/medium built the focused
fixture, transaction, and Qt tests. The high-reasoning primary model owned
transaction/identity policy and final integration, reviewed all delegated diffs,
ran the gate, and did not start another slice.

## 2026-09-09 — Local Nets and Tools & Resources specification

Status: specification and implementation plan complete; implementation not
started.

The approved product direction adds `Plans > Local Nets` as a non-commandable
awareness calendar for Amateur VHF/UHF and GMRS activity. Operating Group
association is encouraged but optional. Local occurrences may appear in Ops
Center and link to SOP guidance, but they cannot enter SchedulerEngine, tune a
radio, launch software, or automatically activate an SOP.

The supporting Resources model is defined as reusable information FIO knows once
and uses contextually. The compact navigation label is `Resources`; the workspace
title is `Tools & Resources`. Initial functional areas are Frequency Catalog, Net
Directory, and Resource Import / Export. Empty future areas such as Forms &
Templates are not exposed before implementation.

Repository review found that the existing `net_resources` table is a combined
row library rather than a normalized net directory. The new contract therefore
separates catalog source, frequency/range/channel resources, net identity,
published sessions, HF subscriptions, Local Net subscriptions, and occurrence
state. General digital standards migrate only to Frequency Catalog; credible net
rows may also create directory sessions; ambiguous rows remain review-required.
The legacy table is preserved through an additive, backup-first, dry-runnable,
transactional, idempotent migration and one canonical writer after cutover.

The implementation plan defines seven independently gated packages: audit and
contract lock; canonical resource store/migration; Resources UI; HF directory
subscription; Local Nets; Ops/SOP integration; and release qualification. It
includes responsive, accessibility, performance, recurrence/timezone, scheduler
isolation, production migration, shutdown, and Linux platform gates. No code,
schema, configuration, or production data was changed during this specification
work.

## 2026-09-09 — Local Nets / Resources LN-0 gate

Status: complete; LN-0 passed and LN-1 is authorized next. No later package was
started during this gate.

The repository audit mapped the duplicate `net_resources` schema/bootstrap
ownership in startup and HF Nets, the direct HF Nets and FreqPlanner writers,
the known-group reader, both projection paths, the separate and unnamespaced
Daily Schedule resource IDs, existing Settings `local_net_profiles`, SOP
consumers, Ops Schedule Outlook, named HF source schedules, and SchedulerEngine's
commandable input boundary.

The locked architecture uses typed text keys for catalog/session/schedule
relationships, a shared transport-neutral Operating Group key, accepted
snapshots for subscriptions, startup-only schema assurance, a Qt-free canonical
repository, and a `legacy` -> `shadow_ready` -> `canonical` cutover. Existing
`local_net_profiles` remains lossless compatibility metadata and never becomes a
schedule. Local Nets receives a separate `commandable=false` projection and is
excluded from generic QSY metadata.

Terra/high performed the schema/call-site audit. Terra/medium produced the UI
geometry and seam contract. Luna/high produced anonymized fixtures, the
SchedulerEngine isolation characterization, and baseline evidence. The
high-reasoning primary model reviewed each result, expanded the hidden-reader
map, resolved identity/migration/navigation/SOP decisions, and independently ran
the gate: 60 passed / 1 skipped core and lifecycle tests, 45 responsive tests,
13 geometry tests, and 12 HF source/projection tests. `git diff --check` passed.
No production database, runtime configuration, application feature code, or
navigation was changed.

## 2026-09-09 — Local Nets / Resources LN-1 gate

Status: complete; LN-1 passed and LN-2 is authorized next. Resources and Local
Nets navigation remain hidden until canonical cutover succeeds in LN-2.

LN-1 adds the Qt-free canonical catalog models/store, deterministic Operating
Group identity adapter, bundled US FCC advisory reference manifest, and a
backup-first shadow migrator. The catalog separates source, integer-Hz frequency
resources, net identities, and published sessions. Queries are bounded to 200
rows, read-only queries never create or journal a database, read-only sources
cannot be edited through the station repository, and usage/version-diff APIs
protect referenced schedules.

The startup-owned migration classifies every legacy `net_resources` and
`local_net_profiles` row before writing. Credible legacy nets receive frequency,
directory, and session identities; general standards remain frequency-only;
ambiguous data remains losslessly audit-mapped as review-required. Parseable
legacy local-profile targets may seed reviewed frequencies but never recurrence
or Local Net schedules. Migration backs up affected existing databases, applies
schema/data/group-key changes transactionally, reconciles legacy deltas, and
checkpoints `shadow_ready`; the old UI remains authoritative. Linked HF rows
retain `resource_id` and receive additive canonical session/version snapshots.

Primary-model review corrected transaction ownership around SQLite schema DDL,
Settings save-path key loss, group-name snapshots, linked-HF snapshot migration,
stale legacy deletion reconciliation, bundled-version refresh, and the regular
startup zero-write fast path. The reference package was rechecked against the
current eCFR capture and remains explicitly advisory; it does not evaluate
license, emission, equipment, location, or transmit authorization.

Delegation: Terra/high implemented the bounded catalog repository and scale
tests; Luna/high implemented the Operating Group identity adapter and migration
fixtures; Terra/medium implemented the read-only reference validator and
versioned manifest. The high-reasoning primary owned schema, migration,
transactions, startup integration, compatibility, regulatory framing, and final
review.

Acceptance evidence: 30 catalog/migration/reference/identity tests pass; the
10,000-frequency/2,000-net/5,000-session corpus enforces a warm filtered query
p95 below 100 ms; 132 scheduler/Plan/projection tests pass with one environment
skip; 42 HF schedule assignment tests pass; 40 SOP tests pass; and five focused
initializer/Settings compatibility tests pass. Python compilation and
`git diff --check` pass. No production configuration was used for validation,
no legacy table was removed, and LN-2 did not begin before this gate passed.

## 2026-09-09 — Local Nets / Resources LN-2 gate

Status: complete; LN-2 passed and LN-3 is authorized next.

The backup-first startup cutover now promotes the resource catalog from
`shadow_ready` to `canonical` transactionally and exposes Tools & Resources only
after that state is durable. Full and compact navigation preserve the Resources
master hierarchy and route to Frequency Catalog, Net Directory, and preview-first
Import / Export workspaces. Catalog reads remain bounded and query-only opening
does not create a database or schema.

Existing HF Nets and Plan Builder resource edits now pass through one Qt-free
compatibility transaction owner, which updates the legacy projection and
canonical records atomically. Direct resource DML and table/schema ownership
were removed from GUI modules. Known Operating Group suggestions read the
canonical compatibility API after cutover. Resource lifecycle actions expose
provenance, versions, usage, clone, retire, and guarded deletion behavior.

Delegation: Terra/high implemented the bounded resource workspaces and later
integrated responsive geometry plus preview-first transfer; Terra/medium built
the compatibility-writer and cutover/static ownership tests; Luna/high produced
the workspace contracts. The high-reasoning primary model owned cutover,
transaction boundaries, writer conversion, navigation integration, and final
review.

Acceptance evidence: 279 focused catalog, cutover, transfer, navigation, HF
resource, Plan, and schedule-assignment tests pass. The dedicated cutover suite
proves backup failure rollback and canonical zero-write startup. Responsive
tests pass at 900x560 and 1000x700, including Large Text, and `git diff --check`
passes. No legacy table was deleted and LN-3 did not begin before this gate.

## 2026-09-09 — Local Nets / Resources LN-3 gate

Status: complete; LN-3 passed and LN-4 is authorized next.

HF Nets now supports a source-first Net Directory subscription workflow from
either HF Nets or Tools & Resources. Operators can select multiple published
sessions, choose a named HF Net schedule, and add review drafts without tuning,
launching, or changing the active scheduler. The existing Save Schedule path
continues to own validation, RF Guard, Plan reprojection, conflict review, and
scheduler refresh. Existing subscriptions show Scheduled/Open Schedule.

Each subscribed row persists its canonical session key, accepted session and
frequency versions, and reviewed snapshot through named source storage and both
live HF schedule projections. Directory or frequency changes, missing records,
and retired sessions surface as review-required status; they never silently
replace local schedule values. Duplicate session adds are suppressed. Creating
a directory net uses the stable mutable station source automatically, and a
station-private one-time option retains stable identity without publishing a
general resource.

Delegation: Terra/high implemented and refined the bounded subscription,
directory, and HF Nets UI; Luna/high implemented the focused subscription,
persistence, wiring, and warning tests. The high-reasoning primary model owned
schema/migration changes, immutable snapshot/version semantics, scheduler and
RF Guard boundaries, delegated-diff review, and final integration.

Acceptance evidence: 203 focused catalog, migration, transfer, HF schedule,
Plan reprojection, assignment, and scheduler tests pass; one pre-existing
macOS-environment shutdown test is skipped because importing QtCore aborts in
that test environment. The 17-test dedicated LN-3 suite passes without skips.
Python compilation and `git diff --check` pass. The legacy row library remains
as an explicitly temporary compatibility surface until full parity; it is not a
second canonical writer. LN-4 did not begin before this gate passed.

## 2026-09-09 — Local Nets / Resources LN-4 gate

Status: complete; LN-4 passed and LN-5 is authorized next. Ops Center and SOP
integration did not begin before this gate passed.

LN-4 adds startup-owned, additive Local Net schedule and per-occurrence state
tables; immutable Qt-free schedule/occurrence models; and deterministic bounded
Daily, Weekly, Periodic, Bi-weekly, and one-time recurrence. Projection uses
IANA timezones, skips nonexistent DST wall times, selects the first ambiguous
fold deterministically, handles overnight and leap-day boundaries, applies
effective/exception dates, and never materializes more than the configured
90-day/500-occurrence bounds. Dismissing one due occurrence does not pause its
recurring schedule.

The lazy `Plans > Local Nets` workspace provides Now/Next/Today/Upcoming and
attention summaries, bounded filters and results, known-directory and custom
creation, optional Operating Group association, Amateur/GMRS and
simplex/repeater support, exact local/UTC review, pause, and active-window-only
dismiss. It visibly states that Local Nets are reminders only and cannot tune a
radio. Full navigation presents the master as `Plans` while retaining its
stable persisted internal key.

Primary integration review removed N+1 catalog access: resource status for as
many as 2,000 schedules now uses a fixed two bounded catalog reads, selected-row
dismiss checks project only that schedule, and directory choices show operator
names rather than internal session keys. Explicit frequency overrides capture
their own accepted snapshots. Contextual Frequency Catalog and Operating Group
handoffs carry an immutable, Qt-free `NavigationIntent`; incomplete editor
values survive the trip, Resources offers an explicit return action, and no
draft is written before Save.

Delegation: Terra/high implemented and refined the responsive Local Nets UI and
navigation package. Luna/high built and strengthened the recurrence, storage,
resource, scheduler-isolation, performance, theme, and geometry tests.
The high-reasoning primary model owned schema/migration boundaries, recurrence
and concurrency review, accepted-snapshot semantics, batch-read performance,
typed navigation integration, delegated-diff review, and the final gate.

Acceptance evidence: the dedicated LN-4 suite passes 29 tests without skips,
including its 1,000-schedule corpus and 900x560/Large Text surfaces. The combined
catalog, migration, HF subscription, shell/navigation, Local Nets, and scheduler
gate passes 231 tests; one pre-existing macOS-environment shutdown test is
skipped because importing QtCore aborts in that test environment. Python
compilation and `git diff --check` pass. Static and runtime tests confirm that
SchedulerEngine never reads Local Net inputs and no QSY, launch, radio mutation,
or automatic SOP activation path exists.

## 2026-09-09 — Local Nets / Resources LN-5 gate

Status: complete; LN-5 passed and LN-6 release qualification is authorized
next. LN-6 did not begin before this gate passed.

Ops Center now receives an immutable, host-owned Local Net projection on a
single background worker. The projection performs bounded recurrence and batch
catalog reads, identifies active/next/up-to-50-later occurrences, preserves the
configured reminder lead time, and carries stable schedule, occurrence,
directory-session, group, resource, and SOP references. It contains no radio,
QSY, launcher, or scheduler command metadata. A collapsed Local Nets section
does not query or rebuild its row presentation, and overlapping refresh
requests coalesce rather than create parallel workers.

Schedule Outlook presents Local Nets in a visually separate, collapsible,
internally scrollable reminder surface with explicit What/Why, group, service,
frequency/channel, countdown, and resource-update health. Thirty- and
fifteen-minute urgency is expressed in text as well as color. Details opens the
stable schedule in Local Nets; Dismiss affects only the projected occurrence;
and Open SOP selects the linked SOP for manual review with Local Net context.
The return path restores Ops Center context. No action activates an SOP or
enters the HF/SOP conflict, RF Guard, or station-control paths.

Delegation: Terra/high implemented the bounded responsive Ops Center reminder
surface. Luna/high implemented the focused projection, safety, SOP intent,
collapse, theme, Large Text, and compact-viewport tests. The high-reasoning
primary model owned immutable projection architecture, batching and worker
concurrency, stable dismissal/SOP navigation, delegated-diff review, regression
integration, specification reconciliation, and the exit gate.

Acceptance evidence: the dedicated LN-5 suite passes 14 tests and the combined
LN-0 through LN-5 feature suites pass 75 tests without skips. The broader Local
Nets, Ops Center, shell, SOP, scheduler-routing, and shutdown regression gate
passes 273 tests with one pre-existing macOS-environment QtCore import skip.
Python compilation and `git diff --check` pass. The 900x560 Light/Dark and
Normal/Large Text matrix is covered, and Local Net projections remain bounded
at 500 recurrence results and 50 later dashboard items.

## 2026-09-09 — Local Nets / Resources LN-6 focused qualification

Status: implementation and focused automated checks complete; the LN-6 exit
gate and release eligibility remain pending the specified 30-minute soak plus
Linux/operator validation. No later slice was started.

Help now covers the Resources catalog, HF subscription, Local Nets reminder
workflow, Operating Group context, reference limitations, update/retirement
behavior, recovery, and manual SOP handoff. Local Nets and Resources expose
contextual Help controls with accessible names and clearer operator-facing
search and status language. The release checklist now carries the migration,
transfer, workflow, performance, responsive, soak, and Linux validation matrix.

Import/export review found and corrected a relationship-fidelity gap: frequency
and Net Directory Operating Group links are now validated during preview and
preserved on apply. Invalid link payloads remain non-mutating. An isolated copy
of the 311 MB production `freqinout_nets.db` rehearsed 73 legacy rows with zero
review-required rows. The source hash remained unchanged, the backup matched
the original, `PRAGMA integrity_check` returned `ok`, and the migrated clone
contained 73 net resources, 110 frequency resources, 61 directory entries, 71
sessions, and 73 legacy mappings. A second cutover performed zero writes and
remained canonical.

The complete 199-module repository test sweep ran each test file in a fresh
process to avoid accumulated Qt worker state: 197 modules passed and two
skip-only modules reported their expected skip status; no module failed.
Release preflight, Python compilation, and `git diff --check` pass. Initialized
all-tab GUI smoke opened all 22 screens with zero failures or missing-schema
warnings. A 60-second automated UI soak exercised Resources and Local Nets with
572 samples, 54 interactions, 14 resizes, 27 navigation switches, 872.3 ms
first-usable time, 11.5 ms maximum event-loop lag, 34.1 ms shutdown, and no Qt
thread/timer hard errors.

The initial 1,000-schedule Ops projection missed its budget at 423.675 ms warm
p95. Primary review replaced full occurrence sorting with a bounded heap merge
that advances only schedules contributing the earliest results. The same
1,000-schedule gate now measures 33.977 ms warm p95 and returns no more than 50
later rows; an automated regression test enforces the 50 ms ceiling.

Delegation: Terra/medium handled Help, accessibility, and operator wording;
Luna/high handled focused release tests and transfer/recovery coverage. The
high-reasoning primary model owned migration and production-clone rehearsal,
relationship-fidelity correction, concurrency and performance architecture,
fresh-process regression integration, delegated-diff review, and the final
focused qualification review.

Remaining release evidence is intentionally not inferred from offscreen macOS
automation: run the full 30-minute soak and the Linux 1920x1080 Normal Text,
compact, Large Text, Light/Dark, keyboard, and operator workflow matrix before
promoting this feature to a release branch.

## 2026-09-10 — Resources catalog operator-language correction

Status: implementation and focused automated checks complete; the outstanding
LN-6 30-minute soak and Linux/operator matrix remain release gates. No later
Local Nets package was started.

Production UI review found that Resources repeated its three internal browser
tabs in main navigation, displayed frequencies as locale-grouped integer Hz, and
surfaced database source/version identifiers. Net Directory also used `Scope`,
`Session`, and `Active` in ways that could be mistaken for listening limits,
station configuration, or a net currently in progress.

Resources is now one direct full/compact navigation destination. Its internal
Frequency Catalog, Net Directory, and Import / Export tabs and contextual deep
links remain intact. Normal catalog surfaces show decimal MHz and friendly,
batched catalog-source names; opaque identifiers remain hidden widget/model data.
Net Directory uses `Source region`, `Published net meeting`, and
`Listed`/`Retired`. The UI explains that a listed directory item is selectable
reference data, not evidence that it is scheduled or on air. Responsive action
layouts avoid horizontal overflow at the compact Dark/Large Text viewport.

The canonical schema, stable keys, import/export payloads, and compatibility APIs
are unchanged. Two bounded, query-only source read methods were added so friendly
labels do not create N+1 database work or write during browsing. No migration or
production-data mutation was required.

Delegation: Terra/high implemented the responsive UI; Luna/high added the focused
usability and geometry regression suite; Terra/medium performed the independent
semantic audit. The high-reasoning primary model owned the bounded store API,
review corrections, documentation, and integration gate.

Acceptance evidence: 253 focused catalog, transfer, HF subscription, Local Nets,
shell/navigation, and startup tests pass in fresh Qt processes. The dedicated
five-test usability suite covers the normal browser/editor surface and 900x560
Dark/Large Text geometry. Release preflight, Python compilation,
`git diff --check`, and an isolated basic GUI smoke across all 22 screens pass;
the smoke reports zero failed tabs.

## 2026-09-10 — Resources hierarchy, export preview, and Shortwave design review

Status: specification complete; implementation has not begun. The existing LN-6
release gates remain unchanged.

The operator clarified that Resources is a master navigation group, Frequencies
is its existing catalog destination, and Shortwave is a new peer destination.
Frequency Catalog, Net Directory, and Import / Export remain internal browser
tabs under Frequencies rather than being duplicated in main navigation. The
specification now requires a human-readable, non-mutating preview before every
frequency/resource export and a single aggregate transfer bound shared by export
and import.

The review audited the local EiBi A26 CSV/README, the generated Shortwave idea
document, Resources navigation/transfer code, catalog persistence, HF scheduling,
manual QSY controls, and observer SDR ownership. EiBi has 9,442 rows and 1,999
unique frequencies, but includes cross-midnight windows, `2400`, complex day/date
rules, inactive/utility records, provider dictionaries, and malformed/ambiguous
values. Shortwave therefore receives a dedicated immutable, versioned schedule
model and provider adapter rather than one Frequency Catalog row per transmission.
The proposed initial workflow is an indexed, bounded Explore view plus explicit
source import/update preview; a later gated package adds receive-only Listening
reminders. Automatic tuning, HF scheduler insertion, PTT, software launch, and
generic SDR control remain out of scope.

Delegation: Terra/high audited Resources navigation and export-preview UX;
Terra/high audited transceiver, SDR, schedule, Ops, and SOP integration; Luna/high
audited the EiBi corpus, parser grammar, provenance, schema fit, and performance
risks. The high-reasoning primary model reconciled the product hierarchy, source
semantics, migration/concurrency/safety boundaries, delivery gates, and final
specification.

Artifacts: `shortwave_resources_spec.md` and
`shortwave_resources_implementation_plan.md`. This review changed documentation
only; it made no runtime, configuration, schema, or production-data changes.

## 2026-09-10 — SDR receiver API and hardware compatibility design review

Status: specification complete; implementation and hardware acceptance have not
begun. SDR receiver control is now the prerequisite implementation priority
before Shortwave packages.

The audit confirmed that current observer SDR profiles and `SDR Follow` are
receive-only identity/advisory features. FIO's existing RigCtlD protocol client
is a useful code seam, but observer policy prevents production SDR tuning today.
The new specification separates hardware support by the listening application,
availability of a FIO application adapter, and acceptance of the exact
hardware/application/API/OS combination.

The hardware-first matrix covers SDR++ RigCTL, SDRangel REST, SDRconnect
WebSocket, Gqrx remote control, and KiwiSDR tuned-URL handoff. It also records why
direct SoapySDR, UHD, RTL-TCP, and vendor-library ownership is deferred: those
interfaces normally own discovery, I/Q streaming, and often exclusive device
access rather than remotely controlling the operator's running receiver.
Application compatibility is never presented as FIO-verified tuning.

The UI contract preserves a first-class manual path for every configured SDR.
It uses `FIO tuning ready`, `Connected; verify tuning`, `Manual tuning`, and
`Receiver unavailable` rather than a misleading supported/unsupported label.
Frequency/mode guidance, copy actions, and an optional explicit receiver launch
remain available when no API adapter exists or a tune fails. Only successful API
readback permits a tuning-success claim.

Delegation: the existing focused integration agent performed a read-only audit
of upstream hardware lists, package caveats, and operator wording using official
project/vendor documentation. The high-reasoning primary model owned the FIO
current-state audit, ownership/concurrency boundary, hardware-first compatibility
model, adapter ordering, manual fallback contract, and final integration review.

Artifacts: `sdr_receiver_control_spec.md`,
`sdr_receiver_control_implementation_plan.md`, and reconciled Shortwave spec/plan.
This review changed documentation only; it made no runtime, schema, configuration,
or production-data changes.

Follow-up concurrency review: the existing scheduler correctly projects active
rows and retains pending intent by radio, but it executes all device commands
through one station-wide control worker and stores failure/backoff plus several
busy/readback values globally. A slow or hung endpoint can therefore delay other
radios despite their correctly scoped schedule rows. The governing contracts now
require a central no-I/O station coordinator with a serialized, failure-isolated
command lane per distinct physical endpoint. Three radios plus two SDRs is the
required hardware acceptance station; eight active fake endpoints provide stress
headroom. This clarification changed documentation only.

## 2026-09-10 — Multi-endpoint scheduler concurrency specification extraction

Status: standalone specification complete; implementation and production
qualification have not begun.

The compact multi-endpoint requirement was extracted from the product and SDR
documents into `multi_endpoint_scheduler_concurrency_spec.md`. A code audit
confirmed that current schedule rows and retained intents are partly radio-scoped,
but control execution, pending identity, timeout/failure/backoff, post-apply
verification, and several actual/busy caches still share station-global workers
or state. The standalone specification therefore treats endpoint isolation as a
release-safety requirement for FIO's critical automated scheduler.

The design uses one no-I/O station coordinator for time/precedence and shared RF
safety, plus one long-lived serialized lane per distinct automated physical
endpoint. It defines alias prevention, immutable intent generations, latest-state
coalescing, endpoint-scoped readback and health, bounded circuit breakers,
non-leaking hung-call handling, startup/reconfiguration/shutdown order, truthful
operator states, and correlated diagnostics. Manual SDRs remain usable without a
worker; receive-only lanes have no transmit surface.

Acceptance requires single-radio compatibility, a physical three-transceiver plus
two-SDR station with a deliberately hung peer, an eight-endpoint 30-minute
synthetic soak, shared-resource safety, bounded resource counts, and Linux/macOS
lifecycle evidence. The work is divided into MES-0 through MES-5 so implementation
cannot proceed past a failed characterization, identity, isolation, status/safety,
SDR, or production gate.

Model: high-reasoning primary model for repository/code audit, concurrency and
lifecycle architecture, performance/reliability gates, extraction, and final
integration review. No implementation was delegated because this task changed
specification artifacts only.

Artifacts: `multi_endpoint_scheduler_concurrency_spec.md`, plus authority links in
`multirig_product_ui_contract.md`, `sdr_receiver_control_spec.md`, and
`sdr_receiver_control_implementation_plan.md`. No runtime, schema, configuration,
or production-data change was made.

## 2026-09-10 — Multi-endpoint scheduler MES-0 characterization

Status: MES-0 exit gate passed; MES-1 not started.

MES-0 added only test/support/tooling artifacts. The production scheduler and
database schema were not changed. The production-code tests freeze the existing
single-radio behavior, deterministic cross-midnight schedule timing, three-radio
projection, per-radio latest-intent coalescing, station-global intent drain,
global failure/backoff and status-cache gaps, shared PTT/RF Guard rejection,
manual-QSY precedence, and shutdown generation invalidation.

The bounded fault harness separately records a successful one-radio
connect/apply/readback transaction and a controlled three-radio scenario. With
Radio A held in apply, both healthy peers are rejected by the shared control
future and never begin endpoint work. Controlled cancellation releases the
worker, and harness thread, file-descriptor, and child-process counts return to
their initial values. The result makes the MES-2 defect concrete without adding
an expected-failing test or changing production behavior.

Delegation and review:

- Primary high-reasoning model: architecture/code audit, concurrency/migration
  safety, worktree protection, review of every delegated file, integrated tests,
  baseline record, and final gate decision.
- `gpt-5.6-terra` high: deterministic fault harness, bounded resource/correlation
  capture, baseline CLI, and six focused harness tests.
- `gpt-5.6-luna` high: ten production-code characterization tests for schedule,
  control/status scope, safety, precedence, and shutdown behavior.

Acceptance evidence:

- pre-change focused baseline: 116 passed, 1 skipped;
- integrated scheduler/SOP/safety gate: 132 passed, 1 skipped in 2.17 seconds;
- `tools/scheduler_multi_endpoint_baseline.py`: exit 0, one-radio success,
  three-radio defect reproduced, cleanup stable;
- `py_compile`: all four new Python artifacts passed; and
- `git diff --check`: passed.

The skip is the existing macOS guard in `test_scheduler_shutdown.py`; new MES-0
shutdown-generation coverage passed on this host. Development-host details,
commands, limitations, and the complete exit checklist are recorded in
`multi_endpoint_scheduler_mes0_baseline_2026-09-10.md`. Linux production
performance and the physical three-transceiver/two-SDR station remain later
release gates.

## 2026-09-10 — Multi-endpoint scheduler MES-1 identity and pure coordinator

Status: MES-1 exit gate passed; MES-2 not started at the time of this entry.

Added a Qt-free coordination boundary with normalized same-family route keys,
resolved-profile projection, immutable schedule snapshots, immutable
generation-tagged intent/result types, deterministic latest-state coalescing, and
fail-closed duplicate-writer validation. The coordinator consumes the existing
schedule projection after NET/SOP/HF precedence has been selected; it performs no
database, file, socket, adapter, timer, thread-pool, or Qt work. Manual and current
observer profiles remain non-automated. Cross-protocol physical-radio identity is
not guessed and requires an explicit future association before routes can merge.

Delegation and review:

- High-reasoning primary model: concurrency architecture, migration safety,
  coordinator implementation, delegated diff review, integrated verification,
  documentation, and gate decision.
- `gpt-5.6-terra` high: read-only resolved-configuration and precedence audit.
- `gpt-5.6-luna` high: 18 focused identity, snapshot, determinism, conflict,
  generation, result, and no-I/O tests. Primary review corrected test annotations
  for the supported Python 3.9 floor.

Acceptance evidence: 18 focused tests passed; the integrated scheduler/SOP/safety
gate passed 150 tests with one existing platform skip; the MES-0 baseline tool
still reproduced the legacy global-worker blocking defect with stable cleanup;
`py_compile` and `git diff --check` passed. Full details are in
`multi_endpoint_scheduler_mes1_evidence_2026-09-10.md`. No schema, configuration,
or production-data change was made.

## 2026-09-10 — Multi-endpoint scheduler MES-2 isolated command lanes

Status: MES-2 exit gate passed; MES-3 not started at the time of this entry.

Added one serialized, long-lived command lane per normalized endpoint route and
connected existing transceiver control through the unchanged scheduler facade.
Apply plus immediate verification readback now remain in the target lane. Pending
generation, timeout, failure/backoff, circuit recovery, last result, and shutdown
suppression are endpoint-local, so a hung or failed radio does not hold healthy
peers. Compatible aliases use one deterministic writer; competing intents for
the same route fail closed and emit profile-specific health/event evidence.

Delegation and review:

- High-reasoning primary model: concurrency architecture, implementation,
  migration safety, delegated diff review, coordinator integration tests,
  combined acceptance, documentation, and gate decision.
- `gpt-5.6-terra` high: legacy compatibility audit and five production-engine
  endpoint-isolation/lifecycle integration tests.
- `gpt-5.6-luna` high: deterministic test audit and thirteen focused
  lane/fault/resource tests.

Acceptance evidence: the 41-test focused integration set passed; the full
scheduler/SOP/safety gate passed 171 tests with one existing platform skip; the
standalone MES-0 baseline remained stable; `py_compile` and `git diff --check`
passed. Details are recorded in
`multi_endpoint_scheduler_mes2_evidence_2026-09-10.md`. No schema,
configuration, or production-data change was made.

## 2026-09-10 — Multi-endpoint scheduler MES-3 status and shared safety

Status: MES-3 exit gate passed; MES-4 not started at the time of this entry.

Added immutable endpoint-scoped status snapshots with one bounded serialized
status worker per normalized route. Status reads are cache-only for consumers;
refresh timeout, failure/backoff, and generation fencing are isolated so a hung
endpoint cannot delay a healthy peer. Explicit target control no longer borrows
primary-radio status. Unknown or stale target/shared PTT evidence fails closed,
while shared PTT/RF/antenna/frontend/amplifier arbitration remains central and
uses target-qualified cached evidence. Scheduler health, events, and PTT
evidence now retain the target radio identity.

Delegation and review:

- High-reasoning primary model: concurrency/safety architecture, scheduler
  integration, compatibility, delegated diff review, combined acceptance,
  documentation, and gate decision.
- `gpt-5.6-terra` high: status/safety audit, endpoint-status registry, target
  runtime-manager safety, and five focused manager tests.
- `gpt-5.6-luna` high: six deterministic status-registry tests and eight
  production-engine integration tests.

Acceptance evidence: 19 focused MES-3 tests passed; the integrated scheduler,
SOP, station-safety, and multi-rig gate passed 213 tests with three existing
platform/optional-environment skips; the MES-0 characterization CLI, Python 3.9
compilation, and `git diff --check` passed. Details are recorded in
`multi_endpoint_scheduler_mes3_evidence_2026-09-10.md`. No schema,
configuration, or production-data change was made.

## 2026-09-10 — Multi-endpoint scheduler MES-4 receive-only lanes

Status: MES-4 automated exit gate passed; MES-5 not started at the time of this
entry. No physical SDR application/hardware combination is claimed as verified.

Added the Qt-free receiver-control contract with no PTT/transmit surface and
integrated verified automated observers into target-qualified isolated endpoint
lanes. Manual, disabled, incomplete, or unverified receivers retain a zero-I/O
Manual tuning path. Receiver rows branch before transceiver scheduling, share
the existing coordinator and worker lifecycle, use central configured RF
resource arbitration, and publish target-scoped cached tune/readback status.
The old observer foreground TCP probe and false reachability/control implication
were removed.

Six startup-owned, additive `device_profiles` fields persist the application,
adapter, target, enablement, verification state, and evidence. Existing rows
receive safe Manual/disabled defaults, and enabling control fails closed unless
the verified receiver identity is complete. The populated-clone migration test
preserved its existing profile and endpoint values.

Delegation and review:

- High-reasoning primary model: architecture, migration, scheduler/runtime
  integration, safety/truthfulness review, delegated diff review, combined
  acceptance, documentation, and gate decision.
- `gpt-5.6-terra` high: read-only seam audit; separate bounded implementation of
  the receiver contract and four focused tests.
- `gpt-5.6-luna` high: seven deterministic five-endpoint/isolation/lifecycle
  tests.

Acceptance evidence: 19 focused MES-4 tests passed; the integrated scheduler,
runtime, multi-rig, SOP, and station-safety gate passed 241 tests with five
existing platform/optional-environment skips; Python compilation and
`git diff --check` passed. Details are recorded in
`multi_endpoint_scheduler_mes4_evidence_2026-09-10.md`. No destructive migration
or production-data rewrite occurred.

## 2026-09-10 — Multi-endpoint scheduler MES-5 lifecycle and qualification

Status: implementation and automated focused/integrated gates complete. The
required real-time soak is in progress; Linux and physical five-endpoint evidence
remain explicit release gates.

Dynamic endpoint reconfiguration now fingerprints only control, identity, and
shared-RF safety fields. Editing, disabling, or removing a profile retires only
its command/status lanes, increments an endpoint configuration epoch, clears its
assumed/pending state, and fences late callbacks; unaffected peers keep running.
Resume, sleep/wake, monotonic reset, and forward/back wall-clock handling discard
stale actual-state assumptions and offer only current schedule authority. Startup
status probes are deterministically staggered, endpoint retry is local, and
shutdown rejects new work and records bounded outstanding-lane evidence without
waiting indefinitely for a hung external adapter.

Station Overview and the station command bar now consume cache-only runtime
snapshots. UI selection, repaint, resize, and theme work therefore do not own
process walks or endpoint I/O. Target-scoped status wording distinguishes
verified schedule state, application, manual tuning, shared-resource waits,
receiver unavailability, and isolated control stalls. Cache-only bounded
scheduler diagnostics are attached to UI hang dumps without credentials.

The new eight-endpoint qualification harness exercises four transceiver and four
receive-only routes with simultaneous transitions, a slow receiver, repeated
disconnect/reconnect, and a recurring-failure endpoint. It reports bounded
healthy-lane latency, queues, thread/file-descriptor/child-process/RSS stability,
timeouts, and diagnostic drops. An isolated production-database clone migration
and rollback rehearsal verified all six receiver fields; the untouched source and
restored clone shared SHA-256
`a9b927910e231f4d677b18f652a773e2ed0d2f25b82be28987e0f4f142361345`.

Delegation and review:

- High-reasoning primary model: lifecycle/concurrency architecture,
  reconfiguration fencing, clock/resume behavior, cache-only UI boundary,
  diagnostics, migration rehearsal, delegated-diff review, integration, and gate
  decision.
- `gpt-5.6-terra` high: read-only lifecycle/UI/shutdown audit; separate bounded
  eight-endpoint soak harness and resource instrumentation.
- `gpt-5.6-luna` high: deterministic lifecycle, clock, reconfiguration,
  unavailability, shutdown, and repeated-start/stop tests.

Acceptance evidence to date: the integrated scheduler, runtime, multi-rig, SOP,
station-safety, and watchdog gate passes 264 tests with five existing
platform/optional-environment skips. A 1,100-cycle accelerated regression passes
8,800 commands, including 1,210 expected faults, with no unexpected failure or
timeout and stable resources. It was added after the first real-time attempt
correctly exposed overflow in the old exponential-backoff intermediate after
roughly 1,024 repeated failures; command/status backoff now shares bounded finite
math and the qualifying run restarted from zero. Python compilation and
`git diff --check` pass. Final full-suite and real-time soak results are recorded
in `multi_endpoint_scheduler_mes5_evidence_2026-09-10.md` when those gates finish.
No production database was modified.

Final primary safety review closed three delegated-audit findings before the
qualifying soak. Fresh readback is now compared with the active endpoint intent
before the UI may say `On schedule · verified`; stale, missing, or mismatched
evidence cannot receive that label. Resume and clock discontinuities now detach
and generation-fence every prior command/status lane before current intent is
reoffered. The project-owned one-worker endpoint executor uses a daemon worker,
bounded production joins, queued-work cancellation, and survivor diagnostics;
explicit cooperative `wait=True` lifecycle calls still fully join. A permanently
hung fake adapter exits cleanly in a subprocess test. Full-run soak percentiles
now use deterministic bounded reservoir sampling, retain exact maximum latency,
and enforce a 250 ms healthy-lane p95 gate. After these amendments, the focused
scheduler/receiver/runtime/watchdog set passes 210 tests with three optional or
platform skips; the focused shutdown/lifecycle/soak subset passes 46 tests.

The final 30-minute real-time eight-endpoint soak passed 13,936/13,936 commands
over 1,742 cycles with 1,917 expected injected failures, zero unexpected
failures/timeouts/queue-instability observations, healthy p50/p95/max latency of
0.507/1.032/6.653 ms, and stable endpoint threads, descriptors, child processes,
and RSS. The final repository assertion gate passes 2,950 tests with 37
environment/platform skips in clean A–H, I–K, L, M, N–R, S, and T–Z processes.
The monolithic process still reproduces the known cumulative native Qt teardown
segmentation fault at the unrelated compact log-viewer construction test; that
test passes alone and in the clean L partition. MES-5 automated macOS evidence is
complete. Linux lifecycle and the physical three-transceiver/two-SDR matrix
remain external release gates, so production verification is not claimed.

## 2026-09-10 — Message ingest/projection performance design

Status: production evidence reviewed and dedicated specification complete;
implementation has not begun.

The supplied FIO/performance logs were prefix snapshots of one Linux launch, not
two independent runs. FIO reached its first usable shell in 43.426 seconds.
Opening Messages then started a 152.752-second native projection of 11,957 rows
and overlapped a 9.229-second foreground file-scan completion handler. A later
change caused another 267.269-second projection of 10,583 rows. The evidence
also captured SQLite lock errors, unchanged 551-file scans above five seconds, a
seven-second Settings save, Station Control Bar callbacks averaging 954.7 ms,
and hang stacks identifying synchronous SOP reconstruction and read-side radio
profile normalization/commit. BLE was waiting in a worker and was not the CPU
hotspot.

`message_ingest_projection_performance_spec.md` requires durable per-identity
dirty work, bounded preparation, one serialized projection writer, atomic
differential message/reference/artifact bundles, a 250 ms traffic coalescing
window, batches capped at 100 bundles or 50 ms writer time, visible Inbox
invalidations capped at twice per second, watermark-only idle checks, post-shell
resumable catch-up, read-only list/get APIs, off-UI file/BBS/SOP work, a separate
Expect fast path, bounded performance logs, and production-shaped qualification.
Normal new-message visibility targets one-half second; unchanged sources perform
no projection writes. MIP-0 through MIP-5 have explicit exit gates and no
destructive migration is authorized.

Before this specification work, the completed MES-0 through MES-5 scheduler was
committed as `71ac840` and pushed to the internal-testing WIP branch. Existing
Shortwave edits, rendered documents, and Office temporary files were excluded.

## 2026-09-10 — Message ingest projection MIP-0 characterization

Status: exit gate passed; production behavior intentionally unchanged.

The production-shaped characterization fixture covers 5,000 CommStat, 5,000
SitRep, 1,000 Spotter, representative JS8 and VarAC, and 551 file records. It
reproduces whole-window replay after one inserted source row, per-bundle schema
assurance, foreground Qt file-scan completion, deterministic SQLite contention,
rollback/restart behavior, invalid-surrogate path failure, and database writes
from a device-profile list operation.

The architecture audit counted 28 schema/introspection SQL operations per
schema-assurance invocation. Applied to the observed 11,957-row production
projection, the lower bound is 1,144,556 schema/introspection statements before
ordinary projection DML and Ops indexing. This quantifies the principal CPU,
GIL, and writer-lock amplification that MIP-1 and MIP-2 must remove.

Model ownership:

- High-reasoning primary model: architecture and concurrency boundaries,
  production-evidence correlation, migration safety, delegated-diff review, and
  exit-gate decision.
- `gpt-5.6-terra` high: core projection/SQLite and UI/thread audits.
- `gpt-5.6-luna` high: characterization fixtures and focused test execution.

Acceptance evidence: `tests/test_message_ingest_mip0_characterization.py`
passes 8 tests in 1.24 seconds; `git diff --check` passes. Details are recorded
in `message_ingest_projection_mip0_evidence_2026-09-10.md`. No destructive
migration or production-data rewrite occurred.

## 2026-09-10 — Message ingest projection MIP-1 writer foundation

Status: exit gate passed.

Projection schema version 3 now creates the durable dirty-work, source-state,
and generation tables through startup's additive migration owner. Runtime row
helpers and the hot Ops index path no longer execute schema assurance. The new
projection bundle writer provides one serialized lane per database, immutable
atomic message/reference/artifact bundles, differential component writes,
source deduplication, affected-only Ops indexing, 100-bundle/50 ms transaction
limits, bounded lock retry, cancellation/backpressure outcomes, generation
advancement, and bounded daemon-worker shutdown. Reprojection also preserves an
operator-read row when stale source material still reports new or unread.

Model ownership:

- High-reasoning primary model: architecture, additive migration, concurrency,
  state precedence, registry integration, delegated-diff review, and gate.
- `gpt-5.6-terra` high: serialized/differential writer module.
- `gpt-5.6-luna` high: migration, schema-free-helper, atomicity, batching,
  zero-write, source-deduplication, and writer-registry tests.

Acceptance evidence: the focused MIP/projection/Ops set passes 56 tests,
including deterministic lock deferral, cancellation, atomic rollback, and the
100-bundle transaction cap. Python compilation and `git diff --check` pass.
Details are recorded in
`message_ingest_projection_mip1_evidence_2026-09-10.md`. No destructive
migration or production-data rewrite occurred.

## 2026-09-10 — Message ingest projection MIP-2 incremental adapters

Status: exit gate passed.

The five native database families now use durable, coalesced, source-scoped
dirty identities and bounded watermarks. Exact targeted adapters reuse the
existing semantic builders, while one serialized writer owns differential
projection, reference, artifact, Ops-index, dirty-completion, and deletion
transactions. VarAC uses a rowid discovery watermark so endpoint-local IDs may
repeat; CommStat deletion markers participate in the queue; and JS8/Spotter
tables created after initial migration install triggers through their schema
lifecycle seam. The active Inbox native worker no longer invokes the 5,000-row
legacy projector.

An independent audit initially held the gate for endpoint identity collisions,
unscoped deletion, late-table trigger installation, missed CommStat tombstones,
content-hash-only diffs, stale Ops indexing, and delete-lane contention. Those
findings were corrected and added to the acceptance matrix.

Model ownership:

- High-reasoning primary model: identity/watermark design, deletion scope,
  concurrency and migration safety, production cutover, delegated-diff review,
  and exit gate.
- `gpt-5.6-terra` high: targeted adapters, read-only concurrency audit,
  full-semantic differential comparison, persisted-row Ops indexing, and
  focused tests.
- `gpt-5.6-luna` high: durable queue, burst/restart/version, endpoint-collision,
  trigger-lifecycle, deletion-marker, and state-consistency tests.

Acceptance evidence: the focused MIP-2 set passes 65 tests, including a
500-message burst in five bounded cycles and forced-restart recovery without
loss or duplicate projection. Compilation, Ruff, and `git diff --check` pass.
Details are recorded in
`message_ingest_projection_mip2_evidence_2026-09-10.md`. No production database
or authoritative source data was modified.

## 2026-09-10 — Message ingest projection MIP-3 file delta pipeline

Status: exit gate passed.

Message-file discovery now emits exact immutable deltas, and a dedicated
off-UI pipeline prepares only changed files, uses the shared serialized writer
for atomic message/reference/artifact projection, persists scanner inventory,
and tombstones removed versions without touching source files. The initial
additive-migration run performs bounded catch-up even when the older GUI cache
already knows the files; subsequent unchanged scans are read-only. Opaque,
reversible path keys and escaped display spellings keep malformed POSIX names
safe at SQLite and UI boundaries.

The active Qt scanner worker now owns discovery and database pipeline work. Its
completion callback only swaps snapshot state, records directory generations,
marks the projection read model stale, and updates status. Legacy cache writes,
BBS sweeps, observation projection, VarAC refresh, signature verification, and
table reconstruction are no longer run synchronously from that callback.

Model ownership:

- High-reasoning primary model: pipeline/concurrency architecture, migration
  catch-up, GUI integration, delegated-diff review, and gate.
- `gpt-5.6-terra` high: delta/path core, bounded file pipeline, derived
  inventory, exact projector-builder reuse, and implementation tests.
- `gpt-5.6-luna` high: 551-file, delta, malformed-name, atomicity, and Qt
  completion-boundary acceptance tests.

Acceptance evidence: the integrated MIP-0 through MIP-3 projection set passes
320 tests in 17.71 seconds. Compilation and `git diff --check` pass. Details are
recorded in `message_ingest_projection_mip3_evidence_2026-09-10.md`. No
production database or authoritative source data was modified.

## 2026-09-10 — Message ingest projection MIP-4 bounded UI read model

Status: exit gate passed.

Inbox rendering now uses an asynchronous, read-only, 200-row projection query.
Rows, total count, and generation come from one SQLite snapshot; late request
and generation results are discarded. Source, group, status, identity, type,
text, and recent/older age filters run before the row limit. Visible
invalidations coalesce within 500 ms, while hidden/inactive tabs defer query and
render work until activation. Normal and forced refresh no longer invoke the
legacy retained-history row builder.

Runtime profile reads used by the Station Control Bar no longer normalize or
repair data, and the bar consumes a cache populated outside repaint. Settings
Save refreshes SOP only when that surface is active. Traffic by Group moved from
a 20,000-message materialization to an exact read-only aggregate, preserving
high-volume counts despite the Inbox page cap.

Model ownership:

- High-reasoning primary model: architecture, full filter semantics, Qt worker
  integration, command-bar cache, lazy SOP, review, and gate.
- `gpt-5.6-terra` high: read model, generation snapshot, group aggregate,
  ControlFreq cutover, and focused core tests.
- `gpt-5.6-luna` high: bounded-model and asynchronous UI acceptance tests.

Acceptance evidence: the core partition passes 150 tests; the clean Qt/UI
partition passes 361 tests. Compilation and `git diff --check` pass. The known
cumulative native Qt teardown abort reproduced only in a combined process; all
affected tests pass in clean partitions. Details are in
`message_ingest_projection_mip4_evidence_2026-09-10.md`. No production database
or authoritative source was modified.

## 2026-09-10 — Message ingest projection MIP-5 startup and qualification

Status: implementation exit gate passed; Linux production confirmation remains
an external release-qualification observation.

Background ingest and projection catch-up now begin after the first usable shell.
One application-owned maintenance lane coalesces source notifications and
jittered reconciliation, and Messages no longer owns a competing native
projection worker. Catch-up uses bounded 100-bundle cycles and retains durable
work across cancellation, shutdown, and restart.

Message Maintenance now provides an explicit Message Index workflow. Its source
estimate, derived-state reset, catch-up, progress, and checkpoint reads all run
off the Qt thread. Preview and confirmation make clear that native messages and
received files remain untouched. Normal startup, tab activation, filters, and
Refresh never request a deep rebuild.

Performance logging now uses a non-blocking bounded queue, batched long-lived
file writes, and bounded rotation. Watchdog hang capture reads only a precomputed,
credential-redacted scheduler/projection snapshot. Projection transaction
duration is recorded for budget verification, and the metrics lane is flushed
before Linux's optional hard-exit fallback.

Model ownership:

- High-reasoning primary model: startup/concurrency architecture, application
  ownership, rebuild UI and safety, shutdown integration, delegated-diff review,
  documentation, and exit gate.
- `gpt-5.6-terra` high: bounded maintenance/rebuild core, source-state helpers,
  cancellation/resume, and focused tests.
- `gpt-5.6-luna` high: buffered telemetry, cache-only watchdog diagnostics,
  redaction, and focused tests.
- `gpt-5.6-luna` focused test package: scheduler/Expect isolation, burst,
  restart, idle-zero-write coverage, and the soak tool.

Acceptance evidence: 225 core/message/Expect tests and 371 clean Qt/UI tests
pass. A current-code real 12,000-row catch-up produced exactly 12,000 unique
rows in 6.194 seconds with a 10.434 ms p95 preparation batch, 0.390 ms p95
unchanged reconciliation, and 43.694 ms maximum write transaction. Queue depth
returned to zero, all concurrent scheduler commands completed, and no endpoint
threads leaked.
A disposable macOS profile reached first usable shell in 921.061 ms and shut
down in 15.293 ms. The 30-minute soak completed 1,787/1,787 scheduler commands
with zero failures, zero RSS growth, zero final projection backlog, no endpoint
thread leak, and a 12.845 ms maximum projection transaction. The external Linux
qualification boundary is recorded in
`message_ingest_projection_mip5_evidence_2026-09-10.md`. No production database
or authoritative source was modified.

## 2026-09-10 — Modern JS8Call variant compatibility

Status: all JSV-S1 through JSV-S4 implementation gates passed; Linux
package/multi-instance hardware confirmation remains external release
qualification.

FIO's JS8Call integration was reviewed against the supplied source for
JS8Call-Improved 3.0.3 and Subspace Edition 4.1.0.478. Linux discovery and
launch resolution now cover canonical Improved, package-installed Subspace,
case variants, direct executables, and the Improved user-install path. Settings
uses application/executable language and generated profiles enable both the TCP
listener and command acceptance. Bounded configuration discovery also includes
Improved and Subspace rig-named settings-file conventions on Linux, macOS, and
Windows.

Native millisecond timestamps are preserved, Ultra and Subspace speed values
have stable names, the Subspace selected-call command is preferred without
removing legacy aliases, and asynchronous Subspace send refusals are visible in
client health. Native polling queues are bounded; the shared hub discards unused
Improved `TX.FRAME` tone arrays and exposes bounded drop counters. Launch
planning rejects multiple local Subspace instances because upstream currently
shares its message store, while one radio-scoped instance remains valid.

The completed storage package adds an additive, idempotent JS8 instance schema;
stable persisted rig names; deterministic platform Qt data-root resolution;
and separate application, settings, lock, message-storage, and SaveDir
identities. Launch previews show the exact effective command and storage
consequence. Duplicate local rig names, duplicate isolated roots, and multiple
local Subspace plans fail before process launch.

Runtime ingestion now owns one cursor per canonical root. Verified isolated
roots retain radio/instance provenance, while shared and unverified file
evidence is retained once without an invented radio attribution. Locked inbox
reads use a short read-only timeout so another source proceeds. Bounded
background reconciliation checks only the three known JS8 message artifacts,
does no history scan, stays write-free when unchanged, and preserves legacy
explicit paths and previously verified mappings.

Settings now shows `Isolated · <rig>`, `Shared`, or `Needs verification`, keeps
Save folder distinct from Message storage, and routes duplicate-root or
duplicate-rig warnings naming both affected radios to configuration review. The
presenter is cache/persistence-only and performs no filesystem work on the Qt
thread.

Model ownership:

- High-reasoning primary model: architecture, migrations, storage resolver,
  runtime reconciliation, provenance/concurrency, safety refinements,
  delegated-diff review, documentation, and final integration gate.
- `gpt-5.6-terra` high: read-only Subspace source/package/API audit.
- `gpt-5.6-terra` high: read-only JS8Call-Improved 3.0.3 source/API audit.
- `gpt-5.6-luna` high: JSV-S1 namespace/migration fixture tests.
- `gpt-5.6-terra` high: JSV-S2 launch planning and exact-command implementation.
- `gpt-5.6-terra` high: JSV-S3 provenance, lock-isolation, and restart tests.
- `gpt-5.6-luna` high: JSV-S4 Settings storage-state/collision UI and help.
- `gpt-5.6-terra` high: JSV-S4 additive migration and round-trip tests.

No destructive migration or production data mutation is part of this work.
The complete contract and remaining production qualification are recorded in
`js8call_modern_variant_compatibility_spec.md`.

Acceptance evidence: 568 tests passed and 27 were skipped across three clean,
non-overlapping partitions: 196 discovery/storage/launch/status tests, 175
native API/send/ingest/concurrency tests, and 197 Settings/UI/control tests.
Coverage includes managed and explicit rig names, default/rig-named roots,
symlink collisions, additive migration, legacy-file immutability, API-observed
variant identity, exact commands, shared/unverified attribution, identical
multi-source traffic, locked inbox isolation, restart checkpoints, native API
framing/backpressure, UTC, speed display, selected-target compatibility,
Subspace refusal reporting, JS8 send/Expect/FLAMP, software status, and
radio-scoped Settings. Python compilation, HTML parsing, and `git diff --check`
pass. No production database, external JS8 settings, or source message file was
modified. Live Linux default/two-rig instance and single-Subspace qualification
remains explicitly external.

## 2026-09-10 — SDR receiver control SDR-0

Status: SDR-0 implementation exit gate passed; no hardware combination is
claimed as FIO-verified.

FIO now has one immutable, versioned, bounded compatibility registry that keeps
receiver-application hardware support separate from FIO control verification.
RTL-SDR remains usable through SDR++, SDRangel, or Gqrx even before automated
control is qualified. The four operator states are `FIO tuning ready`,
`Connected; verify tuning`, `Manual tuning`, and `Receiver unavailable`.

Observer / SDR setup now identifies the hardware and receiver application
separately, saves an optional receiver/VFO label and application endpoint, and
always retains a manual workflow. Saved endpoint fields are described as
configuration rather than connection evidence. The Settings presenter is
cache/data-only and never probes a receiver from the Qt thread.

Model ownership:

- High-reasoning primary model: product/capability architecture, delegated-diff
  review, editable-combo correctness, acceptance gate, and documentation.
- `gpt-5.6-terra` high: bounded Observer / SDR Settings and manual-guidance UI.
- `gpt-5.6-luna` high: compatibility registry and focused registry/manual tests.

Acceptance evidence: 22 focused registry, receive-only contract, Settings,
persistence, responsiveness, hint, and no-I/O tests pass. Python compilation
and `git diff --check` pass. No database migration, device discovery, socket
probe, direct hardware driver, or production data mutation is part of SDR-0.

## 2026-09-10 — SDR receiver control SDR-1 and SDR-2

Status: SDR-1 exit gate passed; SDR-2 implementation and automated gates passed.
SDR-2 remains open for the required live RTL-SDR/macOS/Linux hardware matrix, so
SDR-3 has not begun.

The receive-only core now has bounded capability probing, target enumeration,
readback, cancellation, reversible tune/restore qualification, additive profile
fields, and a zero-I/O manual fallback. Observer control runs on the scheduler's
existing target-qualified endpoint lanes and cannot enter the transceiver/PTT
path. Settings performs no receiver I/O while opening or editing.

The first application adapter is SDR++ RigCTL. It controls SDR++'s selected VFO,
not RTL-SDR hardware directly, and uses one bounded short-lived TCP connection
per command. Frequency set/readback is required; mode is used only when SDR++
advertises it. `Test control` briefly changes frequency, verifies it, restores
the original, and verifies restoration in a worker lane. The operator must then
explicitly enable and save FIO tuning. Verification evidence is bound to the
exact adapter, host, port, and target and is independently enforced by the
store, runtime, readiness presenter, and scheduler binding.

Model ownership:

- High-reasoning primary model: receive-only architecture and qualification
  core, lane and lifecycle ownership, additive migration review/rehearsal,
  qualification coordinator, exact-evidence safety enforcement, upstream SDR++
  protocol audit, scheduler integration, delegated-diff review, documentation,
  and final automated gate.
- `gpt-5.6-terra` high: responsive Observer / SDR setup and verification UX.
- `gpt-5.6-luna` high: receive-only contract, migration, cancellation, and
  focused SDR-1 tests.
- `gpt-5.6-terra` high: named SDR++ RigCTL adapter and protocol boundary.
- `gpt-5.6-luna` high: fragmented TCP, malformed/oversized response, reconnect,
  qualification serialization/supersession, and lifecycle tests.

Acceptance evidence: the combined receiver/UI/integration partition passes 102
tests. The broader endpoint identity, lane, status, fault, lifecycle, MES-4,
soak, shutdown, and command-routing partition passes 111 tests with one
intentional skip. An accelerated 1,800-cycle, eight-endpoint stress run accepted
and completed 14,400/14,400 commands with zero unexpected failures, zero
completion timeouts, no queue instability, no leaked threads or child processes,
and 0.879 ms healthy p95 latency against the 250 ms budget. Python compilation
and `git diff --check` pass. No production database, SDR application setting, or
hardware was modified by automated acceptance. Live tune/readback,
manual-before/after, restart/reconnect, timeout/shutdown, and 30-minute CPU/thread
evidence remain the external SDR-2 gate.

## 2026-09-10 — Release-blocking UI responsiveness remediation

Status: implementation exit gate passed; Linux production requalification is
required before release.

Review of `freqinout (24).log`, `perf_metrics.log`, and seven supplied UI hang
dumps confirmed five interacting causes: recurrent schedule/assignment and
manual-state SQLite work on the Qt timer; live process inventory from scheduler
availability logic; Station Control Bar plan-table reads during rendering; eager
Settings/SOP/Ops projections before first paint; and native message catch-up that
could discover 500 rows per cycle and prepare/write large CPU-heavy units while
the queue was already backlogged.

The scheduler now uses one dedicated serialized projection worker for schedule,
plan, policy, and manual-state data. Timer, FLDigi presentation, construction,
and Station Control Bar paths are cache-only; endpoint availability/application
remains worker-owned and generation fenced. Settings and SOP data are deferred
until first activation, Ops Center starts with a clock-only frame, and optional
index/status work yields until after paint. A deferred unopened Settings surface
cannot autosave blank controls during shutdown.

Message catch-up now drains the durable queue before discovery, applies a global
100-identity discovery cap across all sources, prepares in 25-identity slices,
writes at most 25 bundles per CPU/transaction unit, and propagates cancellation
through lease release and nonblocking close. Startup compatibility repair now
filters for noncanonical group values in SQLite and rebuilds SitRep rollups only
when rows changed. Shared/local operator list and autocomplete reads no longer
run schema assurance or identity repair from a UI activation path.

Model ownership:

- High-reasoning primary model: production trace/hang attribution, scheduler and
  Qt-thread architecture, read-only DB boundary, FLDigi worker dispatch, Station
  Control Bar snapshot integration, startup repair optimization/benchmark,
  delegated-diff review, specifications, and final integration gate.
- `gpt-5.6-terra` high: bounded queue-first message projection, global discovery
  cap, cancellation/lease handling, and focused tests.
- `gpt-5.6-luna` high: deterministic scheduler responsiveness architecture and
  endpoint-isolation acceptance tests.
- `gpt-5.6-terra` high: Settings/SOP/Ops first-paint deferral and focused UI
  regression tests.

Acceptance evidence: 201 scheduler tests passed with one intentional skip; 134
message ingest/projection tests passed; and 91 startup/SOP/Ops/Settings/UI tests
passed. Focused startup/coordination/projection coverage also passed in isolated
processes. A mixed non-Qt/Qt process reached 88 passing assertions before a
PySide lifecycle abort during widget construction; the same partitions passed
cleanly in isolated processes and no application assertion failed. `git diff
--check` passes.

A disposable copy of the 311 MB production database measured the bounded group
repair at 85.208 ms across 17,658 relevant source rows and the full nets startup
schema pass at 1,891.094 ms. The supplied Linux trace measured database init at
79,747.936 ms. No source evidence, production database, external application
settings, or hardware was modified. Live Linux first paint, every-tab/click p95,
message backlog CPU, command-bar p95, idle CPU, and shutdown are still external
release gates.

A final isolated 30-second real-window navigation/resize soak passed with a
701.6 ms first usable shell, 612.4 ms construction, 1.2 ms maximum event-loop
lag across 114 samples, 25 interactions, 13 navigation switches, six resize
cycles, and 62.8 ms shutdown. All Qt worker threads stopped cleanly. A separate
Settings measurement on a disposable copy of the 311 MB store took 266.907 ms
to construct its deferred widget surface and 89.014 ms to populate it on first
activation after startup initialization.

## 2026-09-10 — Shortwave Resources R-1 and SW-1

R-1 is complete. Resources is a workflow master whose implemented child is
Frequencies; Frequency Catalog, Net Directory, and Import / Export remain
internal browser tabs. Catalog export is now contextual, multi-select, bounded,
preview-first, non-mutating on cancel, and revalidated before its one file
write. The R-1 gate passed 82 focused Resources/HF/Local Nets tests.

SW-1 is complete. The additive, startup-owned Shortwave schema, immutable
dataset lifecycle, atomic current-pointer promotion, cancellation, rollback,
bounded read store, EiBi Latin-1 provider, fixed-host HTTPS downloader, and
offline A26 seed packaging are implemented. The parser preserved all 9,442
audited A26 rows in approximately 76 ms, retained the one exact duplicate for
audit, produced explicit diagnostics for ambiguous/rejected input, and matched
both specification hashes. A real 10,000-row import/query test remained bounded
and below the 100 ms warm-query gate.

Model ownership: the high-reasoning primary model owned architecture, schema,
transactions, migration rehearsal, concurrency boundaries, delegated-diff
review, and integration gates; `gpt-5.6-terra` implemented bounded R-1 UI and
the Qt-free EiBi adapter/packaging; `gpt-5.6-luna` implemented focused fixtures,
malformed-input, downloader, migration, rollback, and performance tests.

The combined R-1/SW-1 acceptance gate passed 104 tests. `py_compile` and
`git diff --check` passed. An isolated 326 MB production-database copy reached
canonical authority with all Shortwave tables present, no current dataset
fabricated, and no production data modified. The additive startup migration
took 2,179.4 ms on the first copied run; the broader pre-existing catalog/startup
path dominated that measurement. Shortwave performs no import or network work
at startup or from tab activation. Clean frozen-build packaging is represented
in both `MANIFEST.in` and `FreqInOut.spec`; a platform bundle build remains a
release-environment gate.

## 2026-09-10 — Shortwave Resources SW-2

Status: implementation and automated exit gate passed; interactive macOS/Linux
theme, accessibility, and idle-CPU soak remain release evidence.

Shortwave is now a lazy Resources child with bounded Explore and Data Sources
workspaces. Explore normalizes conventional frequency input, distinguishes
Scheduled now from Starting soon across UTC midnight and winter seasons,
filters broadcast/utility/time/other content, presents decoded home country,
transmitter site, target, language, source age, provenance, and separate
UTC/local times, and keeps provider codes behind Technical details. Data Sources
supports bundled, fixed-host official, and local-file review, explicit atomic
Apply, retained rollback, and bounded diagnostics export. It performs no
automatic download or import.

The two UI task lanes are single-flight and generation-fenced: rapid changes
retain at most one active operation and one newest pending operation. Hidden
pages do not poll, parsing/querying stays off the GUI thread, official download
has a global five-second deadline and cooperative cancellation, and shutdown is
bounded.

Model ownership: the high-reasoning primary model owned schedule evaluation,
query bounds, source semantics, downloader/shutdown review, identity review,
and final integration; `gpt-5.6-terra` implemented the responsive Explore/Data
Sources UI and help; `gpt-5.6-luna` implemented the real-corpus now/soon,
cross-midnight/winter, filters, p95, and Qt worker-lifecycle tests.

Acceptance evidence: 23 focused SW-2 tests passed, including a real 9,442-row
A26 warm-query p95 below 100 ms, rapid-refresh coalescing, Data Sources pending
task regression, hidden technical details, distinct UTC/local presentation,
and clean worker shutdown. The combined R-1/SW-1/SW-2 suite passed 52 tests
before SW-3 opened. Python compilation and `git diff --check` passed.

## 2026-09-10 — Shortwave Resources SW-3

Status: implementation and automated exit gate passed; the macOS/Linux
30-minute interactive soak remains release evidence.

The Shortwave workspace now provides a receive-only Listening calendar. A user
can add a complete Explore result, retain an immutable accepted listing
snapshot, label the reminder, set lead time and notes, and optionally associate
a configured receiver as informational context. Saved receiver identities that
later disappear remain visible as unavailable rather than being silently
cleared. Duplicate adds open the existing reminder without overwriting local
choices.

Source refresh is explicit and safe. Current, changed, missing, and intentionally
kept states are distinguished; changed fields and the proposed current listing
are shown before `Apply listing update`, while `Keep my reminder` preserves the
accepted snapshot. Import promotion and rollback update review state in one
transaction without mutating accepted snapshots. Provider identity includes the
schedule, station, language, target, site, and date-window fields needed to keep
concurrent listings distinct while allowing a last-heard-only change to be
reviewed.

Ops Center has a separate collapsed Shortwave Listening surface. It does no
database query while collapsed or while Ops is inactive, projects at most 200
reminders and 50 later occurrences with fair per-reminder bounds, and offers
only Details and occurrence-scoped Dismiss. No tune, QSY, launch, PTT, scheduler,
Operating Group, SOP, or automated radio path is reachable from Shortwave.
Cross-midnight and cross-year winter recurrences are covered.

Model ownership:

- High-reasoning primary model: accepted-snapshot and source-version
  architecture, additive schema/migration, provider identity, bounded recurrence
  and fairness, import transaction integration, worker/concurrency and radio
  safety boundaries, delegated-diff review, compact/dark-theme review,
  specifications, and final integration gate.
- `gpt-5.6-terra` high: bounded Listening editor and Ops reminder presentation,
  source/provider mechanics, responsive layout, help, and main-window wiring.
- `gpt-5.6-luna` high: source-diff, recurrence, dismissal, performance,
  scheduler-isolation, collapsed/no-query, receiver-label, worker-lifecycle, and
  responsive-layout tests.

Acceptance evidence: 72 focused R-1/SW-1/SW-2/SW-3 tests and 78 additional
Resources/HF/Local Nets regressions passed (150 total). The real 9,442-row A26
corpus retained 9,441 distinct user listings plus its one exact audit duplicate;
bounded reminder refresh and outlook projection remained below the 100 ms gate.
On a disposable production-database copy, the additive schema completed in
5.30 ms on first application and 1.36 ms idempotently, `PRAGMA integrity_check`
returned `ok`, and no current dataset was fabricated. Python compilation and
`git diff --check` passed. A broader Phase 7 shell run passed 118 assertions;
nine existing Station Control Bar assigned-plan fixture failures remain outside
the Shortwave diff and scope. No production database, external application,
radio, or receiver hardware was modified.

## 2026-09-11 — Scheduler projection CPU spin and production attribution

Status: root cause remediated; automated exit gate passed. Settled-idle Linux
confirmation remains a production observation gate.

The local runtime log provided a deterministic signature rather than a generic
CPU symptom. Between 06:39:20 and 06:39:41, FIO wrote 4,165 lines, completed 303
schedule projections (almost all marked forced), logged 307 radio-8 and 301
radio-9 applications, and repeatedly issued unchanged FLRig, FLDigi, and JS8
commands. Projection completion was consuming its snapshot through a path that
requested another forced projection. The same `force` flag then bypassed settled
entry deduplication, producing a self-sustaining worker/control/UI signal loop.

The projection completion path is now one-way: it publishes and consumes the
new immutable snapshot without requesting a successor and without forcing
unchanged endpoint state. Changed schedule keys still apply normally. The
application log moved behind deduplication so it describes an actual queued
command. Cache-only diagnostics now expose projection request, forced-request,
and completion counters. Active-entry UI signals are initially coalesced for
350 ms and status rendering is capped at one pass per two seconds, while the
Station Control Bar continues to enforce its existing bounded refresh cadence.
This makes configuration or endpoint faults visible without allowing their
event rate to become the presentation rate.

A new default-on process CPU watchdog samples only `process_time` against
`monotonic` once per second. Three consecutive samples at or above 75% of one
logical core produce one bounded, credential-redacted report in
`<FIO config>/cpu_hotspots`, including cached scheduler/message-projection
diagnostics and bounded Python thread stacks. Reports observe a 60-second
cooldown and retain at most ten files. Normal samples do not log or inspect the
database, network, endpoints, Qt, filesystem tree, or process list. Set
`FREQINOUT_CPU_WATCHDOG=0` only when explicitly disabling this diagnostic.

Model ownership: the high-reasoning primary model performed log quantification,
root-cause/concurrency review, implementation, specification reconciliation,
and final integration. No subagent was used because this request did not ask for
delegation and the scheduler feedback boundary required single-owner review.

Acceptance evidence: 227 scheduler, health, watchdog, and message-telemetry
tests passed with one intentional skip. Coverage includes the
forced-projection terminal-consumer regression, status-signal coalescing,
CPU threshold/reset/cooldown behavior, bounded reports, and secret redaction.
No migration or production data change was introduced.

Pre-push integration combined Shortwave, Resources, Local Nets, Ops Center,
scheduler, health, watchdog, and message-telemetry coverage in one Qt process.
That run exposed two deferred ControlFreq presentation callbacks that could
arrive after a short-lived page was destroyed. Both callbacks now treat QObject
destruction as cancellation, with a direct lifecycle regression. The repeated
combined gate then passed 420 tests with one intentional skip.

## 2026-09-11 — Linux hotspot attribution and Shortwave native-crash remediation

Status: implementation complete; automated focused gate passed. Linux production
soak remains an external confirmation gate.

The attached second-run evidence confirmed the previous forced scheduler
projection loop is gone: request and completion counters remained matched and
only one request was forced. It also isolated four remaining contention paths.
The UI hang watchdog captured synchronous VarAC status loading during startup;
CPU reports captured repeated settings schema assurance, a 500-row Ops Focus
backfill, manual-control and RF Guard reads initiated by the Station Control Bar,
and repeated busy-evidence writes. A separate hang captured propagation history
modeling on the GUI thread. The Shortwave navigation completed in under one
second, then the process log ended with no Python exception while Data Sources
was being reviewed.

Remediation now assures a settings-store schema only once per store, skips the
nonessential VarAC filesystem/status scan during initial runtime synchronization,
loads only the latest indexed VarAC status row per source when status is later
requested instead of materializing the append-only history,
uses scheduler-published manual and assignment snapshots in the command bar,
limits Ops Focus maintenance to cooperative 25-row units with a 750 ms yield,
and caches operator-identity schema knowledge for the batch. Busy evidence has a
60-second expiry, refreshes at most every 30 seconds while unchanged, clears a
possibly stale prior-process row once and thereafter writes only on a local
state edge, and duplicate busy warning logs are limited to one per 30 seconds.

Propagation history now has a bounded 1,000-event ceiling per band, a dedicated
pooled lookup index and cutoff predicate, and one day-scoped empirical cache
shared by the morning/day/night display windows. This retains historical signal
evidence while removing repeated scans of the same rows during one presentation.

Shortwave's three page-owned `QThread` lanes were replaced with serialized daemon
workers and GUI-thread signal bridges. This was driven by a local native
segmentation-fault reproduction, not inference from an absent traceback. Page
shutdown is now nonblocking and cannot destroy a running Qt thread. Background
failures and duration/cancellation state are recorded for future production
diagnosis.

Model ownership: the high-reasoning primary model performed log correlation,
concurrency and UI-boundary review, implementation, native-crash reproduction,
test authoring, and integration review. No delegated agent was used because this
turn did not request delegation and the remediation required a single owner.

Acceptance evidence at this checkpoint: 78 Shortwave and production-hotpath
tests, 33 busy-evidence/scheduler lifecycle tests (one intentional skip), and
119 of 122 broader Ops/runtime/multi-radio assertions passed. The three failures
are the already-known Wave 3 assigned-plan tests whose synchronous fixture
predates the scheduler's nonblocking projection contract; no new failure was
introduced by this remediation. Python compilation and `git diff --check`
passed. The RF Guard compatibility assertion passes.

## 2026-09-11 — Message ingest/projection CPU convoy remediation

Status: implementation and focused automated gate complete; Linux production
CPU/latency confirmation remains external.

The third Linux capture showed higher aggregate CPU even though the scheduler
feedback loop remained fixed. Startup reached its first usable shell in 45.74
seconds; database initialization consumed 13.86 seconds and Settings construction
8.65 seconds. CPU watchdog reports then measured 95.6%, 162.9%, and 158.6%.
The two sustained samples independently showed `freqinout-ingest_0` importing
JS8 directed traffic while `fio-message-projection` prepared or wrote derived
message bundles. The catch-up lane issued 317 batches in roughly 111 seconds:
1,363 identities prepared, 1,363 bundles submitted, 452 transactions, and 1,313
message/reference upserts. A 100-item write reached 4.142 seconds, and the lane
then chased individual rows as the source importer committed them.

Ordinary projection is now application-paced: one future performs one cycle,
the cycle ceiling is 25 identities, parsing yields every 10, and sliced or
deferred work resumes after a one-second single-shot interval. Projection waits
while the background `messages` job is queued or running, preventing the source
writer and derived writer from competing for the same SQLite database. The JS8
directed storage path also removes a redundant connection/query and relies on
the already-present semantic duplicate check and atomic source-identity conflict
guard. Inbox refresh remains coalesced at one second. Explicit deep rebuild behavior
is unchanged.

Startup database initialization now emits named spans for each high-level schema
or repair family so the next production capture can identify the 8–20 second
variance without adding speculative repairs or destructive migration work.

Model ownership: the high-reasoning primary model correlated the logs and stack
dumps, designed the concurrency boundary, implemented the remediation and tests,
and performed integration review. No subagent was used because this turn did not
request delegation and the ingest/projection ownership boundary needed one
reviewer.

Acceptance evidence: the complete message ingest/projection selection passed
172 tests with one intentional skip; independent startup partitions passed 15;
and the scheduler/UI/performance integration selection passed 59 with one
intentional skip (246 passed, two skipped in total). Python compilation and
`git diff --check` passed. Running all Qt startup partitions in one process can
still trigger the repository's known cross-fixture native abort, so those
partitions were deliberately executed in isolated processes as the product does
for a fresh launch. No schema or authoritative data migration is part of this
change.

## 2026-09-11 — Production Inbox and BBS correction review (PIC-0)

Status: review/specification exit gate passed; no production implementation has
started. P1 Inbox correction is the next authorized slice. P3 BBS presentation
remains blocked until the P1 exit gate passes.

The Linux production screenshot that appeared to show only JS8 traffic was
traced to an auxiliary `Pending JS8 MSGs` queue inserted above the ordinary
multi-source Inbox. Its table height grows to the complete loaded backlog, so 26
rows consume the short production viewport and displace the real Inbox. The
backlog load is also unbounded before client-side status filtering. A read-only
check of the most recently active local lab database found mixed CommStat and
SitRep rows in the default projection, supporting a presentation-masking cause,
but the exact Linux production configuration root was not available. The P1
gate therefore requires active-root and per-source verification before the
finding is considered fully closed.

The BBS screenshots expose a responsive-geometry regression against the existing
BBS contract. The page chooses its side-by-side mode largely from width even
though the usable tab height is short. A fixed four-column radio table, long raw
paths, concatenated location policy labels, duplicated summary prose, and an
always-open location editor then produce clipping and large unused regions.
The correction retains the station-owned BBS model and makes radio/location
selection concise, policy details readable, editing progressive, and responsive
state dependent on the real tab viewport and font metrics.

The new `production_inbox_bbs_correction_spec.md` defines four gated packages:
PIC-0 review/specification, PIC-1 P1 Inbox correction, PIC-2 P3 BBS presentation,
and PIC-3 Linux production qualification. It records query/render bounds,
source/action scope, empty/degraded states, responsive geometry, Light/Dark and
Normal/Large matrices, performance budgets, and the Operational View Framework
design gates. No destructive migration or production data action is authorized.

Model ownership: the high-reasoning primary model owned product hierarchy,
source/data interpretation, concurrency and migration boundaries, slice order,
specification integration, and final review. Terra performed the focused Inbox
audit; Terra performed the focused BBS layout audit; Luna inventoried existing
tests and designed the missing acceptance/performance matrix. All delegated work
was read-only, so there were no delegated diffs to merge; the primary reviewed
each report against the governing UI, message, BBS, and responsiveness contracts.

PIC-0 acceptance evidence: all three production screenshots were reviewed at
their original resolution; implementation and existing test seams were traced;
the active-runtime uncertainty is explicitly carried into PIC-1; governing specs
were cross-referenced; and `git diff --check` passes. Unrelated worktree files
remain untouched.

## 2026-09-11 — Production Inbox correction (PIC-1)

Status: automated software exit gate passed; PIC-2 BBS presentation is now
authorized. Linux interaction remains part of the later combined PIC-3 gate.

The full-height inline `Pending JS8 MSGs` table has been replaced by a compact
`JS8 retrievals · N pending · Review` disclosure inside the normal Messages
workspace. The closed workbench performs a count-only read and creates no hidden
row/action widgets. Review opens a bounded modal workbench, loads at most 100
newest non-retrieved rows from one read snapshot, and provides Newer/Older paging
while preserving source-key/radio/JS8-instance context for Get and Mark
Retrieved. Theme changes and resize are geometry/paint-only for this surface.
Get and Mark Retrieved acknowledge before work begins, then use one serialized
daemon action lane for endpoint and storage I/O. A generation-fenced signal
returns completion to the GUI thread, avoiding both event-loop blocking and Qt
worker-thread shutdown ownership.

The ordinary Inbox is always the primary viewport and has a usable font-aware
minimum at the required production and compact heights. A direct offscreen
1280x720 render showed the compact retrieval disclosure above the mixed-source
table without displacing it. The available September 11 production database
copy independently contains 6,197 CommStat, 6,175 SitRep, 2,059 Spotter, 555
JS8, 477 BBS, 230 FLMsg, 187 VarAC, and 149 FLAMP projected rows, confirming that
the source catalog itself is not JS8-only. One hundred count-plus-page samples
against that 326 MB database and the active local runtime remained below 4 ms
maximum and 1.4 ms p95, so no index or migration was added.

Model ownership: Terra implemented the compact disclosure, review workbench,
bounded SQL paging, and primary-height guard. Luna implemented mixed-source,
action-scope, geometry, closed-workbench, theme, and text-size tests. The
high-reasoning primary model reviewed both diffs, separated count refresh from
hidden row materialization, added accessibility names, verified production-copy
source evidence and query timing, reviewed the rendered UI, and ran integration.

Acceptance evidence: 226 Inbox, Message Intelligence, and MIP-4 projection tests
pass. Coverage includes 26 pending retrievals with five normal source families,
100-row paging, server-side status filtering, Focus All/source semantics,
source-scoped mutation, the 200-row Inbox model, coalescing, and the complete
1280x720/1000x700/900x560 Light/Dark Normal/Large matrix. Python compilation and
`git diff --check` pass. No schema, production data, BBS implementation, or
external endpoint changed during PIC-1.

## 2026-09-11 — Production BBS presentation correction (PIC-2)

Status: automated software exit gate passed; PIC-3 Linux production
qualification is ready and remains operator-assisted.

Radio Service no longer spends the production-height workspace on a fixed
four-column table. A bounded serving-radio selector presents concise name,
serving, publication, and health state; one full-width selected-service editor
keeps the live folder and service choices readable. Native VarAC paths are
available behind `Managed in Radio Settings`, Save is the sole primary action,
and Radio Settings remains an enabled recovery route when a profile needs
configuration.

Locations & Access now derives compact mode from the actual tab viewport and
font height, uses a compact selector at short heights, and presents one selected
policy summary. The Add/Edit editor is collapsed by default, internally
scrollable when open, and includes explicit Save, Disable, and Cancel actions.
Long source paths are safely elided with tooltip/copy access. A platform-native
splitter grip that appeared as dotted/garbled content was made visually quiet.
Resize, theme, and font-change handlers alter geometry only.

Model ownership: Terra implemented the bounded Radio Service and progressive
Locations & Access presentation. Luna implemented the three-size,
two-theme/two-text-scale matrix and focused regressions. The high-reasoning
primary model reviewed every shared-worktree diff, removed the retained hidden
legacy radio table, corrected the recovery-route test expectation, added the
BBS-specific Help route, refined action geometry and splitter presentation,
reviewed offscreen renders, and ran integration.

Acceptance evidence: all 58 focused BBS tests pass. The combined Inbox, Message
Intelligence, MIP-4, BBS catalog/access/retention/publication, contextual-help,
and font-rendering selection passes 397 tests with one platform-dependent skip.
Offscreen Dark/Large renders at 1280x720 and 900x560 confirm reachable actions,
one policy summary, and no page-level horizontal overflow. Python compilation
and `git diff --check` pass. There is no schema migration, production-data
mutation, source-file operation, or BBS ownership/retention semantic change.

## 2026-09-11 — FLMsg arrival visibility correction (FIV-0/FIV-1)

Status: review/specification and automated implementation gates passed; Linux
production confirmation remains operator-assisted.

The supplied Linux log ruled out the configured NBEMS path, extensions, and
file scanner as the observed cause. Incremental discovery advanced from 604 to
605 files, the normalized projection generation advanced, and FIO loaded a
`.k2s` file as `origin=flmsg` without scanner or projection errors. Under the
FLMSG/FLAMP focus, however, the bounded query loaded 11 rows while the client
criteria rendered only 2. Newly projected files can carry provisional labels
such as `FLMsg K2S`; the second client filter accepted only exact `FLMSG` and
`FLAMP` labels. The general 200-row page also sorted primarily by embedded
event time, allowing an old report received today to be omitted despite the
seven-day arrival filter.

Source focus now follows canonical `flmsg`/`flamp` identity with a strict legacy
type fallback, and form filter choices group provisional extension labels under
FLMSG/FLAMP. Bounded Inbox selection is newest-effective-received first with
event time and message ID tie breakers; the keyset cursor uses the identical
order. Two idempotent additive indexes support the corrected read paths. There
is no source-file operation, table rewrite, message mutation, synchronous GUI
I/O, or destructive migration.

Model ownership: the high-reasoning primary model correlated the production
log, wrote the correction spec, owned query/cursor/index architecture and
migration review, reviewed each delegated diff, refined filter presentation,
and performed final integration. Terra implemented the bounded source-family
focus matcher and focused unit coverage. Luna implemented the independent
scanner-to-projection and received-first paging regressions.

Acceptance evidence: the four exact filename shapes (`.k2s`, `.b2s`, and
`.sig.b2s`) pass through scanner, incremental file projection, and the default
bounded query with current arrival mtimes and deliberately old report times.
The regression includes 205 competing newer-event records, the 200-row cap,
repeat determinism, and disjoint keyset pages. The combined relevant gate passed
292 tests; isolated Message Intelligence and PIC-1 partitions passed 191 and
18 tests. Production-copy count-plus-page timing across 100 samples was 0.813
ms median, 0.999 ms p95, and 1.143 ms maximum. Python compilation and
`git diff --check` pass. A monolithic all-message Qt run still encounters the
known cross-fixture native abort after accumulating scheduler-executor threads;
the affected partitions pass when run in isolated processes.

## 2026-09-11 — Message Inbox content-first reader (MIR-0/MIR-1/MIR-2)

Status: review/specification and automated software exit gates passed. MIR-3
Linux production qualification remains operator-assisted.

The fixed Inbox/detail splitter was replaced with two persistent modes in one
stacked workspace. Inbox mode gives the bounded list all available height;
opening a message switches to a full-height reader with Back to Inbox,
Previous, Next, position context, Escape/Alt+Left return, top-of-document
reset, and retained source-specific content and Open Image behavior. Navigation
uses at most the current 200 model rows and performs no Inbox page query,
source scan, parsing pass, or widget reconstruction. Back restores the saved
list position and current message where it remains available. User-initiated
focus/filter/sort changes close and clear the reader before requesting the new
scope, so content from a previous focus cannot appear associated with the new
one.

Tab-active lifecycle and reader-open state are now independent. Reading no
longer freezes projection invalidation. All focus counters arrive together from
one read-only grouped aggregate on the existing background projection-query
lane; the active focus, source refinement, search, and advanced filters do not
distort cross-focus summaries. There is no new timer, polling lane, schema
migration, source read, or GUI-thread database work. Opening an unread message
decrements all applicable visible counters immediately, with the next fenced
projection result providing durable reconciliation.

Model ownership: Terra implemented the bounded content-first reader, stable
navigation, state restoration, keyboard/accessibility behavior, and scope
clearing. Luna implemented focused responsive-reader, lifecycle, request-fence,
and immediate-counter tests. The high-reasoning primary model reviewed both
packages, owned the state/concurrency and aggregate-query design, integrated
the counter worker and local read transition, optimized the production-scale
query, reviewed offscreen renders, and ran final integration.

Acceptance evidence: all 424 message-related tests pass, plus a focused
37-test projection/responsive partition. Python compilation and
`git diff --check` pass. Offscreen 1280x720 and 900x560 renders confirm that the
reader owns the usable content height. On a migrated disposable copy of the
16,029-row production projection database, 50 aggregate samples measured
10.913 ms median, 11.409 ms p95, and 11.471 ms maximum against the 25 ms gate.
No production database or source file was modified. Linux production remains
the required final confirmation for live counter updates, compact-height
reading, theme/text scaling, and idle CPU.

## 2026-09-11 — Message reader Managed BBS actions (MRB-0/MRB-1/MRB-2)

Status: review/specification and automated implementation gates passed. MRB-3
Linux production qualification remains operator-assisted.

Eligible FLMsg, FLAmp, and VarAC file-backed messages now expose `+BBS` in the
content-first reader toolbar. The action is absent for messages that cannot be
published. Invoking it lazily opens the existing station Managed BBS location
checklist with authoritative memberships checked. Apply replaces the open
artifact's membership set; clearing every location unpublishes it everywhere
without modifying its source. A successful reader action remains in context,
shows `BBS · N` or `+BBS`, and provides a concise nonmodal confirmation.

Reader open, Previous/Next, Back, scope changes, resize, theme, and paint add no
BBS database or filesystem work. Eligibility comes from the current projected
row and publication labels use only already-warm or explicitly confirmed cache
state. Location and membership reads occur after the operator invokes the
action. No timer, polling lane, retained row widget, projection rebuild, schema
migration, or source scan was added.

`More Actions` now conditionally offers `Publish Selected to BBS...`. It
deduplicates at most the current 200 model rows and adds selected locations in
one transaction while preserving memberships elsewhere. Missing sources and
ineligible selected rows are skipped and summarized. The operation changes
catalog mappings only; it does not copy, move, delete, rename, or read source
content.

Model ownership: the tightly coupled reader, table-selection, and BBS mapping
change was handled by the high-reasoning primary model to avoid parallel edits
to the same UI module. The primary owned the UX contract, persistence/safety
review, implementation, focused tests, responsive render review, and final
integration.

Acceptance evidence: 585 Messages+BBS tests pass with one platform-dependent
skip, including focused cache-only eligibility/navigation, exact add/remove,
additive bulk, duplicate bounding, source preservation, projected-file,
filename-normalization, station catalog, retention, and responsive reader
coverage. A 900-pixel-wide offscreen reader render with a real `.k2s` file was
reviewed. Python compilation and `git diff --check` pass. No production data or
source file was changed. Linux production confirmation remains for live
membership preselection, add/remove convergence, and compact theme/text-scale
behavior.

## 2026-09-11 — Message reader navigation synchronization correction

Status: implementation and automated regression gate passed; Linux production
confirmation remains operator-assisted.

Production review found that reader position was committed before the target
document was rendered. Because the label update is inexpensive while file/form
decoding and read-state handling can take longer, the toolbar could visibly
advance one message ahead of the document; a rapid second activation could make
the mismatch appear persistent.

Reader navigation now disables re-entry, renders the target document first,
and then commits its stable identity and `N of M` position together. Buttons
are released after a short 100 ms input debounce, allowing Qt's normal event
loop to paint the coherent state. A render exception commits an explicit error
document with the target position instead of retaining the previous body.

Follow-up Linux review found that whole-reader `setUpdatesEnabled(False)` can
produce a compositor-level blanking or "swipe and vanish" effect. That paint
suppression was removed immediately and is now prohibited by the reader spec.
Render-first ordering and the input re-entry fence remain; widget painting is
continuous throughout navigation.

A second production observation showed that a zero-delay event-loop release
was still weaker than the actual visual boundary: sufficiently fast clicks
could advance the lightweight position label before a complex document paint.
The reader initially requested an explicit viewport paint acknowledgement after
each manual navigation. Position, stable identity, BBS context, and button
release were intended to commit only after that paint completed. Additional
activations remained disabled until the displayed document caught up.

The initial paint-acknowledgement handler changed sibling toolbar state from a
`QTextEdit.paintEvent`, which reintroduced the Linux "swipe and vanish"
symptom. Queuing that callback did not eliminate the production symptom, so
the specialized reader and paint observer were removed entirely. The current
implementation installs the target document and then commits identity and
position in the same handler; Qt paints that coherent state normally after the
handler returns. A 100 ms single-shot input debounce prevents rapid-click
re-entry. No widget is hidden, updates are never suppressed, no repaint is
forced, and no application state is changed from a paint callback. Sparse
`MESSAGES|reader_open`, `reader_navigate`, and `reader_close` records now
distinguish an intentional close from a Linux repaint artifact.

## 2026-09-11 — Message reader recoverable FLMSG/FLAMP delete action

Status: specification and automated implementation gate passed; Linux
production qualification remains operator-assisted.

The reader now exposes `Delete…` only for an existing regular FLMSG or FLAMP
source file resolved from the already-loaded row. FIO already supported this
operation from the Inbox table: after explicit confirmation, the exact source
file is moved to operating-system Trash/Recycle Bin, FIO cache/projection state
is removed, and an audit record is written. The reader action does not broaden
that authority.

Confirmation names the source and exact path, explains recovery and current-view
effects, and discloses known or possible Managed BBS publication impact. Cancel
does nothing. Failure retains the reader and reports the problem. Success closes
the reader, returns to the refreshed Inbox, suppresses the matching projection,
and confirms the exact filename. Context actions are disabled while reader
navigation is in its render/commit debounce, preventing deletion of a stale
prior identity.

Model ownership: the high-reasoning primary model owned the paint/lifecycle
correction, delete-authority review, safety contract, implementation, and tests.

Acceptance evidence: focused reader, navigation, and Managed BBS action coverage
passes 21 tests, including the absence of a custom paint lifecycle, rapid-click
rejection, render-before-position commit,
cache-only delete eligibility, missing-file rejection, exact target removal,
audit/projection handling, and Inbox return. No schema migration, recursive
filesystem action, direct unlink path, BBS query on render, or polling lane was
added.

The recoverable-delete adapter now also uses native Finder Trash on macOS. The
command is passed as an argument vector with an escaped POSIX path and no shell;
failure leaves the file intact. Windows retains native Recycle Bin handling and
Linux retains `gio trash`, `trash-put`, and KDE trash-service fallbacks.

The broader reader, responsive-layout, asynchronous projection, Inbox query UI,
and Message Intelligence partition passes 237 tests. Python compilation and
`git diff --check` pass. The navigation correction adds no database query,
source scan, polling lane, background worker, schema migration, or filesystem
mutation.

## 2026-09-11 — Message reader apparent one-click lag: duplicate file identity correction

Status: implementation and automated gate passed; Linux production confirmation
remains operator-assisted.

The new sparse reader diagnostics showed that each reported click completed a
synchronous FLMSG render in 5–31 ms and committed one new stable row identity.
Inspection of the same production projection database then found 56 FLMSG file
references representing only 28 distinct physical file versions. Each pair had
the same path, modification time, size, subject, and body but different source
identities: the legacy display-path identity and the newer reversible
SQLite-safe path identity. The first click therefore advanced to an identical
duplicate; the second reached the next actual file. This exactly reproduced the
reported counter/body behavior and disproved paint latency as its cause.

All projection Inbox reads now collapse duplicate file-version identities before
page limits, totals, and focus counters. The SQLite-safe source identity is
preferred deterministically, with newest projection time and message id as tie
breakers. An additive covering index keeps the correlated identity check
bounded. Projection-primary mode no longer writes reconstructed presentation
rows through the legacy projector; the application coordinator remains the sole
writer. Existing derived rows and source files are not deleted or rewritten.
Unknown future payloads also replace the prior body with an explicit unsupported
format document, closing the only other code path that could advance position
without replacing content.

Model ownership: Terra performed the independent reader/loader audit and
identified the unsupported-payload stale-document risk. Luna added a real Qt
single-physical-click regression using the production JS8 renderer. The
high-reasoning primary model correlated lifecycle telemetry with the production
database, identified the dual file identities and second writer lane, designed
the read-model compatibility rule, implemented the architecture correction,
and performed final integration.

Acceptance evidence: 283 reader, projection, file-pipeline, responsive Inbox,
and Message Intelligence tests pass. The focused production database read now
returns 28 FLMSG/FLAMP rows and a total of 28 from 56 retained file references
representing 28 unique physical file versions. Twenty-five read-only samples on
that database measured 14.549 ms median; the 97.939 ms cold maximum remains on
the background query lane. Python compilation and `git diff --check` pass. No
source file or production database was modified, and no destructive migration
was introduced.

## 2026-09-11 — Startup dedication and community support message

Status: implemented and automated gate passed.

The lightweight startup splash now includes the dedication, “Dedicated to my
Dad, now SK, who learned digital HF TriMode at age 86.” It also carries a
restrained invitation: “If FIO serves your station, please consider supporting
its continued development,” followed by the recognizable Buy Me a Coffee name
and `buymeacoffee.com/n1mag`. The support message is informational and never
blocks, delays, or requires interaction during startup. Both messages are also
included in the splash accessibility description.

Acceptance evidence: the focused startup-splash content/accessibility test
passes, Python compilation and `git diff --check` pass, and the rendered
540×270 splash was visually reviewed with a live startup-status line.

## 2026-09-11 — Linux desktop-panel FIO icon restoration

Status: implemented; Linux production confirmation remains operator-assisted.

The application now declares `FreqInOut`, organization `N1MAG`, and desktop
file id `freqinout` immediately after `QApplication` construction and before
the splash creates the first window. Linux and macOS prefer the PNG application
artwork while Windows continues to prefer the multiresolution ICO. Both source
and PyInstaller `_MEIPASS` asset roots are supported. The generated Linux
desktop entry now declares matching `StartupWMClass=FreqInOut`, allowing
Mint/Cinnamon and other desktop shells to associate the running window with
`freqinout.desktop` instead of displaying a generic gear. Missing runtime
artwork is logged rather than silently ignored.

Acceptance evidence: 12 focused application-identity, icon-loading, splash,
and font-surface tests pass. Python compilation, installer shell syntax, and
`git diff --check` pass. No startup polling, filesystem scan, or blocking work
was added.

## 2026-09-11 — Compose adoption of the target selected in JS8Call (CMW-5)

Status: automated implementation gate passed; Linux production qualification
remains operator-assisted.

JS8Call, FIOSpotter, and CommStat RF Compose now issue one bounded background
`RX.GET_CALL_SELECTED` request for the selected radio when entering the mode,
changing radios, or explicitly refreshing. A callsign or group returned by
JS8Call appears in a highlighted inline cue using the radio's short name:
`Already selected in JS8Call ...`. The live value remains separate from every
draft until the operator chooses `Use Target`; matching drafts show
`Target in Use`. Adoption does not send, weaken preflight, or silently replace
another target.

The request lane is single-in-flight with latest-request coalescing. Each result
must match its generation, radio ID, and resolved host/port endpoint identity,
so a late result from another radio cannot appear in the current workbench.
Empty, unsupported, timed-out, and unreachable results remain non-blocking.
The worker is included in bounded shutdown. There is no poll timer, and payload
typing, preview, resize, paint, and theme paths do not perform socket work.

Model ownership: the GPT-5 Codex high-reasoning primary owned the API contract,
endpoint/concurrency/lifecycle design, specification, delegated-diff review,
integration, and final gate. `gpt-5.6-terra` at medium reasoning implemented the
bounded responsive cue and explicit adoption hooks. `gpt-5.6-luna` at medium
reasoning implemented the focused CMW-5 tests. During review, the primary
replaced a radio/instance-only stale key with the actual mapped endpoint
identity, connected the one-shot worker, added latest-request serialization,
routed adoption through normal target-change behavior, added highlighted
matching-state presentation, and removed cue restyling from ordinary body
keystrokes.

Acceptance evidence: 17 focused CMW-5 tests pass. The wider Compose, JS8 API,
guarded send, Expect, NBEMS, and Managed BBS partition passes 190 tests. Python
compilation and `git diff --check` pass. An offscreen 1000x700 Compose render was
reviewed with the highlighted selected-group cue and reachable `Use Target` and
`Refresh Target` actions. A 120-edit real QTextEdit signal-path probe measured
0.750 ms median, 0.899 ms p95, and 1.295 ms maximum against the 16 ms p95 and
50 ms maximum Compose budgets. No schema migration, production-data mutation,
periodic polling, or destructive operation was introduced.

## 2026-09-12 — Software administration SCA-S0 specification and read model

Status: implementation and automated exit gate passed.

The Settings Configuration Assistant now defines the software-centered operator
workflow `choose software -> see radios -> choose radio -> choose task ->
configure`, explicit software ownership boundaries, scoped-save behavior,
cache-only navigation, responsive/accessibility requirements, and five gated
delivery slices. A new immutable, DB-free software-administration read model
builds deterministic reverse radio assignments for JS8Call, Fast Light, VarAC,
CommStat, External Spotter, and FIO Spotter from already-loaded configuration
rows. It retains disabled linked assignments, identifies missing and unassigned
instances, discloses shared instances, and consumes only supplied cached
readiness evidence. It performs no database, filesystem, process, socket, or
radio work and introduces no schema or runtime-data mutation.

Model ownership: the high-reasoning primary model owned the information
architecture, ownership taxonomy, immutable model design, implementation,
delegated-diff review, and exit gate. `gpt-5.6-luna` at medium reasoning added
the focused pure-model tests. The primary rejected and corrected the first test
contract because it regrouped JS8Call, FIO Spotter, External Spotter, and
CommStat under one JS8 family, which would have contradicted the specification.

Acceptance evidence: `pytest -q tests/test_software_administration_model.py`
passes 6 tests; Python compilation and `git diff --check` pass. SCA-S0 is closed
and SCA-S1 may begin.

## 2026-09-12 — Software administration SCA-S1 workspace and navigation

Status: implementation and automated exit gate passed.

Settings now exposes Software as a first-class administration context beside
Main and Radios. The new cache-only Software Administration workspace presents
software families first, then the radios that use the selected software, then
task choices for that family. Radio chips disclose disabled, missing, shared,
and unassigned configuration without opening a database, scanning a path,
probing a process, or contacting an endpoint. Radio Profile software actions
deep-link to the same family and radio context instead of opening a competing
legacy surface. The workspace retains usable controls at 900x560 and 1000x700,
uses stable deterministic selection, and exposes accessible state text.

Model ownership: the high-reasoning primary model owned navigation architecture,
cached integration, compatibility routing, delegated-diff review, and the exit
gate. `gpt-5.6-terra` at medium reasoning implemented the bounded workspace UI.
`gpt-5.6-luna` at medium reasoning implemented focused widget and integration
tests. Primary review corrected nondeterministic family fallback, excess minimum
height at the supported compact size, shared-instance wording, and two test
drafts that assumed APIs or layout restrictions outside the approved contract.

Acceptance evidence: the combined software workspace, immutable model, and
radio-scoped software settings suite passes 170 tests under the offscreen Qt
platform. A real deferred SettingsTab smoke test opens the Software context with
radio chrome hidden and the workspace selected. Python compilation and
`git diff --check` pass. No schema migration or production-data mutation was
introduced. SCA-S1 is closed and SCA-S2 may begin.

## 2026-09-12 — Software administration SCA-S2 task ownership and editors

Status: implementation and automated exit gate passed.

Software Administration now keeps the operator in one software-centered
workspace while switching family, radio, and task. Declarative task editors
cover JS8Call, Fast Light, VarAC, CommStat, External Spotter, and FIO Spotter
using the existing radio software state keys. JS8Call no longer visually owns
CommStat, the external Spotter launcher, or legacy Expect administration.
Built-in FIO Spotter Settings is limited to dependencies and radio mapping and
links to the top-level operational workspace. The old monolithic JS8Call,
Fast Light, and VarAC forms remain hidden compatibility adapters for existing
load/capture behavior rather than navigable duplicate editors.

Ordinary task navigation is cache-only and preserves registered editor widgets
and drafts. Dotted message-folder state round-trips without flattening, returned
editor state is defensively copied, compact form rows wrap, controls have
accessible names, and literal ampersands remain visible in task labels. Settings
startup no longer performs the four synchronous hidden-table refreshes for the
legacy Expect and imported-Spotter review UI.

Model ownership: the high-reasoning primary model owned product boundaries,
state-adapter design, Settings integration, startup behavior, delegated-diff
review, visual QA, and the exit gate. `gpt-5.6-terra` at medium reasoning added
the durable task-editor registry and revised the S1 routing contract.
`gpt-5.6-luna` at medium reasoning audited legacy ownership/coupling and added
the focused SCA-S2 ownership/editor tests. Primary review rejected the first
test draft's invented constructor and state-cache API, aligned it to the actual
host-owned draft architecture, and corrected defensive nested-state copying.

Acceptance evidence: the combined SCA-S0/S1/S2 and radio-scoped settings suite
passes 178 tests under offscreen Qt. Python compilation and `git diff --check`
pass. A 1000x700 offscreen render was visually reviewed with the software,
radio, and task choices plus the exact JS8Call/FIO-A editor scope all visible.
No schema migration or production-data mutation was introduced. SCA-S2 is
closed and SCA-S3 may begin.

## 2026-09-12 — Software administration SCA-S3 scoped drafts and saves

Status: implementation and automated exit gate passed.

Software editor drafts are now keyed by radio and software family. Family and
radio chips, the identity banner, and editor state all include explicit
`Unsaved changes` text; color is supplementary. The selected editor's save
label names the exact family and radio. Saving merges only that family's owned
keys into a fresh persisted base and preserves unrelated fields, nested
message-folder ownership, and other family drafts. Shared instances disclose
the other affected radios before confirmation. Failed saves retain draft and
dirty state. A deliberate secondary Save All action saves all staged families,
while global Save Settings explicitly leaves Software drafts untouched.

Primary review also corrected the legacy bundle writer so default host/port
values alone do not create unrelated JS8Call or Fast Light instance records.
Wrong-radio source identity rejection remains in force, and the existing
single-active-radio legacy projection runs only after successful scoped writes.

Model ownership: the high-reasoning primary model owned persistence partitions,
merge semantics, shared-instance confirmation, exact-scope and Save All
integration, failure behavior, legacy projection review, and the exit gate.
`gpt-5.6-terra` at medium reasoning implemented the accessible dirty-state and
Save All workspace UI. `gpt-5.6-luna` at medium reasoning implemented the pure
partition/merge tests. The primary added behavioral save/failure/shared-instance
tests and corrected a nondeterministic JS8 offset comparison that could have
created an unrelated JS8 record during a Fast Light save.

Acceptance evidence: the combined SCA-S0 through SCA-S3 and radio-scoped
settings suite passes 193 tests under offscreen Qt. Python compilation and
`git diff --check` pass. No schema migration or destructive data operation was
introduced. SCA-S3 is closed and SCA-S4 may begin.

## 2026-09-12 — Software administration SCA-S4 discovery and qualification

Status: implementation and automated exit gate passed; software-centered
Settings delivery is complete.

Software task editors now expose an explicit `Find installed software` action.
Discovery runs in one Settings-owned `QThread` lane from a captured Settings
mapping, coalesces repeated requests to the newest request, requests cancellation
of superseded work, and rejects stale generation or wrong family/radio/task
results. Only blank fields are filled; existing values are preserved and the
editor receives a calm completion summary. Navigation and repaint remain
cache-only. Shutdown is bounded to 1.2 seconds and retains an unusually delayed
worker until it exits so Qt cannot destroy a running thread.

The compact-height workspace now removes redundant prompt text while preserving
software, radio, and task chips, the context banner, explicit actions, and a
substantially larger editor. Help documents the new mental model, exact-scope
saves, discovery/check behavior, shared instances, and FIO Spotter/BBS ownership.

Model ownership: the high-reasoning primary model owned worker architecture,
generation/context correctness, lifecycle and shutdown safety, Settings
integration, compact-layout review, delegated-diff review, visual QA, and final
integration. `gpt-5.6-terra` at medium reasoning handled the bounded help and
operator-documentation package. `gpt-5.6-luna` at medium reasoning handled the
light/dark, supported-size, Large Text, accessibility, repaint, and cache-only
qualification package. The primary added worker/state-transition tests and
refined the compact editor after reviewing the 900x560 render.

Acceptance evidence: 246 focused and adjacent Settings/status tests pass with
23 intentional environment skips; the SCA-only combined gate passes 213 tests.
Python compilation, contextual-help anchor validation, and `git diff --check`
pass. A full-repository run was non-gating and was stopped after unrelated
legacy tests accumulated scheduler executor threads and stalled in a theme-heavy
Inbox test; the interrupt exposed that run's existing Qt teardown fault. Linux
window-manager and installed-software discovery checks remain operator-assisted
and are explicitly listed in the controlling spec. No migration or destructive
operation was introduced.

## 2026-09-12 — Software navigation orphan-window regression

Status: corrected; focused exit gate passed.

Opening Software exposed an unowned section-navigation button as a top-level Qt
window. The button was created for every Settings section, but Software has no
secondary section-button layout because its family/radio/task chips own local
navigation. Visibility refresh styled and showed the parentless button, creating
the full-screen blue `Software Administration` surface; every click refreshed
visibility and made it recur. Section buttons are now created only for global or
radio layouts and receive an explicit Settings parent. Software and hidden
compatibility sections create no orphan control.

Model ownership: the high-reasoning primary model matched the screenshot's
left-aligned, vertically centered button text to the parentless section control,
implemented the ownership correction, reviewed both delegated findings, and ran
the integration gate. `gpt-5.6-terra` at medium reasoning performed a bounded
widget/sizing audit; because it inspected the shared tree after the primary fix,
its alternative stack-sizing hypothesis was not adopted. `gpt-5.6-luna` at
medium reasoning added the real SettingsTab interaction regression test. Primary
review moved its top-level-window baseline before opening Software so the test
would fail on the reported initial overlay as well as on recurrence.

Acceptance evidence: opening Software and clicking the JS8Call family, FIO-B
radio, and API & Radio task produces no additional visible top-level widget; the
workspace remains embedded and active. The combined focused and adjacent gate
passes 246 tests with 23 intentional environment skips. No migration or
destructive operation was introduced.

## 2026-09-12 — Software Administration responsive content correction

Status: corrected; automated exit gate passed.

Production screenshots showed the Software editor beginning near the bottom of
the page with its fields and actions clipped. A complete family/task audit found
that Settings fixed the shared section stack to the initial placeholder's
one-time height. The later-created editor could not enlarge that ancestor. The
same audit found dead editable-looking forms in the All context, a duplicated
page heading, empty form scrollers and save controls on informational tasks, and
excess secondary chrome at compact height.

The Software section now follows the live Settings viewport and is resynchronized
after editor creation and window resize. It no longer inherits the largest
hidden legacy page or creates outer horizontal/vertical overflow. The editor
keeps form content in its bounded internal scroller and anchors discovery,
operational, dirty-state, and exact-save actions in a responsive footer. The
footer uses one row when space permits and wraps when narrow. Compact height
hides duplicated status and secondary assignment text so the actual task stays
usable. Informational and read-only tasks no longer advertise a save operation.

All now renders a cached, read-only summary of radio assignments, instance
names, readiness, shared use, and unassigned instances. It cannot accept or
silently discard radio-owned edits. Choosing a radio restores every applicable
task. The embedded duplicate heading was removed.

Model ownership: the high-reasoning primary model owned the viewport and All
context architecture, implementation, delegated-diff review, visual QA, and
integration gate. `gpt-5.6-terra` at medium reasoning performed the read-only
cross-family layout/root-cause audit. `gpt-5.6-luna` at medium reasoning added
the bounded family/task/theme/text-size matrix; primary review replaced its
future-seam and always-save assumptions with the implemented aggregate and
editable-field contracts, and added the real SettingsTab viewport regression.

Acceptance evidence: the focused layout/editor/workspace/model partition passes 73
tests. The broader software, Settings, help, status, persistence, and discovery
partition passes 307 tests with 23 intentional environment skips. Offscreen
renders at 1000x700 and 900x560 show complete task fields and footer actions,
zero page-level horizontal or vertical scroll, and a full Settings Help button.
Python compilation and `git diff --check` pass. No schema migration, endpoint
I/O on navigation, production-data mutation, or destructive operation was
introduced.

Follow-up production review found the remaining no-field task defect: hiding
the form scroller did not clear its layout stretch, so Health, Overview, and
other action-only tabs distributed their heading, explanation, status, and
button over the full editor height. Every no-field task now removes that
stretch and top-aligns its meaningful content. The neutral `Not checked` copy
is now `Not yet verified`, with a tooltip explaining that no current
verification evidence exists and directing the operator to Health. Luna added
the complete no-field/action matrix and terminology regression; the primary
strengthened it with geometry-order and zero-stretch assertions after reviewing
Terra's structural audit.

## 2026-09-12 — Guided multi-instance software administration

Status: implementation complete; automated exit gate passed. Live Linux
qualification with two simultaneous instances of each installed family remains
an operator-assisted release check.

Recovery checkpoint `55065a0` preserves the completed software-centered
workspace before this lifecycle work. The new in-workspace assistant follows
Purpose, Find or create, Identity, Connections, Files, Launch, and Review. It
starts with a FIO-guided local setup, requires an owning radio, proposes the
next unused family ports and a stable JS8 rig name, imports only a specifically
selected discovery result, shows family-specific names instead of generic
fields, and keeps every external write visible as `None from this review`.
Discovery is bounded, explicit, asynchronous, and stale-result protected.

An additive `software_instance_manifests` table now records management mode,
provenance, executable/configuration/data roots, launch command, endpoint and
exclusive-resource claims, verification state, and bounded evidence. Saving a
reviewed instance, linking it to a radio, creating its launch items, and adding
optional VarAC cluster membership is one `BEGIN IMMEDIATE` transaction; any
collision or error rolls the whole operation back. Legacy application-table
collisions are checked even when no manifest exists. Canonical exclusive paths,
JS8 TCP/UDP, FLRig/FLDigi endpoints, duplicate application ownership, VarAC
node paths, and cluster instance numbers cannot be silently reused.

Launch planning now carries FIO-managed `--rig-name` for stock JS8Call 2.2.0,
Improved 3.0.3, and the approved rig-scoped Subspace assumption. It preserves
Fast Light's instance-specific FLRig/FLDigi profile and XML-RPC arguments,
orders FLRig before FLDigi, and blocks duplicate launch endpoints/resources.
Cluster VarAC requires a configured cluster/instance pair and an explicit
instance launch command. FIO manages the durable launch recipe but does not
claim to rewrite third-party native settings; live reachability and identity
remain explicit Health verification.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): specification, architecture, transaction and schema review,
  integration, canonical collision hardening, delegated-diff review, adjacent
  regression gate, and final exit decision;
- `gpt-5.6-terra`, high reasoning: bounded discovery/adoption-plan and launch
  preflight core plus focused tests;
- `gpt-5.6-luna`, high reasoning: seven-step responsive assistant and workspace
  integration plus focused UI tests;
- `gpt-5.6-luna`, medium reasoning: independent manifest/transaction rollback,
  replacement, startup, cluster, and no-sharing tests;
- inherited primary-class delegated source/test audit (no model override;
  exact host identifier not exposed): JS8Call Improved/Subspace launch and
  Settings-adapter regression fixtures.

Primary review corrected a nonexistent VarAC profile-column write exposed by
the rollback package, made cluster membership part of the same transaction,
propagated Fast Light native launch arguments through the station planner,
removed a JS8 discovery fallback that confused `SaveDir` with message storage,
made imported candidates operator-managed, and replaced ambiguous managed-copy
claims with the implemented launch-ownership boundary.

Acceptance evidence: the final combined Software Administration, Settings,
instance/adapter/manifest, storage, discovery, database, and launch partition
passes 340 tests. Python compilation,
HTML parsing, and `git diff --check` pass. A full repository attempt reached 59%
but the test process segfaulted after unrelated scheduler tests accumulated many
live worker threads; an earlier unrelated ingest-source fixture also fails in
isolation because its JS8 messages are filtered. Neither failure occurs in or
is caused by this change's focused/adjacent partitions. No production database,
external application configuration, endpoint, radio, or filesystem content was
mutated by the implementation or tests.

## 2026-09-12 — Radio-first software ownership and replacement

Status: MIS-5 implementation complete; automated exit gate passed. Live Linux
multi-process and real radio/PTT qualification remains an operator-assisted
release check.

Software Administration now makes ownership explicit before configuration. Its
family, radio, and task chip rows are exclusive and retain one visible selection
after repeated clicks. Radio chips identify `Available` or `Assigned:
<instance>`, while the primary action changes between `Create a radio first`,
`Create or use instance`, and `Replace instance`. This is a family-scoped rule:
one radio may use JS8Call, Fast Light, and VarAC together, but it can own only one
runtime from each family and one independently controlled runtime cannot serve
two radios.

The assistant no longer creates operational orphan instances. An empty station
routes directly to Guided Add Radio. Existing unassigned records remain bounded,
family-filtered recovery choices. An occupied family slot requires an explicit
replacement acknowledgement and keeps the current/proposed comparison visible
through Review. The Settings adapter sends the expected current instance ID so
stale UI state is rejected before mutation.

The store centralizes ownership validation and performs replacement in one
`BEGIN IMMEDIATE` transaction. The radio link, application/manifest state,
FIO-managed launch items, and applicable VarAC membership either change together
or remain unchanged. Replacement preserves every other software family on the
radio. The prior record is retained disabled for recovery. The confirmed
Advanced `Disassociate` action clears only the selected family link/use flags,
FIO-managed launch items, matching control backend, and applicable VarAC
membership; it retains external applications, profiles, databases, messages,
inboxes, outboxes, logs, and files. Legacy shared links are not destructively
rewritten and remain visible for operator recovery.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): product architecture, lifecycle/cardinality decisions, migration
  judgment, host integration, delegated-diff review, documentation, acceptance
  gate, and final integration;
- `gpt-5.6-terra`, high reasoning: centralized ownership validation, atomic
  replace/disassociate lifecycle, rollback behavior, and focused store tests;
- `gpt-5.6-luna`, high reasoning: exclusive radio-first workspace and guided
  replacement UX with responsive/accessibility coverage;
- `gpt-5.6-luna`, medium reasoning: independent integrated regression audit and
  cross-family preservation coverage.

Primary review refined the delegated work by wiring confirmed disassociation
through Settings, clearing a matching JS8Call/FLRig control backend to Manual,
making ownership—not cached verification—the radio-chip label, routing an empty
station directly to radio creation, filtering Assign Existing to retained
unassigned records, and qualifying launch cleanup as FIO-managed so legacy
operator launch records are never guessed at or removed.

Acceptance evidence: the final focused ownership/assistant/workspace/Settings
partition passes 82 tests. The broader Software Administration, Settings,
storage, discovery, manifest, database, and launch partition passes 361 tests.
Python compilation, HTML parsing, and `git diff --check` pass. A wider historical
suite still contains pre-existing scheduler expectations and can hit the known
Qt/worker teardown segmentation fault after accumulating scheduler threads;
neither failure touches the files in this correction. No production database,
external application configuration, endpoint, radio, or external filesystem
content was mutated.

## 2026-09-12 — Software family-selection swipe/vanish correction

Status: corrected; automated exit gate passed. Linux production confirmation
remains operator-assisted.

Production review found that Settings > Software could paint normally, then
appear to swipe away immediately after JS8Call was selected. The click path was
confirmed to be cache-only and contained no route away from Settings. Both live
radio-to-JS8 assignments were valid, and the available log contained no matching
exception. The failure was a layout feedback loop: the shared Settings stack
was hard-pinned to the inner viewport during a deferred-load reflow, even though
that viewport's transient size was partly determined by the child being pinned.

Software sizing now uses the height already allocated to the outer Settings
scroll container, with a 240-pixel startup floor. The active page and shared
stack use one stable bound, and the stack restores its non-expanding policy so
large hidden legacy forms cannot make it grow beyond the screen. Normal resize
settling recalculates the bound. Repeated JS8Call family/radio/task selection and
same-snapshot rebuilding no longer collapse the workspace or produce outer
scrollbars at supported sizes.

Model ownership: the high-reasoning primary model owned lifecycle diagnosis,
layout architecture, delegated-diff review, specification and work-log updates,
and final integration. `gpt-5.6-terra` at high reasoning audited navigation,
runtime assignment validity, and the shared-stack feedback path. `gpt-5.6-luna`
at high reasoning implemented the bounded layout correction and focused
reflow/resize regression. `gpt-5.6-luna` at medium reasoning independently
stressed delayed selection and snapshot refresh across light/dark themes,
Normal/Large Text, and 1920x1080, 1000x700, and 900x560.

Acceptance evidence: the focused Software Administration workspace, layout
matrix, radio-first ownership, Settings adapter, and radio-scoped Settings gate
passes 239 tests. The independent layout stress partition passes 64 tests, and
20 repeated JS8Call selection/task/reflow cycles retained the active section and
editor with zero outer scroll and no additional visible top-level window. The
broader multi-instance, persistence, discovery, status, workspace, and Settings
integration partition passes 336 tests with 4 intentional environment skips.
Python compilation and `git diff --check` pass. No migration, database write,
endpoint I/O, external application change, or destructive action was introduced.

## 2026-09-12 — Software instance-assistant swipe/vanish correction

Status: corrected; automated exit gate passed. Linux production confirmation
remains operator-assisted.

The JS8Call `Create or use instance` and `Replace instance` actions correctly
opened an embedded assistant, but a subsequent cached Settings refresh selected
the family summary or radio task editor over it. The assistant and its draft
were still alive; the host had merely hidden the active workflow, producing the
same apparent swipe-and-vanish failure seen in production.

Passive family-summary, task-editor, snapshot, and health/status refreshes now
update their cached state while preserving the assistant as the current stacked
page. They do not overwrite the assistant's draft or status. Explicit
navigation still tells the operator to finish or cancel the workflow. Cancel
restores the current radio task editor, or the refreshed All-radio summary; the
latter is explicit so a previously viewed radio editor cannot leak into the
All-radio context. Successful persistence remains the only non-cancel path that
closes the assistant.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): deterministic host-level reproduction, lifecycle architecture,
  delegated-diff review and refinement, specification/work-log updates, and
  final integration gate;
- `gpt-5.6-terra`, high reasoning: read-only signal, parent, modality, deferred
  refresh, and ownership audit that isolated the unconditional stack selection;
- `gpt-5.6-luna`, high reasoning: bounded workspace correction and focused
  lifecycle regression implementation;
- `gpt-5.6-luna`, medium reasoning: independent final verification across both
  actions, host refreshes, themes, text sizes, resizing, and Cancel restoration.

Acceptance evidence: the focused assistant/workspace/layout/Settings partition
passes 95 tests. The broader Software Administration, Settings, persistence,
discovery, manifest, and status partition passes 338 tests with 4 intentional
environment skips. Independent manual offscreen verification passed All-radio
Create and selected-radio Replace through cached refresh, dark/light themes,
Normal/Large Text, and 1000x700, 900x560, and 760x460 resizes, with no additional
top-level window or page-level horizontal scrollbar. Python compilation and `git diff --check`
pass. No migration, database write, endpoint I/O, external application change,
or destructive action was introduced.

## 2026-09-12 — Systemic Settings swipe/vanish root correction

Status: corrected; automated exit gate passed. Linux production confirmation
remains operator-assisted.

The recurring disappearance was reproduced from the real MainWindow deferred
startup path at the 900x600 application minimum. The earlier correction changed
which transient height was sampled but retained the underlying feedback
mechanism: Settings copied that height into equal minimum and maximum bounds on
both the active page and its shared stack. During the 75 ms deferred load, the
viewport and child then sized one another. At compact height the Software editor
extended beyond its clipped parent while the old test incorrectly required no
vertical scrollbar, producing the apparent swipe-and-vanish behavior.

The permanent correction introduces a constant-time current-page stack used by
Settings, Software Administration, and the instance assistant. Hidden pages no
longer influence size hints, the outer Settings scroll area owns the viewport,
and all hard height mirroring was removed. Compact layouts may use bounded
vertical scrolling so the primary action remains reachable; no recurring timer
or child scan is added. MainWindow now binds deferred screen-local callbacks to
a navigation epoch, discards stale callbacks after navigation, preserves each
tab's established activation semantics, and uses the actual main stack for
compact-navigation selection.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): real-path diagnosis, geometry/lifecycle architecture, navigation
  epoch integration, delegated-diff review, documentation, and final gate;
- `gpt-5.6-terra`, high reasoning: read-only deferred refresh, nested stack,
  visibility, and sizing audit that isolated the hard-bound feedback loop;
- `gpt-5.6-luna`, high reasoning: reusable current-page stack implementation,
  bounded integration, and focused geometry tests;
- `gpt-5.6-luna`, high reasoning: independent real MainWindow first-launch
  reproduction across desktop and compact sizes;
- `gpt-5.6-luna`, medium reasoning: independent regression-gap audit of startup,
  lifecycle, and top-level-window coverage.

Acceptance evidence: the focused geometry, navigation-epoch, Software
Administration, and startup partition passes 113 tests. The broader Software
Administration, persistence, discovery, status, Settings, and selected shell
navigation partition passes 360 tests with 4 intentional environment skips.
Independent real offscreen MainWindow verification passed immediate Settings >
Software entry before deferred loading at requested 900x560 (the application
minimum clamps to 900x600) and at 1000x700. Settings settled without subsequent
geometry oscillation; JS8Call radio/task and Create/Replace assistant ownership
remained stable; compact vertical scrolling was bounded, horizontal scrolling
was zero, Cancel restored the editor, MainWindow stayed visible, and no other
visible top-level window appeared. Python compilation and `git diff --check`
pass. No migration, database write, endpoint I/O, external application change,
or destructive action is part of this correction.

## 2026-09-12 — P1 FLRig verification and QSY continuation

Status: implementation complete and automated exit gate passed. Linux
production confirmation remains operator-assisted.

Production `freqinout (28).log`, the running local configuration, and scheduler
events show that this is not a basic FLRig connection outage. FIO-A and FIO-B
resolve to independent `127.0.0.1:12345` and `127.0.0.1:12346` endpoints. Direct
read-only XML-RPC checks returned FLRig 2.0.10 PTT off, valid VFO A, and current
frequencies immediately. The database also contains successful post-command
verification for both endpoints.

The regression is the scheduler's asynchronous status handoff. A cold or expired
target status request returns its stale placeholder immediately, so the safe PTT
gate holds QSY. The completed fresh result clears health state but does not
resume the exact held intent. A later schedule tick commonly arrives after the
short safety freshness window, starts another poll, and holds again. Separately,
the cache-only control bar correctly avoids endpoint I/O but degrades to
`Applied · verification unavailable` after its cached evidence ages because no
independent active-endpoint status cadence owns liveness.

The implementation package will add generation-fenced, endpoint-scoped status
continuations; a paced active-endpoint liveness refresh; target-qualified manual
QSY preflight; and truthful pending/queued/blocked feedback. Unknown PTT remains
fail-closed and every continuation re-runs shared-resource, RF-guard, busy,
ownership, and deduplication checks. There is no destructive migration.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): live/production evidence correlation, scheduler concurrency and
  safety architecture, specification, implementation integration, delegated
  diff review, and final gate;
- `gpt-5.6-terra`, high reasoning: read-only root-cause and endpoint-routing
  audit;
- `gpt-5.6-luna`, high reasoning: focused QSY/status regression design and test
  implementation;
- `gpt-5.6-luna`, medium reasoning: independent specification and acceptance-gate
  audit.

Implementation and review evidence: the high-reasoning primary implemented the
endpoint/configuration-epoch-fenced continuation, paced runtime status cadence,
stable recent-readback presentation, result-bearing manual QSY contract,
target-qualified shared-PTT preflight, and checked FLRig/rigctld PTT reads. The
primary reviewed every delegated test diff, retained deterministic event-based
coordination, and expanded the gate where the first delegated cadence test did
not exercise the timer path directly. Luna then added direct `_on_timer`
two-endpoint cadence coverage plus pending/blocked/legacy QSY feedback tests.

Acceptance evidence: 14 new P1 regressions pass. The final scheduler, endpoint
status/lane/isolation/fault/lifecycle, manual-control, shared-PTT, runtime-routing,
station presentation, and QSY regression partition passes 283 tests with 5
intentional platform/environment skips. Python compilation and `git diff
--check` pass. Read-only live checks returned FLRig 2.0.10, PTT off, VFO A, and
valid frequencies on both configured local endpoints. The running FIO process
was not restarted, so the new binary behavior and production Linux QSY remain
explicit external checks. No migration, device write, app restart, or destructive
action was performed during diagnosis or validation.

### P1 follow-up — complete endpoint evidence across liveness and coalescing

After the operator restarted FIO, runtime evidence confirmed that the new
endpoint continuation worked: held QSY operations resumed and applied to the
correct `127.0.0.1:12345` and `127.0.0.1:12346` FLRig endpoints. The remaining
`Applied · verification unavailable` label had two status-lifecycle causes.
FLRig liveness polling replaced a complete post-apply snapshot with one that
omitted the expected JS8 offset, and rapid lane coalescing could suppress the
last successful readback merely because a newer intent was queued or running.
Global process-inventory state could also suppress a valid configured endpoint
probe.

The follow-up makes liveness collect every field in the endpoint's expected
state, including the mapped JS8 offset for an FLRig-controlled schedule. It
probes instantiated endpoint clients according to their persisted backend
without using global process detection as an eligibility gate. The latest
successful readback remains cached while a newer generation is only queued or
running and is replaced only by later successful evidence. When RF readback
matches but JS8 offset evidence is genuinely unavailable, operator wording is
now `RF verified · verify JS8Call` instead of implying that FLRig verification
failed.

Work packages and models: the high-reasoning primary GPT-5 model owned runtime
correlation, concurrency semantics, production changes, specification, and
integration review. `gpt-5.6-terra` (high) independently audited the fallback
path and identified the coalesced-generation and process-inventory hazards.
`gpt-5.6-luna` (high) reproduced the expected-state omission and implemented
focused deterministic regressions. No schema migration, device write,
application restart, or destructive action was performed.

Acceptance evidence: the final scheduler, endpoint status/lane/isolation/fault/
lifecycle, manual-control, shared-PTT, runtime-routing, station presentation,
and QSY partition passes **289 tests with 5 intentional skips**. The focused P1
file passes 20 tests, Python compilation succeeds, and `git diff --check` is
clean. The current FIO process started before this follow-up source change, so
one additional restart and operator confirmation remain the external gate.

## 2026-09-12 — Station Control Bar attention summary

Status: implementation complete and automated exit gate passed. The ambiguous
`! N` indicator is now `ATTN: N` at roomy and compact densities and remains the
short `! N` form only in the most constrained layout. Its accessible name states
the affected radio/source count, and duplicate transitional snapshots do not
inflate that count.

Activating the chip opens a bounded summary with one row per affected source,
the highest-priority reason available from cached PTT, shared-resource,
off-schedule, RF Guard, endpoint, warning, or software-service state, and a
`Review` action that opens Station Health focused on that source. Three rows are
shown before an overflow route; `Open Station Health` always exposes the full
cross-station view. The disclosure path uses only the snapshots and caches
already held by the command bar. It performs no database/configuration read,
process inventory, endpoint/API request, schedule projection, command, worker,
or timer activity.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): interaction contract, cached-data/concurrency boundary, governing
  specification, review and correction of every delegated diff, final
  integration, and exit gate;
- `gpt-5.6-terra`, medium reasoning: bounded chip/menu UI implementation;
- `gpt-5.6-luna`, medium reasoning: focused attention-summary regression tests.

Acceptance evidence: the focused attention and adaptive-shell suite passes 21
tests across one-radio Light/Dark, two-radio-plus-Mesh, three-radio, duplicate,
roomy, compact, condensed, bounded-overflow, focused-navigation, and fail-fast
no-I/O cases. The adjacent presenter, state, navigation-epoch, and shell suite
was also run: 157 tests passed; 10 pre-existing legacy assigned-plan/settings
contract failures remain outside this change and are unchanged by it. Python
compilation and `git diff --check` pass. No migration, external endpoint action,
application restart, or destructive operation was performed.

## 2026-09-12 — FIO Spotter RF summary, Expect ownership, and bulk form dates

Status: implementation complete and automated exit gate passed. Live RF and
operator workflow confirmation remains production-assisted.

FIO Spotter Activity is now explicitly a bounded local-RF workspace. CommStat
payloads received by a configured local JS8Call instance remain JS8 source
records, but render as the `CommStat` form with a compact normalized status and
only meaningful exceptions. The exact receiving radio, application instance,
transport, and raw evidence remain available in detail. Internet-only CommStat
and imported JS8Spotter history are not presented as newly heard Spotter RF
activity; their real provenance remains available to Messages and the shared
projection.

Expect administration now owns the complete automatic-response workflow.
Messages Compose creates or locates a disabled response draft and immediately
opens the fresh rule in FIO Spotter instead of leaving an invisible write. The
Forms page distinguishes `Compose and send` from `Make available by E?`, shows a
concise availability state, and stages new form rules disabled for response and
access review. Reusable-policy administration and request history are collapsed
by default to keep the normal operator path small. None of these surfaces adds
polling, activation-time filesystem scans, or render-path I/O.

The Expect page adds the explicit `Update Expect form dates…` maintenance
action. It performs one bounded preview and confirmation, computes one datecode
for the operation, and atomically replaces or appends the trailing datecode on
eligible static `F!nnn`/`F!nnnA` responses. A response may begin with the form
key or one valid JS8 callsign/group destination followed by the key. Dynamic Q,
free-text, signed, mismatched, malformed, and ambiguous responses are skipped.
The update preserves access, scope, enablement, safety limits, reply history,
and non-date response content, never transmits, and records management audit for
each changed rule.

Work packages and models:

- high-reasoning primary GPT-5 model (exact host runtime submodel identifier not
  exposed): RF provenance and workflow architecture, datecode safety and
  transaction review, specification integration, review/correction of every
  delegated diff, and final acceptance gate;
- `gpt-5.6-terra`, medium reasoning: bounded CommStat projection and focused
  projector tests;
- `gpt-5.6-luna`, high reasoning: pure datecode updater, atomic store operation,
  audit records, and focused store tests;
- `gpt-5.6-luna`, medium reasoning: focused Activity, Forms, Expect deep-link,
  bulk-action, and no-op UI tests.

Primary review widened the safe response-prefix rule to support one real JS8
destination (including custom group punctuation), added immutable import-origin
evidence, excluded imported history from RF Activity, and updated one obsolete
form-discovery regression to the existing background-worker architecture.

Acceptance evidence: the final Spotter projection, store, access-policy, FLAMP
Q, selected-target, JS8 send/runtime/schema, message-ingest, Message
Intelligence, and lazy UI partition passes **356 tests**. The final Python
compilation and `git diff --check` gates pass. No schema migration, RF
transmission, device write, application restart, destructive action, or
Store-and-Forward implementation is part of this correction. Store and Forward
remains a separately gated future package in the updated specifications.

## 2026-09-13 — FIO Spotter operator workflow refinement

Status: implementation complete and automated exit gate passed. Live RF and
production Linux visual confirmation remain operator-assisted.

FIO Spotter now follows the operator's service mental model. Its tabs are
Activity, Watches, Expect, Access Policies, Forms, and Imports. Expect presents
one station service switch and one per-response `Auto reply` state; the three
legacy compatibility flags remain stored but cannot independently broaden
permission. `Saved only` responses remain available for manual use. Bounded
selected-response bulk changes validate usable content and resolved access,
write all compatibility flags together, audit each change, and report skipped
rows. Access Policies has its own responsive workspace, usage counts, bounded
references, and in-use deletion protection.

Expect `Send now…` opens the standard Message Compose guarded-send workflow
with an editable working copy. Compose can also start from the bounded saved
response list or the form catalog. Static non-form responses are supported as
saved messages; dynamic FLAMP Q is request-only. Eligible MCForm date fields
and datecodes are refreshed once on the outgoing copy before signing. The
stored response is unchanged and preview/queued payload share the same final
serializer and timestamp.

Activity can stage an unsaved Watch from a meaningful selected row. Where
available, sender plus normalized topic/status becomes explicit AND criteria.
The additive watch schema preserves legacy rules, prevents normalized
duplicates, and enforces 100-enabled/500-total caps inside the transaction.
Message projection owns runtime evaluation: it reloads a bounded compiled
snapshot at a paced cadence, performs pure matching on already-prepared
candidates off the UI thread, and persists one idempotent batch keyed by watch
and message identity. Watch failures are advisory and cannot fail, retry, or
roll back message projection.

Work packages and models:

- high-reasoning primary GPT-5 model: specification, legacy safety model,
  Access Policies/Expect/Activity integration, projection concurrency,
  additive dedupe schema, review/correction of every delegated diff, related
  specifications/work log, and final integration gate;
- `gpt-5.6-terra` (high): Expect visible-state and bounded bulk APIs,
  validation/audit, policy usage and safe deletion, and focused tests;
- `gpt-5.6-luna` (high): saved-response Compose, guarded manual send, outgoing
  date/signing handling, responsive layout behavior, and focused tests;
- `gpt-5.6-luna` (high): structured watch compiler/store, duplicate and
  concurrent cap enforcement, cached matcher/batch primitives, and focused
  tests.

Primary review broadened Compose from MCForm-only saved entries to every static
Expect response while retaining dynamic-Q exclusion; added an Expect deep-link
intent; separated policy administration; added compact in-memory response
filters and capped `Select shown`; connected the cached matcher to the
background projection completion lane; and made match counting idempotent
across projection retries.

Acceptance evidence: the primary Spotter, Expect, FLAMP-Q, Compose, Message
Intelligence, projection, selected-target, runtime, and responsive UI partition
passes **392 tests**. An adjacent Compose/ingest/projection partition passes
**142 tests**, for **534 passing tests total**. Python compilation and
`git diff --check` pass. No destructive migration, RF transmission, device
write, external endpoint action, application restart, or Store-and-Forward
implementation was performed.

## 2026-09-13 — FIO Spotter editor-first responsive correction

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

Expect and Access Policies now place their selected-item editors above their
full-width saved-item tables. Both editors use balanced columns on wide screens
and stack at compact widths; the page owns vertical overflow and does not gain
a horizontal scrollbar. The tables retain the remaining height. Resizing,
filter typing, and chip geometry changes do not perform store, filesystem,
process, endpoint, or network work.

Watches keeps the useful side-by-side comparison at wide widths while giving
the table the dominant share. Type, pattern, and match mode are one condition
row, Enabled is grouped with actions, and the optional AND condition remains
clear. Compact layouts stack the table and editor, and the editor's size hint
cannot force the page back above its compact breakpoint.

FIO Spotter Compose now reserves a bounded readable setup width (480 px in the
embedded wide view and 520 px in the full workbench), keeps the form/preview
workspace dominant, and stacks at compact widths. `Start from`, category, form,
and guidance controls wrap or use local setup-pane scrolling instead of being
clipped. Splitter sizes are only applied when they materially change, avoiding
geometry feedback churn.

Screenshot follow-up found three gaps in the first automated geometry gate.
The Expect editor still consumed too much high-DPI height, the Watch action row
could absorb unused vertical space, and Access Policy lookup/usage text could
compete inside wrapped form rows. Expect metadata/actions now use single wide
rows; Watches stacks at 1200 px and below and top-packs at wider sizes; and
policy evidence now has a dedicated summary row. Spotter Compose also resets
its local setup scroll to the top only when the operator opens Spotter Compose
or intentionally changes its saved-response/form context. Preview refresh and
ordinary editing preserve the current scroll position.

Work packages and models:

- high-reasoning primary GPT-5 model: responsive architecture, integration,
  delegated-diff review, two-column policy refinement, non-overlapping policy
  summaries, specification/work-log reconciliation, and final exit gate;
- `gpt-5.6-terra` (high): Expect and Access Policy editor-above-list foundation
  plus focused lazy/no-I/O resize regressions;
- `gpt-5.6-luna` (high): compact Watch editor and payload/geometry regressions;
- `gpt-5.6-luna` (high): Compose setup-pane sizing, local overflow, geometry
  coalescing, intentional scroll restoration, and focused tests;
- `gpt-5.6-luna` (high): read-only screenshot-dimension and large-font geometry
  audit across Expect, Watches, Access Policies, and Compose.

Acceptance evidence: 11 screenshot-shaped responsive regressions pass at both
normal and 1.5 high-DPI scale. The expanded primary
Spotter/Expect/FLAMP-Q/Compose/Message Intelligence/projection/runtime/UI suite
passes **403 tests**; the adjacent Compose/ingest/projection suite passes **142
tests**, for **545 passing tests total**. Python compilation and
`git diff --check` pass. No migration, RF transmission, external endpoint
action, device write, application restart, or destructive operation was
performed.

## 2026-09-13 — Project-wide task-oriented workspace design guideline

Status: documentation and governance integration complete. No product code,
schema, migration, runtime data, or device behavior changed.

Created `task_oriented_workspace_design_guideline.md` as the common execution
contract for new and meaningfully redesigned operator-facing workspaces. It
requires a redesign brief before coding, selects a task-appropriate workspace
archetype, establishes a single scan and action hierarchy, defines responsive
reading order and scroll ownership, preserves safety through progressive
disclosure, and makes cache-only render/resize/typing behavior part of the UX
exit gate. It intentionally does not mandate editor-first layout for every
screen: compact editors may sit above lists, while scan-heavy views retain a
dominant table and contextual inspector.

The guideline now makes shared-theme and component reuse mandatory. Redesigned
screens must consume the central palette, application stylesheet, font-scale,
control-sizing, combo-fitting, button-role, LED, splitter, focus, table, chip,
and icon treatments where applicable. Missing semantics are added centrally
before use; screen-local palettes, hard-coded text-bearing metrics, or cloned
component styles require a documented and tested exception. Light/Dark,
Normal/Large Text, selected, disabled, focus, warning, destructive, and high-DPI
states are part of the required acceptance matrix.

The rule is referenced from `AGENTS.md`, `project_delivery_rules.md`, the
multi-rig product/UI contract, UI layout standards, the Operational View
Framework, and the current FIO Spotter workflow specification so future feature
and redesign work discovers it at both governance and implementation layers.

Work packages and models:

- high-reasoning primary GPT-5 model: authority and precedence, guideline
  architecture, shared-theme/component contract, governing cross-references,
  delegated-findings review, final integration review, and exit gate;
- `gpt-5.6-terra` (high): read-only audit of existing UI contracts, precedence,
  missing execution rules, and recommended reference points;
- `gpt-5.6-luna` (high): read-only audit of representative FIO workspaces,
  reusable donor patterns, problem layouts, and screenshot-shaped acceptance
  cases.

Acceptance evidence: governing references and required guideline sections were
verified locally, and `git diff --check` passes. Runtime UI tests were not
required because this slice changes documentation and project governance only.

## 2026-09-13 — Task-oriented guideline applied to FIO Spotter

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

Applied the project-wide task-oriented workspace guideline to Activity,
Watches, Expect, Access Policies, Forms, Imports, and FIO Spotter Compose.
Activity is now a dominant bounded traffic table with a responsive contextual
inspector and selection-aware routes to Inbox, Map, Operator, Reply, and a
reviewed Watch draft. Refresh preserves a selected projection when it still
exists. Watches keeps its table dominant, stacks before the editor becomes
crowded, represents state as `On`/`Off`, and has one normal enabled-state
control rather than a checkbox plus a competing toggle action.

Expect and Access Policies preserve the editor-above-list task sequence. Policy
selection resolves usage from the bounded refresh snapshot rather than issuing
a selection-time store query. Radio scope uses known FIO names in the normal
view while retaining legacy identifiers internally. Forms uses the explicit
sequence `folder → routes → review/use`; selection is cache-only, source-file
reading occurs only on `Preview selected`, and Compose receives the exact
selected form intent. Its status column now shares Expect's operator language.
Imports uses `choose → preview → import` and keeps commit unavailable until the
current source has a valid preview.

FIO Spotter Compose retains a readable bounded setup pane, a dominant form and
preview workspace, local overflow, intentional scroll restoration, and guarded
target/send behavior. Shared `button_style`, `label_style`, combo-fitting,
font-derived control sizing, splitter treatment, application palette, and
Light/Dark theme reapplication replace local screen-specific presentation.
Render, resize, selection, theme, chip layout, and field/filter typing paths
remain free of filesystem, database, process, endpoint, device, and network I/O.

Work packages and models:

- high-reasoning primary GPT-5 model: redesign brief, architecture, semantic
  control/status reconciliation, shared-theme/lazy-theme integration, review
  and correction of all delegated diffs, specifications/work log, and gate;
- `gpt-5.6-terra` (high): Expect and Access Policies implementation plus
  focused layout, cached-selection, theme, and radio-label regressions;
- `gpt-5.6-luna` (high): Activity and Watches implementation plus focused
  selection, action, hierarchy, theme, and responsive regressions;
- `gpt-5.6-terra` (high): Forms, Imports, and Spotter Compose implementation
  plus preview-gate, no-selection-I/O, handoff, and responsive regressions.

Acceptance evidence: **72 focused tests** and **555 expanded integration tests**
pass. All changed Python modules compile and `git diff --check` passes. No
migration, RF transmission, device write, external endpoint action,
application restart, destructive action, or production-data mutation occurred.

## 2026-09-13 — Policy-first Expect and Compose View workflow

Status: implementation complete and automated exit gate passed. Production
Linux visual and live-RF confirmation remain operator-assisted.

Expect now presents compact text-bearing chips for the station service and
FLAMP-Q index, one named Access Policy, one `Saved only` / `Auto reply on`
choice, and collapsed radio/rate-limit Options. New automation requires an
enabled named policy. Existing active inline-access rules remain compatible and
unchanged; inactive legacy rules cannot be newly activated without a policy.
Legacy access is disclosed only on applicable records and may be copied into an
unsaved named-policy draft for deliberate review and assignment.

Expect now owns MCF response discovery. `New response…` routes MCF creation into
the shared Compose implementation. `View` opens an exact stored response in a
read-only Compose state with explicit `Edit working copy` and `Back to Expect`.
Viewing performs no date refresh or write. Explicit saving of an existing
working copy preserves policy, access, routing, and automation metadata; new
MCF entries return as Saved only. Send Now continues through the existing
guarded JS8 path and refreshes eligible dates only on the outgoing copy.

The shared theme now explicitly styles line, combo, numeric, decimal, date,
time, and date/time editors—including focused, selected, enabled, disabled,
suffix, and step-control states—so Dark theme does not lose Max replies or
Cooldown values. Render, resize, selection, filter/field typing, chip detail,
and View/Edit transitions remain I/O-free.

Work packages and models:

- high-reasoning primary GPT-5 model: architecture and compatibility boundary,
  store/API policy gate, read-only View semantics, shared-theme integration,
  review/correction of every delegated diff, specs/work log, and final gate;
- `gpt-5.6-terra` (high): policy-first Expect editor, compact service/status
  chips, progressive Options/legacy disclosure, shared control styles, and
  focused UI tests;
- `gpt-5.6-luna` (high): Expect/Compose View and Create handoff, exact stored-
  payload behavior, Back/Save workflow, metadata preservation, and focused
  Compose tests; and
- `gpt-5.6-luna` (high): independent focused acceptance coverage for policy,
  compatibility, theme, navigation, persistence boundaries, and cache-only UI
  behavior.

Acceptance evidence: **95 focused tests** and **557 expanded integration tests**
pass. All changed Python modules compile and `git diff --check` passes. No
migration, RF transmission, device write, external endpoint action,
application restart, destructive action, or production-data mutation occurred.

## 2026-09-13 — Expect reply comfort and all-mode Compose alignment

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

The Expect saved-response editor now gives Reply a bounded multiline surface.
E? Token, named Access Policy, resolved policy summary, and delivery mode form
one inline wide scan row, wrap 2×2 at medium width, and stack in task order at
compact/Large Text widths. Responsive transitions preserve editor state,
release stale scroll-area width, and perform geometry-only work.

Compose now checks font/control-derived readable width before placing the setup
surface beside the mode-specific work surface. FLMsg/FLAmp, FIOSpotter, and
CommStat RF stack before either side is starved; JS8Call retains its direct
workflow. The live JS8Call-target explanation occupies a full-width row with
`Use Target` and `Refresh Target` beneath it, eliminating the reported vertical
letter-by-letter rendering. Fields and exact preview use a clear vertical scan
path, while the separate radio-guidance cue remains one concise line with full
Why text in its tooltip. Compose muted/information text uses shared theme
roles. No payload, policy, persistence, staging, signing, RF Guard, busy/PTT,
or guarded-send behavior changed.

Work packages and models:

- `GPT-5` high-reasoning primary (deployment identifier not exposed): UI
  architecture, FSW-7/CMW-6 redesign briefs, performance and persistence
  boundary, review/correction of all delegated diffs, specs/work log, and exit
  gate;
- `gpt-5.6-terra` (high): Expect metadata/reply implementation and focused
  tests;
- `gpt-5.6-luna` (high): all-mode Compose responsive implementation,
  shared-theme correction, and focused tests; and
- `gpt-5.6-luna` (medium): independent responsive geometry,
  draft-preservation, numeric-control, and cache-only acceptance coverage.

Acceptance evidence: **133 focused Expect/Compose UI tests** and **564 expanded
Compose, Spotter, Expect, JS8 integration, message-ingest, Message
Intelligence, projection, and responsive UI tests** pass. Changed Python files
compile and `git diff --check` passes. No migration, RF transmission, device
write, external endpoint action, application restart, destructive action, or
production-data mutation occurred.

## 2026-09-13 — CommStat density and feedback-banner shell stability

Status: implementation complete and automated exit gate passed. Production
Linux full-screen visual confirmation remains operator-assisted.

The CommStat StatRep content previously advertised a fixed 240-pixel minimum
even though its six-row, twelve-selector condition matrix needs approximately
328 pixels at the normal development font. Qt therefore compressed selectors
below readable height when Compose divided the available vertical space. Core,
brevity, and status controls now derive their floors from the shared font/control
helpers, status labels retain a single readable line, and the scroll child
publishes its active layout minimum. Compact layouts scroll locally instead of
overlapping or clipping fields.

The transient action-feedback banner also exposed stale Linux sibling geometry:
after `Settings saved, but…` auto-hid, the fixed-height Station Control Bar could
retain the compressed allocation and stale paint. The bar now uses its natural
minimum vertical policy. Banner visibility transitions enqueue one coalesced,
cache-only geometry flush that republishes the current responsive arrangement,
activates the existing parent layout, and repaints. It performs no station
refresh, configuration/database read, endpoint work, or repeating timer loop.

Work packages and models:

- `GPT-5` high-reasoning primary (deployment identifier not exposed): root-cause
  analysis, CMW-7/shell contract, banner lifecycle implementation, delegated-
  diff review and test correction, final integration, specs/work log, and gate;
- `gpt-5.6-terra` (high): CommStat font-derived geometry implementation and
  wide/compact/Large Text focused test; and
- `gpt-5.6-luna` (medium): feedback-banner auto-hide/control-bar regression test
  scaffold, reviewed and strengthened for repeated full-screen, compact, and
  Large Text transitions by the primary model.

Acceptance evidence: **11 focused tests** and **366 expanded Compose, Spotter,
Settings, and Station-shell tests** pass. Changed Python files compile and
`git diff --check` passes. No migration, RF transmission, device write, endpoint
action, application restart, destructive action, or production-data mutation
occurred.

## 2026-09-13 — UI tab conformance UIA-2

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

The application shell, Ops, Messages/Compose, Map, Managed BBS, and FIO Spotter
now conform to the shared font-derived geometry and theme authority for this
slice. Ops rows/headers and semantic status treatments scale with the active
font. Map filters reflow from measured control widths, and generated detail and
marker HTML uses shared theme roles. Compose stacks ordinary form controls,
keeps horizontal scrolling off its setup surface, preserves draft/scroll state,
and settles without height ratcheting. Managed BBS uses visible shared splitter
handles and font-derived chips; BBS and Spotter retain cache-only snapshot and
selection behavior.

Work packages and models:

- high-reasoning primary GPT-5: architecture, shell/shared-theme work,
  breakpoint and contrast corrections, review of every delegated diff,
  integration, specifications/work log, and exit gate;
- `gpt-5.6-terra` (high): Ops and Map UI implementation and focused tests;
- `gpt-5.6-luna` (high): Messages/Compose implementation and focused tests;
- `gpt-5.6-terra` (high): BBS/Spotter implementation, Map semantic-theme
  follow-up, and focused tests.

Acceptance evidence: **207 integrated UIA-2 tests** pass. The Map regression
suite passes **243 tests**, the BBS/Spotter suite passes **39 tests**, and the
narrow Expect check passes ten repeated runs. The audit reports no finding for
any UIA-2 owning file and the repository backlog is now **22 hard findings and
48 candidates** for later gated slices. Changed Python modules compile and
`git diff --check` passes. No migration, RF transmission, device write,
endpoint action, application restart, destructive action, or production-data
mutation occurred.

## 2026-09-13 — UI tab conformance UIA-3

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

JS8, FLDigi/SSB, and VHF/UHF net-control workspaces now reflow from live font
and control measurements, retain one page-level vertical scroll owner, and keep
their operational actions and dominant roster/list surfaces usable at the audit
viewports. FLDigi macro mapping no longer requires an oversized dialog. HF and
local operator history, Local Callsigns, and Local Reports now use shared theme
roles, font-derived rows/controls/detail areas, responsive control bands, and
compact dialogs with reachable action footers. Resize and theme paths are
layout/cache-only and preserve the current draft, selection, and detail.

Primary review corrected delegated magic breakpoints and theme-foreground
guesses by adding a shared content-measured horizontal-layout breakpoint and
using the shared filled-surface contrast helper. It also restored count-chip
theme refresh and reconciled stale test doubles/static responsiveness checks
with the already-implemented receiver-control and bounded-worker contracts.

Work packages and models:

- high-reasoning primary GPT-5: architecture, shared primitives, review and
  correction of all delegated diffs, integration, specs/work log, and gate;
- `gpt-5.6-terra` (high): JS8 and VHF/UHF NCS UI plus focused tests;
- `gpt-5.6-luna` (high): FLDigi/SSB NCS and macro dialog UI plus focused tests;
- `gpt-5.6-terra` (high): HF/Local Callsigns and Local Reports UI plus focused
  tests.

Acceptance evidence: **247 combined UIA-3/regression tests pass with 1 skipped**;
the scanner/harness suite adds **16 passing tests**. No UIA-3 owning file remains
in the static findings, and the repository backlog is **22 hard findings and 34
candidates** for UIA-4/UIA-5. Changed Python files compile and `git diff --check`
passes. No migration, RF transmission, device write, endpoint action,
application restart, destructive action, or production-data mutation occurred.

## 2026-09-13 — UI tab conformance UIA-4

Status: implementation complete and automated exit gate passed. Production
Linux visual confirmation remains operator-assisted.

Resources, planning/schedule/SOP, Settings and Software Administration now use
font-derived multiline and visible-row geometry, semantic shared-theme roles,
and compact task-order reflow. Resource filters and selection render from a
coherent off-thread catalog snapshot; stale generations are discarded and
queued work is coalesced. Settings theme repaint is explicitly cache-only and
cannot schedule dependency probes. Ordinary form surfaces retain one vertical
scroll owner while intentionally wide data surfaces keep local scrolling.

Primary review corrected schedule table clamps, active-theme use in peer
schedule validation, Resources request coalescing, Software filled-surface
contrast and strip sizing, and remaining UIA-4 static classifications.

Work packages and models:

- high-reasoning primary GPT-5: architecture/concurrency, shared primitives,
  delegated-diff review and corrections, integration, docs and exit gate;
- `gpt-5.6-terra` (high): Resources workspace and focused tests;
- `gpt-5.6-luna` (high): planning, SOP and schedules plus focused tests; and
- `gpt-5.6-terra` (high): Settings/Software mechanical conformance and focused
  tests.

Acceptance evidence: **462 integrated tests pass with 19 platform skips**; a
corrected planning subset adds **30 passing tests**; the Local Nets 1,000-row
warm projection budget passes independently. UIA-4 owning files have no scanner
finding; the repository remainder is **7 hard findings and 0 candidates** for
UIA-5. Changed Python modules compile and `git diff --check` passes. No
migration, RF transmission, device write, endpoint action, application restart,
destructive action, or production-data mutation occurred.

## 2026-09-13 — UI tab conformance UIA-5 and final integration

Status: implementation and automated exit gate complete. Native Linux visual
qualification remains operator-assisted.

Shortwave, Help, Logs, startup splash, HF subscription and remaining secondary
dialogs now complete the repository-wide font-derived geometry, shared-theme,
responsive reflow and scroll-ownership program. Shortwave keeps its source and
broadcast insight readable at compact widths. Help publishes one coherent
background-loaded document snapshot and navigates it without repeated file
reads. Logs performs a bounded off-thread tail read, coalesces refresh requests,
retains the prior coherent view on failure, and keeps search/theme/resize paths
cache-only. Splash and dialog geometry now follow application typography rather
than pixel-era assumptions.

Primary review corrected four material issues before acceptance: delegated Log
rendering still reread the complete file on the GUI thread; Help topic changes
still touched the filesystem; splash text was scaled twice; and a lazy widget
released via `deleteLater()` could destroy its child QThread before the snapshot
controller stopped it. The corrected shared worker fences callbacks and shuts
down first. Test reconciliation also preserved per-source JS8 provenance, used
non-expiring relative ingest timestamps, consumed the scheduler's published
asynchronous cache rather than forcing GUI-thread projection work, and assigned
the required VarAC instance before editing its radio-scoped settings.

Work packages and models:

- high-reasoning primary GPT-5: architecture, concurrency and lifecycle review,
  shared primitives, delegated-diff correction, integration, specifications/work
  log and final exit gate;
- `gpt-5.6-terra` (high): Shortwave and HF subscription implementation and
  focused tests;
- `gpt-5.6-luna` (high): Help, Logs and startup splash implementation and
  focused tests;
- `gpt-5.6-terra` (high): optional/lazy coverage manifest and scheduler
  cache-fixture reconciliation; and
- `gpt-5.6-luna` (high): full-suite stale-fixture reconciliation for JS8 source
  identity, ingest age, scheduler projection and VarAC assignment.

Acceptance evidence: **291 UIA-5 changed-surface/integration tests**, **24
scanner/harness tests**, and **132 Phase 7/native-construction stress tests**
pass. The complete inventory also passes in bounded process-isolated shards:
**1,090 passed/2 skipped**, **5 passed** for the Local Nets release file
(including its unchanged 50 ms p95 benchmark), **1,107 passed**, **814 passed/7
skipped**, **461 passed/27 skipped**, and **220 passed/1 skipped**. The
repository semantic scanner reports **0 hard findings and 0 candidates**.
Changed Python files compile and `git diff --check` passes.

The shard boundary is test-process isolation only: no assertion is disabled or
relaxed. A monolithic macOS offscreen PySide run accumulates legacy native Qt
state across thousands of tests and can abort inside Qt even though each owning
file passes from a clean process. No migration, RF transmission, radio/device
write, endpoint action, application restart, destructive action, or production
data mutation occurred.

## 2026-09-13 — First-render and Map activation stability remediation

Status: implementation and automated gate complete; Linux/Windows production
visual and Windows multi-monitor qualification remain operator-assisted.

- Replaced the main page stack and Messages Inbox/Compose mode stack with the
  active-page geometry primitive so hidden tall pages cannot expand or distort
  the visible workspace.
- Made deferred-page replacement atomic and added one navigation-generation-
  fenced first-visible layout settlement, eliminating the transient adjacent
  page behind the recurring swipe/vanish symptom.
- Reworked Map resize handling into one coalesced cache-only pass. Filter grids
  and splitters now skip unchanged resolved geometry instead of repeatedly
  resizing the native WebEngine surface.
- Added a bounded Leaflet post-layout `invalidateSize(false)` contract keyed by
  page generation and real viewport size; it does not reload HTML or data.
- Replaced the Windows native child-view warm-up with page-only WebEngine
  warm-up so preheating cannot move, resize, or reassign the FIO top-level
  window to another monitor.

Work packages: `gpt-5.6-sol` (high reasoning) owned lifecycle architecture,
cross-platform implementation, integration and documentation;
`gpt-5.6-terra` (high) audited/tested Map activation and geometry;
`gpt-5.6-luna` (high) reproduced/tested hidden-page first-render geometry; and
`gpt-5.6-terra` (high) independently audited responsive re-entry paths. Primary
review accepted both delegated test diffs and added the unchanged-drawer
splitter regression.

Acceptance evidence: **336 tests** pass for first-render, Map, current-page,
Phase 7 shell and related Map behavior; **52 tests** pass for the UI lifecycle,
design-control, theme and feedback geometry gate; **36 tests** pass for the
multi-rig main-shell gate; and **306 tests** pass for the high-use
Messages/Compose/Spotter/Map regression gate. Changed Python files compile and `git diff --check`
passes. No migration, runtime configuration/data write, RF/device command,
commit, or push occurred.

### P1 follow-up after continued Map swipe report

The first remediation removed adjacent-page exposure and Map resize churn but
left the primary stack willing to propagate the active Map page's transient
native minimum-size hint. A focused Qt reproduction switched a shown 900x560
window to a synthetic 2400x1800 Map-like page and observed top-level growth to
2418x1818. The primary stack now uses `QSizePolicy.Ignored` on both axes, which
preserves the 900x560 window while layout stretch continues to fill the
available workspace.

The active runtime log provided a second causal sequence: cold Map page load
emitted `ApplicationInactive`, Map was paused while loading, `loadFinished`
deferred its update, and `ApplicationActive` triggered a second Map render and
visible refresh. Inactive state now has a 1.5-second settlement grace period.
Transient WebEngine focus/surface events do not change the settled application
state or child lifecycle; sustained inactivity still pauses and explicit
hidden/suspended states pause immediately.

The `gpt-5.6-sol` high-reasoning primary owned lifecycle design and production
changes. `gpt-5.6-terra` (high) independently reproduced the top-level size-hint
failure and added focused regressions; `gpt-5.6-luna` (high) traced every outer
and inner loading-stack transition; and a second `gpt-5.6-terra` (high) audit
confirmed Map splitter re-entry was already bounded. Primary review corrected
the delegated top-level test so it applies the production shell policy rather
than testing an intentionally unconstrained generic stack.

Follow-up acceptance: **342 first-render/Map/shell tests**, **88 combined UI
lifecycle/design-control and multi-rig shell tests**, and **307 high-use
Messages/Compose/Spotter/Map tests** pass. Native Linux and Windows multi-monitor
confirmation remains operator-assisted.

### P1 follow-up from macOS Map screen recording

The 19:51 macOS recording and matching runtime events isolated a native
WebEngine cold-start focus transition rather than another primary-stack resize:
after Map was selected, FIO's full-screen Space slid away to the Terminal
desktop while the helper process started, then returned to the still-full-screen
FIO Map. The earlier inactive-state grace correctly suppressed duplicate child
pause/resume work, but it could not prevent the operating-system Space
animation.

The existing page-only WebEngine prewarm is now enabled by default on macOS as
well as Windows. It is initiated during shell startup, creates no native view,
and first Map navigation is deferred if the one-time warmup has not completed.
Linux remains explicitly configurable. Warm Map re-entry was separately made
idempotent: visibility and focus changes no longer invent dirty data, clean
re-entry reuses the live page, and routine refresh status remains in a single
font-derived compact strip rather than expanding and collapsing the map
viewport.

Work packages and models: `gpt-5.6-sol` (high-reasoning primary) owned video/log
correlation, lifecycle architecture, production integration, delegated review,
specifications and exit gates; `gpt-5.6-terra` (high) performed frame-level
recording review; `gpt-5.6-luna` (high) traced Map visibility/refresh callbacks;
and `gpt-5.6-terra` (high) audited native WebEngine geometry and cross-platform
prewarm safety. The primary reviewed all findings and made the production/test
changes directly; delegated packages made no production edits.

Focused acceptance adds platform-default prewarm, clean warm re-entry,
inactivity-without-dirtying, compact live-refresh status, application-state and
geometry checks. The integrated Map/shell gate passes **350 tests**, and the
high-use Messages/Compose/Spotter/Map gate passes **331 tests**. Native
design-control, theme, responsiveness and feedback checks add **47 passing
tests**. Native macOS/Linux/Windows qualification remains operator-assisted. No
migration, runtime configuration/data write, RF/device command, commit, or push
occurred.

### P1 follow-up — final-parent native surface and first-painted-map gate

The subsequent recording/report showed that page-only process warm-up was not
the final defect: the native WebEngine surface was still constructed from the
Map visibility callback before the queued first-visible splitter settlement.
The browser could therefore attach while its canvas still had provisional
geometry, producing the remaining full-screen swipe/bounce and lower-left
compositor-origin appearance. The active local launch was confirmed to use
`/Users/bill/RadioCode/runtime/multi-rig` through `start-multi-rig.sh`; matching
runtime events showed the first WebEngine attachment still emitted a transient
`ApplicationInactive`, while the lifecycle grace correctly prevented a second
pause/resume render.

Map activation now completes its cache-only first-visible page/layout/splitter
settlement synchronously before publishing Map visibility. Native construction
is bounded until the Map canvas has positive final geometry. The real
`QWebEngineView` is created in its permanent stack parent, remains current for
the entire cold load, and is covered by an opaque shared-theme Qt loading
surface. It is no longer switched back to a hidden loading page, explicitly
resized, or allowed to take focus while its compositor attaches. The loading
surface is released only after page load, nonzero canvas and WebEngine geometry,
the first actual map payload, and a page-owned two-animation-frame callback.
One-time geometry/state events were added so any remaining platform issue can be
correlated without changing window placement or polling native state.

Work packages and models: `gpt-5.6-sol` (high-reasoning primary) owned runtime
correlation, native-surface lifecycle architecture, production integration,
delegated diff review, specifications and the exit-gate decision;
`gpt-5.6-terra` (high) performed the widget-construction and callback-order
audit; `gpt-5.6-luna` (high) added the focused geometry, reveal-gate and overlay
regressions; and `gpt-5.6-terra` (high) independently reviewed the Qt 6.8
WebEngine visibility/render contracts and platform-safe lifecycle options. The
primary accepted the delegated test approach, corrected its synthetic focus
fixture after adding the pre-reveal focus fence, and made all production edits.

Acceptance evidence: **351 Map/first-render/current-page/Phase 7 shell tests**,
**208 high-use Messages/Compose/Spotter/Map tests**, **51 shared-theme,
font-derived geometry, lifecycle and design-control tests**, and **179 multi-rig
shell tests** pass. Changed production/test Python files compile and
`git diff --check` passes. The automated implementation gate is complete;
native full-screen and secondary-monitor confirmation on macOS, Linux and
Windows remains operator-assisted. No migration, configuration/data write,
RF/device command, application restart, commit, or push occurred.

### P1 follow-up — geometry-quiescence gate after full-screen collapse evidence

The restarted local runtime proved the final-parent implementation was active
and captured the remaining failure precisely. `webview_created` recorded a
1208x545 browser inside a 1470x923 full-screen window; one second later
`surface_revealed` recorded only 508x327, with no intervening application-
inactive transition. The residual swipe, apparent minimize toward the left and
half-screen return therefore correlated with a late internal geometry
negotiation after native attachment, rather than duplicate navigation or the
already-fenced focus lifecycle.

Cold Map activation now has three bounded quiescence barriers: before native
WebEngine construction, after attachment and before page load, and immediately
before revealing the painted map. Each barrier requires the complete relevant
geometry signature to be unchanged for two samples and at least 150 ms;
resize/responsive reflow invalidates a pending settlement. The Map stack and
browser also ignore dynamic WebEngine content size hints, leaving the splitter
as the sole viewport-size owner. Construction telemetry records the top-level
rectangle/state/screen immediately before and after native attachment; reveal
telemetry records those fields again with final canvas size. Any further
window-manager transition can therefore be distinguished from internal Map
layout without changing geometry.

Work packages and models: `gpt-5.6-sol` (high-reasoning primary) owned runtime
correlation, quiescence/concurrency architecture, production integration,
delegated review, specification and final gate; `gpt-5.6-terra` (high) audited
the native-surface lifecycle and isolated the post-creation geometry collapse;
`gpt-5.6-luna` (high) added normal/maximized/full-screen preservation tests and
the focused quiescence regressions; and `gpt-5.6-terra` (high) reviewed official
Qt 6.8 Cocoa/WebEngine behavior and confirmed that no supported child-view API
requires or authorizes a top-level state change.

Acceptance evidence: **357 Map/first-render/current-page/Phase 7 shell tests**,
**82 focused high-use Messages/Compose/Spotter/Map tests**, **45 shared-theme,
font-derived geometry, lifecycle and design-control tests**, and **135 multi-rig
shell tests** pass. The delegated quiescence file contributes **20 passing
tests** and is included in the Map gate. A combined long-lived offscreen Qt run
accumulated native Qt state and aborted in an unrelated Logs construction test;
the same Phase 7 tests pass in the clean Map process and the multi-rig files pass
in their clean 135-test process. Changed Python files compile and `git diff
--check` passes. Native Linux/Windows/macOS full-screen and multi-monitor
confirmation remains operator-assisted. No migration, runtime configuration or
data write, RF/device command, application restart, commit, or push occurred.

### P1 follow-up — isolate macOS cold navigation until payload-ready presentation

The 21:00 macOS recording and its matching runtime telemetry separated native
attachment from first page navigation. `webview_created` preserved the
1470x923 full-screen top-level window, its screen and the nonzero final Map
canvas. The top-level window remained unchanged until the first visible
WebEngine navigation; at `surface_revealed` it had become a 670x761 normal
window at the upper-left of the same screen. There was no application-inactive
event during that interval and the Map render itself consumed only about 59 ms.
The prior geometry barrier was therefore waiting for an already-normalized
state rather than preventing the platform transition.

On macOS, only the first WebEngine page load is now isolated as a non-current
child of its permanent, fully sized Map stack. The stable shared-theme
`Preparing map` page remains current while Chromium navigates and the first
real payload is applied. FIO then makes the loaded page current exactly once
behind the opaque loading overlay, performs no top-level geometry/state/screen
operation, waits for the post-presentation geometry barrier and two page-owned
animation frames, and only then removes the overlay and enables focus. Hidden
cold pages also skip Leaflet viewport invalidation until presentation. Warm
reloads retain the already-visible Map and do not use this isolation path.

Work packages and models: `gpt-5.6-sol` (high-reasoning primary) owned the
recording/log correlation, platform lifecycle architecture, production code,
specification, delegated-diff review and final integration; `gpt-5.6-luna`
(high) added the bounded cold-load, exactly-once presentation, top-level
invariant and warm-reload regression package. An earlier delegated video-only
review was stopped once the runtime telemetry established the causal interval,
to contain further cost. Primary review added the hidden-page viewport fence
regression alongside the production guard.

Acceptance evidence: the lifecycle file contributes **24 passing tests**; the
integrated Map/first-render/current-page/Phase 7 shell gate passes **361 tests**;
the high-use Messages/Compose/Spotter/Map gate passes **82 tests**; the
shared-theme, font-derived geometry, lifecycle and design-control gate passes
**45 tests**; and the multi-rig shell gate passes **135 tests** in clean
process-isolated shards. A combined 135-test macOS offscreen Qt process
completed every assertion but exited 139 during accumulated native Qt
interpreter teardown; all owning shards then passed and exited normally.
Changed Python files compile and `git diff --check` passes. Native macOS
full-screen confirmation remains operator-assisted; Linux and Windows Map
first-load/multi-monitor qualification remains required. No migration, runtime
configuration or data write, RF/device command, application restart, commit,
or push occurred.

### P1 follow-up — detached Map document and single presentation state

The 21:29 macOS recording is a failed native acceptance gate and supersedes the
prior conclusion that a non-current `QWebEngineView` was sufficient isolation.
Matching runtime telemetry showed `webview_created` preserving the 1470x923
Built-in Retina full-screen window, followed immediately after the real document
navigation by `page_load_finished` with the same rectangle and screen but
`WindowNoState`. The Map data path was healthy: the render completed with 328
markers and reached Ready. The retained `Refreshing All Stations` surface was a
second failure: competing `attached` and `postload_present` callbacks repeatedly
reset the same geometry-quiescence tracker, so the overlay could never be
released. Existing synthetic QWidget tests did not exercise either native page
navigation or the competing presentation phases and therefore produced a false
green gate.

The macOS cold path now loads the real document on a retained, page-only
`QWebEnginePage` that is not attached to the native view. After load success and
first-payload application, FIO attaches that prepared page to the permanent Map
view and makes the view current exactly once behind the opaque loading surface.
One generation-fenced `present` state owns final geometry settlement and the
two-animation-frame page acknowledgement; stale preparation, resize,
JavaScript, title and reveal callbacks are no-ops. A bounded eight-second
deadline replaces a stuck loading surface with calm retry guidance without
moving, resizing, normalizing, activating, or otherwise manipulating the main
window. Warm Map reloads continue to reuse the attached page.

Work packages and models: `gpt-5.6-sol` (extra-high/high-reasoning primary)
owned the causal telemetry review, concurrency and lifecycle architecture,
specification correction, production integration, delegated-diff review and
final gate; `gpt-5.6-sol` (extra-high forensic delegate) independently
correlated the full-screen state loss and quiescence loop; and `gpt-5.6-luna`
(high) analyzed the recording timeline and added detached-navigation,
exactly-once attachment, stale-generation, timeout, post-presentation resize
and warm-path regressions. The primary reviewed and accepted every delegated
change.

Acceptance evidence: the focused Map lifecycle file passes **30 tests**; the
integrated Map/first-render/current-page/Phase 7 shell gate passes **367 tests**;
the high-use Messages/Compose/Spotter/Map gate passes **82 tests**; the
shared-theme, font-derived geometry, lifecycle and design-control gate passes
**45 tests**; and the multi-rig shell gate passes **135 tests** in clean,
process-isolated shards. Changed production/test Python files compile and `git
diff --check` passes. The automated exit gate is complete; native macOS
full-screen confirmation of this corrected page-only load remains
operator-assisted, followed by Linux and Windows first-load/multi-monitor
qualification. No migration, runtime configuration or data write, RF/device
command, application restart, commit, or push occurred.

### P1 recovery — restore the proven direct Map lifecycle

Native macOS qualification of the detached-page build failed, and the same
build then failed on Linux. The automated gate was therefore a false green and
the detached document/presentation design is rejected. Review against commit
`ff967a0` and the single-rig tree at `/Users/bill/Radio/FreqInOut` confirmed the
stable recovery boundary: one persistent `QWebEngineView` in the Map stack,
direct `setUrl`/`setHtml` navigation, and immediate stack selection when
`loadFinished` succeeds. The recovery removes the detached page, `setPage`
presentation handoff, opaque loading overlay, geometry-quiescence state
machine, reveal generations/deadline, and repeated Leaflet viewport-settlement
callbacks. It also removes the Map-specific synchronous main-shell layout pass.

Windows retains its previously proven offscreen `QWebEngineView` startup
warm-up. macOS and Linux no longer prewarm WebEngine by default. The retained
shutdown-only page replacement matches both stable baselines and is not part of
navigation; regression coverage forbids `setPage` within the construction and
live-load path, rejects the removed lifecycle symbols globally, and verifies
direct navigation, immediate successful presentation, warm view reuse, and no
top-level move/resize/state operation.

Work packages and models: the `gpt-5.6-sol` high-reasoning primary owned the
failed-gate decision, architecture boundary, baseline comparison review,
production integration, specifications, delegated-diff review, acceptance and
push; `gpt-5.6-luna` (high) independently compared the failed implementation
with `ff967a0` and the single-rig lifecycle; `gpt-5.6-terra` (high) removed the
rejected lifecycle machinery without changing Map intelligence or data/UI
features; and `gpt-5.6-luna` (high) replaced the misleading synthetic lifecycle
tests with direct-flow regressions. Primary review retained the stable
shutdown-only `QWebEnginePage` use and narrowed the delegated source guard to
the live navigation/construction region.

Acceptance evidence: the direct lifecycle file passes **12 tests**; the
integrated Map/first-render/current-page/Phase 7 shell gate passes **349 tests**;
the focused high-use Messages/Compose/Spotter/Map gate passes **82 tests**; the
shared-theme, font-derived geometry, lifecycle and design-control gate passes
**45 tests**; and the multi-rig shell gate passes **135 tests** in clean,
process-isolated shards. Changed production/test Python files compile and `git
diff --check` passes. This recovery is committed and pushed to the private
testing branch for native Linux/macOS qualification. No migration, runtime
configuration/data write, RF/device command, or application restart occurred.
The approved persistent nonmodal Map-window design remains the next slice and
will not begin until this recovery build's native gate is confirmed.
