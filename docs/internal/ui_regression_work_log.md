# UI Regression Work Log

## 2026-09-29 — Per-radio launch control gate

Status: `Awaiting maintainer pass approval`

Governing specification:
`per_radio_launch_control_gate_hotfix_spec.md`.

Private implementation commit: `a4cefb1` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

Launch Control now treats each active radio as a bounded startup stage. Radios
are ordered by their existing display order and stable ID, dependencies are
ordered inside the owning radio, and a configured FLRig or JS8Call control
application is placed first when it is included in that radio's startup
bundle. RigCtlD and already-running control applications use the same gate
without launching a duplicate.

Before any remaining application for a radio starts, FIO requests fresh,
endpoint-scoped control evidence through the existing dependency-status
worker. The exact configured endpoint must return a positive read-only
frequency: FLRig uses `rig.get_vfo`, RigCtlD uses Hamlib `f`, and JS8Call uses
its native endpoint-scoped frequency request. A responsive application API by
itself is not sufficient evidence that the physical radio is available.

If the control application or radio readback is unavailable within the bounded
timeout, the remaining applications for that radio are recorded as
`blocked_radio_control`; startup then continues with the next radio. Manual and
observer profiles retain an explicit bypass. Existing process attribution,
endpoint duplicate prevention, JS8 identity handling, cancellation, and
selected-radio **Start Startup Apps** behavior remain in force. The final
launch summary reports radio-control skips separately.

No schema, migration, persistent setting, launch-bundle, or saved radio-profile
change was made. Control metadata exists only in the transient launch plan.
The UI continues to consume immutable cached status; the new radio readback is
performed only by the existing endpoint worker and never on the Qt GUI thread.

Work-package ownership:

- primary `gpt-6-astra` (high reasoning) owned the specification, launch-plan
  and orchestrator state machine, cache/worker integration, focused tests,
  regression review, and final integration;
- no delegated code package was used because the change is a tightly coupled
  launch-state correction across the planner, worker snapshot, and executor.

Acceptance evidence:

- focused per-radio gate and exact FLRig/RigCtlD/JS8 endpoint-readback tests:
  **8 passed**;
- launch identity, bundle isolation, dependency, JS8/VarAC, guided launch,
  status/cache, Windows subprocess, and startup-surface partition:
  **229 passed, 4 platform skips**;
- changed Python compilation and `git diff --check`: pass;
- the monolithic whole-suite run reached an existing macOS Qt/native test
  isolation crash in `test_compose_workbench_acceptance`; that individual test
  passes alone, and no changed launch/status file appears in its failure path.

Maintainer pass gate: configure startup applications for two radios, with the
first radio powered off and the second powered on. Start the station and
confirm only the first radio's configured control application is attempted,
its remaining applications are skipped, and the second radio's complete stack
then starts in order. Power on the first radio and use that radio's **Start
Startup Apps** action; confirm its control readback succeeds before its
remaining applications launch. Repeat once with both radios powered on and
confirm heavy applications from the two radio stacks never start in parallel.

## 2026-09-29 — Radio operational health/cache correction

Status: `Approved—queued for next point release`

Governing specification:
`radio_operational_health_cache_hotfix_spec.md`.

Private implementation commit: `7ad67f1` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

Maintainer approval: approved on 2026-09-29 after private WIP testing. This
queues the correction for the next accumulated point release and does not
authorize an individual public push.

The Station Control Bar and Station Overview now use the scheduler's existing
immutable, endpoint-scoped operational summaries as their shared source of
truth. Cache-only runtime snapshots preserve missing evidence as neutral
`control_ready=None` instead of manufacturing an unavailable warning. Verified
RF operation is green, stale or absent evidence is neutral, and only an actual
operational impairment—such as endpoint backoff/unavailability or readback
mismatch—turns the selected radio yellow.

Application and setup imperfections remain visible as advisories in Health
Details without changing a working radio's aggregate color. An explicit empty
radio-scoped service cache no longer falls back to station-global process
status, so one endpoint cannot inherit another endpoint's condition. Explicit
operator radio selection is also stable and cannot be immediately displaced
by another radio's attention score.

No endpoint polling, process scanning, database access, settings access,
schema, migration, or persistent-setting change was added. The command bar and
overview remain cache-only. The separately proposed per-radio gated startup
sequence is explicitly outside this hotfix.

Acceptance evidence:

- endpoint lifecycle/cache partition: **30 passed**;
- command-bar and Station Overview regression partition: **48 passed**;
- station-control cache-only source-contract partition: **2 passed**;
- receiver-lane and multi-rig runtime compatibility partition: **17 passed**;
- state-matrix coverage verifies green, neutral, yellow, endpoint isolation,
  stale/unknown handling, and JS8 advisory behavior;
- explicit empty radio-cache regression verifies no global fallback;
- changed Python compilation and `git diff --check`: pass.

Test-harness note: a combined run of several Qt-heavy files encountered the
existing macOS native Qt abort in `NetScheduleTab` construction. The affected
status/cache partitions pass independently and the abort occurs outside the
changed surfaces.

Maintainer pass gate: run two configured radios with one endpoint working and
one endpoint offline or deliberately stalled. Confirm the working radio shows
**Operational**/green, the failed radio alone shows **Review**/yellow, and an
optional stopped helper remains an advisory without changing green. Then stop
or invalidate current endpoint evidence and confirm the affected radio becomes
neutral **Checking**, not green or yellow.

## 2026-09-29 — Consistent fresh-install first-run onboarding

Status: `Awaiting maintainer pass approval`

Governing specification: `first_run_onboarding_spec.md`.

Private implementation commit:
`dc5009ff422388f3db48ce20065d373f4c45795d` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

The existing behavior was state-driven but incomplete. A legacy single-rig
profile correctly entered the mandatory pre-shell **Upgrade Existing Station**
gate, while a fresh profile opened directly to Ops Center with no first-radio
prompt. The generic readiness banner led first to callsign settings and could
be dismissed, so a new user could lose the in-application route to the primary
setup task.

The private hotfix keeps upgrade and fresh setup separate. A station with
authoritative `fresh_default_ready` status, the current fresh-blank-slate
migration marker, no saved radio profile, and no prior welcome choice now sees
one main-window-owned **Welcome to FreqInOut** message after the usable shell
and splash have completed. **Set Up First Radio…** opens Settings > Radios and
the existing Guided Add Radio workflow; **Set Up Later** records the choice and
continues to Ops Center. Closing the welcome behaves as Set Up Later.

While no radio profile exists, the existing Ops Center readiness card becomes
a persistent **Set Up First Radio…** recovery action. Generic Dismiss and Do Not
Remind controls are hidden only in that state. After any profile is saved, the
ordinary readiness text, actions, and dismissal behavior return. This provides
the easy in-application setup route requested by the maintainer and does not
rely on documentation.

The acknowledgement key is explicitly ignored by legacy-use detection. The
fresh eligibility check fails quiet if authoritative state cannot be read,
creates no default records, and does not change migration, backup, scheduler,
radio, Mesh, or companion-application behavior. Packaged `--smoke-test` runs do
not show or acknowledge the interactive welcome.

Work-package ownership:

- primary `gpt-6-astra` (high reasoning) owned state/lifecycle policy,
  persistence safety, the bounded UI seams, tests, visual review,
  specification, work-log reconciliation, and final integration review;
- no delegated code package was used because the small change couples startup
  lifecycle, fresh-versus-upgrade classification, and the sole UI route into
  Guided Add Radio.

Acceptance evidence:

- focused onboarding, upgrade, runtime-status, readiness, startup-surface,
  deferred-screen, projection-lifecycle, performance, and shell UX partition:
  **267 passed, 4 platform skips**;
- larger Guided Add Radio/settings partition: **216 passed**, plus one
  unrelated baseline source-contract mismatch expecting a Settings-navigation
  visibility line absent from the current branch; the onboarding diff does not
  touch that construction block;
- fresh-profile offscreen visual review: welcome hierarchy and persistent Ops
  Center action passed at 1280×800;
- fresh packaged startup smoke: passed without presenting or acknowledging the
  interactive welcome;
- changed Python compilation and `git diff --check`: pass.

Maintainer pass gate: install or run this private commit with a clean temporary
profile. Confirm the welcome appears only after the painted shell, **Set Up
Later** leaves a persistent **Set Up First Radio…** action in Ops Center, and
that action opens Settings > Radios > Guided Add Radio. Restart after deferring
and confirm the modal does not repeat while the Ops Center action remains.
Then save one radio and confirm the no-radio action transitions back to normal
readiness guidance. A legacy single-rig profile must continue to show only the
mandatory pre-shell Upgrade Existing Station gate.

## 2026-09-29 — Windows startup helper-window flash hotfix

Status: `Approved—queued for next point release`

The maintainer confirmed on 2026-09-29 that the final correction fixed the
Windows flashing behavior. The approval queues this item for the next
accumulated point release and does not authorize an individual public push.

Governing specification:
`windows_startup_helper_flash_hotfix_spec.md`.

Private implementation commit: `7ab70df07f263b28c1f7ea253376626c761c202d`
on `wip/private-testing-multi-rig-1.2.3-not-ready`.

Follow-up startup-surface commit:
`e0aebbfa91bf91e0442a4d993b6cd4961ca88486` on the same branch.

Startup-surface diagnostic commit:
`3517ba3cc1cbc5d2610ec1287be2b11da43ca0e3` on the same branch.

Transient-widget lifecycle correction commit:
`cb226c26f950ed6e11a4e54cb7967f0b3af81622` on the same branch.

The supplied Windows recording showed a blank process frame before the FIO
splash and additional blank process frames behind the otherwise stable splash.
This was not screen warming: the packaged FIO executable is already windowed,
and deferred screen prewarming is disabled by default. The remaining
noninteractive startup commands, `tzutil /g` and `rigctl -l`, did not request
Windows no-console execution.

The bounded hotfix adds an opt-in internal subprocess policy using
`CREATE_NO_WINDOW` plus hidden `STARTUPINFO` on Windows and no additional
arguments elsewhere. Only the timezone and radio-catalog probes use it.
Operator-launched companion applications, catalog behavior, radio identity,
database state, upgrade routing, and first-launch guidance are unchanged.

Maintainer retest of private commit `7c6eca2` removed the desktop redraw but
did not remove the remaining transient windows. The only configured Launch
Control application was then isolated by disabling both monitoring and startup;
the behavior was unchanged. Frame review localized an unpainted surface before
the first splash frame and an untitled FIO-sized surface changing geometry
behind the splash during `Loading application settings...`, before the
intentional main-window `show()`.

The follow-up correction prepares the splash's complete first frame before its
native surface is mapped. It also shields the constructing `MainWindow` with
`WA_DontShowOnScreen` on Windows only, normalizes any logical visibility before
removing the shield, and then uses the existing single deliberate `win.show()`.
Upgrade dialogs run before this window exists, and user-launched companion
applications are outside the shield. Linux and macOS behavior is unchanged.

The subsequent `IMG_0937.MOV` retest rejects the hypothesis that the remaining
flash is one unpainted main-window frame. The initial blank frame is gone, but
10-fps inspection shows several differently sized blank FIO windows repeatedly
mapping behind the splash from about 5.2 through 7.6 seconds during `Loading
application settings...` and `Building station dashboard...`. A transparent
first-paint experiment was discarded without commit or push because it cannot
identify or safely control multiple independent surfaces.

The next private diagnostic is intentionally observational. A Windows-only,
startup-bounded application event filter writes `STARTUP_SURFACE_TRACE` records
to `freqinout.log` for top-level Qt show events, including elapsed time, current
startup stage, class, object name, title, geometry, visibility, native-display
shield state, and parent. It stops at the usable shell and is not installed on
Linux or macOS. The diagnostic itself does not hide, resize, reparent, or
otherwise change any surface.

The resulting `freqinout (83).log` is decisive. Settings construction showed
28 parentless widgets between 3.188 and 6.888 seconds: the Settings navigation
scroll area and 27 content containers. Qt reparented each immediately afterward,
but Windows had already mapped each visible parentless widget as a native
top-level window. After the main shell was deliberately shown, Station Control
twice unparented still-visible `stationCommandSourceRail` and
`stationCommandPrimaryContext` widgets before deferred deletion, producing the
remaining small late flashes.

The surgical correction parents Settings content before applying visible
state, waits until the nested Settings row belongs to the page before showing
its navigation scroll area, and hides retiring Station Control widgets before
the existing unparent/delete sequence. It does not globally suppress windows,
delay startup, or change upgrade, splash, main-window, or companion-application
lifecycle. The confirmation trace now records only show events to avoid the
hundreds of irrelevant child hide/reparent lines from the first diagnostic.

Work-package ownership:

- primary `gpt-6-astra` (high reasoning) owned scope, implementation,
  integration, tests, documentation, and exit-gate review;
- independent `gpt-5.6-terra` (low reasoning) performed the initial read-only
  subprocess-policy audit and follow-up process/surface-boundary audit and made
  no file changes.

Current acceptance evidence: the focused splash ordering, Windows shell-shield
ordering/release, subprocess-policy, runtime, and startup-deferral partition
passed **32 tests** with 3 platform skips. An isolated offscreen startup smoke
reached the first usable shell and shut its workers down cleanly. Changed Python
compilation and `git diff --check` pass.

Diagnostic-pass evidence: the trace, splash-ordering, and startup-deferral
partition passed **11 tests**; changed-file compilation and `git diff --check`
pass. The broader startup, subprocess-policy, and database-initialization
partition passed **26 tests** across isolated invocations. A combined macOS UI
run again encountered the repository's existing native Qt abort in the
unrelated Help-layout case after 25 prior passes; isolating the splash audit
passed.

Correction-pass evidence: **24 focused tests passed**; isolated Settings
construction produced zero top-level show events; an isolated complete main
shell produced zero pre-show top-level events and only the expected MainWindow
QWidget/QWindow pair after deliberate presentation; changed-file compilation
and `git diff --check` pass.

Maintainer pass gate: install the next Windows candidate, fully exit any
running FIO process, and record a cold launch from the candidate executable.
The pass condition is one fully painted splash followed by one fully built FIO
window, without any untitled or blank FIO window before or behind the splash. A
legacy profile must still present `Upgrade Existing Station`, and a configured
companion application must still open normally after restoring its startup
choice. Approval queues the hotfix for the next point release and does not
authorize a public push.

## 2026-09-28 — Cross-platform release packaging RP-1

Status: `Hosted package candidates passed; native field qualification pending`

Governing specification:
`cross_platform_release_packaging_spec.md`.

The first packaging slice adds a non-publishing internal workflow for Windows
x86-64 and Linux amd64. Both jobs build a PyInstaller one-folder application,
smoke it from a fresh profile, build the native installer/package, install that
artifact, smoke the installed application, uninstall it, verify profile
retention, and retain the artifact, checksum, and resolved dependency inventory
for three days. All GitHub-maintained actions are pinned to immutable commit
SHAs. The short retention matches the repository policy and keeps repeatable
private candidates from consuming long-lived Actions storage.

The reviewed public allowlist now carries the narrow set of package-build
inputs needed to reproduce future public artifacts, while the internal
candidate workflow, private tests, tools, specifications, and work logs remain
excluded. Public release publication and signing are intentionally not part of
RP-1.

Work-package ownership: primary `gpt-6-astra` (high reasoning) owns release
architecture, build scripts, workflow security, integration, specification,
and exit-gate review. The initial implementation used no sub-agent under the
active constraint. Retention-policy reconciliation received an independent
read-only audit from `gpt-6-astra` (high reasoning).

Local acceptance evidence:

- packaging and public-export contract — 10 passed;
- combined packaging, export, source-installer, launcher, and application-icon
  partition — 43 passed;
- release preflight, metadata validator, Python compilation, workflow YAML
  parse, and `git diff --check` — pass;
- PyInstaller 6.22.3 with hooks 2026.7 built a 487 MB macOS ARM64 one-folder
  diagnostic artifact from the shared spec, and that frozen application
  completed the packaged `--smoke-test` with a fresh isolated profile.
- GitHub-hosted `Package Candidates #1` at commit `0b22e8c` passed input
  verification, Windows x86-64 installer build/install/smoke/uninstall, and
  Linux amd64 Debian build/install/smoke/uninstall in 10m22s; both artifacts
  were available for download.

RP-1 remains open until the downloaded artifacts complete native fresh-install
checks on one real Windows system and one supported Linux system. RP-2
packaged-upgrade safety, complete platform dependency constraints, Windows
signing, and public release publication remain separately gated.

## 2026-09-28 — Public FreqInOut 2.0.3 hotfix release

Status: `Released in public FreqInOut 2.0.3`

Public release commit: `90f6ed0de72bc1535eec9dfd813a57ef10db4dec`.
Public references: `main`, `release/public-2.0.3-candidate`, and annotated tag
`v2.0.3` all resolve to that commit.

This point release is intentionally limited to the two approved runtime
hotfixes below: Windows verified-backup finalization and JS8 native-default
identity with endpoint isolation. It includes the matching 2.0.3 version,
changelog, public installation documentation, and rendered upgrade guide. It
does not add packaging automation or unrelated runtime behavior.

Work-package ownership: primary `gpt-6-astra` (high reasoning) owns release
reconciliation, versioning, documentation, runtime export, exact public-diff
review, validation, tagging, and publication. The underlying hotfixes retain
their recorded independent `gpt-6-luna` (high reasoning) read-only audits.

Release-candidate evidence:

- release preflight — pass;
- installer and launcher compatibility partition — 80 passed;
- JS8 launch, status, and native-client partition — 139 passed, 2 skipped;
- public runtime export contract — 2 passed;
- isolated full-suite accounting — 382 modules accounted for: 380 passing and
  2 intentionally skip-only; the two Qt real-widget modules were verified as
  33 individually isolated passing nodes;
- the one timing-sensitive Local Nets measurement passed three immediate
  reruns after one 62.8 ms outlier against its 50 ms threshold;
- changed-file compilation, full-package compile, and `git diff --check` —
  pass; and
- the six-page 2.0.3 DOCX guide rendered and passed visual inspection.

The monolithic in-process pytest invocation remains unsuitable as a release
oracle because Qt teardown left a JS8 reader thread alive and caused a native
segmentation fault at 3%. The isolated accounting above covers the same test
modules without that cross-test native-state contamination.

## 2026-09-28 — Windows verified-backup finalization hotfix

Status: `Released in public FreqInOut 2.0.3`

Governing specification:
`windows_verified_backup_finalization_hotfix_spec.md`.

Private implementation commit: `f075c593f9d00770c4ff7c1b6c972f93c1307ce8`
on `wip/private-testing-multi-rig-1.2.3-not-ready`.

The affected Windows station repeatedly completed database validation, backup
copying, hash comparison, copied-database validation, and manifest creation,
then received `[WinError 5] Access is denied` only when the verified
`.pre-install-*` directory was renamed to its preferred timestamped name. A
second attempt produced the same result, disproving the initial transient-lock
workaround.

The installer now retries only that final rename with short bounded backoff. If
Windows persistently denies it, FIO preserves the already verified staging
directory, reports its exact path, records it in the installation receipt, and
continues. No ACL is changed and no verification is waived. Any copy, database,
hash, manifest, missing-directory, or non-permission rename failure still
cleans up and blocks launch.

Work-package ownership:

- primary `gpt-6-astra` (high reasoning) owned backup-integrity semantics,
  implementation, integration, specification, and exit-gate review;
- independent `gpt-6-luna` (high reasoning) performed the read-only Windows
  filesystem edge-case and test audit; it made no file changes.

Acceptance evidence:

- focused installer upgrade-gate suite — 18 passed;
- broader installer and launcher compatibility partition — 80 passed;
- regressions prove normal naming, transient denial recovery, persistent
  denial preservation, verified manifest/content, actual-path receipt
  recording, and fatal cleanup for non-permission finalization errors.

Maintainer pass gate: on the affected Windows station, pull the private branch,
close FIO and companion applications, and rerun
`py -3.11 install_freqinout.py` without elevation. Confirm it reports a verified
backup path, reaches `Installation verified`, and starts through
`start-freqinout.cmd`. Approval queues this hotfix for the next public point
release; it does not authorize a public push by itself.

## 2026-09-28 — JS8 native-default identity and endpoint isolation hotfix

Status: `Released in public FreqInOut 2.0.3`

Private implementation commit: `18ca8d2f98586e9a390ab91639a0604a5bb7bff5`
on `wip/private-testing-multi-rig-1.2.3-not-ready`.

Governing specification:
`js8_default_identity_multi_endpoint_hotfix_spec.md`.

The production log and supplied database showed two JS8Call radios using the
same `/usr/bin/js8call-subspace` executable: the upgraded FTDX-10 native-default
profile on `127.0.0.1:2442` and the explicit `--rig-name FT-710` profile on
`127.0.0.1:2443`. The generic empty-argument matcher treated the named process
as an exact match for the default identity, suppressed the intended default
launch, and produced repeated process-global `js8net` endpoint warnings.

The bounded correction makes absence of `-r`/`--rig-name` part of only the
legacy/native-default JS8 process identity, including selected-radio status.
It preserves the working default profile and makes no JS8-owned file or
database change. Endpoint status fallbacks now receive the requested port, and
a process-global legacy fallback owned by another endpoint is rejected without
crediting, controlling, or warning about that sibling.

Work-package ownership:

- primary `gpt-6-astra` (high reasoning) owned identity and endpoint-lifecycle
  design, implementation, integration, specification, and exit-gate review;
- independent `gpt-6-luna` (high reasoning) performed the read-only edge-case
  and focused-test audit; it made no file changes.

Acceptance evidence:

- focused launch, process-status, JS8 native-client, and legacy-fallback
  partition — 139 passed, 2 skipped;
- broader JS8 identity, managed configuration, scheduler routing, readiness,
  and status partition — 149 passed, 6 skipped;
- focused regressions cover split, equals, bare, repeated, and conflicting
  rig-name selectors, the true argument-free default, selected-radio status,
  requested-port preservation, distinct endpoint registries, and cross-port
  fallback suppression;
- changed-file compilation and `git diff --check` — pass.

Maintainer pass gate: close all JS8Call processes, start FIO, and launch both
configured radios. Confirm the FTDX-10 native-default profile opens on port
2442, the FT-710 named profile opens on port 2443, each remains independently
visible/ready, and the log contains no repeated `shared js8net connection is
using a different endpoint` warning. Approval queues this hotfix for the next
public point release; it does not authorize a public push by itself.

## 2026-09-28 — Surviving generated-profile JS8 transition hotfix

Status: `Released in public FreqInOut 2.0.2`

Private implementation commit: `96a98a2` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

The production pull/restart log showed that the JS8Call process launched before
the default-profile correction remained alive after FIO exited. Its command line
still carried `--rig-name fio-default_js8_instance-50f8a9bb`. The corrected
native-default launch has no selector arguments, so the generic executable match
credited the surviving generated-profile process and suppressed the intended
default-profile launch.

Launch Control now inspects only the fresh launch-owned process inventory and
only for a legacy-default JS8 item. If the matching configured executable still
has an obsolete `fio-default_js8_instance-*` selector, FIO fails closed with an
instruction to close that JS8Call window/process and choose Launch again. FIO
does not terminate the process or risk a duplicate endpoint. A genuine
argument-free default process and every explicit/non-default rig name keep their
prior behavior.

Work-package ownership: primary `gpt-6-astra` owned the transition-safety design,
implementation, integration review, tests, specification, guide update, and exit
gate. The runtime did not expose a trustworthy reasoning-effort label. No
delegation was used for this process-lifecycle-sensitive correction.

Acceptance evidence:

- launch-bundle, GRS-4, JS8 identity, and process-title partition — 107 passed;
- focused regressions prove the generated-selector conflict, actionable result,
  no-spawn boundary, and native-default non-conflict case;
- changed-file compilation and `git diff --check` — pass.

Maintainer pass gate: with the old generated-profile JS8Call still running,
choose Launch and confirm FIO identifies the specific transition conflict. Close
that JS8Call, choose Launch again, and confirm the installed default station
profile opens. Approval queues this hotfix for the next public point release; it
does not authorize a public push by itself.

## 2026-09-28 — MeshCore hard BlueZ failure retry-containment hotfix

Status: `Released in public FreqInOut 2.0.2`

Private implementation commit: `6b70667` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

The production log showed repeated in-FIO Linux pairing/service attempts
progressing through `failed to discover services`,
`br-connection-canceled`, and finally `No powered Bluetooth adapters found`.
Later attempts consumed the full 30-second BLE timeout. The installed MeshCore
Python dependency was present; the failure was BlueZ/device availability, not
the earlier dependency-receipt problem.

FIO now treats a powered-off Bluetooth adapter, a BlueZ-cancelled connection,
and a failed in-FIO Linux pairing attempt as immediate operator-attention states.
The worker publishes `needs-attention` after one attempt and waits for an
explicit Connect instead of continuing automatic reconnects. Ordinary timeouts
retain the existing bounded retry budget. Powered-off and cancelled errors now
give recovery-specific wording instead of always appending PIN guidance. FIO
does not toggle Bluetooth, remove a bond, store a PIN, or modify the card.

Work-package ownership: primary `gpt-6-astra` owned BLE lifecycle/safety design,
implementation, integration review, tests, specification, guide update, and exit
gate. The runtime did not expose a trustworthy reasoning-effort label. No
delegation was used for this concurrency-sensitive correction.

Acceptance evidence:

- Mesh lifecycle, foundation, and reconnect partition — 162 passed;
- focused regressions prove hard-error classification, immediate worker pause,
  no automatic second attempt, powered-off guidance, and unchanged timeout
  backoff;
- changed-file compilation and `git diff --check` — pass.

Maintainer pass gate: restore Linux Bluetooth, power-cycle the T1000-E, wait for
advertising, choose Connect once, and accept the PIN prompt if presented. Confirm
FIO either reaches Companion-ready or pauses after one actionable failure with
no reconnect churn. Approval queues this hotfix for the next public point
release; it does not authorize a public push by itself.

## 2026-09-28 — Legacy JS8Call default-profile launch hotfix

Status: `Released in public FreqInOut 2.0.2`

Private implementation commit: `b8070e43c52c7a6a5be3493e2822f4837cdc30f4` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

The supplied production database confirmed the reported FT-DX10 failure. Its
legacy/default JS8 row correctly retained `/usr/bin/js8call-subspace`, API
endpoint `127.0.0.1:2442`, and the established
`/home/bill/.local/share/JS8Call` message root, but launch planning appended
`--rig-name fio-default_js8_instance-50f8a9bb`. JS8Call uses `--rig-name` as
both an application title suffix and a settings namespace, so FIO selected a
new unconfigured profile instead of the working profile used by the installed
Linux package launcher.

The bounded fix recognizes only the untouched `default_js8_instance`
migration identity: a blank rig name or the exact former machine-generated
fallback now launches through the saved executable with no `--rig-name`. The
configured API and file paths remain authoritative and no JS8-owned file is
copied, rewritten, moved, or deleted. FIO then makes a best-effort, PID-scoped
desktop title update to `JS8Call — <radio name>`; a desktop that refuses the
presentation-only title does not invalidate or alter the correctly configured
launch. Explicit operator-selected rig names and all non-default managed
multi-instance identities retain their existing `--rig-name` isolation.

Work-package ownership:

- Primary `gpt-6-astra` owned the migration-safety analysis, implementation,
  supplied-database replay, integration review, tests, specification, and exit
  gate. The runtime did not expose a trustworthy reasoning-effort label. No
  delegation was used for this small migration-sensitive correction.

Acceptance evidence:

- focused planner, orchestrator, JS8 application/storage, launch-bundle, and
  software-identity partition — 150 passed;
- broader launch, GRS-4, JS8, and process-title partition — 303 passed;
- a temporary copy of the supplied database planned the FTDX-10 command as
  `/usr/bin/js8call-subspace` with no launch arguments, retained the default
  data/profile paths, marked the effective identity `legacy_default`, and
  derived `JS8Call — FTDX-10` for the exact launched PID;
- explicit-rig and non-default-instance regressions remain covered;
- changed-file compilation and `git diff --check` — pass.

Maintainer pass gate: restart FIO on the production Linux station, start JS8Call
for FTDX-10 from FIO, and verify that it opens the same configured station used
by the installed package launcher, connects on `127.0.0.1:2442`, and shows the
FTDX-10 radio label in the title bar. Approval moves this item to the next
public point-release bundle; it does not authorize a public push by itself.

## 2026-09-28 — In-FIO MeshCore Linux authentication hotfix

Status: `Released in public FreqInOut 2.0.1`

Private implementation commit: `1d8cc9d9eb2163b36f6abdb480f4c379741db97f` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

The Linux operator report established that a secured MeshCore device could
briefly appear, then disappear while the official client failed during GATT
service discovery. Requiring the operator to close FIO and pair from a separate
Bluetooth-control workflow is not an acceptable routine connection path.

MeshCore BLE Connect now keeps normal bonded connections unchanged. If the
initial Linux connection instead reports authentication or the exact secured
service-discovery failure, the same FIO worker makes one bounded Bleak
pair-before-connect attempt. The desktop's registered BlueZ agent presents the
PIN prompt; FIO does not collect, persist, or log the PIN. After the operating
system completes pairing, FIO automatically retries the official MeshCore
Companion connection without an application restart. Bleak 1.0 is now the
minimum because its constructor-level `pair=True` performs pairing before
service discovery; the MeshCore package's later `pair()` call cannot resolve
this failure order.

The repair deliberately does not replace an explicit stale bond such as
`Peer removed pairing information` / CoreBluetooth Code 14. That remains a
visible operator-recovery state rather than an automatic credential mutation.
Cancellation, process-wide BLE session ownership, official-client send/receive,
and the dependency-receipt installer guard remain intact.

Acceptance evidence:

- Mesh foundation, lifecycle, reconnect, Settings, and outbound partition —
  180 passed;
- installer and dependency-receipt partition — 25 passed;
- focused regressions prove normal official-client BLE connection remains a
  single attempt, the Linux discovery failure orders pair/connect/disconnect/
  retry within FIO, and stale bonds are not automatically replaced;
- changed-file compilation and `git diff --check` — pass.

Maintainer pass evidence: after repairing the supported FIO environment so the
packaged `meshcore` dependency was present, the production Linux host completed
the exact **Scan → Use Device → Connect** workflow. The desktop presented the
MeshCore PIN prompt and FIO continued the connection as expected without being
closed or handing the operator off to a separate Bluetooth-control workflow.
The maintainer approved this bounded authentication fix for inclusion in the
public 2.0.1 hotfix bundle. Approval does not authorize an immediate public push.

## 2026-09-28 — MeshCore BLE outbound and saved-device repair hotfix

Status: `Released in public FreqInOut 2.0.1`

Private implementation commit: `a0d782471fa22249f12c1a387be9a14f6e162712` on
`wip/private-testing-multi-rig-1.2.3-not-ready`. Automated qualification is
complete; a real MeshCore BLE send/receive pass remains required before this
entry can be approved for the 2.0.1 candidate.

Installation follow-up commit: `53238486741d467abfec5c7691b4baae2810d00c`.
The tester report exposed a stale-environment gap: `meshcore` was already in
the shipped requirements, but a source pull could keep a valid same-version
install receipt and launch the older virtual environment without reinstalling
changed dependencies. The installer now records the exact requirements hash,
both neutral launchers reject a missing or stale hash and direct the operator
back to the installer, and isolated install verification imports the packaged
MeshCore, Meshtastic, and BLE dependencies before writing a successful receipt.

Production database and log copies exposed four related failures. The saved
device library contained one valid MeshCore BLE record and two enabled TCP
copies of the same BLE identity with no TCP host. Compose read every saved
enabled record, so it presented the invalid copies as offline even though the
active BLE session was connected. Settings retained a hidden send flag while
labeling that transport unavailable. Runtime dispatch also routed MeshCore BLE
to FIO's legacy raw receive-only implementation even though the packaged
official MeshCore client supports BLE, channel send, direct send, and bounded
acknowledgement evidence. Finally, the existing `mesh_nodes` table lacked the
later `public_key_or_hash` column used for direct destinations, producing
repeated persistence warnings.

The repair stays within the existing outbound architecture:

- MeshCore BLE now uses the official MeshCore client on FIO's persistent event
  loop and retains the existing process-wide BLE ownership/teardown gate;
- the established worker-owned send lock, operator `Allow Send`, channel/node
  policy, preview/confirmation, result evidence, and redacted audit remain
  unchanged;
- incomplete TCP/serial transport-switch copies are pruned only when a valid
  BLE record owns the exact device id and the copied record has no endpoint for
  its selected transport; complete connections and distinct devices remain;
- editing a saved device replaces its prior protocol/transport/endpoint key
  instead of silently appending another row, and unsupported transports cannot
  preserve a hidden true send permission;
- Compose consumes active runtime configurations, aligning its source list with
  the worker rather than advertising disconnected saved-library artifacts; and
- schema initialization adds `public_key_or_hash` to an existing `mesh_nodes`
  table as an additive migration.

Automated acceptance evidence:

- complete focused mesh partition — 197 passed;
- contextual Help and main-shell UI regression partitions — 142 passed;
- exact three-record production-shape repair, edit-in-place identity, official
  BLE factory dispatch, BLE worker lifecycle, outbound capability, Settings
  state, and legacy schema migration regressions are included;
- changed-file compilation and `git diff --check` — pass;
- installer/launcher contract — 11 passed, including stale dependency receipt
  rejection; isolated runtime verification imported the packaged mesh clients;
- Ruff is not installed in the project environment and therefore was not run;
- the unrelated Compose workbench file still reproduces its existing macOS Qt
  teardown segfault when multiple tests run in one process; the affected test
  passes alone and all Local Mesh Compose coverage in the mesh partition passes.

Maintainer approval: the maintainer explicitly approved this hotfix for the
public 2.0.1 bundle after the production Linux MeshCore connection and pairing
work. It is queued with its automated saved-device/schema, official-client BLE,
outbound, audit, and reconnect evidence. Approval does not authorize an
immediate public push.

## 2026-09-28 — Guarded Local Mesh outbound Compose hotfix

Status: `Released in public FreqInOut 2.0.1`

Private hotfix commit: `3501e021b1d03d8b2f637cf0591a1e27ddd4f22b` on
`wip/private-testing-multi-rig-1.2.3-not-ready`. Implementation and focused
automated qualification are complete; representative Meshtastic and MeshCore
hardware/operator qualification remains pending.

FIO now sends explicit operator-authored Local Mesh messages through the same
worker and adapter session that owns receive and connection lifecycle. The
protocol-neutral request/result contract preserves adapter identity,
channel-versus-direct addressing, exact text, request identity, timeout, and
acknowledgement policy. Results distinguish API acceptance, Companion command
completion, node/routing acknowledgement, acknowledgement timeout,
cancellation, and failure without claiming that a person read the message.

At the time of this original slice, Settings exposed default-off `Allow Send`
for Meshtastic TCP/USB/BLE and MeshCore TCP/USB while the raw MeshCore BLE path
remained receive-only. The later approved official-client MeshCore BLE hotfix
supersedes that limitation and qualifies MeshCore BLE outbound through the same
guarded contract. Message Compose adds a Local Mesh mode with a connected source,
accepted channel or known direct-node destination, byte limit, exact payload
preview, and final operator confirmation. The worker rechecks channel/node
policy, serializes each adapter send, and appends requested plus final evidence
events to `mesh_send_audit`; audit rows retain only payload length and SHA-256,
not plaintext. MeshCore node persistence now retains the public-key identity
required for direct messages. Automatic relays and JS8/Mesh bridging remain
deferred.

Work-package ownership:

- Primary `gpt-6-astra` owned architecture, concurrency and safety decisions,
  implementation, integration, tests, specification, user guide, and exit
  review. No reliable primary reasoning-effort label was exposed.
- `gpt-5.6-terra` at `low` performed the read-only adapter/API and persistence
  audit. It identified the official Meshtastic acknowledgement callback,
  MeshCore Companion send/retry surfaces, evidence semantics, session
  serialization requirement, and unqualified raw MeshCore BLE transmit path.
- `gpt-5.6-luna` at `low` performed the read-only Compose/Settings workflow
  audit. It identified the explicit Local Mesh mode, exact preview and
  confirmation, connected/send-enabled gating, and evidence-specific operator
  status requirements. Delegates changed no files; the primary reviewed and
  implemented their findings.

Automated acceptance evidence:

- complete focused MeshCore/Meshtastic settings, lifecycle, reconnect, channel,
  projection, persistence, and outbound partition — 193 passed;
- Settings UI, responsive Compose geometry, and operational view contracts —
  27 passed;
- Local Mesh Compose preview/confirmation/request acceptance — 1 passed;
- Compose guidance, FIOSpotter compatibility, and wide/compact/Large Text UI
  audit — 52 passed;
- contextual Help and guide integration regression — 23 passed;
- changed-file compilation and `git diff --check` — pass.

Maintainer approval: the maintainer explicitly approved the guarded Local Mesh
outbound Compose hotfix for the public 2.0.1 bundle. Its qualified transport
matrix now includes the separately reviewed official-client MeshCore BLE path.
The existing automated channel acceptance, direct ACK/timeout, disconnect,
reconnect, and redacted-audit evidence remains the release record. Approval
does not authorize an immediate public push.

## 2026-09-25 — BBS automation restoration and 1.2.8 upgrade hardening hotfix

Status: `Released in public FreqInOut 2.0.1`

Private hotfix commit: `8ea27181f42a24eb5433bfebf7d2c77f58bf688b`.
Implementation and focused regression qualification are complete. The runtime
slice is already present in the current 2.0.1 tester candidate and the
maintainer has approved it for the public 2.0.1 bundle.

The top-level FIO BBS workspace now restores station-owned Automation Rules as
a first-class page between Publishing and Visitor Preview. Operators can stage,
save, update, delete, and revert sender/text-filtered rules for VarAC incoming,
FLMsg, and FLAmp sources. Rules require explicit match evidence and one or more
enabled destinations. Background message-file discovery applies them off the UI
thread, and a durable source-version/destination ledger prevents a forced scan
or restart from copying the same unchanged arrival again. Canonical station
metadata is authoritative; legacy saved rules are a one-release fallback only
until the station value is saved.

Radio Service now exposes `Initialize BBS…`. The confirmed action preflights the
selected radio's existing live BBS folder, creates or reuses the adjacent
station managed root and Default location, optionally imports current live
files, and enables the selected radio's catalog publication. It never replaces
or deletes the live folder, and repeated initialization skips same-named files
instead of producing duplicate imports.

The 1.2.8-to-2.0 audit confirmed one startup-ordering defect: message projection
could query `js8_messages.source_key` before the additive multi-radio JS8 cache
schema had run. A shared idempotent schema owner now upgrades and backfills
legacy JS8 rows before projection schema and dirty-trigger installation.
Historic rows keep their original primary key as `source_id` and a blank legacy
`source_key`. The backup-and-operator-confirmed multi-radio migration gate is
unchanged. The Linux upgrade guide now checks migration version 3 and points
existing VarAC BBS operators to the new initialization action.

Work-package ownership:

- Primary `gpt-6-astra` owned design, implementation, integration, tests,
  documentation, and final verification.
- `gpt-5.6-luna` at `low` performed the read-only BBS automation ownership and
  persistence audit.
- `gpt-5.6-terra` at `medium` performed the read-only 1.2.8 upgrade and BBS
  initialization audit.

Acceptance evidence:

- BBS automation/initialization, Station BBS, responsive layout, UI manifest,
  message ingest/projection, JS8 policy, sweeper, catalog, and database-manifest
  suites — 112 passed;
- contextual Help and guide rendering/export regression suites — 23 passed;
- Messages BBS worker/helper, reader action, and BBS core identity/contract
  suites — 15 passed;
- public 1.2.8 upgrade rehearsal, migration preview, multi-radio Wave 1, and
  settings thread-affinity suites — 48 passed;
- changed-file compilation and `git diff --check` — pass.

Maintainer approval: the maintainer explicitly approved the restored BBS
Automation page, Radio Service initialization, and 1.2.8 JS8 schema-ordering
hotfix for the public 2.0.1 bundle. The recorded automated BBS, migration,
projection, and responsiveness evidence remains the release record. Approval
does not authorize an immediate public push.


## 2026-09-22 — In-app help coverage and navigation alignment

Observable acceptance route: open Help and use the table of contents to reach
FIO Spotter, Station Control Center, Local Reports, Radios, Guided Add/Edit
Radio, Local Mesh, and Condition Alerts. From Configuration, the Local Mesh and
Condition Alerts Help actions must open their specific sections rather than the
generic guide overview. The Main Menu reference must use the current left-rail
labels, and every internal guide link must resolve to one unique anchor.

The guide now documents the current grouped navigation, the seven-step atomic
Add/Edit Radio workflow, radio versus software ownership, station-scoped FIO
Spotter and BBS responsibilities, receive-only MeshCore/Meshtastic transport,
multi-device Control Center state, local-report review, and the three Condition
Alert action modes. Messages explains mesh Inbox/Map evidence, and the current
top-level BBS service is distinguished from older Managed BBS Library wording.
Local Mesh and Condition Alerts now have real registered contextual-help
targets. Major sections that do not currently expose a local Help button remain
available from the guide table of contents; no unrelated screen controls were
added during this documentation slice.

Production review then exposed a render-completion defect hidden by the earlier
read-only snapshot assertion: `QTextBrowser.setHtml()` was called with an
unsupported second base-URL argument. The worker successfully read the guide,
then raised before rendering it or building the table of contents, leaving the
initial `Loading the FreqInOut guide…` placeholder indefinitely. The completion
handler now sets the document base URL separately, renders with the supported
one-argument call, builds the table of contents, and replaces the placeholder
with an actionable display error if rendering itself fails. The regression now
requires rendered guide text, a populated table of contents, and the expected
base URL—not merely a completed file read.

The main navigation's station-setup group is now labeled
`Configuration`, with `Config` used only on the narrow compact rail and
`Configuration` retained as its accessible name. The internal `Settings`
screen key, persisted data names, and component-specific settings terminology
remain unchanged. Existing expanded/collapsed navigation preference is migrated
from the former `Settings` group key. The guide, contextual-help title, Plan
Builder assignment guidance, status prompts, and other operator-facing routes
now consistently direct users to Configuration while preserving the real
`Save Settings` control and application-specific settings names.

Help PDF export now resolves each existing local guide image to an explicit
absolute file URL before Qt builds the document. This removes the broken-image
placeholder Qt previously printed for the FIO logo (and applies the same rule
to other bundled guide images) while leaving remote and embedded image URLs
unchanged. The on-screen guide and exported PDF now consume the same resolved
document, preventing viewer/export asset drift.

The guide's long-form reference sections now use stable reference labels in
place of conversational prompts. The navigation overview is `Tabs Explained`
with `Purpose`, `What's There`, and `Why It Matters`; the detailed index is
`Sections and Sub-Tabs` with `Description`. Repeated labels such as `Why you
use it`, `Explain this to me`, `Plain-language workflow`, and question-form
behavior headings are now `Operational use`, `Overview`, `Typical workflow`,
and named behavior or diagnostic references. Quick Start and live operating
workflows remain procedural where ordered operator action is the subject.

Operating Groups are now a distinct Quick Start prerequisite before HF Daily,
HF Nets, and Plan Builder. The former Quick Start link targeted an ID on a
table row, which Qt did not expose as a reliable scroll destination; it now
targets the registered `settings-hf-groups-details` heading used by contextual
Help. The reference section now covers scheduling dependency, the minimum
useful configuration, multiple band/mode configurations, known-group preview
and enablement, accepted frequency formats, VFO and FLDigi expectations,
Auto-Tune on QSY, group-scoped condition levels and Fast Light naming, change
impact, and the current control labels. A rendered-Help regression confirms the
Operating Groups heading can be selected through its anchor.

Work packages and exact model ownership:

- `gpt-6-astra`, high reasoning: coverage matrix, implementation comparison,
  guide/registry/test/spec integration, delegated-audit reconciliation, and
  final acceptance.
- `gpt-5.6-terra`, low reasoning: read-only initial coverage audit and
  post-change operator-language/wiring audit. The primary accepted its stale
  navigation, missing-section, BBS/Mesh, static fallback, Condition Alert
  wording, and stale Map cross-link findings. It also removed unreachable
  speculative context registrations instead of claiming UI wiring that was not
  present.

Acceptance evidence: help registry contains `39` contexts; the guide contains
`107` unique anchors; all `29` literal contextual-help callers are registered;
focused contextual-help tests `8 passed`; adjacent Help/shell/FIO Spotter tests
`152 passed`; the combined Configuration-label, reference-language, and
Help/PDF regression set reports `304 passed`; changed Python files compile;
`git diff --check` passes.
The 76-page PDF was rendered to PNG for visual review: the FIO logo is present
on page 1 and the bundled support image is present on page 76. Existing
operator edits to the DOCX guide and rendered upgrade output were not touched.

## 2026-09-21 — Public 2.0 runtime-only promotion plan recorded

The maintainer selected multi-rig 2.0 as the future public base application,
with single-radio operation retained as a supported configuration. Public
`N1MAG/FreqInOut` remains a user runtime distribution: application source,
runtime assets, user documentation, dependencies, installers, licenses, and
release notes only. Tests, engineering tools, specifications, internal
worklogs, diagnostic evidence, and the private WIP history remain private.

`public_2_0_runtime_release_plan.md` now defines a private reconciliation plus
allowlisted public export; current-single-rig backup/consent/idempotence/
rollback acceptance; README and contextual-help completeness; runtime
requirements parity; install/update/uninstall and packaging verification;
2.0 metadata/provenance; clean-host qualification; and final public-tree audit.
The plan is deliberately dormant until the maintainer explicitly declares the
release candidate ready, so small private review patches may continue.

Planning review found that all 37 registered contextual-help entries resolve to
real guide anchors, while the Local Mesh and Condition Alerts Settings callers
currently fall back to generic Help and must be completed before release. It
also identified a blocking migration-consent contradiction between the Linux
upgrade guide and installer finalization, plus private-WIP defaults throughout
the current public-facing documentation. Those are now explicit release gates.

Work packages and exact model ownership:

- `gpt-6-astra`, high reasoning: publication architecture, runtime boundary,
  upgrade/data-safety gates, help/dependency/installer/release checklist,
  delegated-audit reconciliation, and final documentation integration.
- `gpt-5.6-terra`, low reasoning: read-only release-readiness audit across
  README/help, dependencies, installers, packaging, migration documentation,
  existing tests, and remaining external qualification. The primary accepted
  its migration-consent, artifact provenance, clean-host E2E, launcher
  ownership, public-channel, and documentation-parity findings.

Acceptance evidence: documentation-only change; help-registry audit reports
`37` registered contexts, `100` guide anchors, and zero missing registered
anchors; static caller audit reports the two release-gated generic fallbacks
named above. No public branch, runtime code, installer behavior, production
configuration/data, external application, tag, or release artifact changed.

## 2026-09-21 — Legacy VarAC argv and managed Wine-runtime recovery

Observable acceptance route: restart FIO with an older saved FT-DX10/FT-710
VarAC configuration, then use Launch Control and Software Administration. A
legacy FT-DX10 `C:\VarAC\VarAC.ini` selector must remain one exact argv item;
an older FIO-managed FT-710 `Z:\...\.freqinout...\VARA.exe` runtime must move
to a new unused Wine-drive runtime (or reconcile to an already-correct native
runtime), with every FIO projection aligned and the old folder retained.

The implementation adds a read-only structured legacy launch projection and a
bounded Linux/Wine startup repair. The repair requires one linked managed node,
an exact qualified writer, a readable nonsymlink source, and a VarAC INI inside
a verified Wine drive. It defers while VarAC/VARA is running, uses the existing
journaled backup/readback/rollback transaction for external changes, updates
the node, manifest, canonical identity, and Launch Control together, and never
deletes or overwrites the old runtime. New Linux/Wine preparation now refuses
to manufacture a host-root Z: executable path. Windows retains native
`VarAC.exe + member INI` argv and never enters the Wine migration.

Work packages and exact model ownership:

- `gpt-6-astra`, high reasoning: evidence analysis, compatibility architecture,
  transaction/projection implementation, specifications/help/work-log updates,
  delegated-diff review/correction, acceptance testing, and integration.
- `gpt-5.6-terra`, low reasoning: independent read-only cross-platform and
  transaction-safety audit. The primary accepted its finding that legacy
  recovery must never overwrite an existing canonical recipe and added the
  guard/regression.
- `gpt-5.6-terra`, low reasoning: independent read-only specification and test
  audit. The primary added full four-projection, reconcile-only,
  running-process, and Windows acceptance coverage from that review.

Primary review also corrected a persistence-boundary mismatch found by the new
production-shaped test: raw store launch rows use `app_name`,
`launch_at_startup`, and `readiness`, so repair now normalizes those fields
before rewriting and preserves enabled/startup/monitor choices.

Acceptance evidence: changed-file compilation passed; focused VarAC native,
transaction, arrangement, structured-launch, launch-bundle, and recovery suite
`126 passed`; Software Administration/identity/persistence/operator-route suite
`70 passed`; `git diff --check` passed. The implementation gate is closed.
Live Linux/Wine repair and native Windows launch remain the external operator
qualification gate.

## 2026-09-21 — Radio-visible FLMsg, FLAmp, and VarAC instance titles

Observable acceptance route: create or repair two radio-managed Fast Light /
VarAC identities, launch each radio's components through Launch Control, and
see the radio's saved human name in the child application title without any
change to profile roots, endpoints, dependencies, executable selection, or
VarAC's structured executable/INI vector.

FLMsg 4.0.24 and FLAmp 2.2.14 expose the FLTK `-title` option. Their canonical
managed recipes now persist `FLMsg — <radio name>` and
`FLAmp — <radio name>` in the exact argument vector. The narrow component
repair detects absent/stale titles and repairs them together with the existing
radio-specific NBEMS/endpoint identity. Opaque FIO draft and application IDs
never appear in the title.

VarAC 13.2.7 has no qualified title argument or INI key; inspection of its
managed .NET entry/form construction confirmed the executable builds the main
caption from fixed product/version fields. FIO therefore leaves the supported
VarAC argv byte-exact, persists `VarAC — <radio name>` as presentation metadata,
and after launch performs a bounded asynchronous PID-scoped title update on
Windows (`SetWindowTextW`) or Linux/Wine under X11/XWayland
(`_NET_WM_PID`/`_NET_WM_NAME`). Failure is logged once and never blocks launch
or readiness. Generic-caption matching, executable patching, and invented INI
keys are prohibited.

Work packages and model ownership:

- `gpt-6-astra`, high reasoning: native-contract verification, architecture,
  implementation, delegated-audit review/correction, specifications, and final
  integration.
- `gpt-5.6-luna`, low reasoning: bounded read-only launch/repair/test impact
  audit. The primary retained the useful affected-file inventory, rejected the
  unsafe suggestion to make presentation titles launch blockers, and verified
  VarAC independently before integration.

Acceptance evidence: focused recipe/repair/Software Administration/VarAC
round-trip suite `66 passed`; broader launch-bundle/status/managed-directory
and GRS-6.2 suite `99 passed, 2 skipped`; process-title dispatch suite
`3 passed`. The combined non-overlapping result is `168 passed, 2 skipped`.
Live Windows and Linux/Wine title-bar behavior remains the external
qualification gate because the current macOS environment cannot host either
native VarAC route.

## 2026-09-21 — GRS-13.4a Fast Light component-scoped legacy repair

The controlling specifications now define a dedicated Repair FLMsg/FLAmp
components review action. It is narrower than Replace instance, preserves
FLRig/FLDigi identity (with only additive ARQ pairing arguments), unrelated
families, and Launch Control preferences, writes no external app files, and
requires one optimistic atomic transaction with readback, parity validation,
and rollback.

Implementation packages and evidence:

- Architecture, persistence transaction, production-copy validation, and final
  integration: primary high-reasoning model. The repair updates FLMsg/FLAmp and
  the required additive FLDigi NBEMS/ARQ arguments while proving FLRig and all
  unrelated canonical families unchanged.
- Bounded store/test audit and specification update:
  `gpt-5.6-luna`, low reasoning. The primary reviewed and integrated the diff.
- Acceptance: `89 passed` across the component repair, GRS-13 identity,
  Software Administration, launch identity, persistence, workspace, and guided
  recipe suites. An additional disposable copy of the attached production
  settings database repaired FT-710 to distinct ARQ port `7323` and returned no
  Fast Light parity issues; the original attachment remained read-only.
- A repository-wide `pytest -q` attempt reached 3% before the existing macOS
  Qt/background-reader exit fault interrupted the process in Compose layout
  acceptance. The named Compose test passes by itself (`1 passed`); no failure
  was attributed to this repair slice.
- The implementation gate is closed. Live operator confirmation that the second
  FLMsg/FLAmp pair launches and exchanges traffic remains the external
  qualification gate.

Follow-up screenshot evidence found that the action was hidden on the visible
`FLAmp & Signing` task because its internal key is `flamp_signing`, while the
initial route checked the nonexistent `flamp` key. The availability contract
now includes FLMsg, FLAmp & Signing, Message Folders, and Launch, and also
detects a stale Software Administration message-folder projection even when the
canonical launch component was already partly corrected.

All new entries must follow the authoritative multi-model delivery contract in
`docs/internal/project_delivery_rules.md` and record the required package/model,
primary-review, acceptance, and exit-gate evidence.

## 2026-09-16 — Startup and Guided Add Radio performance recovery

Status: implementation exit gate passed locally; production-sized Linux
confirmation remains operator-assisted.

The supplied Linux log showed a 77.0-second startup: propagation schema
assurance consumed 24.1 seconds, MainWindow construction consumed 48.1 seconds,
and first usable shell arrived at 76.3 seconds. Hotspot captures also showed
Guided Add Radio parsing JS8Call profiles synchronously on Qt's GUI thread for
more than one minute, plus first-render propagation scoring saturating the GUI
thread. These were code defects, not a launch-command or screenshot mismatch.

Propagation schema assurance now updates only malformed legacy rows and runs
the expensive event-key duplicate collapse only before the unique index is
created. Startup progress inside MainWindow no longer pumps the global Qt event
queue, which had been executing deferred tab work before the window existed.
Mesh starts at the existing post-shell lifecycle boundary. Ops Center
propagation scoring and Guided Add Radio application/profile discovery now use
single-worker background lanes with stale/late-result guards and bounded
shutdown ownership.

Guided Add Radio repairs stale disabled protected Operating Models in place and
always presents a real compatible model ID. Receive-only SDR Software permits
independent JS8Call and built-in FIO Spotter selection; FIO Spotter is limited
to receive/decode, forms, watches, and Inbox projection by the receive-only
Operating Model, while external JS8Spotter remains unavailable.

Work packages and model ownership:

- Primary high-reasoning model: log/hotspot integration, startup/concurrency
  architecture, propagation schema repair, worker integration, specification,
  delegated-diff review, and exit gate.
- `gpt-5.6-terra` high: independent launch/hotspot diagnosis.
- `gpt-5.6-luna` medium: exact Add Radio model/FIO Spotter UI audit and focused
  observer regression test.
- `gpt-5.6-terra` medium: performance boundary audit and focused schema,
  post-shell mesh, background discovery, and protected-model tests.

Acceptance evidence: **269** focused startup, propagation, guided setup,
Settings, mesh, and Ops Center tests pass; **299** broader radio-scoped software,
runtime, soak-controller, and main-shell tests pass after updating two obsolete
source-shape assertions for the new worker boundary. A real isolated 20-second
Qt smoke reached first usable shell in **604.0 ms**, constructed MainWindow in
**550.9 ms**, observed **32.9 ms** maximum event-loop lag, and shut down in
**19.2 ms**. A 100,000-row mature propagation database completed repeat schema
assurance in **64.7 ms** with **zero row changes**. Changed Python modules
compile and `git diff --check` passes. No runtime endpoint, production database,
commit, or remote repository was changed by the automated gate.

## 2026-09-16 — Add Radio Step 2 Operating Model assignment

Status: implementation and code-level automated acceptance complete. Native
operator confirmation remains external qualification; this correction was
reviewed and validated from the named `Settings > Radios > Add Radio` code path
without relying on screenshots or an application launch.

The reported defect was present in code. Step 2 was explicitly applicable only
to an observer/SDR, its selector was disabled, and a conventional transceiver's
guided-save path discarded any staged Operating Model. Only the first
transceiver sometimes received an implicit default later during primary-radio
activation; subsequent transceivers could remain unassigned. Editing an
observer also defaulted its disabled selector instead of reliably preserving a
custom receive-only assignment.

Step 2 is now applicable and selectable for both transceivers and observers.
Transceivers see all enabled models; observers see only enabled receive-only
models. The current assignment is preselected during edit, the Review card names
the selected model, and Save requires a real persisted model ID. A fresh blank
station idempotently creates only the protected default and receive-only model
rows before presenting Step 2; this does not create a radio, assignment, or
runtime-primary projection.

Guided creation now uses a safety-ordered two-phase sequence supported by the
existing persistence APIs: save the radio inactive, assign the reviewed model,
then activate a first transceiver or first/only observer. Assignment failure
leaves the saved radio inactive and recoverable. Changing the draft between
observer and transceiver roles selects that role's preferred model instead of
carrying an observer receive-only choice silently into a transmit-capable draft.

Work packages and models: `gpt-5` primary/high reasoning owned interpretation,
architecture, persistence ordering, core helper, delegated-diff review,
integration corrections, specifications, and the final exit gate.
`gpt-5.6-terra` high performed the read-only persistence and compatibility
audit. `gpt-5.6-luna` medium implemented the bounded Step 2 presentation and
selection work. `gpt-5.6-terra` medium implemented and repaired focused test
coverage. Primary review removed synthetic unpersisted model choices, added the
blank-station built-in-model persistence seam, reset the preferred model across
role changes, added first-radio assignment-before-activation coverage, and
updated stale delegated/test-suite API expectations.

Acceptance evidence:

- `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q tests/test_software_admin_radio_first_ui.py -k settings_add_radio_dialog_keeps_guided_steps_available` — 4 passed, 18 deselected; the exact Settings Add Radio action exposes an enabled, visible selector with a persisted model ID for both roles at desktop and constrained sizes.
- `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q tests/test_sdr_operating_model_core.py tests/test_sdr_receiver_setup_ui.py tests/test_software_admin_radio_first_ui.py` — 46 passed.
- Delegated stale-contract repair: `./.venv/bin/python -m pytest -q tests/test_sdr_operating_model_assignment_ui.py tests/test_radio_scoped_software_settings_1_2_3.py` — 161 passed.
- Full guided-radio regression gate: `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q tests/test_sdr_operating_model_core.py tests/test_sdr_operating_model_assignment_ui.py tests/test_compose_observer_safety.py tests/test_sdr_receiver_setup_ui.py tests/test_guided_setup.py tests/test_radio_scoped_software_settings_1_2_3.py tests/test_software_admin_radio_first_ui.py tests/test_settings_software_instance_adapter.py tests/test_js8_expect_runtime.py tests/test_receiver_software_launch.py tests/test_multi_rig_wave1_slice_a.py` — 363 passed.

The implementation exit gate is closed. Native macOS and Linux confirmation of
the selector and saved assignment remains operator-assisted release evidence;
no schema migration, hardware operation, or runtime configuration mutation was
performed by this work session.

## 2026-09-16 — Guided Add Radio stable seven-step navigator

Status: implementation and focused automated acceptance complete. Exact-script
startup was confirmed on native macOS; final visual confirmation after replacing
the operator's already-running pre-fix process remains operator-assisted.

The Settings > Radios > Add Radio dialog assigned stable numbers before it
filtered steps by applicability. It then hid Operating Model for a conventional
radio and hid Connection until a software/control selection required endpoint
fields. The resulting first render showed `1, 3, 5, 6, 7`, which incorrectly
looked like missing setup work and was unrelated to the launcher or Settings >
Software assistant.

The navigator now always renders the complete contract: Radio, Operating Model,
Software, Connection, RF Guard, Schedule, and Review. A step that does not apply
remains in its stable position, is disabled and theme-muted, carries an explicit
`N/A` label, and explains why it is skipped through its tooltip and accessibility
description. Back, Next, direct navigation, and save gating continue to traverse
only applicable steps. If a role or software choice changes applicability while
the dialog is open, focus moves to the nearest following applicable stable slot
instead of jumping to the beginning or leaving stale guidance visible. Controls
use font-derived heights and the three-column grid wraps at constrained width.

Work packages and models: `gpt-5` primary/high reasoning owned root-cause
analysis, architecture, specification, delegated-diff review, transition
hardening, contract correction, and the exit gate. `gpt-5.6-luna` medium implemented the bounded
wizard presentation correction and focused UI expectations. `gpt-5.6-terra`
medium added the production-route regression through the real SettingsTab Add
Radio action at desktop and constrained sizes. The primary rejected the earlier
Settings > Software evidence as the wrong workflow and reviewed the corrected
route before acceptance. A final `gpt-5.6-luna` medium read-only contract audit
identified the exact-state and geometry gaps; `gpt-5.6-terra` medium extended
the test, and the primary corrected its optional-Hamlib assumption before the
final gate.

Acceptance evidence: 356 guided setup, SDR receiver, operating-model,
radio-scoped software, Settings adapter, and production-route tests pass. The
production-route test requires exactly seven visible stable controls and checks
Operating Model and Connection explicitly for both observer and conventional
radios at 1920x1080 and 900x560. Its observer path selects or types `RTLSDR`,
chooses the actual `Receive-only SDR` setup type, verifies the resulting observer
role, and checks navigator containment and non-overlap. The exact operator launcher
`/Users/bill/RadioCode/FreqInOut-multi-rig/start-multi-rig.sh` resolves to this
worktree and successfully started a separate isolated validation runtime. The
already-running pre-fix Python process remained the macOS accessibility target,
so it correctly continued to display the old missing-step UI; Python does not
hot-reload this change. No launcher change, migration, or persisted user-setting
change is required.

Process correction: `project_delivery_rules.md` now makes the operator-feedback
interpretation gate explicit. A named navigation route is binding acceptance
scope, an adjacent workflow is not valid evidence, an unreproduced report remains
real pending exact-route investigation, and material ambiguity requires one
concise clarifying question. Native relaunch evidence must also prove that the
pre-fix Python process exited before current-code visual evidence is accepted.

Final commands and outcomes:

- `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q -o faulthandler_timeout=20 tests/test_software_admin_radio_first_ui.py -k settings_add_radio_dialog_keeps_guided_steps_available` — 4 passed, 17 deselected.
- `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q tests/test_sdr_operating_model_core.py tests/test_sdr_operating_model_assignment_ui.py tests/test_compose_observer_safety.py tests/test_sdr_receiver_setup_ui.py tests/test_guided_setup.py tests/test_radio_scoped_software_settings_1_2_3.py tests/test_software_admin_radio_first_ui.py tests/test_settings_software_instance_adapter.py tests/test_js8_expect_runtime.py tests/test_receiver_software_launch.py tests/test_multi_rig_wave1_slice_a.py` — 356 passed.
- `./.venv/bin/python -m py_compile ...` for every changed Python production and focused test file — passed.
- `git diff --check` — passed.

The implementation/WIP-push gate is closed. Native current-code macOS visual
confirmation, Linux Light/Dark and Normal/Large Text geometry, and live
RTL-SDR/SDR++ qualification remain explicit external testing gates; this is not
a release qualification.

## 2026-09-16 — Software-instance assistant visible step navigation

Status: implementation, focused automated acceptance, and exact-launch native
macOS visual confirmation complete; native Linux light/dark and
normal/large-text visual confirmation remains operator-assisted.

The embedded Software Administration assistant exposed only a `Step 1 of 7`
label, so operators could not see the workflow ahead. It now presents all seven
themed, font-height-derived steps in a compact grid: Purpose, Find or create,
Identity, Connections, Files, Launch, and Review. The current step stays
selected, prior steps are available, and only the next eligible step is enabled;
radio and replacement gates cannot be skipped. Re-clicking the current step
does not clear its selected state.

A related initial-state defect matched the placeholder entry when the inventory
contained exactly one available radio. The sole radio is now selected
automatically, while multiple radios still require an explicit choice. A radio
supplied by the invoking Settings context is selected before first render, and
both `Next` and the next-step control refresh immediately after radio or
replacement changes.

Work packages and models: GPT-5 primary/high reasoning owned diagnosis,
architecture/integration review, specification, delegated-diff review, and the
exit gate. `gpt-5.6-luna` medium implemented the bounded assistant UI/state
correction and focused tests. `gpt-5.6-terra` medium added the radio-scoped
initial-state regression. The primary identified an invalid Qt visibility
assertion; the focused test package corrected it, and the primary reviewed the
checkable-step re-click behavior before acceptance.

Acceptance evidence: 271 Software Administration, Settings adapter, SDR guided
setup, assignment, and radio-scoped settings tests pass. A final focused
production-route partition passes 41 tests, including the public Software
Administration Add action at 1920x1080 and 900x560. The primary also launched
the application through the operator's exact
`/Users/bill/RadioCode/FreqInOut-multi-rig/start-multi-rig.sh` path, navigated
Settings > Software > Create or use instance, and visually confirmed that all
seven controls render in the live macOS application. The launcher resolves to
this worktree, its `.venv`, and the established
`/Users/bill/RadioCode/runtime/multi-rig` runtime; no launcher change is
required. Changed Python files compile and the whitespace gate passes.
Unrelated documentation/render artifacts were preserved.

## 2026-09-16 — Receive-only SDR operating model and isolated JS8Call setup

Status: implementation and focused automated acceptance complete; native Linux
RTL-SDR, SDR++, and dual-JS8Call launch qualification remains operator-assisted.

Guided Add Radio now gives an observer/SDR the same coherent ownership flow as
a transceiver while preserving the receive-only boundary: `Radio -> Operating
Model -> Software -> Connection -> Review`. Every installation receives one
protected **Receive-only SDR** operating model. It enables receive-side
Messages, Map, ingest, and Launch Control while disabling scheduler ownership,
QSY, PTT, Compose, Expect replies, Net Control, and other transmit authority.
An observer can be activated only after that model is assigned, and a first or
only observer can never become the compatibility primary radio.

The Software step now treats SDR++ and an optional observer-owned JS8Call as two
separate applications. Both expose an explicit FIO autolaunch choice and appear
separately in Review. A requested JS8Call is created only after the observer has
a durable radio ID, uses its own profile/data paths and API endpoint, and is
adopted with `replace_existing=False`. Endpoint/storage collisions are rejected;
an existing primary-radio instance is never silently reused or replaced. The
radio-scoped launch bundle preserves the SDR++ item when JS8Call is added, and
all observer launch entries carry the `receive_only` execution scope.

The same flow is available later through Settings > Software. Selecting an
unassigned SDR now enables the new-instance assistant correctly. The quick
JS8Call checkbox no longer surfaces the internal replacement-contract error; it
reverts the incomplete toggle and opens the scoped setup assistant. Cancellation
does not create a radio, assignment, manifest, or launch recipe. Recoverable
post-radio failures leave the observer inactive with a route back to setup.

Transmit safety is enforced twice: observer sources are absent from Compose,
Spotter Expect, and JS8 Net Control choices, and final send/retrieval boundaries
reject observer-owned JS8 provenance. Inbox retrieval continues to support
legacy rows without provenance. The primary integration review also corrected
case-insensitive matching of persisted JS8 instance IDs so a valid transceiver
source such as `FIO-B` still routes correctly when evidence records `fio-b`.

Work packages and models: GPT-5 primary/high reasoning owned architecture,
migration policy, integration, concurrency/safety review, the Inbox boundary,
specification, and the exit gate. `gpt-5.6-terra` high implemented the core
model/migration, observer JS8 adoption, launch persistence, collision guards,
and core tests. `gpt-5.6-luna` medium implemented the guided/settings UI and
focused UI tests. `gpt-5.6-terra` medium performed the initial read-only gap and
message-flow audit. `gpt-5.6-luna` medium added focused transmit-boundary tests.
The primary reviewed every delegated diff, fixed the guided step order and quick
toggle state capture, added final send-boundary enforcement, and preserved all
unrelated working-tree changes.

Acceptance evidence includes 115 focused core tests, 210 broader
multi-radio/runtime tests, 132 additional migration/shared-state/launch tests
with 4 skips, and 311 guided/settings/software UI tests. The adjacent Message
Inbox suite passes 196 tests; selected-target/compose guidance passes 45 tests;
pending-Inbox correction passes 18 tests; observer transmit safety passes 5
tests. All 10 real-widget Compose acceptance probes pass in isolated processes;
the existing macOS/PySide harness can segfault while tearing down multiple real
Compose widgets in one interpreter, so that harness behavior is recorded
separately and is not counted as a product failure. Changed Python files compile
and the whitespace gate passes. The remaining exit gate is native Linux testing
with unique SDR++/JS8 endpoints and data directories, restart/autolaunch proof,
receive-only ingest provenance, and light/dark plus normal/large-text visual
checks at desktop and constrained window sizes.

## 2026-09-16 — Inbox Spotter filtering, actions, selection, and comments

Status: implementation complete; focused automated acceptance passed; native
light/dark and normal/large-text visual qualification remains operator-assisted.

Historical Spotter projections stored under the internal `sitrep` source were
loaded by the MAGNET query but rejected by the public Spotter focus predicate,
which produced an empty result. Focus and source filtering now share semantic
source aliases, including the existing rule that CommStat evidence removes the
legacy Spotter alias. A focused regression proves Spotter + MAGNET preserves
the historical row while projected CommStat remains separated.

The crowded View/flag/Relay/BBS/Delete text cluster is now one themed
row of compact, always-visible action chips with a font-derived fixed column;
no menu click or overlapping text links are required. The selection
header is a real visible `Select all`/`Clear all` button with font-derived
geometry, shared theme inheritance, accessible naming, and bounded-visible-page
semantics even when no extra filter is active. Optional Spotter comments now
match SuperSpotter's compact 34-column presentation and enforce a documented
50-character authored limit without truncating received legacy evidence.

A field retest exposed a second group-filter boundary: the visible predicate
expanded an operator parent family, but the earlier indexed projection query
used only the literal parent. The query and its cache key now use the same
expanded family, so removing an unrelated configured group cannot discard
Spotter child-group rows before evaluation. Age filtering now covers every
historical day with non-overlapping review bands and separately labeled
cumulative cleanup scopes. Both bounds execute in the indexed projection query
and retain the 200-row UI limit.

Work packages and model: the primary GPT-5 model at high reasoning owned the
filter semantics, UI redesign, comment compatibility review, specification,
implementation, review, and acceptance gate. The active execution contract
prohibited subagent delegation, so no delegated diff was produced.

Attached logs showed bounded Inbox filter applications, generally 40–108 ms
with isolated 145–182 ms applications while scopes were changing; no Inbox
filter loop or UI hang appeared. A hotspot sample identified a cache-contract
violation in Station Control Bar health refresh: it reopened the device-profile
store on the GUI thread. That lookup now uses the already-maintained immutable
profile/choice caches, with a source-level no-database regression. Other samples
captured bounded background Spotter form discovery, Mesh adapter work, and a
station-shell reflow; none implicated the Inbox predicate or action renderer.

Acceptance evidence: the follow-up partition passed 510 Inbox, Spotter,
projection-store/projector, asynchronous reader, action, age, main-shell, and
responsive-layout tests. The prior broader partition passed 446 Compose,
BBS/Relay, codec, and cache-contract tests. Changed Python files compile and
the diff whitespace gate passes. Native visual qualification remains open.
Existing unrelated documentation changes and rendered artifacts were
preserved.

## 2026-09-15 — Settings Use Radio native-window stability

Status: implementation and focused automated acceptance complete; native
macOS activation retest remains open.

Activating a newly configured radio incorrectly published both the global
`settings_saved` signal and the radio-specific `device_profiles_changed` signal.
The global fan-out reapplied the application theme and refreshed unrelated tabs,
causing a visible swipe/vanish event on macOS even though FIO remained running.

A full emitter audit found the same broad signal in immediate theme/text-size,
radio create/edit/default, Multi-Rig migration/defer, guided plan assignment,
and HF operating-group save paths. Signal ownership is now explicit:
`settings_saved` belongs only to the explicit Save Settings action;
`appearance_changed`, `device_profiles_changed`, and
`operating_groups_changed` drive their scoped consumers. The appearance handler
also rejects an unchanged theme signature, preventing an accidental future
stylesheet reapplication. Radio changes retain runtime-client, ingest,
station-health, Map listener, plan-context, and scheduler refreshes without the
global repaint. Operating-group changes retain schedule/planner/message/NCS
refreshes without rebuilding radio clients or unrelated presentation.

Regression coverage proves that activation persists, the global settings
signal remains silent for scoped actions, domain signals fire once, and the
source contains only one global emitter. Acceptance passed 170 scoped-save,
multi-rig/settings, and application-lifecycle tests plus 19 targeted
main-shell/runtime/settings tests, Python compilation, and the diff whitespace
gate. Native macOS activation and scoped-save retesting remain open.

## 2026-09-15 — Release-candidate control, ingest, and Mesh stability

Status: automated implementation gate passed; native rig/Mesh hardware and
Linux/macOS/Windows soak qualification remain open before public release.

Ordinary application focus loss no longer destroys scheduler endpoint intent or
turns a valid FLRig/rigctld frequency readback into `Applied - verification
unavailable`. Native hidden/suspended and detected clock-discontinuity paths
retain their stale-completion fencing. A complete fresh matching readback now
reconstructs the verified endpoint read model without sending a duplicate rig
command. PTT is explicitly limited to pre-retune safety, and JS8 offset evidence
is required only for entries where JS8 has offset authority.

Dynamic FLAMP Expect offsets now persist through the same durable worker-owned
settings path as Spotter offsets, preventing historical DIRECTED records from
being replayed on each ingest cycle. Enabled unavailable Mesh adapters now stop
after three consecutive failures. The third failure persists a visible
needs-attention state; timer polls remain quiet until explicit Connect/Reconnect
resets only the selected adapter and starts one fresh bounded series. Success
resets the series; retained Inbox/Map data is untouched.

Work packages and models:

- high-reasoning primary GPT-5 model: runtime-stability spec, scheduler and
  lifecycle architecture, persistence/Mesh production changes, primary diff
  review and corrections, integration, and gate decision;
- `gpt-5.6-terra`, high reasoning: read-only Mesh retry/lifecycle audit;
- `gpt-5.6-luna`, high reasoning: focused cursor/lifecycle/dedup regression
  package.

Primary review added readback intent adoption, PTT-versus-frequency proof, JS8
authority scoping, retry-exhaustion health persistence, live per-adapter manual
reset, connection-signature coverage, and multi-adapter isolation tests.
Acceptance: 69 focused scheduler/ingest/lifecycle tests; 291 adjacent scheduler,
dynamic Expect, and background-ingest tests (2 skipped); 144 focused and 167
adjacent Mesh/source-connection tests; Python compilation; and clean diff
whitespace. A full-repository attempt hit the existing shared-QApplication/
worker-thread Qt abort in an early Compose test; that exact test passes alone and
the changed modules are absent from its stack. This harness limitation and the
remaining native hardware/soak gate are not waived.

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

### P1 redesign — persistent nonmodal Map window

The approved second step replaces embedded Map navigation with one reusable,
nonmodal Map window. Selecting Map now leaves the current main workspace,
top-level geometry, state, and screen unchanged; it opens the window on first
use and raises the same instance thereafter. The Map widget and its one
WebEngine view are constructed lazily in their final parent and are never
reparented. Closing the title bar hides the window for warm reuse; application
shutdown stops Map work and destroys the owned surface exactly once. Existing
Map-to-Messages, Compose, SOP, scheduler, plan-context, and background-ingest
handoffs resolve through an explicit main-application host rather than the Map
window.

Normal rectangle, maximized state, and display identity persist through a
600 ms change-detected write boundary. Restore validates the current display,
prefers hardware serial identity when available, falls back through screen name
and prior available geometry, clamps valid rectangles, and uses a bounded
main-screen rectangle for malformed or disconnected placement. Full-screen,
minimized, and always-on-top state are not restored. Loading and retry feedback
live inside the Map window, so opening Map does not create or resize the main
status bar. The prior hidden WebEngine warmup was removed because it would be a
second native surface owned by the main window.

Hidden work is bounded: closing before first paint cancels queued construction;
clean warm re-entry performs no refresh; real hidden source changes coalesce to
one refresh; and an in-flight projection that completes while hidden stores only
the newest payload without calling WebEngine. Reopen applies that payload once
when current, while a newer hidden source change supersedes it with one current
projection.

Work packages and models: the `gpt-5.6-sol` high-reasoning primary owned window
and lifecycle architecture, concurrency, placement persistence, production
integration, delegated-diff review, specifications and the final gate;
`gpt-5.6-luna` (high) audited existing window/persistence and Map host-routing
patterns; `gpt-5.6-terra` (high) implemented the bounded placement, singleton,
reuse, shutdown and main-window-invariance test package; and `gpt-5.6-luna`
(high) independently reviewed the completed UX/lifecycle slice and identified
the hidden projection completion race. The primary reviewed the Terra diff,
corrected its offscreen maximize assertion to test FIO's validated geometry
contract, added clean/dirty re-entry and hidden in-flight coverage, and closed
all actionable independent-review findings.

Acceptance evidence: **415 Map/first-render/current-page/startup/Phase 7 shell
tests**, **82 high-use Messages/Compose/Spotter/Map tests**, **44 shared-theme,
font-derived geometry, lifecycle and design-control tests**, and **135 multi-rig
shell tests** pass. Changed Python files compile and `git diff --check` passes.
The automated exit gate is complete. Native macOS, Linux and Windows
normal/maximized/full-screen-main and secondary-monitor qualification remains
operator-assisted. No migration, production configuration/data write,
RF/device command, application restart, commit, or push occurred.

### P1 first-show refinement — permanent Map central surface

Native macOS qualification found one remaining first-launch-only disturbance:
the pop-out painted its loading widget as the `QMainWindow` central widget, then
replaced that central widget as the cold Map/WebEngine surface attached. That
handoff changed top-level layout hints at the same moment the platform created
the native child surface, making the window appear to be torn down before the
Map rendered.

The Map pop-out now installs one stable stacked central surface during window
construction. Loading and Map pages live inside it, Map construction uses that
container as its final parent, and first-use readiness changes only the current
page. The stable container reports neutral size hints, and the regression gate
requires central-widget identity, top-level geometry, restorable normal
geometry, size hint, and minimum-size hint to remain unchanged through queued
first show and simulated native attachment. Show/hide lifecycle is also owned
by the pop-out so a retained Map cannot remain logically visible after its
window is hidden. The cold WebEngine child now has ignored size policy, zero
minimum size, and no focus eligibility until its document is ready, closing the
remaining first-attachment size and focus paths identified by the independent
macOS audit.

Work packages and models: the `gpt-5.6-sol` high-reasoning primary owns the
failed native-gate diagnosis, lifecycle architecture, production integration,
specification and final acceptance; `gpt-5.6-terra` (high) isolated the central
replacement seam and added the focused first-show regression; and
`gpt-5.6-luna` (high) independently audited macOS Qt ownership and first-paint
behavior. The primary reviewed the delegated test diff before changing
production code.

Acceptance evidence: the integrated Map/first-render/current-page/startup/
Phase 7 shell gate passes **416 tests**; the focused high-use
Messages/Compose/Spotter/Map gate passes **45 tests**; the shared-theme,
font-derived geometry and responsiveness gate passes **40 tests**; and the
multi-rig shell gate passes **135 tests** in five clean, process-isolated
shards. Changed production Python files compile and `git diff --check` passes.
The automated gate is complete. Native macOS first-open qualification remains
operator-assisted because the reported disturbance is a Cocoa/WebEngine
compositor behavior; warm reopen, Linux, Windows, maximized/full-screen-main,
and secondary-monitor cases remain in the native acceptance matrix. No
migration, runtime configuration/data write, RF/device command, application
restart, commit, or push occurred.

### P1 architecture replacement — native Qt Location Map

Native macOS and Linux qualification proved that moving Chromium/WebEngine into
a separate window did not remove its destructive first-native-surface behavior:
the operating system could still flash, swipe, reposition, or reconstruct the
top-level window before the first Leaflet paint. Further timing, overlay,
prewarm, detached-page, reveal, and geometry-state-machine changes were rejected
because they attempted to manage a browser compositor rather than remove the
unstable boundary.

The Map rendering foundation is now one native `QQuickWidget` and Qt Location
provider-free `itemsoverlay` Map. Bundled North American vector geography is
drawn locally below the operational overlays; no tile, API key, online provider,
or network request is part of the Map runtime. The widget is created once in the
persistent pop-out's final hidden content stack before the first `show()`. No
WebEngine import, HTML/Leaflet build, browser navigation, JavaScript payload,
page-title action bridge, post-show native-child attachment, or renderer
reparenting remains in the active Map path. Failure to load QML or item-overlay support is
shown calmly in the existing Map window and cannot start a retry or geometry
loop.

The existing Map intelligence is projected into typed native collections for
stations, paths, direction indicators, polygons, Maidenhead grid lines and
labels, city labels, weather/alert/infrastructure markers, propagation fills,
legend, and Regional Intel summary. Expensive static geometry is simplified and
cached on the projection worker, and every collection has an explicit cap before
it reaches QML. Generation fencing preserves newest-wins behavior; a completion
while hidden retains only the latest snapshot, while resize, repaint, theme,
focus, show, and hide perform no source I/O or projection rebuild. Map selection
uses direct Qt signals and shared-theme semantic roles.

Frozen-build support now explicitly collects the FIO QML source, Qt
Location/Positioning/Quick QML modules, item-overlay and positioning components, and
a runtime hook that preserves their bundled search roots. Source packaging also
includes QML and the runtime hook. Deployment guidance now describes the native
Qt Location dependency rather than QtWebEngine.

Work packages and models: the `gpt-5.6-sol` high-reasoning primary owned the
architecture decision, lifecycle and concurrency boundaries, production
integration, packaging, specification updates, delegated-diff review, and final
acceptance; `gpt-5.6-terra` (medium) implemented the bounded renderer/QML
surface; `gpt-5.6-terra` (high) modernized focused lifecycle, geometry, parity,
and packaging tests; and `gpt-5.6-luna` (high) performed the read-only legacy
parity and packaging audit. The primary reviewed every delegated change and
retained unrelated workspace files.

Slice exit gates: the native foundation passed **51 tests** before core parity
began; core parity then passed **248 tests** before advanced parity/packaging
began. The final native Map/first-render/current-page/geometry gate passes
**255 tests**; the affected multi-rig shell gate passes **85 tests**; focused
Messages/Compose and Spotter gates pass **53** and **73 tests**; and the shared
theme, font-derived geometry, control-bar, Software layout, and hidden-projection
gate passes **61 tests**. Changed Python files and the frozen-runtime hook
compile, the PyInstaller spec parses, and `git diff --check` passes. PyInstaller
is not installed in this development environment, so a frozen executable was
not built locally; packaged macOS/Linux/Windows qualification remains an
explicit release gate. No migration, production configuration/data write,
RF/device command, application restart, commit, or push occurred.

### P1 offline Map and prior-overlay parity correction

Operator qualification rejected the online-provider result: the native Map
displayed an API-key requirement, did not provide the expected zoom behavior,
and its pins did not reliably reach the detail action. The immutable contract is
now explicit: Map geography and interaction remain fully usable offline, with
no tile download, API key, provider initialization, or network fallback. The
prior Leaflet overlay language is the behavioral reference; only the unstable
browser rendering substrate is replaced.

The Qt Location surface now uses provider-free `itemsoverlay`. A cached worker
projection supplies 207 bundled US, Canadian, and Mexican vector rings beneath
the operational layers. The basemap uses its own 220-polygon renderer cap, so
the 180-item operational-polygon limit no longer clips Mexican geography.
Projection application remains a pure in-memory GUI-thread swap.

The prior overlay affordances are retained on the native scene: semantic
station/weather/alert/infrastructure markers; station status and QSY cues;
callsign labels inside the pin target; single-action wide path targets; hover
and accessibility guidance; a bottom-docked inline legend with a separate
Regional summary; saved-view/zoom controls; and debounced, viewport-bounded
2/4/6-character Maidenhead grids. Mouse wheel, touchpad, pinch, visible-button
zoom, and drag pan change only the existing scene. Marker, path, and polygon
selection crosses QML by stable ID; Python resolves the current snapshot after
the pointer callback, preventing recursive QVariant conversion. The bridge is
owned by the `QQuickWidget`, which keeps it alive through QML teardown and
prevents null-context shutdown-log storms.

Work packages and models: the `gpt-5.6-sol` high-reasoning primary owned the
offline architecture, concurrency and lifecycle boundaries, cap separation,
delegated-diff review, production integration, specifications, legacy-test
migration, and final gate; `gpt-5.6-luna` (high) audited assets, repository
history, provider behavior, interaction failures, and prior overlay parity;
`gpt-5.6-terra` (medium) implemented the bounded provider-free renderer and QML
parity surface; and `gpt-5.6-terra` (high) implemented the focused offline and
real-QQuick interaction tests and stabilized their cold-delegate readiness.

Exit evidence: the focused native/offline/pop-out gate passes **52 tests**. The
offline contract passes **12 tests in each of 10 fresh processes (120/120)**,
including a real visible pin click that emits exactly one action. The
process-isolated Map, first-render, current-page, startup, layout, multi-rig,
release-follow-up, and font-derived UI matrix passes **418 tests in 16 clean
shards**. A monolithic macOS run passed once, then reproduced the suite's known
cumulative native-Qt teardown segfault while constructing a later QQuick
fixture; functional qualification therefore remains process-isolated. Changed
production Python files compile and `git diff --check` passes. Packaged
macOS/Linux/Windows first-open,
offline, zoom/pan, and pin qualification remains an operator-assisted release
gate. No migration, production configuration/data write, RF/device command,
application restart, commit, or push occurred.

### P1 native Map layer and theme parity correction

Operator screenshots confirmed four gaps after the native replacement: Regions
did not visibly express FEMA R01–R10, the selected-station panel could retain
light document colors on a dark surface, a runtime theme switch could leave the
Map and Settings navigation on different palettes, and permanent city labels
could overlap. A read-only comparison with the prior Leaflet surface also found
lost five-band SNR path colors, state labels, and concise SitRep/Regional color
meaning.

The bounded worker projection now supplies true FEMA-colored state geometry,
R01–R10 labels, state abbreviations, preserved regional
gray/blue/yellow/orange/red levels, SitRep summary/status keys, and a legend
derived only from the current snapshot. Paths retain the original five SNR
bands. When Propagation and Regions are combined, FEMA colors remain visible
and propagation annotates region labels. City candidates remain capped at 400,
ordered deterministically, and are decluttered in O(n) QML work after a 120 ms
viewport debounce so zoom can restore labels without data I/O.

MainWindow is the runtime palette authority. It reloads settings once, passes
one theme snapshot into the persistent Map window and Settings, and the Map
forwards it to its retained QML bridge and rich-text selection panes without a
projection, rebuild, or geometry operation. Existing selection documents are
rebuilt from their cached payload. Settings theme painting is failure-isolated
so an optional runtime status failure cannot leave its navigation in the old
theme. The shared stylesheet now includes `QTextBrowser`.

Work packages and models:

- `gpt-5.6-sol` (high), primary: slice architecture, theme ownership,
  concurrency/performance boundary, delegated-diff review, Propagation/Regions
  integration correction, specifications, acceptance, and final review;
- `gpt-5.6-luna` (high): read-only native-versus-Leaflet layer parity audit;
- `gpt-5.6-terra` (high): read-only shared-theme/cache audit;
- `gpt-5.6-terra` (high): focused red regression package;
- `gpt-5.6-terra` (high): bounded projection, renderer, and QML layer mechanics.

The implementation gate passed **62 focused Map/theme tests** before this
documentation/acceptance slice began. The final process-isolated Map,
first-render, current-page, startup, multi-rig, release-follow-up, font, and
theme matrix passes **411 tests in 16 clean processes**; the additional shared
theme/design-control matrix passes **77 tests in six clean processes**. The
offline/native interaction contract also passes **12 tests in each of 10 fresh
processes (120/120)**. Python compilation and `git diff --check` pass. The
primary reviewed every delegated change and preserved unrelated workspace
artifacts. No migration, production configuration/data write, network/tile
dependency, RF/device command, application restart, commit, or push occurred.

### Native Map operator legend, filter row, and coherent first paint

Operator screenshots established three remaining defects after the native Map
parity correction: the default legend did not explain station-pin status, the
principal filters were split across rows because hidden controls retained grid
positions, and the Age chooser could open beyond the active screen. The report
also identified a brief empty-scene flash before the first station projection.

The production SitRep pin language is restored as a bounded, textual legend:
`Functioning`, `Partially Functioning`, `Not Functioning`, and
`Unknown / No Report`, using the same fixed data colors as the station fills.
State boundaries, active city labels, and station identity remain explicit;
the first-position station-status group wraps internally without separating its
heading or truncating the status labels, and the inline QML legend changes no
top-level geometry. View, Topic,
Group, and Age now occupy the first responsive row in that order. Only visible
mode-specific fields participate in layout, and mode visibility changes request
one coalesced cache-only reflow. The Age popup is pre-sized and clamped to the
button's screen, including right and bottom edges.

The native surface no longer becomes visible merely because QML loaded. The
existing calm loading page remains current until one complete worker projection
has synchronously populated all bridge collections; the retained QQuickWidget is
then revealed for its first paint. No window operation, renderer rebuild, source
I/O, or refresh was added to this handoff.

Work packages and models: `gpt-5.6-sol` (high), primary, owned lifecycle and
responsive-layout architecture, implementation, delegated-diff review,
integration, documentation, and the exit gate; `gpt-5.6-luna` (high) performed
the read-only legend/layout/first-render audit; `gpt-5.6-terra` (high) added the
focused red regression package. The primary reviewed and refined the delegated
test expectations so `Stations` and `SitRep Status:` remain explicit rather
than silently replacing layer identity with color swatches. Luna's independent
post-implementation review identified Large Text truncation and group-separation
risk in the first flat Flow implementation; the primary replaced it with one
first-position station group whose natural-width status items wrap together.

Acceptance evidence: the focused legend/layout/popup/first-paint and related
Map baseline passes **54 tests** in five clean processes. The final
process-isolated Map, lifecycle, first-render, startup, theme, and font matrix
passes **290 tests in 13 clean processes**. The offline/native interaction
contract also passes **12 tests in each of 10 fresh processes (120/120)**.
Changed Python modules compile and `git diff --check` passes. Packaged macOS,
Linux, and Windows visual qualification remains operator-assisted. No migration,
runtime data write, network/tile dependency, RF/device command, application
restart, commit, or push occurred.

### Map-to-FIO foreground navigation

A maximized Map could obscure the main navigation and required the operator to
use operating-system window controls to return to FIO. The Map toolbar now
keeps `Show FIO` visible, and selected-detail Inbox/Compose actions navigate to
their destination before raising and activating the existing main window.
Activation uses the explicit application host already owned by the persistent
Map tab. It clears only a minimized state and performs no geometry, Map
lifecycle, projection, data, or persistence work. Platform refusal to grant
focus is failure-isolated after navigation completes.

Work packages and models: `gpt-5.6-sol` (high), primary, owned the activation
contract, implementation, integration, specification, and exit review;
`gpt-5.6-luna` (high) performed a read-only Map-toolbar and station-route audit;
`gpt-5.6-terra` (high) supplied focused route-order and geometry-preservation
regressions. Acceptance passes **303 tests in 11 clean processes**, including
direct normal, maximized, full-screen, and minimized main-window state checks;
changed Python files compile and `git diff --check` passes. Packaged macOS,
Linux, and Windows foreground behavior remains an operator-assisted release
gate. No migration, runtime data write, network/tile dependency, RF/device
command, application restart, commit, or push occurred.

#### P1 follow-up: direct `Show FIO` hang

Live sampling of the reported frozen macOS process showed the main thread
blocked in `QObject::connectImpl` while a `QTimer.singleShot` callback was being
registered during nested Qt event delivery. The process was sleeping at low
CPU, confirming a UI-thread deadlock rather than Map rendering or data work.
Inbox navigation had avoided the precise timing, while the direct toolbar
action synchronously changed top-level activation from inside the
`QPushButton.clicked` dispatch.

All Map-to-main handoffs now enqueue the registered `present_main_window` slot
with `QMetaObject.invokeMethod(..., Qt.QueuedConnection)`. This reaches the next
event-loop turn without allocating or connecting a transient timer. The
redundant direct `QWindow.requestActivate()` call was also removed; the retained
widget-level raise/activate sequence is sufficient after a user gesture and
generates less platform state churn. A focused regression proves that a real Qt
host is not presented until after the originating dispatch returns. The full
post-correction Map, lifecycle, first-render, theme, font, startup, and
multi-rig matrix passes the same **303 tests in 11 clean processes**; changed
Python files compile and `git diff --check` passes.

### Complete MCForms and native JS8 stored-message interoperability

The supported offline SuperSpotter surface now uses one shared MCForm grammar,
parser, payload codec, and operational-status classifier. This closes the
observed `[ST]`/`[GR]` field loss and extends the same handling to every
structured prompt in the reference catalog. Explicit starred defaults are
honored, unmarked choices remain unanswered, every form has a multiline
Comments control, and saved Expect responses round-trip choices, bracket
fields, Comments, and datecodes without collapsing them into one opaque string.
Alphabetic `F!BDN` is supported consistently across discovery, filters, date
maintenance, native ingest, and presentation.

Operator identity defaults use a reviewed form-and-field allowlist. Callsign,
state, and grid are filled only for the operator/reporting-station semantics;
incident, affected-area, assessment-area, destination, wildfire, medivac, and
`other area` prompts remain operator-entered. JS8Call and FIOSpotter Compose now
offer an explicit, off-by-default `Send as MSG`. Each compose mode retains its
own local draft value, the option reflows below the destination in the bounded
setup rail, and preview and guarded-worker command are byte-for-byte aligned.
Native JS8 inbox and live/directed ingestion remove the MSG transport wrapper
before classifying the form. Save to Expect remains target-neutral and can be
completed before a radio exists.

MAGNET `F!701B` and `F!701C` now share conservative Green/Yellow/Red status
classification across ingestion, Activity/Inbox intelligence, SitRep fusion,
and Map routing. Richer existing decoded summaries retain precedence over the
generic normalized label. Catalog parsing remains cached off field-edit,
resize, paint, and theme paths; status backfill uses one latest-per-station
window query and runs once per ingestor/mapping signature.

Work packages and models: `gpt-5.6-sol` (high), primary, owned the catalog and
legacy-code audit, grammar/codec architecture, persistence and RF safety
contracts, implementation, cross-consumer integration, regression correction,
specification, and exit review. The package remained primary-owned because its
parser, persistence, ingest, guarded-send, and shared-status boundaries were
not safely separable; no delegated diff was integrated.

Acceptance evidence: the final focused MCForm, Compose/Expect, JS8 ingest,
Message Intelligence, projection, SitRep, UI reflow, and per-mode draft matrix
passes **398 tests**. A full process-isolated repository sweep produced **3,805
passes and 42 skips** before its one package-related setup-width failure was
corrected; the affected 51-test recheck passes. Four unrelated baseline
failures remain: one Local Nets timing threshold, two Settings source-shape
assertions against already-refactored theme code, and one native-Map harness
missing an attribute accessed by the committed implementation. Changed Python
files compile and `git diff --check` passes. Live JS8Call stored-message
round-trip and packaged macOS/Linux/Windows visual confirmation remain
operator-assisted. No migration, runtime data write, external endpoint action,
RF/device command, application restart, commit, or push occurred.

### Compose shared-layout conformance correction

The Messages Compose and pop-out Compose Workbench mode selector had inherited
an aggregate `QGroupBox.sizeHint()` through the shared font-accessibility pass.
Because that aggregate included `QListWidget`'s default viewport hint, a compact
four-mode selector acquired a sticky 230–443 px minimum and fragmented every
compose task with blank vertical space. The shared guard now protects a group
title from font clipping without promoting descendant viewport hints into the
container minimum. Compose additionally releases and recalculates the selector
container from its actual wrapped rows, so an already-laid-out surface can
recover after font and theme changes.

The four complete mode labels reserve their delegate padding and explicitly
disable elision. JS8Call now joins FLMsg/FLAmp, FIOSpotter, and CommStat RF in
the readable setup-rail layout when a wide viewport can support both the rail
and a dominant work surface; all modes retain the existing vertical promotion
at medium and compact sizes. The implementation changes only geometry and
does not rebuild forms, discover endpoints, mutate drafts, persist settings, or
send RF traffic.

Acceptance evidence: the process-isolated affected Compose, workbench, Expect,
CommStat, theme, and font-derived geometry set passes **138 tests**. The broader cross-tab
UI audit passes **81 tests**, and the rapid Large Text mode-switch regression
passes in five fresh processes. The 1920x1080 live-widget probe holds the mode
container at 78 px in all four modes, with a zero selector scroll range and a
larger work surface than setup rail. Changed Python files compile and
`git diff --check` passes. A monolithic repository sweep reached the existing
Settings source-shape failure after **1,436 passes and 3 skips**; continuing in
one process later reproduced the repository's unrelated native-Map Qt crash,
which is why Map qualification remains process-isolated. No application
restart, persistence mutation, endpoint discovery, network call, RF/device
command, commit, or push occurred.

Follow-up live screenshots exposed a second wide-rail defect hidden by the
former oversized mode selector. The shared row-reflow helper capped every
non-compact row to one control line even when the row used a deliberate
two-line grid. JS8Call and FIOSpotter therefore painted `Send as MSG` over the
destination editor. Grid-backed rows now retain their natural multi-row height;
only genuine one-line box layouts receive the one-line cap. The setup card also
stops at its derived content height instead of stretching its border through
the full work-surface height. The surrounding scroll rail continues to own
bounded overflow and resize behavior.

### Startup verification lifecycle correction

Status: implementation complete and automated gate passed; restarted Linux
hardware confirmation remains operator-assisted.

The operator reported that manual QSY and later Resume both reached verified
state, while launch with FLRig/JS8Call already running remained at
`Applied · verification unavailable`. The supplied Linux log identifies the
runtime configuration root as `/home/bill/.freqinout` and records the causal
ordering: the startup schedule command applied at 20:27:53, the first native
`ApplicationActive` event arrived at 20:27:54, and an `app_resume` refresh ran at
20:27:55. MainWindow had sampled QApplication as inactive while its native
window was still being presented, then treated that first activation as a true
resume. Scheduler resume recovery consequently retired the just-completed
lanes and cleared expected/applied state. A simultaneous FLDigi/PTT safety hold
prevented immediate reconstruction, leaving the control bar without a usable
intent/readback pair.

MainWindow now records whether FIO itself committed a sustained inactive,
hidden, or suspended transition. Only that evidence authorizes
`SchedulerEngine.handle_resume()`. Initial window activation still resumes UI
timers, reactivates children, and refreshes the visible page, but cannot erase
startup scheduler state. Genuine background/resume retains the existing full
safety recomputation. Endpoint readback is additionally purpose-scoped so an
already-running endpoint's in-flight liveness request cannot be reused as the
fresh post-command verification or later downgrade that evidence.

The high-reasoning primary GPT-5 model owned log correlation, lifecycle and
endpoint-concurrency implementation, regression coverage, specification, and
exit review. Delegation was unavailable under the active execution contract,
so no delegated diff was integrated. Deterministic event/barrier tests avoid
timing-only assumptions. The focused lifecycle and P1 endpoint-verification
partition passes **53 tests**; the broader routing, endpoint-lane,
manual-control, executor-bounds, UI-responsiveness, and navigation partition
passes **86 tests**. Python compilation and `git diff --check` are final exit
gates below. No runtime configuration/database write, endpoint command,
application restart, commit, or push was performed.

## 2026-09-14 — Expect View to FIOSpotter Compose freeze remediation

Status: implementation complete; automated qualification passed and operator
reproduction remains pending.

The supplied application/performance logs and nine CPU hotspot snapshots cover
both the failed Expect-to-Compose handoff and a successful post-restart retry.
No snapshot captured a Python lock deadlock and the UI heartbeat continued, but
the process repeatedly consumed approximately one CPU core inside Qt's native
event loop. The failed transition overlapped first construction of Messages /
Compose, signature verification, message projection, and periodic VarAC
runtime-profile discovery. This evidence identifies a native geometry/event
storm plus serialized UI work rather than a single blocking Python frame.

The Compose resize handler was forcing a geometry refresh even when the
responsive mode and coarse viewport signature had not changed. That pass
mutates splitter limits, size policies, and child geometry, which can generate
another native resize and sustain the same forced cycle. Resize and pop-out
workbench callbacks now use signature de-duplication; explicit semantic mode
changes retain a bounded forced pass. A running-pass fence accepts one newer
signature while discarding same-signature feedback.

Expect View handoff now batches mode selection, saved-source selection, form
rebuild, decoded values, radio selection, and preview derivation as one logical
Compose update. Intermediate preview requests collapse to one successful final
render. Every exit path releases loading and batching guards. A failed handoff
is logged and leaves the Compose surface active with a warning that directs the
operator to retry View or start a new draft. Saved-source selection is invoked
inside the navigation call stack so malformed payload failures reach that
recovery boundary instead of escaping through Qt signal dispatch.

The hotspot series also showed the five-second VarAC timer repeatedly building
multi-rig runtime status and reading SQLite on the GUI thread. Timer eligibility
checks are now cache-only. Initial and settings-triggered eligibility discovery
runs on the bounded realtime worker and publishes a Boolean snapshot back to
the controller thread before changing cadence or scheduling vault work.

Acceptance evidence: the focused Compose/Expect/workbench partition passes
**38 tests**, including new convergence, one-preview transaction, guard-release,
and visible recovery checks. The background-ingest/adaptive-cadence partition
passes **25 tests with 1 platform skip**. Broader UI and projection gates plus
the final implementation pass add **91 Compose/send-contract tests**, **26
first-render and UI-geometry tests**, and **157 message-ingest/projection
tests**, all passing. A further **27 UI/scheduler responsiveness-contract
tests** pass. Python compilation and `git diff --check` are final exit checks
below.
No application restart, runtime database mutation, endpoint command, commit, or
push was performed.

## 2026-09-15 — Message Inbox and FIOSpotter Activity consolidation

Status: implementation complete; automated gate passed; native-platform visual
qualification remains operator-assisted.

Message Inbox is now the single operational traffic surface. FIOSpotter
Activity no longer runs or renders a duplicate catalog; its compatibility page
routes directly to Inbox Spotter focus or Inbox All, while FIOSpotter retains
ownership of Watches, Expect, policies, Forms, and Imports. Inbox All separates
Source from Kind, reports relative Age, recognizes JS8 `RRSR` traffic as a
CommStat status receipt, and presents human meaning before technical provenance.

The cached reader adds stable Map, Operator, Reply, and Add to Watch actions.
Add to Watch stages an unsaved, source-neutral rule in FIOSpotter. The compiled
watch model accepts Source and Kind conditions and the projection coordinator
supplies those canonical fields on its existing bounded background path; no
schema migration or GUI-thread history scan was introduced.

Operational table columns now autofit the bounded retained content using active
font metrics and semantic caps. Categorical fields remain compact, narrative
content receives surplus width, and a repeated CommStat Kind cannot stretch
across a maximized screen. Fit requests are idempotent and coalesced and use no
source, filesystem, database, endpoint, or network access.

Primary Codex GPT-5 with high reasoning owned architecture, implementation,
integration review, focused tests, and documentation. The active execution
contract prohibited spawning new subagents, so no delegated diff was integrated.
The integrated affected partition passes **407 tests**; the established
process-isolated Compose/projection/performance partition passes **94 tests**.
Changed modules compile and `git diff --check` passes. A monolithic repository
run encountered a native JS8/Qt worker process abort in a Compose test that
passes in the isolated partition; no gate was silently waived. Native Linux and
macOS Light/Dark, Normal/Large Text, 1920x1080, 1000x700, and 900x560 visual
review remains operator-assisted. No runtime state, endpoint, commit, or remote
repository was changed.

## 2026-09-15 — Inbox first-frame and Spotter vocabulary follow-up

Status: implementation complete; automated qualification passed; native first-
launch confirmation remains operator-assisted.

The operator approved the consolidated Inbox but reported one first-render
`swipe & vanish` event. The Inbox now installs its bounded, font-measured table
profile before the first visible frame and applies header resize modes plus
fixed widths as one update-suppressed publication. It does not add source I/O,
parsing, model resets, event pumping, or resize-time work.

The obsolete FIOSpotter Activity page and its traffic-query UI were removed;
Watches is prebuilt as the stable default page and performs its bounded store
read only when FIOSpotter is activated. The Watch editor uses a font-derived
minimum instead of a long-placeholder size hint so its wide layout remains
table-dominant.

Inbox vocabulary now exposes `Spotter` rather than the legacy `SitRep` storage
family. Historical `sitrep` projections are still queried for Spotter and
CommStat scopes, then separated by cached semantic evidence so a CommStat row
cannot also match Spotter. No database migration or stored source identity was
changed.

Acceptance evidence: **441 integrated Inbox/Spotter/source/first-render tests**
and **49 focused GUI-soak, production-hotpath, asynchronous UI, and projection
performance tests** pass. Changed Python modules compile and `git diff --check`
passes. No application restart, production-state mutation, endpoint command,
commit, or remote push was performed.

## 2026-09-15 — FIO Spotter section-selector consistency

Status: implementation complete; automated qualification passed; native visual
confirmation remains operator-assisted.

The centered document-style FIO Spotter tabs are replaced by a left-aligned,
wrapping, mutually exclusive chip selector. Its normal, selected, hover, focus,
and disabled treatments come from the shared theme helper also used by Compose;
Light and Dark therefore cannot acquire separate local palettes. Item and
container heights derive from the active font, complete labels do not elide,
compact widths wrap without scrollbars, and keyboard or programmatic selection
remains synchronized with the hidden lazy page stack.

The selector's resize, font, and theme paths are bounded geometry/paint work.
They do not rebuild pages, refresh stores, pump events, or perform database,
filesystem, endpoint, device, process, or network work. Focused Spotter,
theme/geometry, and embedded/pop-out Compose qualification passes **82 tests**.
Changed Python modules compile and `git diff --check` passes. No application
restart, runtime-state mutation, endpoint command, commit, or remote push was
performed.

## 2026-09-16 — Inbox row-action paint and shared-theme correction

Status: implementation complete; automated and offscreen visual gates passed;
native operator confirmation remains pending.

The operator screenshot showed native rectangular View/Delete buttons painted
over duplicated fallback action text. The model supplied a visible
`View · Flag · Delete` string, the delegate cleared a copied style option, and
then the base delegate reinitialized that option from the model before painting.
Native push buttons were subsequently drawn on top, producing overlapping text
and platform-inconsistent chrome.

The action model now returns no display fallback for the delegate-owned column.
The delegate paints the item background exactly once and draws direct View,
Flag, Relay, BBS, Archive, and Delete actions with centralized shared-theme chip
colors and font-derived metrics from `theme.py`. The action column and row floor
use the same metrics as the painted rectangles, so hit targets and visuals stay
aligned at Normal and Large Text. Hover detection is bounded to the retained
row snapshot and performs no source, database, filesystem, endpoint, or network
work.

Primary Codex GPT-5 with high reasoning owned the renderer/model correction,
shared component addition, integration review, tests, and documentation. The
active execution policy prohibited spawning a subagent without an explicit
delegation request, so no delegated diff was produced. Focused Inbox action,
shared-theme/font geometry, and responsive-shell qualification passes **142
tests**. The complete process-isolated Inbox/projection/Spotter/performance
partition passes **496 tests**. A combined macOS offscreen process reproduced
the repository's native Qt/worker lifecycle segfault during reader paint
settlement; all 19 reader tests pass individually in clean processes. Offscreen
Light/Normal and Dark/Large renders show one clean label per chip with no overlap
or native button chrome. Changed modules compile and `git diff --check` passes.
No runtime state, endpoint, commit, or remote repository was changed.

## 2026-09-16 — Source-aware Inbox icon actions

Status: implementation complete; automated qualification passed; native
operator confirmation remains pending.

The compact action renderer now uses theme-colored eye, flag, Relay, Archive,
and trash iconology with explicit `+BBS`/`-BBS` text where a generic symbol
would be ambiguous. Delete is neutral at rest and exposes its danger role on
hover before the existing confirmation. Tooltips name every icon action.

Eligibility is now derived from the bounded row and cached external references,
not from readiness probes in paint. Projected FLMsg/FLAMP rows therefore retain
Flag and expose Managed BBS plus applicable Relay actions. BBS and Relay state
can be reversed without deleting the received source; BBS removal keeps its
explicit confirmation. Projection-backed flags persist through the unified
projection store, while source-native rows retain their existing flag stores.

The delegate no longer calls BBS database discovery, Relay parsing, source
lookup, or filesystem existence checks while painting, resizing, or hovering.
Focused action/theme/projection qualification passes **32 tests**. The broader
Inbox, projection, source, theme, layout, and shell partition qualified **613
tests** after correcting the compose explicit-floor property exposed by that
gate. All **20** reader tests pass in fresh processes; the known macOS Qt
worker/widget teardown fault can still crash a combined offscreen process.
Light/Normal and Dark/Large Text offscreen renders show non-overlapping icons,
legible `+BBS`/`-BBS`, active state, and neutral-at-rest trash treatment.
The Actions cell has no section-wide tooltip; guidance appears only for the
specific icon under the pointer.
Changed Python modules compile and `git diff --check` passes. No runtime state,
endpoint, commit, or remote repository was changed.

## 2026-09-16 — One-session SDR++ receiver setup

Status: implementation gate passed; live RTL-SDR/SDR++ qualification remains
operator-assisted and is required before the hardware combination is labeled
FIO-verified.

The receive-only wizard now performs its reversible SDR++ RigCTL qualification
before the first profile save. It skips the empty conventional Software step,
keeps the receiver application, adapter, target, endpoint, verification result,
and tuning opt-in in one scan path, and tells the operator to continue through
Review after a pass. The result is persisted only when Save Radio completes;
Cancel creates no profile or evidence.

Draft qualification uses a bounded opaque request ID through the shared
receiver endpoint lane. Profile ID zero remains a draft rather than a database
identity. MainWindow tracks pending work by request ID, every lane outcome
reattaches that correlation, and the dialog accepts only its current request.
Late, duplicate, stale, malformed-ID, negative-ID, timeout, and superseded
results cannot update newer evidence. Existing saved-profile validation,
readback requirements, exact host/port/target evidence matching, receive-only
adapter isolation, and the absence of PTT/transmit capability remain unchanged.

Saved observer profiles now expose a direct **Receiver Setup…** action and a
receiver-specific application/control chip. **Receiver Setup: Test Control**
opens the receiver connection card directly. Observer profiles no longer claim
that conventional radio apps are required, while ordinary transceivers retain
their existing Apps warning and software-administration behavior. All visual
treatment uses shared theme button roles and existing font-derived layout
helpers.

Work packages and model ownership:

- Primary Codex GPT-5, high reasoning: workflow architecture, concurrency and
  stale-result review, UI integration, specification/work-log reconciliation,
  delegated-diff review, and final exit gate.
- `gpt-5.6-terra`, high reasoning: bounded receiver-qualification coordinator,
  MainWindow correlation integration, and focused concurrency tests.
- `gpt-5.6-luna`, medium reasoning: bounded UI/test-design audit covering the
  one-session path, saved-observer route, and transceiver non-regression.

Acceptance evidence: the focused guided receiver, qualification coordinator,
MainWindow correlation, guided setup, and radio-profile UI partition passes
**245 tests**. The broader receiver-control, scheduler-lane, SDR compatibility,
SDR++ adapter, guided setup, and radio-profile partition passes **292 tests**.
Changed modules compile and `git diff --check` passes. Native Light/Dark,
Normal/Large Text, compact-window, and live RTL-SDR/SDR++ verification remain
operator-assisted. No runtime database, endpoint, commit, or remote repository
was changed by the automated gate.

## 2026-09-16 — Receive-only SDR++ software launch during guided setup

Status: implementation exit gate passed; live Linux/Windows/macOS launch and
RTL-SDR/SDR++ qualification remain operator-assisted acceptance items.

The observer workflow now includes a dedicated **Receiver Stack** step. The
operator can choose SDR++ and opt into **Launch this receiver application with
FIO** while creating or editing the receiver. Selection proposes a portable
launch target (`sdrpp`, `sdrpp.exe`, or `open -a SDR++`), while retaining a
browseable override for packaged executables and application bundles. The
ordinary FLRig/FLDigi/FLMsg/FLAmp/JS8/VarAC checklist remains absent from the
observer path.

Launch intent remains dialog-local until Save Radio. FIO saves the observer
profile first, then writes the approved launch item to the existing radio-scoped
launch-bundle store using the real profile ID. Cancel writes nothing. Bundle
failure reports that the profile was saved and instructs the operator to reopen
Receiver Setup; it is never reported as complete. Existing observer bundles
reload into the guided controls.

The runtime boundary is capability-enforced. Observer launch rows carry
`execution_scope=receive_only`; the planner rejects conventional or unapproved
items for observer profiles. The initial allowlist contains SDR++ only, and its
target validator rejects an approved label paired with an arbitrary executable.
Receiver items add no PTT, transmit, scheduler, radio-control, or conventional
app dependencies. Test control remains independent of launch ownership and
continues to work with an operator-started SDR++. PATH-resolved and platform
launcher commands use cached SDR++ process tokens, preventing duplicate launch
and false readiness timeouts without adding a UI-thread process scan.

No database schema migration was required. The existing readiness JSON extension
stores the receive-only execution scope. Conventional transceiver launch
planning and default launch catalogs remain unchanged.

Work packages and model ownership:

- Primary Codex high-reasoning model: architecture and safety boundary,
  integration/persistence, arbitrary-command hardening, specification and work
  log, delegated-diff review, and final acceptance gate.
- `gpt-5.6-terra`, high reasoning: Qt-free receive-only launch contract,
  launch-bundle serialization, planner enforcement, SDR++ discovery/readiness,
  and focused core tests.
- `gpt-5.6-luna`, medium reasoning: bounded Receiver Stack guided UI, responsive
  shared-theme controls, staging metadata, and focused UI coverage.
- `gpt-5.6-terra`, medium reasoning: independent persistence, capability,
  readiness, and migration audit.

Acceptance evidence: the core launch, persistence, discovery, process status,
planner, receiver adapter, and concurrency partition passes **212 tests** with
**4 environment-dependent skips**. The guided receiver UI, radio-scoped
software settings, launch persistence, and guided setup partition passes **242
tests**. Changed Python modules compile and `git diff --check` passes. No runtime
database, receiver endpoint, commit, or remote repository was changed by the
automated gate.

## 2026-09-16 — Unified guided radio and software configuration specification

Status: specification exit gate passed; implementation has not started.

`guided_radio_software_configuration_spec.md` now defines one radio-first setup
contract for transceivers and receive-only SDRs. It makes application identity
an atomic bundle, brings JS8Call and Fast Light under the same
create/import/manual lifecycle, permits enforced receive-only Fast Light for an
SDR, and requires all selected transceiver software—including standalone and
Cluster VarAC—to use the shared guided software-instance workflow. Every
configured external application must finish with an exact launch recipe or an
explicit operator-start state.

The specification replaces the old observer `RF Guard / Schedule N/A` policy
with Receiver Guard and Receive Schedule while preserving the no-PTT/no-transmit
boundary. It defines separate SDR++ receiver-control and companion-application
cards, conditional native writers with preview/backup/readback/restore,
resumable fallback when a writer is unavailable, atomic save/recovery,
single-snapshot discovery, structured performance telemetry, measurable GUI
responsiveness, and sequential GRS-0 through GRS-5 implementation gates.

Work packages and model ownership:

- Primary high-reasoning model: authority reconciliation, workflow and durable
  identity architecture, concurrency/performance contract, persistence and
  recovery boundaries, delivery slices, and final integration review.
- `gpt-5.6-terra`, high reasoning: Fast Light current-state and receive-only
  capability audit.
- `gpt-5.6-terra`, high reasoning: VarAC node/cluster ownership, launch, safety,
  persistence, and history audit.
- `gpt-5.6-luna`, medium reasoning: guided-workflow wording, responsive UX,
  error/recovery, and acceptance-test review. The primary review rejected its
  recommendation to skip observer Guard/Schedule because the approved product
  decision and existing receiver scheduler architecture require those steps.

Evidence: the new specification and related authority links were reviewed for
conflicts; Markdown whitespace and repository diff checks pass. No Python,
database schema, runtime configuration, native application profile, endpoint,
commit, or remote repository was changed in this specification-only slice.

## 2026-09-17 — Guided radio/software configuration GRS-0 authority model

Status: GRS-0 implementation exit gate passed; GRS-1 discovery/proposal work
has not started.

Added the Qt-free `guided_radio_software_model.py` authority layer. It defines
the reviewed observer/transceiver capability matrix, canonical software-family
and persisted-role adapters, immutable guided selections, complete atomic
instance bundles, exact launch identity plus separate startup/health/readiness
policy, source versus management ownership, collision-safe endpoints and
resources, imported-bundle identity locking, and a fail-closed native-writer
capability registry. FIO Spotter is modeled as built-in rather than an external
launch item. Observer policy grants no transmit/PTT/transmit-schedule authority,
rejects VarAC/Cluster/FLRig, and requires a reviewed external-TX-disabled claim
for receive-only JS8, Fast Light, and CommStat bundles where applicable.

The module imports only Python standard-library modules and performs no UI,
filesystem, process, socket, database, endpoint, radio, discovery, planner, or
writer action. No schema, migration, persistence adapter, runtime setting,
external profile, or launch behavior changed in this slice. Existing durable
manifests and launch/store records remain authoritative until their later
sequential adapters pass the applicable exit gates.

Work packages and model ownership:

- Primary high-reasoning model: architecture, capability and atomicity
  reconciliation, management/source and identity-policy separation, delegated
  diff review, specification/work-log update, and final integration gate.
- `gpt-5.6-terra`, high reasoning: bounded pure-model implementation.
- `gpt-5.6-luna`, medium reasoning: focused capability, atomic-bundle,
  immutability, writer-registry, and boundary tests.
- `gpt-5.6-terra`, medium reasoning: read-only compatibility audit across the
  current stores, manifests, launch planner, guided setup, receiver paths, and
  VarAC boundaries.

Acceptance evidence: the focused GRS-0 contract suite passes **20 tests**. The
adjacent manifest/persistence, launch, guided setup, managed JS8, receiver
control, SDR core, and VarAC partitions pass **169 tests**, for **189 passing
tests** in the slice gate. The new module and tests compile, and repository diff
checks pass. No runtime database, radio, application, endpoint, commit, or
remote repository was changed by the automated gate.

## 2026-09-17 — Guided radio/software configuration GRS-1 discovery and proposals

Status: GRS-1 implementation exit gate passed; GRS-2 unified guided UX has not
started.

Added one Qt-free guided discovery coordinator with immutable request and
result snapshots, bounded concurrent phases, coalescing, cache reuse,
generation/draft stale-result rejection, cancellation, and structured timing
telemetry. Read-only source adapters now scan JS8 profiles once per request,
reuse those profiles for path projection, avoid repeated Fast Light application
searches, and treat saved receiver settings as evidence without contacting an
endpoint. Settings Add Radio and explicit Software Auto-Fill use the same
coordinator from worker threads. The legacy Multi-Rig preview scan also moved
off the GUI thread.

The pure proposal layer defines complete imported identity bundles,
deterministic collision/resource planning, and new-instance plans for JS8,
Fast Light, receiver, and VarAC workflows. Existing JS8 API port `2442`
therefore proposes `2443` only as part of a distinct profile/data/message/form
and launch identity; an imported profile remains unchanged as one locked
bundle. Discovery remains evidence only and performs no save, ownership,
launch, socket, radio, process-control, or native-configuration write.

Work packages and model ownership:

- Primary high-reasoning model: concurrency architecture, coordinator and
  scanner-source integration, performance/safety review, Settings integration
  review, legacy-test migration, specification/work-log reconciliation, and
  final exit gate.
- `gpt-5.6-terra`, high reasoning: bounded pure proposal and resource-planning
  model.
- `gpt-5.6-terra`, medium reasoning: Settings worker integration and a separate
  discovery/compatibility audit.
- `gpt-5.6-luna`, medium reasoning: focused coordinator, source-adapter,
  cancellation, stale-result, and projection tests.

Acceptance evidence: the focused discovery/proposal/authority/performance
partition passes **43 tests**. The adjacent Settings Auto-Fill, guided-radio,
radio-scoped software, config discovery, Software Administration, receiver
launch, and VarAC partitions pass **264 tests**, for **307 passing tests** in
the slice gate. Changed Python and test modules compile and `git diff --check`
passes. No runtime database, native profile, receiver endpoint, radio, commit,
or remote repository was changed by the automated gate.

## 2026-09-17 — Guided radio/software configuration GRS-2 unified UX

Status: GRS-2 implementation exit gate passed; GRS-3 family completion has not
started.

Add Radio now keeps the approved seven step positions visible for every role:
Radio, Operating Model, Software, Connections, Safety, Schedule, and Review &
Save. Observer content is expressed as Receiver Guard and Receive Schedule;
transceiver content remains RF Guard and Radio Schedule. The workflow retains
operator edits while moving among steps, uses one responsive vertical scroll
owner, exposes keyboard and accessibility names, and presents role-aware
responsibility, permission, endpoint, launch, file, Guard, and Schedule review
cards. Existing transceiver operating-model selection is preserved.

Selected JS8Call, Fast Light, and VarAC families open the authoritative
Software Instance Assistant against an opaque unsaved-radio owner key. The
assistant therefore provides the same source, atomic identity, connection,
file, launch, conflict, and review language used by Software Administration
without inventing a negative or otherwise fake persisted radio ID. Its result
returns to the Add Radio draft only. App configuration is preview-only inside
the dialog, Cancel performs no save or external write, and final persistence
remains reserved for the reviewed transaction slices.

The dialog takes one Frequency Plan snapshot and reuses it while the operator
navigates. Identical draft/plan validation is memoized, removing repeated plan
queries and projections from ordinary step changes. Discovery remains on the
shared GRS-1 worker coordinator. Shutdown waits added for these workers are
bounded, cancel-aware, and explicitly tracked by the responsiveness contract.

Work packages and model ownership:

- Primary high-reasoning model: stable wizard and role-content architecture,
  unsaved-owner Software Instance Assistant contract, transceiver regression
  repair, plan-snapshot performance integration, delegated-diff review,
  specification/work-log reconciliation, and final exit gate.
- `gpt-5.6-terra`, medium reasoning: bounded guided UI implementation for the
  seven steps, responsibility cards, role-aware Guard/Schedule pages, complete
  Review, and the preview-only Software Administration handoff.
- `gpt-5.6-luna`, medium reasoning: focused operator-visible, responsive,
  accessibility, Cancel, observer, transceiver, and shared-assistant tests.
- `gpt-5.6-terra`, medium reasoning: independent read-only architecture,
  persistence, scheduler, and performance audit; its duplicate-editor finding
  was closed by integrating the authoritative assistant before the gate.

Acceptance evidence: the guided workflow, Software Instance Assistant,
observer/transceiver, radio-scoped settings, receiver launch, Auto-Fill, and
legacy-compatibility partition passes **326 tests**. The adjacent Software
Administration layout, theme/font geometry, runtime theme propagation, first
render, responsiveness, and performance contracts pass **79 tests**, for
**405 passing tests** in the slice gate. Changed Python modules compile and
`git diff --check` passes. No runtime database, native profile, external
application, radio, endpoint, commit, or remote repository was changed by the
automated gate.

## 2026-09-17 — Guided radio/software configuration GRS-3 family completion

Status: GRS-3 implementation exit gate passed; GRS-4 native writers and launch
integration has not started.

Completed the family-specific behavior behind the shared guided workflow.
JS8Call create/import/manual plans preserve one atomic app/profile/API/message
identity, and imported rows remain source-locked. Fast Light defaults to
receive-safe; observer profiles can persist only a reviewed FLDigi-led
receive-only bundle and receive no FLRig launch item, CAT/PTT capability, or
transmit authority. Transceiver Advanced TX requires an explicit reviewed
acknowledgement in durable manifest evidence. Direct store writes enforce the
same observer restrictions.

VarAC now has pure standalone, create-cluster, and join-cluster plans plus an
atomic persistence path for node, manifest, radio link, launch identity,
cluster, membership, and optional gateway. Case-insensitive cluster IDs,
duplicate active member numbers, stale replacement ownership, shared/local
database aliasing, and launch/resource collisions fail before commit or roll
back the complete change. Settings -> Software now passes the selected radio
role to the authoritative assistant and uses the same observer and cluster
safety boundaries as Add Radio. Add Radio cannot complete a newly selected
JS8Call, Fast Light, or VarAC family until its reviewed instance draft exists.

Work packages and model ownership:

- Primary high-reasoning model: store transaction and direct-persistence
  architecture, Add Radio and Software Administration integration, delegated
  diff review/corrections, normalized VarAC resource enforcement,
  specification/work-log reconciliation, and final exit gate.
- `gpt-5.6-terra`, high reasoning: Qt-free JS8Call/Fast Light completion policy,
  observer receive-only validation, and Advanced TX acknowledgement model.
- `gpt-5.6-terra`, high reasoning: pure VarAC standalone/create/join planner,
  ownership, launch identity, membership, replacement, and collision policy.
- `gpt-5.6-luna`, medium reasoning: independent GRS-3 family acceptance suite.

Primary review corrected the observer Fast Light legacy-port compatibility
adapter, role propagation from Software Administration, editable VarAC cluster
placeholder handling, observer startup-profile validation, exact VarAC working
directory persistence, create-cluster transaction wiring, and normalized
shared-database collision enforcement.

Acceptance evidence: the combined GRS-0 through GRS-3 authority, discovery,
proposal, guided UI, Software Administration, family, store, manifest, launch,
observer/transceiver, responsiveness, and adjacent regression partition passes
**438 tests**. Changed Python modules compile and `git diff --check` passes. No
runtime database, native application profile, receiver/radio endpoint, commit,
or remote repository was changed by the automated gate.

## 2026-09-17 — Guided radio/software configuration GRS-4 native writers and launch integration

Status: GRS-4 implementation exit gate passed; GRS-5 scheduler, Guard,
integration, and live qualification has not started.

Added an exact native-writer registry for supported JS8Call 2.2.0, Improved
3.0.3, and Subspace 4.1.0 create/update operations on Linux, macOS, and
Windows. A writer qualifies only when family, variant, version, platform,
operation, and explicit `.ini` target match. Final Add Radio and Software
Administration saves run backup, apply, exact readback, and restore on a Qt
worker rather than the GUI thread. Unsupported or incomplete writer evidence
leaves the native file unchanged, saves an operator-action recovery state, and
does not claim FIO changed third-party configuration. Post-write failures and
later FIO persistence failures restore the exact backup target, including
removing a file that did not exist before the apply.

Launch planning now retains executable, command, arguments, working directory,
safe environment/profile selector, configuration paths, dependencies,
readiness, monitor policy, execution scope, known-recipe state, startup
inclusion, bundle enablement, and explicit operator-start state. Manual and
startup launch use the same StationLaunchPlanner with only radio scope changed.
Shared applications deduplicate only when their durable launch identity and
effective endpoint identity match. VarAC executes in its reviewed per-node
working directory. Primary integration review also found and corrected a gap
where guided Fast Light adoption could lose selected FLMsg/FLAmp identities;
those selections now persist and appear in launch review as explicit
operator-start components when no exact recipe is assigned.

Work packages and model ownership:

- Primary high-reasoning model: native-writer registry and transaction
  architecture; exact backup/readback/restore; background Settings apply and
  recovery integration; durable writer evidence; JS8 variant/version UX;
  Fast Light component persistence correction; delegated-diff review;
  specification/work-log reconciliation; and final exit gate.
- `gpt-5.6-terra`, medium reasoning: bounded launch-planner/orchestrator
  implementation covering exact launch identity, review states, dependency
  planning, manual/startup unification, environment/cwd execution, and focused
  launch tests.
- `gpt-5.6-luna`, medium reasoning: independent native-writer and launch
  acceptance coverage, including fault injection, platform command handling,
  startup/operator policy, and dependency cases.

Primary review corrected the delegated acceptance fixture so native-write tests
use exact qualification metadata, implemented the rollback seam identified by
its fault injection, tightened unsupported-writer no-mutation assertions,
reviewed the launch diff line by line, and added the missing FLMsg/FLAmp
adoption-to-review contract.

Acceptance evidence: the combined GRS-0 through GRS-4 authority, discovery,
proposal, guided UI, Software Administration, family/store/manifest, native
writer fault-injection, exact restore, launch identity, observer safety,
startup/manual planning, receiver, JS8 storage/launch, responsiveness, and
adjacent regression partition passes **505 tests**. Changed Python and test
modules compile and `git diff --check` passes. Automated tests changed no
runtime database, real native application profile, receiver/radio endpoint,
commit, or remote repository.

## 2026-09-17 — Guided radio/software configuration GRS-5 scheduler, Guard, and integration

Status: GRS-5 automated implementation exit gate passed. Operator-assisted
external application, platform, and radio evidence remains explicitly pending;
it is not inferred from automated tests.

Receiver Guard now persists observer shared antenna and front-end claims into
the same pair-scoped coordination graph used by the scheduler. The policy is a
hard hold for an automatic observer retune, because an unattended receiver
lane cannot stop for an interactive RF prompt. It affects only the radios in
the declared pair; unrelated endpoint lanes continue independently. Amplifier
claims remain transceiver-only and no observer PTT or transmit authority was
introduced.

Receive Schedule now lists only compatible receive-only plans for observers,
returns the selected plan from final Save, and assigns it through the existing
schedule store/coordinator. A verified exact receiver identity can retune;
manual, failed, missing, or changed evidence remains reminder-only. Receiver
Guard, Receive Schedule, Review, and post-save recovery use one cache-only
five-state vocabulary. Endpoint operational summaries now carry exact
receiver hold/failure reasons and recovery actions through Station Overview.

Work packages and model ownership:

- Primary high-reasoning model: scheduler/Guard architecture, observer policy
  persistence, pair-scoped fail-closed arbitration, recovery telemetry,
  delegated-diff review and corrections, adjacent regression integration,
  release/specification reconciliation, and the final exit gate.
- `gpt-5.6-terra`, medium reasoning: bounded guided Receiver Guard / Receive
  Schedule UI, role-compatible plan filtering, accessible state cards, and
  recovery presentation. Primary review corrected intentional-manual recovery
  wording and missing-evidence classification.
- `gpt-5.6-luna`, medium reasoning: focused receiver qualification, safety,
  cancellation, and live-gate acceptance tests. Primary review replaced its
  generic endpoint guardrail-only coverage with actual Receiver Guard
  persistence, runtime arbitration, no-endpoint-on-hold, and recovery-summary
  coverage.

Acceptance evidence: the combined GRS-0 through GRS-5 authority, discovery,
proposal, guided UI, Software Administration, family/store/manifest, native
writer, launch, receiver scheduler, cross-radio Guard, runtime recovery,
responsiveness, and adjacent regression partition passes **600 tests**.
**Eight tests are explicitly skipped live gates**: RTL-SDR/SDR++ reversible
control, real shared-resource arbitration, macOS/Linux guided setup, Windows
guided setup, a physical transceiver/backend, live Fast Light, stock/Improved/
Subspace JS8, and a multi-node VarAC Cluster. Changed Python modules compile and
`git diff --check` passes. Automated tests changed no runtime database, native
application profile, external application, radio/receiver endpoint, commit, or
remote repository.

## 2026-09-17 — Existing-station guided-flow operator qualification review

Status: live qualification failed safely because the operator canceled before
configuration. Release remains blocked pending GRS-6 implementation and a repeat
of the exact existing-station TriMode route.

Observable report: from `Settings -> Radios -> Add Radio`, the operator chose a
TriMode transceiver with Fast Light, JS8Call, FIO Spotter, CommStat, and VarAC,
then used Configure Automatically and entered Software Administration. The flow
mixed new-instance intent with existing-station evidence: it selected an
existing JS8 profile/endpoint and later showed the existing settings/data paths;
Fast Light appended its family name and exposed unresolved profile/launch work;
CommStat appeared to require duplication; FIO Spotter exposed an MCF folder;
and existing VarAC made cluster intent ambiguous. Step 2 also showed a
schedule-like Operating Model name and redundant receive-only wording.

Primary review found that the pure atomic proposal model is not connected to
the production Add Radio path. Add Radio hand-builds loose drafts, omits the
existing-instance inventory supplied by Software Administration, and can defer
collision and completeness checks until persistence. This invalidates the
earlier live-readiness inference even though the pure-model automated suites
passed.

Specification decisions:

- operator-facing roles are `Transceiver` and `Receive-only SDR`; Step 2
  describes FIO behavior and never uses a schedule-like model name;
- `Create a distinct instance` may reuse a qualified binary/recipe but always
  creates a stable radio-name-derived identity, dedicated profile/data/message
  roots, collision-free TCP/UDP claims, manifest, and launch identity;
- imported bundles are source-locked; identity edits require `Clone as
  distinct`;
- qualified JS8 and Fast Light recipes hide custom launch fields from the
  normal flow and show the resolved effective commands;
- FIO Spotter resolves its built-in MCF catalog; CommStat is one shared process
  with explicit per-JS8 bindings; additional VarAC defaults to standalone and
  cluster mode is opt-in; and
- final Save consumes the reviewed bundle fingerprint and inventory generation
  or fails before mutation.

Work packages and model ownership:

- `gpt-5.6-sol`, high reasoning: operator-feedback interpretation, architecture,
  configuration/transaction safety, cross-family decisions, specification,
  release-checklist reconciliation, and final integration review.
- `gpt-5.6-terra`, medium reasoning: read-only JS8/Fast Light production-path,
  profile/root/port, launch-recipe, and acceptance-gap audit.
- `gpt-5.6-luna`, medium reasoning: read-only CommStat/VarAC/FIO Spotter/MCF,
  label-consistency, and acceptance-gap audit.

No code, runtime database, native application profile, endpoint, radio, or user
configuration was changed. Documentation checks for the new contract are the
only gate in this review slice; implementation and automated acceptance begin
with GRS-6 package 1.

## 2026-09-17 — GRS-6.1 authority and inventory

Status: automated exit gate passed. Add Radio and Software Administration now
use the same immutable saved-plus-retained inventory model. Unsaved instances
carry opaque, family-scoped draft keys that survive navigation and display-name
changes. Retained drafts reserve endpoints for later proposals in the same
transaction.

Imported instances are source-locked complete identities. The normal fields
cannot create a mixed old/new bundle; the operator must choose `Clone as a
distinct instance`, which keeps only safe executable/version evidence and
allocates a fresh identity and endpoints. Both production save paths re-read
the imported application and reject a missing or changed source fingerprint
before any mutation. The atomic store remains the final endpoint/resource
collision and rollback boundary.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: authority/inventory architecture,
  immutable fingerprint model, persistence-time source validation, atomic
  manifest preservation, delegated-diff review, performance correction,
  integration tests, and specification/work-log reconciliation.
- `gpt-5.6-terra`, medium reasoning: bounded assistant/Add Radio wiring,
  imported-source lock presentation, explicit clone interaction, retained
  draft inventory, and focused UI tests. Primary review corrected opaque key
  propagation and reduced Add Radio inventory loading to one manifest read.
- `gpt-5.6-luna`, medium reasoning: focused pure proposal/inventory acceptance
  tests for shared inventory, stable ownership, distinct identities, source
  locks, cancellation, and retained endpoint reservations.

Automated tests use temporary or in-memory state only. No runtime database,
native application profile, external process, endpoint, radio, commit, or
remote repository was changed. Dedicated JS8/Fast Light roots and qualified
launch recipes remain the next sequential GRS-6.2 slice.

## 2026-09-17 — GRS-6.2 JS8 and Fast Light managed recipes

Status: automated exit gate passed. Add Radio and Software Administration now
pass the same Settings-owned `managed-instances` root into the instance
assistant. The executable-search folder is no longer misused as a profile
root, and a missing managed root fails closed instead of producing relative
paths.

Known JS8Call recipes resolve one stable draft/profile identity, exact
`--rig-name`, distinct TCP/UDP endpoints, configuration/save/forms roots, and
the platform/rig-specific Qt application-data root. Native profile planning and
launch review now use the same opaque draft identity. Qualification is exact
for stock 2.2.0, Improved 3.0.3, and Subspace 4.1.0.478; another build remains
inactive with an Advanced recovery route. Fast Light resolves separate FLRig
and FLDigi roots, commands, dependencies, endpoints, readiness, and execution
scope. An observer receives FLDigi only; FLMsg/FLAmp remain explicit shared
utility components.

Qualified recipes hide their derived profile/data/custom-command fields from
the normal form and show the exact effective recipe in Launch and Review.
Unsupported or operator-managed flows retain the Advanced fields. The durable
manifest stores the reviewed recipe fingerprint, and atomic adoption projects
the same component commands into the launch bundle.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: recipe/path/version architecture,
  JS8 native-planner alignment, Settings managed-root integration, canonical
  qualification, persistence projection, delegated-diff review and correction,
  full integration gate, and specification/work-log reconciliation.
- `gpt-5.6-terra`, medium reasoning: bounded Launch/Review presentation,
  derived-field visibility, accessible navigation, horizontal-scroll guard,
  and focused UI tests. Primary review corrected the managed-root source and
  JS8 profile/data semantics before accepting the UI package.
- `gpt-5.6-luna`, medium reasoning: focused Settings/core integration tests for
  fail-closed roots, canonical version/platform alignment, draft-key native
  planning, and workspace-to-assistant root propagation. Primary review
  replaced a source-introspection assertion with a behavioral Qt test.

Acceptance evidence: the combined GRS-0 through GRS-6.2 guided authority,
discovery, proposal, Software Administration, family/store/manifest, native
writer, launch, receiver scheduler/Guard, inventory, recipe, responsive UI, and
adjacent regression partition passes **479 tests** with **eight explicitly
skipped live gates**. Focused integration tests pass 81 tests before the final
UI refinement and the delegated UI partitions pass 66 and 47 tests. Changed
Python modules compile and `git diff --check` passes. No runtime database,
native application profile, external application, endpoint, radio, commit, or
remote repository was changed.

## 2026-09-17 — GRS-6.3 supporting-family decisions

Status: automated exit gate passed. FIO Spotter now uses a packaged 30-form
station catalog by default across setup, compose, receive decoding, FIO Spotter,
JS8 NCS, and SOP. The radio flow no longer asks for an MCF folder or invents a
Spotter process. An optional custom catalog remains an explicitly Advanced
station override.

CommStat is presented and persisted as one station-shared process with one
binding per radio-owned JS8 endpoint. Launch rows use the same durable identity,
so planner deduplication produces one process with all radio bindings; removing
one binding preserves the others. VarAC starts as standalone and exposes Create
cluster and Join cluster only as explicit choices with Why guidance.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: supporting-family architecture,
  packaged-catalog resolver and runtime consumers, CommStat plan/persistence/
  launch integration, VarAC blueprint contract, delegated-diff review,
  integration corrections, packaging checks, and specification/work-log
  reconciliation.
- `gpt-5.6-terra`, medium reasoning: bounded Add Radio, Software
  Administration, supporting-family editor, CommStat binding, Spotter built-in,
  and VarAC intent UI plus focused real-widget tests.
- `gpt-5.6-luna`, medium reasoning: focused supporting-family core contracts,
  planner deduplication, binding preservation, bounded inventory, cancellation,
  and packaged-catalog acceptance tests.

Acceptance evidence: the combined GRS-6.3 UI/settings partition passes 357
tests; supporting-family/store/launch/Spotter partitions pass 87 tests, the FIO
Spotter UI passes 27 tests in isolation, and the message-ingest/Spotter/NCS
partition passes 76 tests with the SOP partition passing two. Changed Python
modules compile and diff checks pass. The monolithic pytest process can abort
when unrelated Qt widgets and background readers share one interpreter; the
same affected tests pass in isolated partitions, so project-standard isolated
partitions remain authoritative. No runtime database, native application
profile, external process, endpoint, radio, commit, or remote repository was
changed.

## 2026-09-17 — GRS-6.4 task-oriented role and behavior UX

Status: automated exit gate passed. Fresh Add Radio now offers only the exact
operator roles `Transceiver` and `Receive-only SDR`; existing legacy gateway
values are retained through a compatibility-only edit choice and cannot be
silently converted. Step 2 is `FIO Behavior`, with protected normal choices
shown as `Standard transceiver operations` and `Receive-only monitoring`.
Existing custom behavior names and durable Operating Model IDs remain intact.

The normal flow shows a resolved role, a capability-oriented behavior summary,
and a concise Why explanation. Schedule timing and Frequency Plan selection
remain in the Schedule step, and redundant receive-only suffixes are gone.
Qualified managed JS8Call and Fast Light recipes no longer ask the operator to
edit recipe-owned executable/profile/data/custom-command fields in Connections;
exact resolved component facts remain visible in Review. Unsupported recipes
retain a fail-closed Advanced recovery route.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: role/behavior architecture, protected
  fresh-default naming, compatibility and migration review, delegated-diff
  review, integration corrections, exit-gate execution, and specification/work
  log reconciliation.
- `gpt-5.6-terra`, medium reasoning: bounded Add Radio role/behavior labels,
  resolved/Why summaries, managed-recipe field visibility, compatibility-only
  legacy-role presentation, and focused real-widget tests.
- `gpt-5.6-luna`, medium reasoning: focused pure contract tests for exact roles,
  FIO Behavior/schedule separation, and qualified-versus-unsupported recipe
  behavior.

Acceptance evidence: the core/migration partition passes 126 tests; protected
built-in behavior/store and performance checks pass 15 tests; and the
independently rerun GRS-6.4 language, real-widget, responsive guided/settings,
SDR, and assistant partition passes 79 tests. Changed Python modules compile
and `git diff --check` passes. No runtime database, native application profile,
external process, endpoint, radio, commit, or remote repository was changed.

## 2026-09-17 — GRS-6.5 transactional Final Save

Status: automated exit gate passed; the operator-assisted TriMode/current-
station route remains an explicit release blocker.

Final Save now carries and revalidates the reviewed inventory generation and
fingerprint before native work and again after asynchronous native apply.
Software Administration applies the same fail-before-mutation check. Stale
reviews save nothing, restore an already-applied qualified native change, and
present the Software/Review recovery task.

Add/Edit Radio now executes radio, behavior assignment, software application,
manifest, launch, CommStat binding, schedule, activation, and receive-only
launch-bundle persistence within one explicit-completion SQLite transaction.
Nested store commits are deferred to the outer owner. Any early return,
injected later-family failure, receiver-launch failure, or commit exception
rolls back the full FIO change set. Qualified native changes continue to use
the existing backup/apply/readback/restore worker. No schema migration or
destructive data rewrite was introduced.

The Review page shows a compact inventory generation/reference. Final Save is
single-activation and paints progress before work begins; duplicate clicks are
ignored. The final fingerprint includes only selected retained software
drafts, avoiding a false stale-review failure after deselection.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: transaction/concurrency architecture,
  transaction-aware receiver launch persistence, pre/post-native inventory
  revalidation, failure/rollback integration, delegated-diff review,
  regression correction, exit-gate execution, and specification/work-log
  reconciliation.
- `gpt-5.6-terra`, medium reasoning: bounded reviewed-inventory card,
  accessible stale-review recovery presentation, nonblocking single-submit
  progress guard, narrow/large-font behavior, and focused real-widget tests.
- `gpt-5.6-luna`, medium reasoning: focused cancellation, native-failure,
  duplicate-submit, fingerprint-stability, and missing-revalidation acceptance
  tests. Primary integration closed the reported revalidation gap and added
  behavioral stale-inventory and atomic multi-write fault-injection coverage.

Acceptance evidence: focused GRS-6.5/store/UI/settings tests pass 27 tests; the
combined guided integration partition passes 293 tests; 56 selected guided,
multi-rig, receiver, SDR, schedule, launch, responsive, and performance files
collect 789 tests and pass when run in fresh project-standard processes; the
focused performance/responsiveness partition passes 75 tests. A monolithic Qt
run reproduced the already-documented GUI/background-thread process abort, so
isolated files remain authoritative. Python compilation and `git diff --check`
pass. Automated tests used temporary state and changed no runtime database,
third-party profile, external process, endpoint, radio, commit, or remote.

## 2026-09-17 — GRS-7 prepare-first specification reconciliation

Status: specification exit gate passed; implementation and operator-assisted
qualification have not begun. The release gate remains open.

Observable report: in `Settings -> Radios -> Add Radio`, Software Administration
and technical fields appeared before FIO's automatic discovery/preparation had
produced a concise plan. VarAC topology was presented after node details, Fast
Light technical output could push actions offscreen, and the production-shaped
station required a safe recommendation for one existing standalone VarAC node
with no cluster.

Read-only production evidence from
`/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db` confirmed one linked
FTDX-10 radio, one standalone VarAC node, no VarAC cluster or membership, eight
enabled JS8Call rows with only one linked, eight enabled Fast Light rows with
only one linked, duplicate legacy endpoint claims, and no software-instance
manifests. The review used SQLite read-only/immutable access and changed no
database.

Specification decisions:

- Step 3 now follows software/source choice, VarAC arrangement, explicit
  `Prepare selected software automatically`, compact prepared plan, and
  exception-only correction in that order.
- Add Radio remains radio-first. Standalone Software Administration remains
  software-first but consumes an Add Radio prepared plan without clearing it.
- With standalone VarAC node(s) and no cluster, create-cluster-with-existing is
  Recommended but never preselected; standalone remains explicit, and only final
  reviewed Save may mutate topology.
- Technical paths, commands, dependencies, fingerprints, and diagnostics begin
  collapsed under family-scoped `Show details`; safety, Why, confirmation,
  unsaved state, and existing-object impact remain visible.
- The guided surface has a fixed header/footer and exactly one body vertical
  scroll owner. Available work area and font metrics replace a fixed dialog
  assumption.
- Incomplete/unlinked application records are diagnostic-only, remain
  unchanged, retain normalized conservative resource claims, and require a
  separately specified maintenance workflow for cleanup.
- Empty manifests remain supported without querying operational history, and
  the production-shaped incomplete-record fixture is a binding GRS-7 gate.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: governing-contract review, workflow and
  VarAC architecture, persistence/concurrency and orphan-safety boundaries,
  cross-spec reconciliation, acceptance matrix, delegated-review integration,
  and final exit-gate decision.
- `gpt-5.6-terra`, high reasoning: independent task sequence, wording,
  progressive-disclosure, accessibility, responsive-layout, and contradiction
  audit. No edits were delegated.
- `gpt-5.6-luna`, high reasoning: read-only production database audit and
  production-shaped safety, performance, cancellation, empty-manifest, and
  acceptance-fixture review. No edits were delegated.

Files updated:

- `docs/internal/guided_radio_software_configuration_spec.md`
- `docs/internal/multi_instance_software_administration_spec.md`
- `docs/internal/ui_regression_work_log.md`

Acceptance evidence:

- `git diff --check`: passed.
- Contradiction scan for the superseded normative phrases `Additional VarAC
  nodes default to standalone` and `Cluster mode is an explicit branch after
  node configuration`: passed with no matches in the two governing specs.
- Focused atomic/cancel/UI contract partition:
  `tests/test_grs65_atomic_guided_store.py`,
  `tests/test_grs65_save_transaction_ui.py`, and
  `tests/test_guided_radio_unified_ux.py`: **19 passed**.

No application code, schema, runtime configuration, native profile, process,
endpoint, radio, git history, or remote repository was changed. GRS-7 package 1
is the next implementation slice and may not begin until explicitly requested.

## 2026-09-17 — GRS-7.1 prepare-first Add Radio sequence

Status: automated exit gate passed. The Add Radio Software step now leads with
capability/source intent and `Prepare selected software automatically` before
technical correction. Per-family completion, responsibility, launch, and
correction controls remain unavailable until preparation succeeds. VarAC
arrangement is an explicit pre-prepare intent and does not mutate topology.

Preparation stays on the Software step and uses the compact states `Discovery
in progress`, `Ready`, and `Needs attention`. Changes to selected families,
source, role, setup type, or VarAC arrangement invalidate a prepared plan as
`Stale — reprepare required`. Back/Next preserves draft choices. Technical
correction dialogs now use `<family> setup for <radio>` titles. The existing
generation/revision fence and dialog-close cancellation behavior remain in
place, and Cancel remains a no-write boundary.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: architecture and scope boundary,
  concurrency/stale-publication review, delegated-diff review, integration,
  exit-gate execution, and specification/work-log reconciliation.
- `gpt-5.6-terra`, high reasoning: bounded prepare-first UI, compact status and
  stale-plan presentation, source/arrangement invalidation, task-specific
  correction titles, and compatible UI contract updates.
- `gpt-5.6-luna`, high reasoning: focused ordering, hidden-technical-surface,
  collapsed-details, Back/Next preservation, Cancel purity, and async-fence
  tests.

Acceptance evidence: `freqinout/gui/settings_tab.py` compiles; `git diff
--check` passes; and the independently rerun GRS-7.1, unified guided UX,
supporting-family, asynchronous autofill, performance, and radio-scoped
settings partition passes **189 tests**. No schema, persistence behavior,
native writer, software ownership, launch behavior, existing radio
configuration, VarAC topology, runtime database, external process, endpoint,
commit, or remote repository was changed.

The next permitted slice is GRS-7.2 inventory classification. GRS-7.3 VarAC
topology, GRS-7.4 progressive disclosure/layout, and GRS-7.5 qualification
remain blocked on their preceding exit gates.

## 2026-09-17 — GRS-7.2 classified software inventory

Status: automated exit gate passed. One immutable inventory snapshot now
classifies saved and retained rows as usable existing, explicit recovery-only,
diagnostic-only, or retained draft using durable link, completeness, and source
evidence. Enabled flags and executable paths do not imply ownership.

Incomplete or provenance-unknown rows remain visible as disabled diagnostic
evidence with exact reasons and cannot be imported, recommended, assigned,
source-locked, or launched. Complete source-evidenced unassigned bundles are
available only through explicit Find/import and are labeled recovery candidates.
Linked complete rows remain usable with an empty manifest table, while visibly
showing `Configuration provenance unverified` and no FIO native-ownership claim.

Conservative endpoint/path claims are normalized and deduplicated. Duplicate
legacy rows reserve the real resource once and retain a count for diagnostics;
they do not trigger repeated scans or repeated port increments. Initial Add
Radio, standalone Software Administration, bounded native discovery, and
pre-Save revalidation now use the same link/manifest/classification evidence.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: classification architecture,
  source-evidence and explicit-recovery boundary, Settings integration,
  delegated-diff review, regression fixes, exit-gate execution, and spec/work-
  log reconciliation.
- `gpt-5.6-luna`, high reasoning: pure classifier, normalized resource claims,
  production-shaped synthetic fixtures, and focused core tests.
- `gpt-5.6-terra`, high reasoning: diagnostic/recovery/provenance presentation,
  accessibility, and focused real-widget tests.

Acceptance evidence: changed Python compiles; `git diff --check` passes; and the
integrated inventory, assistant, Settings, Software Administration, async,
transaction, prepare-first, and performance partition passes **105 tests**.
Tests used temporary/synthetic state. The production database was not opened or
changed in this package. No cleanup, deletion, disable, relink, migration,
schema, native file, process, endpoint, radio, commit, or remote change occurred.

The next permitted slice is GRS-7.3 conditional VarAC topology. GRS-7.4 and
GRS-7.5 remain blocked on their preceding exit gates.

## 2026-09-17 — GRS-7.3 conditional VarAC topology

Status: automated exit gate passed. Add Radio now consumes one pure,
display-ready VarAC topology recommendation built from the already-loaded
classified instance, radio-link, cluster, and membership snapshots. The UI does
not synthesize missing paths or infer membership from incomplete legacy rows.

Fresh setup defaults to standalone. Existing clusters expose named Join choices
and core-calculated next member numbers while retaining standalone as the safe
default. The production-shaped one-standalone/no-cluster case shows the exact
existing-setup summary and named Recommended create choice with no mutating
preselection. Multiple standalone candidates require an explicit named choice.
Arrangement metadata survives radio rename and shared-assistant review, clears
when returning to standalone, and reaches persistence only for create-cluster.

The store transaction for create-with-existing revalidates the existing linked
standalone node, creates the cluster, adds existing member 1 and new member 2,
saves the new node/manifest/launch/radio link, and applies reviewed gateway/PTT
policy atomically. Duplicate, stale, missing-link, observer, and injected-failure
paths leave the original standalone relationship unchanged and create no new
topology.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: matrix and transaction architecture,
  member-number ownership, delegated-diff review, integration, exit-gate
  execution, and documentation.
- `gpt-5.6-luna`, high reasoning: pure recommendation/snapshot adapter, store
  transaction extension, and focused core/fault-injection tests.
- `gpt-5.6-terra`, high reasoning: conditional Add Radio controls, exact copy,
  shared-assistant metadata round trip, refresh safety, and real-widget tests.

Acceptance evidence: focused partition **63 passed**; adjacent family,
manifest, supporting-family, launch-recipe, radio-scoped settings, Software
Administration, unified UX, and performance partition **250 passed**. Changed
Python compiles and `git diff --check` passes. Tests used temporary state and did
not alter a schema, migration, production database, native profile, process,
endpoint, radio, commit, or remote.

The next permitted slice is GRS-7.4 progressive disclosure and responsive
layout. GRS-7.5 remains blocked on that exit gate.

## 2026-09-17 — GRS-7.4 progressive disclosure and responsive layout

Status: automated exit gate passed. Add Radio and the shared software-instance
assistant now have a fixed purpose/step header, exactly one vertical body scroll
owner, and a fixed action footer. The 900x560 Large Text route keeps Back, Next,
Cancel, and Save reachable; the long Fast Light Files route scrolls only its
body and has no horizontal overflow.

The assistant presents compact prepared Facts, Why, safety, unsaved state, and
existing-configuration impact on every step. Family/radio-scoped `Show details`
starts collapsed and owns exact paths, commands, dependencies, fingerprints,
and diagnostics. Disclosure, focus, and per-step scroll survive refresh,
navigation, resize, theme changes, and cache publication. Qualified JS8Call and
Fast Light recipes resolve before Files so generated roots are prepared facts,
not blank technical questions.

Primary integration additionally enforced create-versus-existing intent in the
shared autofill path: a distinct JS8Call instance cannot inherit an existing
profile/message bundle, a distinct VarAC node cannot inherit node-local files,
visible instance names equal the radio name, and create-cluster gets a core-
generated collision-free identity. Conflicting VarAC gateway policies now fail
before mutation.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: prepared-plan and intent architecture,
  Add Radio fixed header/footer integration, VarAC generated identity and
  gateway validation, delegated review, gate execution, and documentation.
- `gpt-5.6-terra`, high reasoning: shared-assistant progressive disclosure,
  one-scroll responsive layout, compact facts, and state preservation.
- `gpt-5.6-luna`, high reasoning: focused real-widget responsive/theme/
  accessibility/disclosure tests.

Acceptance evidence: focused partition **101 passed**; adjacent Settings,
family, persistence, Software Administration, recipe, guided-setup, and
performance partition **328 passed**. Changed Python compiles and `git diff
--check` passes. Tests used temporary state and did not change production data,
native profiles, applications, endpoints, radios, commits, or remotes.

The next permitted slice is GRS-7.5 final qualification.

## 2026-09-17 — GRS-7.5 production-shaped and final automated qualification

Status: automated gate passed; operator-assisted live release gate remains
open. The exact Add Radio TriMode/Transceiver route is now covered by real-widget
tests through software/source intent, built-in FIO Spotter, station-shared
CommStat, explicit VarAC arrangement, bounded preparation publication, and
prepared-family review actions. Add Radio retains one body scroll owner, no
page-level horizontal overflow, and a reachable fixed footer at 1920x1080,
1000x700, and 900x560.

The supplied production `freqinout.db` was audited with SQLite immutable
read-only access. It confirmed the intended one-linked-plus-seven-diagnostic
JS8Call and Fast Light shape, one standalone VarAC node, no cluster or manifest,
and normalized duplicate endpoint claims. SQL tracing confirmed that no message,
traffic, ingest, sync, observation, or operational-history table supplied
ownership evidence. File size and modification time were unchanged, and the
426 MB `freqinout_nets.db` was not opened.

Primary final review found and corrected an observer-path regression: SDR
preparation no longer requires JS8Call, and selected receive-safe Fast Light or
distinct JS8Call companions are prepared while FLRig and VarAC control/TX
ownership remain excluded. JS8Call generated configuration is now described as
a profile/configuration folder.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: final architecture/concurrency,
  receiver-only correction, delegated-diff review, transaction/fault and
  acceptance integration, production-data safety, and documentation.
- `gpt-5.6-luna`, high reasoning: immutable production-data audit,
  production-shaped classification/claim checks, forbidden-table trace, and
  no-write evidence.
- `gpt-5.6-terra`, high reasoning: exact TriMode operator route, worker-boundary
  prepared publication, responsive layout, fixed footer, and one-scroll tests.

Acceptance evidence: GRS-7.5 route/production audit **8 passed**; focused
preparation/inventory/topology/assistant/UI **103 passed**; core proposal,
recipe, transaction, fault, and production-shaped **91 passed**; discovery,
performance, save, manifest, and Settings adapter **65 passed**; independent
adjacent regression partition **328 passed**. Changed Python compiles and `git
diff --check` passes. No schema, migration, cleanup, production database, native
profile, application process, endpoint, radio, commit, or remote changed.

The remaining gate is operator-assisted: relaunch from the current worktree and
active configuration root, exercise real installed-app discovery, and complete
Save, Cancel, and recovery/fault routes with live JS8Call variants, Fast Light,
VarAC, receiver/radio control, and the required macOS/Linux/Windows platforms.
This build is ready for that testing; it is not yet a completed release
qualification.

## 2026-09-18 — GRS-8 zero-entry managed bundle and thread-safe Save remediation

Status: automated remediation gate passed; live Linux installed-application
Save/launch qualification and the separate sustained-CPU gate remain open.

The supplied log proved that final Guided Add Radio Save resumed directly on a
native-configuration worker thread. That continuation entered the radio
transaction, refreshed Qt state, and called the GUI-owned SettingsManager,
which raised its thread-affinity guard and destabilized the error path. Guided
native and VarAC worker results now cross SettingsTab-owned queued signals, and
both success and failure continuations are proven on the GUI thread before
settings or presentation access.

Prepare now constructs and retains complete qualified managed JS8Call and Fast
Light drafts using the common distinct identity and launch-recipe core. A
qualified preparation makes technical Details optional; an unsupported or
ambiguous recipe remains fail-closed with one bounded corrective action. FIO
Spotter remains built in, CommStat remains one station-shared process with a
per-radio JS8 endpoint binding, and neither creates an external per-radio
instance. New radios default to `Assign later`, which is a completed optional
schedule state. The final Save predicate rechecks current preparation context,
qualified recipe status, radio name, and retained identity; software, source,
management, launch, setup, role, backend, or name changes cannot reuse a stale
auto-prepared draft.

Primary integration review added two corrections after delegated work: exact
existing JS8Call variant/version evidence is reused only when its reviewed
executable identity matches the selected binary, and programmatic return from
the optional technical assistant is signal-blocked so policy synchronization
cannot invalidate the just-reviewed draft. The acceptance route was also
strengthened from “Save button enabled” to a real accepted dialog payload,
which found and corrected a missing radio-name gate.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: crash architecture, queued completion
  boundary, transaction and stale-context review, delegated-diff integration,
  exact accepted-payload test, specification, and final regression gate.
- `gpt-5.6-terra`, high reasoning: bounded Prepare publication, optional versus
  required detail presentation, safe schedule default, stale auto-draft
  invalidation, and focused UI tests. The primary reviewed and corrected the
  complete diff.
- `gpt-5.6-luna`, high reasoning: independent log/crash-path and acceptance-gap
  audit. It identified the missing Prepare-to-retained-draft assertion and the
  stale-draft risk; no files were changed by this package.

Acceptance evidence from the final tree: guided setup/operator/unified UX and
native Save partitions **120 passed**; family/store/identity/guard/inventory/
recipe/persistence/adapter partition **153 passed, 8 skipped**; adjacent guided
UI partitions **25 passed**, **13 passed**, **29 passed**, and **8 passed**.
Changed Python compilation and `git diff --check` pass. One broad Qt aggregate
was stopped and rerun in deterministic partitions; the responsive-layout file
completed independently in about 30 seconds. Tests used temporary state. No
production database, native profile, external application, endpoint, radio,
commit, remote, or unrelated DOCX/rendered-document change was modified.

The attached hotspot was also treated as a separate bounded performance work
package. It showed dynamic Spotter form discovery repeated once per backfilled
status row. The primary changed the ingestor to resolve mapped status-form IDs
once per ingest run and pass the immutable set through each upsert. Two focused
regressions prove one discovery per run and zero rediscovery when a precomputed
set is supplied. The independent Luna audit also identified repeated
profile-backed control-context construction when a radio runtime is absent and
duplicate SettingsManager/schema initialization inside each background VarAC
poll. The primary added a short per-radio, endpoint-revision-fenced fallback
cache with bounded warning publication and passed the existing worker-owned
settings snapshot into VarAC status. Per-radio isolation and revision
invalidation have focused coverage.

The combined final guided-save, ingest, scheduler-routing, and background-status
gate is **199 passed, 2 skipped**; adjacent endpoint lifecycle/isolation/fault
coverage is **78 passed**. Changed Python compilation and `git diff --check`
pass. The hotspot's
frequent schedule projection remains an explicit follow-up gate because its
safe correction requires authoritative invalidation evidence across schedules,
assignments, manual-control state, and source-backed plans; it was not folded
into the crash remediation without that proof.

Performance package ownership:

- Primary `gpt-5.6-sol`, high reasoning: bounded ingestion cache, per-radio
  fallback-context architecture/invalidation, VarAC settings reuse,
  integration review, tests, and documentation.
- `gpt-5.6-luna`, high reasoning: read-only log/hotspot and scheduler-path
  audit, prioritization, and regression recommendations. No files were changed
  by this delegated package.

## 2026-09-17 — Native VarAC cluster configuration (VNC-1 through VNC-5)

Status: automated implementation exit gate passed; disposable live Windows and
Linux/Wine VarAC 13.2.7 qualification remains open.

FIO now prepares and transactionally writes the exact qualified native VarAC
cluster projection for creating the first two-member cluster from an existing
standalone node or joining an existing native-managed cluster. The slice adds
distinct bounded VARA runtime clones and ports, Windows-visible path projection
for Linux/Wine, explicit email-gateway sender ownership independent of the
legacy gateway field, one effective cluster-shared database, complete plan and
readback fingerprints, additive persistence and apply journal, split
filesystem/FIO commit, immediate Cancel/stale compensation, crash recovery,
launch blocking, and generation-fenced worker/UI publication. VarAC 15.0.18 is
still not qualified for native writing.

Primary integration review corrected four issues before this gate: unexpected
runtime exceptions now enter the same exact rollback path as I/O failures; an
outer Add/Edit Radio Cancel or failed adjacent-family apply immediately rolls
back an already-applied native session; legacy node-local database uniqueness
no longer rejects the required shared database for reviewed native cluster
members; and Linux/Wine native INI/launch values use `C:`/`Z:` paths while FIO
retains host paths. The primary also expanded the plan fingerprint to include
all reviewed policy, controlled values, launch identity, pre-write state, and
the complete bounded runtime snapshot.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: VNC-1 specification and architecture;
  VNC-3 migration, transaction, recovery, concurrency, shared-database and
  path semantics; delegated-diff review; final corrections and integration.
- `gpt-5.6-terra`, high reasoning: VNC-2 bounded parser/writer/runtime-clone
  mechanics and VNC-4 guided UI/progressive-disclosure implementation. Every
  delegated diff was reviewed and corrected by the primary before integration.
- `gpt-5.6-luna`, high reasoning: VNC-5 deterministic acceptance extensions
  for cross-platform preparation, rollback, recovery, generation fencing,
  gateway separation, and launch blocking. The primary reviewed the test diff
  and added split-transaction, cancellation, fingerprint, shared-database, and
  Wine-path cases discovered during integration.

Automated acceptance evidence:

- native writer/preparation/transaction/integration/topology: **49 passed**;
- focused assistant/workspace/responsive/geometry/disclosure: **33 passed**,
  40 intentionally deselected by the focused expression;
- multi-radio store, manifest, launch identity, JS8Call, receiver launch: **140
  passed**;
- Add Radio transaction/operator route/VarAC arrangement/unified UX/performance
  boundaries: **40 passed**;
- adjacent guided VarAC, schema/guard/transfer, operator sync, VarAC BBS,
  Settings adapter, launch recipes, and native-writer launch: **82 passed**;
- changed Python compilation and `git diff --check`: passed;
- additive migration/read-only safety was exercised against an isolated copy of
  `/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db`; the source file was
  hash/stat checked and not modified.

One earlier broad combined Qt batch was stopped after entering the repository's
known long-running aggregate behavior. It did not identify a slice failure;
the deterministic focused UI and adjacent partitions above were rerun from the
final tree and passed. Tests used temporary state. No production database,
native application profile, process, endpoint, radio, commit, remote, or the
operator's unrelated DOCX/rendered-document changes were modified by this
slice.

## 2026-09-18 — GRS-9 operator-feedback audit and specification correction

Status: specification exit gate passed; product implementation deliberately
not started in this slice.

The operator's screenshots and `freqinout (38).log` were reviewed against the
current Add Radio, Software Administration, native VarAC preparation, discovery,
review, save-validation, and Radios-layout code. The review confirmed that the
reported behavior is caused by deterministic state/projection defects rather
than operator uncertainty:

- VarAC native preparation generated technical facts but never hydrated the
  retained VarAC instance draft, leaving INI, database, incoming, outbox,
  working-directory, and command fields blank below a valid technical summary.
- TriMode selected VarAC, while nested-editor cancellation left the parent's
  VarAC selection and retained state intact. The later log session scanned only
  Applications, Fast Light, and JS8 profiles; VarAC in Review was therefore
  stale UI/session intent, not a newly discovered application.
- the managed-bundle publication path covered JS8Call and Fast Light only;
  Review and Save could still consult checkbox/loose-field/legacy-plan state,
  so no single authoritative selected-family bundle existed;
- Fast Light and application discovery returned zero candidates quickly, while
  JS8 profile discovery took 17.319 seconds and also returned zero. Save then
  lacked qualified managed recipes without displaying the exact missing
  executable/version evidence as its blocker; and
- the Radios compact header remained visible after all semantic children were
  hidden, while expandable layout policy permitted an elastic blank region
  above the readiness content.

GRS-9 now defines an authoritative draft session and current-generation
prepared-bundle map; synchronous family-state purge; unambiguous Back versus
Remove-family semantics; outer-cancel isolation; complete canonical VarAC field
mapping; zero-entry ownership for all safely derivable paths, files, commands,
ports, and directories; bounded Linux executable/version discovery; exact
operator-visible blocker codes; telemetry needed to reconstruct the workflow;
and objective layout, performance, transaction, and real-widget exit gates.
The spec explicitly treats an existing VarAC installation as source evidence,
not a workspace, and requires FIO to derive distinct managed incoming/outbox,
working-directory, INI/runtime, ports, and launch facts for a qualified new
member.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: architecture and state-machine review,
  VarAC/FL/JS8 ownership contract, performance and transaction boundaries,
  integrated specification, and final review.
- `gpt-5.6-terra`, high reasoning: read-only UI/state and prepared-bundle audit,
  including the exact VarAC projection gaps, stale selection semantics, Save
  predicate, and Radios empty-header cause.
- `gpt-5.6-luna`, high reasoning: read-only log/timeline and focused code-path
  audit, including the 17.319-second JS8 zero-result scan, absence of a VarAC
  scan in the second attempt, and limitations of the existing telemetry.

No product source, database, external application configuration, native file,
or test fixture changed. The production databases and unrelated DOCX/rendered
document changes were not modified. Documentation whitespace validation and
the specification-diff review are the only gates appropriate to this
specification-only slice; automated product acceptance remains explicitly open
under GRS-9.

## 2026-09-18 — GRS-9 safe-default guided software implementation

Status: integrated automated exit gate passed; installed-Linux operator
qualification remains open.

This slice implemented the corrected GRS-9 contract. Guided managed recipes now
retain complete isolated plans under four explicit outcomes: Ready, Ready with
warnings, Saved with launch setup pending, and Blocked for safety. Incomplete
version evidence and missing executables no longer discard a safe radio draft;
pending recipes persist with launch disabled. Explicit overwrite/reuse,
identity/path/endpoint/resource collision, unsafe shared mutation, transaction,
and RF-safety conditions remain blocking.

The canonical VarAC native presentation now hydrates the real assistant draft
with the application, INI, database, incoming, outbox, working directory, VARA
runtime/INI, ports, and launch command. Selected-family state is purged
synchronously on deselection, the nested editor distinguishes Back without
changes from Remove family, and removed VarAC state no longer contributes to
Connections, Review, Save validation, or the accepted payload. Radios layout
now collapses its empty compact header and top-aligns content.

Discovery now prefers reviewed/saved executable identities, bounds known-path
work, and applies a two-second per-phase budget. Timed-out results are excluded
from the current snapshot and cache, while safe partial evidence remains usable.
JS8Call executable-name evidence distinguishes stock, Subspace, and Improved
families without fabricating an exact version. Exact generated commands, roots,
endpoints, working directories, confidence, and evidence persist to launch
items; launch-pending bundles explicitly disable automatic launch.

During primary integration review, one delegated SQL placeholder-order defect
was found and corrected before release: the new launch-pending flag had been
bound in the timestamp position. A regression assertion now verifies that the
bundle timestamp remains an ISO string while pending launch remains disabled.
The primary also tightened arbitrary JS8 Browse targets so only app-specific
executable identities qualify for warning-ready launch.

Work packages and model ownership:

- Primary `gpt-5.6-sol`, high reasoning: specification and safety architecture,
  discovery concurrency/timeout design, canonical VarAC fingerprint and
  projection integration, review of every delegated diff, SQL and JS8 identity
  corrections, production-copy audit, final acceptance, and documentation.
- `gpt-5.6-luna`, high reasoning: bounded recipe/discovery/store implementation
  and focused recipe/persistence tests.
- `gpt-5.6-terra`, high reasoning: selected-family UI state, Back/Remove-family
  behavior, VarAC draft hydration, Radios layout correction, and focused
  real-widget tests.

Final-tree evidence:

- integrated focused/adjacent acceptance matrix: **396 passed**;
- immutable production-shaped inventory plus disposable-copy migration checks:
  included in that matrix and **3 passed** as an independently observed
  partition;
- changed Python compilation: passed;
- `git diff --check`: passed.

The production source at
`/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db` retained the same
size, timestamp, and SHA-256 hash. Tests used temporary state; no production
database, external application profile, process, endpoint, radio, commit, or
remote was changed. The operator's unrelated DOCX and rendered-document changes
remain untouched. The exact supported-applications Linux walkthrough is the
remaining live qualification and is intentionally not claimed by this entry.

## 2026-09-18 — GRS-10 canonical software bundle and platform launch correction

Status: integrated automated exit gate passed; live Windows and Linux/Wine
operator qualification remains open.

Operator testing reopened the prior completion claim. A prepared VarAC plan
was visible inside Software Administration while Add Radio retained blank
fields; intentional first-cluster database sharing triggered the generic
private-storage collision; draft completion attempted native mutation; Review
could display an installation directory as a command; and persistence flattened
Wine argv before POSIX reparsing. These were implementation defects rather than
missing operator input.

The governing specifications now require one cross-service prepared bundle and
structured launch identity. The implementation:

- generates an installation-adjacent unique VarAC INI for qualified Windows
  and Linux/Wine installations while retaining distinct managed VARA runtime,
  incoming/outbox identity, ports, and member policy;
- permits a reviewed cluster-owned database to be shared only by members of
  that cluster and retains destructive/private collision blockers;
- makes Software Administration `Save as draft` non-mutating and moves native
  apply to accepted outer `Save Radio and Software`;
- projects the same prepared executable, INI, database, folders, VARA facts,
  ports, argv, cwd, environment, generation, and fingerprint into Add Radio
  before Details, through final apply, persistence, and launch;
- removes qualified VarAC from the contradictory generic read/import-only
  presenter and shows the exact executable/arguments rather than a directory;
- persists structured VarAC launch facts through the additive readiness JSON
  seam, suppresses stale legacy fallback, and passes exact argv/cwd/environment
  to `subprocess` with `shell=False`; and
- leaves JS8Call/Fast Light managed recipes, built-in FIO Spotter, and the one
  station-shared CommStat process with per-radio bindings under their existing
  canonical contracts.

Work packages and exact model ownership:

- Primary `gpt-5.6-sol`, high reasoning: GRS-10/VNC-6 architecture and
  specification, final transaction placement, fingerprint reconciliation,
  production-copy migration audit, review of every delegated diff, integration
  corrections, and final exit-gate decision.
- `gpt-5.6-luna`, high reasoning: bounded VarAC native layout/draft mechanics
  and focused preparation/writer/assistant tests. Primary added and verified
  the final Add Radio apply boundary.
- `gpt-5.6-terra`, high reasoning: bounded parent/nested UI projection, exact
  Review presentation, legacy-plan exclusion, and real-widget tests.
- `gpt-5.6-luna`, high reasoning: bounded launch-store/planner/orchestrator
  round trip and focused Windows/Linux-Wine tests. Primary added the direct
  process-runner assertion.

Sequential exit gates and final evidence:

- specification/diff gate: passed;
- native preparation/draft/final-transaction gate: **49 passed**;
- Add Radio/Software Administration UI gate: **45 passed**;
- structured persistence/planner/launcher gate: **73 passed**;
- final non-overlapping integrated partitions: **508 unique tests passed**;
- immutable production source and migrated disposable-copy checks: included;
- changed Python compilation and `git diff --check`: passed.

The first monolithic Qt aggregate was stopped after entering the repository's
known long-running teardown behavior. Its two completed failures were stale
pre-GRS-9 expectations (`unsupported`) rather than product regressions; those
assertions were corrected to the governing `blocked_for_safety` and
`launch_pending` outcomes, then every partition passed. The interrupted Qt
process emitted a teardown crash after the interrupt; no application process
or production data was involved.

The source at `/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db`
retained its size, modification timestamp, and SHA-256 hash. The copied
database opened through additive migrations, accepted and reloaded a structured
VarAC bundle, and replanned the exact Wine argv. The operator's unrelated DOCX
and rendered-document changes remain untouched and uncommitted. No production
database, native application profile, process, endpoint, radio, commit, or
remote was modified. Live Windows/Linux-Wine launch, native readback, cluster
mailbox, and RF/application behavior remain explicit external release gates.

## 2026-09-19 — VarAC cluster-shared BBS projection correction

Status: automated implementation gate passed. Live Windows/Linux-Wine operator
qualification remains open.

Observable route: `Settings > Radios > Add Radio > managed VarAC cluster`.
The reported defect was a prepared INI/database/incoming/outbox bundle with
blank BBS and archive fields. The correction defines `bbs_path` and
`bbs_archive_path` as cluster-shared canonical bundle facts, durable as
`varac_clusters.shared_bbs_path` and
`varac_clusters.shared_bbs_archive_path`. Create-cluster inherits nonblank
standalone BBS/archive paths or derives `<VarAC install>/BBS` and
`<VarAC install>/BBS/Archive`; join-cluster uses the selected cluster's durable
paths. Incoming/outbox remain member-local.

Settings projects the prepared BBS/archive facts read-only into Connections and
Review. The accepted final transaction may create reviewed missing directories,
never changes existing content, and compensates only FIO-created empty
directories recorded as created by that successful apply. Post-native readback
enrichment is expected output rather than a review-invalidating change; final
mutation compares durable inventory against the frozen reviewed payload.
Cleanup for `complete`, `fio_committed`, and `rolled_back` is idempotent;
`recovery_required` remains a launch-blocking operator-recovery state. Recovery
never triggers an automatic forward apply.

Work packages and exact ownership:

- Primary `gpt-5.6-sol`, high reasoning: schema/migration, transaction,
  concurrency/freshness, integration review, and final gate.
- `gpt-5.6-terra`, medium reasoning: Settings UI projection, focused UI test,
  and specification/work-log update.
- `gpt-5.6-luna`, medium reasoning: focused regression coverage.

Primary integration review additionally disabled the Browse actions attached to
prepared read-only BBS fields, synchronized later cluster-path changes to all
enabled member-profile projections, and rejected overlap between member-local
incoming/outbox and cluster-shared BBS resources.

Final evidence:

- the focused integrated guided-radio, transaction, cluster persistence,
  native writer/preparation, production-shaped audit, and real-widget matrix:
  **139 passed**;
- changed Python compilation: passed;
- a SQLite backup of the supplied production database migrated, saved, and
  reloaded the shared BBS/archive columns and passed `PRAGMA integrity_check`;
- the production source retained SHA-256
  `7840e95f30d2d2862e7e128852da20d9225acf51b24749e7017319a3f48ae074`,
  size `966656`, and mtime `1789686201` before and after the copy audit; and
- `git diff --check`: passed.

No production database, native application configuration, process, endpoint,
radio, commit, or remote was changed. The operator's unrelated DOCX and
rendered-document changes remain untouched. Automated fixtures do not satisfy
the live Windows/Linux-Wine VarAC/VARA operator release gate.

## 2026-09-19 — GRS-11 automatic preparation and compact software cards

Status: implementation and automated exit gate passed.

The Add Radio Software step no longer requires the operator to select software,
scroll to a separate Prepare command, and then revisit a second expanded set of
controls. The checkbox grid is the sole family-selection surface. Selection or
policy changes schedule one coalesced background preparation automatically;
the former primary button is hidden and appears only as a retry after failure.
VarAC arrangement remains an explicit prerequisite and is never inferred.

The existing discovery coordinator, canonical dialog draft map, and final-save
transaction remain authoritative. Each worker captures the full software-plan
context. A mid-flight edit cancels or invalidates that generation; only a result
matching the live context may publish, and one replacement request is scheduled.
This UI slice introduces no schema, migration, writer, or external mutation.

Ready, ready-with-warning, and launch-pending family cards collapse to status,
compact source/launch/endpoint facts, and Details. Preparing cards hide forms.
Needs-choice and blocked cards expose the one operator decision or recovery
action, and the first unresolved card is revealed only once per prepared
context. Software Next now mirrors final Save's managed-recipe, VarAC-native,
detected-choice, and safety-block rules. Warnings and launch-pending plans may
continue; stale, preparing, missing-choice, and safety-blocked plans may not.
The existing fixed footer and one body-scroll contract are unchanged.

Sequential work packages and exact model ownership:

- Gate 1 — primary `gpt-5.6-sol`, high reasoning: interaction/state contract,
  concurrency and no-migration decision, GRS-11 specification, and exit gates.
- Gate 1 audit / Gate 2 UI — `gpt-5.6-terra`, medium reasoning: bounded review
  of the live dialog seams, then the Settings-only automatic preparation,
  compact-card, navigation, and one-time reveal implementation.
- Gate 1 test audit / Gate 3 tests — `gpt-5.6-luna`, medium reasoning: focused
  real-widget test design and implementation for automatic/coalesced selection,
  stale-result rejection, selection/payload preservation, card severity,
  non-mutating Details cancel, VarAC topology, one-time reveal, and fixed footer.
- Final integration — primary `gpt-5.6-sol`, high reasoning: reviewed every
  delegated diff, corrected policy-widget overlap, completed context/Next/card
  state contracts, refined operator language and compact facts, and ran the
  integrated acceptance partitions.

Exit evidence:

- focused GRS-11 operator-route, prepare-first, and layout suite: **35 passed**;
- guided Add Radio, VarAC, native writer/preparation/transaction, and final
  apply partition: **82 passed**;
- canonical inventory, launch recipe, proposal, persistence, and launch-bundle
  partition: **118 passed**;
- production-shaped inventory and structured launch round-trip partition:
  **15 passed**;
- total non-overlapping final-tree evidence: **215 passed**;
- changed Python compilation and `git diff --check`: passed.

No production database, native application configuration, process, endpoint,
radio, commit, or remote was changed. The unrelated DOCX and rendered-document
changes remain untouched. Live application qualification remains an external
release gate.

## 2026-09-19 — GRS-11.1 detected JS8 choice and detail-window correction

Status: implementation and automated regression gate passed.

Operator report: Add Radio requested a JS8Call selection, but making that
selection did not permit the operator to continue. Opening `Review Launch
Setup` then appeared to swipe away.

Code review found that the detected-app handler updated a path only when its
hidden target field was blank. If preparation had already supplied any value,
the explicit operator choice was ignored. Even when the choice resolved the
ambiguity, the handler updated Review readiness only; outside Review that
function returns early, so the Software-step Next gate retained its disabled
state. The chosen executable also did not invalidate and rebuild the pending
launch recipe.

The correction makes explicit detected and Browse selections authoritative,
replaces a different provisional path, invalidates the old recipe, and
coalesces one replacement preparation. The sole-candidate automatic path is
still consumed inside its current result and cannot recurse. Matching choices
republish navigation without an unnecessary worker. Launch-pending plans now
label Details as optional, consistent with the warning policy. Both managed
and general detail dialogs are clamped to available screen geometry with an
80-pixel desktop margin; their own body scroll and fixed footer handle compact
screens without an oversized native modal being moved off-screen.

Primary `gpt-5.6-sol`, high reasoning, performed the code-path diagnosis,
state/concurrency correction, geometry correction, regression implementation,
and final review. No delegation was used for this bounded follow-up.

Evidence:

- exact multiple-candidate JS8 selection regression: passed within the GRS-7.5
  file (**15 passed** total in that file);
- combined operator-route, prepare-first, unified-layout, assistant responsive,
  active-page geometry, and Software Administration reflow suite:
  **101 passed**;
- changed Python compilation: passed;
- no schema, migration, native writer, external application file, production
  database, process, endpoint, radio, commit, or remote was changed.

## 2026-09-19 — GRS-12 full TriMode VarAC preparation lifecycle

Status: automated implementation gate passed; external Linux operator
qualification remains open.

Operator evidence: in `Settings > Radios > Add Radio > Software`, TriMode with
FIO Spotter, CommStat, Fast Light, JS8Call, and an explicit **Create VarAC
cluster** choice displayed all application candidates but left VarAC at an
unexplained attention state and disabled Continue. Repeated attempts also
showed brief UI stalls.

The correction establishes one family state across the card, Software setup
status, and navigation gate. Native VarAC planning is independently pending
after general software discovery; it publishes Preparing rather than Ready.
A qualified bundle enables Continue. A qualified bundle that requires VarAC
and VARA to be stopped before the transactional final Save is a visible,
non-blocking warning. A non-ready result persists and displays its exact native
reason, marks the card Blocked, and prevents Continue. Planning remains
read-only while applications run; the final apply transaction is unchanged as
the no-write safety boundary.

Rendering/review consume the prepared native bundle and must not call legacy
synchronous VarAC filesystem discovery. Candidate publication is batch-only.
The matrix covers JS8 only, Fast Light only, JS8 + FIO Spotter + CommStat,
full TriMode without VarAC, and full TriMode with Create VarAC cluster. It also
covers delayed native preparation and deselecting VarAC during that delay: the
remaining prepared families regain Continue immediately and a late stale VarAC
result cannot reinsert the draft or gate navigation.

Work-package ownership:

- Primary `gpt-5.6-sol`, high reasoning: production-evidence review,
  architecture, native-plan lifecycle/concurrency and stale-result fencing,
  final-save safety boundary, delegated-diff review, integration corrections,
  and final automated gate.
- `gpt-5.6-terra`, medium reasoning: bounded real-dialog combination matrix,
  pending/ready/warning/blocked native-state coverage, stale deselection
  coverage, legacy-discovery prohibition, and specification/work-log update.
- `gpt-5.6-luna`, medium reasoning: bounded performance audit of the supplied
  production log and CPU hotspot captures.

Final evidence:

- exact GRS-12 focused suite: **30 passed**;
- broader guided-radio, native preparation/writer/transaction, inventory,
  launch, and production-shaped suite: **180 passed**;
- Software Administration layout and assistant suite: **77 passed**;
- changed Python compilation and `git diff --check`: passed.

No schema, migration, production database, application file, process,
endpoint, radio, commit, or remote was changed. The unrelated DOCX and rendered
document changes remain untouched. The live Linux operator run of the complete
reported route, including no visible UI stall and successful progression to
Connections, remains required before release qualification.

## 2026-09-19 — GRS-12.1 Wine Desktop symlink compatibility

Status: automated implementation gate passed; Linux-Wine operator
qualification remains open.

Operator evidence: native VarAC preparation reported `Symlink path is not
allowed` for a BBS directory below
`~/.wine/drive_c/users/<user>/Desktop`. The writer applied its strict native
configuration/runtime symlink policy to data-directory ancestry. Wine commonly
uses that Desktop alias to expose the operator's Linux desktop, so the plan was
blocked before Save despite having no native INI or executable symlink target.

The correction separates the two safety policies. Native VarAC INI and VARA
runtime/configuration paths still reject all symlink ancestry. Reviewed BBS,
archive, incoming, and outbox directories may traverse a stable Wine directory
alias. Their resolved destinations are captured in the immutable plan and its
fingerprint, checked again immediately before creation, and checked after
creation. Broken aliases, file targets, or aliases retargeted after review are
blocked before a directory is created.

Evidence: **47 native preparation/writer/transaction acceptance tests passed**
and the broader guided-radio/native suite passed **184 tests**,
including a production-shaped Wine Desktop alias and a retargeted-alias
rejection. Changed Python compilation and `git diff --check` passed. No schema,
migration, production database, external application file, process, endpoint,
radio, commit, or remote was changed.

## 2026-09-19 — GRS-12.2 VarAC review identity and discovery single-flight

Status: automated implementation gate passed; live Linux operator qualification
remains open.

Observable reproduction: in `Settings > Radios > Add Radio`, the operator
prepared and reviewed a managed VarAC setup, but final Save reported that the
prepared plan had changed and routed back to Software. The supplied log also
shows same-session guided discovery generations 1 and 2 each timing out the
JS8 profile phase after about two seconds while application, Fast Light, and
VarAC evidence came from cache. The supplied CPU captures place a discovery
worker inside `read_js8call_multisettings` / `ConfigParser.read` during the
sustained hotspot.

Code review found two distinct lifecycle faults. Final native apply required
the cached preparation's UI generation to equal the reviewed generation even
when the immutable plan and operator intent were identical. Separately, timed-
out discovery futures remain active, but the in-flight key included generation,
allowing another generation to submit the same parser work.

The correction makes immutable plan plus reviewed/current draft fingerprints
the final-apply identity while retaining generation fencing for async UI
publication. A newer equivalent preparation is accepted; changed live intent,
missing identity, or a different plan remains blocked before writer start.
Identical active discovery phases now single-flight across generations within
one assistant session and input fingerprint.

Work-package ownership:

- Primary `gpt-5.6-sol`, high reasoning: evidence review, transaction and
  concurrency design, implementation, delegated-diff review, integration,
  specification reconciliation, and final exit gate.
- `gpt-5.6-luna`, low reasoning: read-only focused apply-boundary and regression
  audit. It identified the missing changed-live-intent assertions; the primary
  added them at both entry points.
- `gpt-5.6-terra`, medium reasoning: read-only hotspot/concurrency audit. It
  confirmed duplicate cross-generation JS8 scan eligibility and recommended
  the bounded same-session single-flight correction.

No schema, migration, production database, native application file, process,
endpoint, radio, commit, or remote is changed by this slice. The unrelated DOCX
and rendered-document changes remain untouched.

Final evidence:

- focused VarAC/discovery regression suite: **28 passed**;
- broader Add Radio, guided software, native preparation/writer/transaction,
  and save suite: **230 passed**;
- Software Administration assistant/persistence/layout suite: **141 passed**;
- changed Python compilation and `git diff --check`: passed.

The automated exit gate is closed. A relaunched Linux production run of the
exact reported route is still required for external qualification; the running
application cannot hot-load this source correction.

## 2026-09-19 — GRS-12.3 VarAC data-root inheritance and planner transaction boundary

Status: automated implementation gate passed; live Windows and Linux/Wine
operator qualification remains open.

Operator evidence: the prepared FT-710 VarAC cluster member showed Incoming
and Outbox below `~/.freqinout/managed-instances/ft-710/varac-native`, while the
reviewed existing VarAC station data was below the Wine Desktop `VaraFiles`
area. Selecting **Open scheduler** for Daily + Nets then appeared to freeze the
UI. In `freqinout (42).log` at local `14:14:08`, Add Radio synchronously opened
the lazy Plan Builder during its guided `BEGIN IMMEDIATE` save transaction.
Plan Builder construction attempted to write
`freqplanner_selected_hf_daily_schedule_set_id`, waited `5012 ms`, raised
`sqlite3.OperationalError: database is locked`, and made
`main_window.set_screen` take `5039 ms`.

The correction has two bounded parts:

- native VarAC preparation now uses the reviewed existing member's
  incoming/outbox parents as station location evidence and creates distinct,
  filesystem-safe `<radio-name>_In` / `<radio-name>_Out` targets. It checks all
  durable profile mailbox claims and applies one deterministic suffix to the
  pair on collision. A real Advanced correction remains authoritative; a path
  merely republished by an older native plan is re-derived. With no reviewed
  mailbox evidence, the previous per-radio managed-root fallback remains.
- Add/Edit Radio now records planner handoff intent inside the save but queues
  navigation only after the transaction has exited successfully. Failed saves
  do not navigate. Passive Plan Builder source projection updates controls
  without writing Settings, leaving persistence to explicit operator choices
  or the explicit post-commit guided handoff.

The native writer's no-damage policy is unchanged. Incoming/outbox remain
member-local, BBS/archive remain cluster-shared, stable Wine aliases retain
resolved-target fingerprints, and existing content is never moved, cleared,
or deleted. No schema or migration was introduced.

Work-package ownership:

- Primary `gpt-5.6-sol`, high reasoning: log/runtime diagnosis, path and
  transaction architecture, implementation, test reconciliation, specification
  and work-log updates, delegated-audit review, and final integration gate.
- `gpt-5.6-terra`, medium reasoning: read-only scheduler transition and CPU
  audit. It identified the exact in-transaction lazy-tab construction and
  Settings write that produced the five-second UI-thread lock wait.
- `gpt-5.6-luna`, low reasoning: read-only VarAC path/no-damage audit. It
  identified the managed-root default, durable BBS/profile evidence, Wine alias
  safety boundaries, and preservation tests. Both delegated packages were
  advisory; the primary model reviewed the findings and owned all final diffs.

Regression evidence:

- focused VarAC preparation and post-commit planner tests: **18 passed**;
- full applicable guided-radio, Add Radio, planner, Software Administration,
  native preparation/writer/transaction, and VNC acceptance partition:
  **288 passed**;
- changed Python compilation and `git diff --check`: passed.

Three stale source-structure assertions in `test_guided_setup.py` were already
failing at baseline commit `4fec3cb`; the detached baseline reproduced the
failures. Their assertions were updated to the existing parameterized dialog
size helper, batched detected-app call, current VarAC guidance source, and
prepared outbox projection without changing application behavior. The complete
applicable partition now passes with no deselections.

No production database, native VarAC/VARA file, process, endpoint, radio,
commit, or remote was changed. The unrelated DOCX and rendered-document
workspace changes remain untouched. External qualification must confirm the
new member paths below the existing `VaraFiles` parent and a responsive
post-save Daily + Nets Plan Builder handoff.

## 2026-09-19 — GRS-13 unified native-storage and software-instance specification

Status: specification gate ready for maintainer review; implementation and live
qualification remain open.

The maintainer clarified the required operator mental model after reviewing
Add Radio, Software Administration, Fast Light multi-instance storage, message
attribution, FLAmp Q/BBS stability, and established operator directory layouts:

- FIO manages qualified configuration without hiding native application or
  operator content below its private configuration root;
- existing single- and multi-instance layouts are preserved and adapted;
- a new instance normally follows the application's qualified standard layout
  or becomes a distinct sibling beside an established compatible instance;
- application instances belong to radios; completed content enters one station
  message library; FIO Spotter FLAmp Q and FIO BBS are station-scoped
  publication services; operating groups are receipt/filter metadata and
  access-policy subjects;
- mutable application state is local-first, while NAS/removable storage is an
  explicit asynchronous publication/archive target unless an exact recipe
  qualifies network-backed runtime state; and
- Add Radio, Software Administration, persistence, launch, Messages, Spotter,
  and reconciliation consume one canonical bundle and verified projections.

GRS-13 was added to
`guided_radio_software_configuration_spec.md` as the controlling cross-service
contract. It supersedes older generic station-sharing, managed-root fallback,
and split-persistence authority. It defines no-lock-in directory precedence,
existing-instance adoption and sibling creation, JS8Call/Fast Light/FLMsg/
FLAmp/VarAC family rules, safe message intake and presentation,
access-controlled station FLAmp Q/BBS publication, NAS outage behavior,
projection parity, exact structured launch, unified UI language, and a
Windows/Linux-Wine production-shaped acceptance matrix.

Maintainer review then corrected the initial publication scope: receipt
locations remain radio/application-specific, but imported content belongs to
the canonical station library. FIO Spotter FLAmp Q and FIO BBS are station
publication services with independent per-item publication state and ACLs.
Operating groups are provenance/filter metadata and access-policy subjects, so
authorized sharing may cross groups without copying content or changing receipt
history. Response-radio selection is a separate station-service routing and RF
preflight decision. Native VarAC cluster BBS storage remains a distinct VarAC
runtime resource and is not the FIO BBS content store.

`multi_instance_software_administration_spec.md` now points to GRS-13 and
removes conflicting claims that FLMsg/FLAmp runtime state is shared by default,
that the radio link is an independent source of truth, or that final Software
Administration can never invoke the qualified native transaction used by Add
Radio.

Work-package ownership:

- Primary `gpt-6-astra`, high reasoning: product/storage architecture,
  persistence authority, no-damage and NAS policy, specification edits,
  delegated-audit review, and final consistency gate.
- `gpt-5.6-terra`, medium reasoning: read-only persistence and cross-spec
  consistency audit. It identified the split source-of-truth language,
  managed-root conflict, missing scope taxonomy, and draft/final native-writer
  contradiction.
- `gpt-5.6-luna`, medium reasoning: read-only acceptance-matrix, edge-case, and
  follow-up publication-scope audits. It identified the initial group-ownership
  conflict, missing operating-group UI contract, NAS/offline behavior, FLAmp
  Q/BBS stability, and required cross-group ACL, unpublish, Windows/Linux,
  rename, rollback, and launch-round-trip cases.

This slice changes documentation only. It introduces no schema, migration,
code, runtime data, application file, process, endpoint, radio, commit, or
remote mutation. The implementation exit gate and live Windows/Linux-Wine,
external-application, NAS, and radio qualification gates remain open.

Documentation verification: `git diff --check` passed; all three edited
Markdown files passed UTF-8/readback and terminating-newline checks. No runtime
or implementation test result is claimed by this specification-only slice.

## 2026-09-20 — GRS-13.1 canonical software identity parity implementation

Status: automated implementation gate passed; the wider GRS-13 native-storage,
station-message-library, publication/NAS, and live Windows/Linux-Wine gates
remain open.

This slice implements the unambiguous Add Radio / Software Administration
identity rule. Add Radio now commits one immutable canonical identity record for
every selected radio application, Fast Light child, built-in binding, external
companion, and station-service binding. Software Administration projects those
same bundle, component, binding, and fingerprint identities. Canonical-backed
compact fields are read-only; changes use the authoritative Add/Replace
assistant so application rows, manifests, exact structured launch rows, and the
canonical set move in one generation-fenced guided transaction.

The migration is additive. New `radio_software_identity_sets` and
`radio_software_identity_records` tables retain a versioned complete set per
radio. There is no destructive migration and no automatic legacy backfill.
Missing or mismatched projections produce `Needs attention`; manual launch is
blocked only for the affected radio and startup skips that radio without
blocking unrelated launch lanes. Read-only Settings/launch validation uses a
read-only SQLite connection and performs no filesystem, process, endpoint, or
native-application operation.

The independent final audit stopped the first release candidate and the primary
corrected five integration defects before closing the gate:

- a JS8Call assignment no longer implicitly selects FIO Spotter or CommStat;
- built-in FIO Spotter is validated as an internal component and is not required
  to have an external executable or launch row;
- CommStat and external JS8Spotter retain JS8Call as explicit cross-family
  dependencies without violating an atomic family-local launch graph;
- radio-scoped owners and all non-empty radio bindings are checked against the
  target radio before persistence; and
- the exact reviewed manifest key is retained by the canonical identity instead
  of being silently normalized to a different bundle key.

CommStat remains one durable `commstat:station` process identity. Each radio's
canonical set references that same process and contributes one distinct JS8
endpoint binding; this does not duplicate the process. FIO Spotter is an
explicit built-in component and binding, never an inference from JS8Call.

Work-package ownership:

- Primary `gpt-6-astra`, high reasoning: package decomposition, architecture,
  additive schema, concurrency and transaction boundaries, canonical model
  integration, projection and launch enforcement, delegated-diff review,
  specification/work-log reconciliation, acceptance gates, and final review.
- `gpt-5.6-terra`, medium reasoning: bounded core identity/validation work and
  independent schema/transaction/projection audit. Its audit found the implicit
  service defaults, built-in executable false block, CommStat dependency drift,
  cross-radio validation gap, and write-capable read path; the primary reviewed
  and corrected each finding.
- `gpt-5.6-luna`, medium reasoning: bounded Software Administration/UI projection
  and focused launch-drift tests, followed by an independent UI/spec parity
  audit. Its audit identified the missing production-shaped all-family linked-
  row acceptance gate and CommStat wording ambiguity; both were resolved.

Final automated evidence:

- focused canonical identity, linked projection, launch recipe, VarAC launch,
  and manifest suite: **73 passed**;
- broader canonical/core/Software Administration model suite: **148 passed**;
- guided Add Radio and Software Administration UI suite: **141 passed**;
- complete software/launch/guided regression partition: **717 passed, 4
  skipped**;
- changed Python compilation and `git diff --check`: passed.

The production-shaped identity gate persists and reloads linked JS8Call, all
four selected Fast Light components, VarAC and VARA, FIO Spotter, external
JS8Spotter, CommStat, and a separate receive-only SDR++ radio. It verifies exact
application paths/endpoints, manifest identity, structured argv/cwd/environment/
dependencies/readiness, station bindings, and canonical fingerprints. Dedicated
tests also prove explicit service selection, built-in validation, cross-radio
rejection, stale-generation rollback, read-only validation, family-specific
drift reporting, and affected-radio-only launch blocking.

No production database, native application file, process, endpoint, radio,
commit, or remote was changed. Existing unrelated DOCX, rendered-document,
VarAC native-writer, and planner worktree changes were preserved. Live testing
must still qualify external application behavior on Windows and Linux/Wine; the
wider GRS-13 native directory, canonical message library, FLAmp Q/FIO BBS
publication, ACL, and NAS behaviors are not claimed complete by this slice.

## 2026-09-20 — GRS-13.2 Software Administration chip-strip layout correction

Status: automated UI gate passed; operator relaunch verification remains open.

Operator evidence showed the selected Software Administration family chip
almost completely covered by a blue horizontal scrollbar after returning from
Add Radio. The compact-height path capped each family/radio/task strip at the
chip height alone. When the row overflowed, the platform scrollbar consumed
that same vertical space and obscured the controls.

Each horizontal chip strip now computes its height from the scaled chip row plus
a distinct scrollbar lane whenever overflow exists. Range changes recompute the
lane, so font scaling, family count, radio count, task count, and window resizing
cannot reintroduce the overlap. The correction does not alter selection,
persistence, discovery, launch, or canonical identity behavior.

Regression evidence:

- constrained family-strip geometry and all-radio layout matrix: **13 passed**;
- complete Software Administration layout, radio-first UI, and canonical UI
  suite: **60 passed**;
- guided setup, unified Add Radio, Software Administration model, and workspace
  suite: **130 passed**;
- complete software/launch/guided regression partition: **718 passed, 4
  skipped**;
- changed Python compilation and `git diff --check`: passed.

No production database, application configuration, process, endpoint, radio,
commit, or remote was changed. The operator must relaunch the local application
to load the updated widget geometry.

## 2026-09-20 — GRS-13.3 JS8Call native identity and active-settings correction

Status: automated implementation gate passed; operator relaunch and fresh
Add/Replace Radio verification remain open.

Operator evidence showed a saved FT-710 JS8Call instance in Software
Administration with an opaque `fio-draft-js8call-*` value embedded in the
application-data directory and `DIRECTED.TXT` path. A read-only query of the
active local database confirmed that the same draft-derived rig identity had
been stored in the application row, manifest paths, and canonical projection.
No screenshot interpretation or launch-path assumption was used to diagnose
the defect.

The integration review found three linked causes:

- the production JS8 recipe derived `--rig-name`, settings, and data paths from
  the transaction-only draft key instead of the reviewed radio identity;
- the Software Instance Assistant did not retain the separately allocated
  durable application key when an operator opened and returned from detailed
  review, allowing the draft key to become the manifest/canonical identity;
- the native writer treated `--rig-name` and JS8Call's independent `--config`
  selector as if they were interchangeable. It targeted the detected default
  `JS8Call.ini` and wrote `MultiSettings/<radio>` while launch supplied only
  `--rig-name`, leaving the prepared values outside the active
  `Configuration` group.

The corrected contract allocates two intentionally different non-editable
identities when a draft begins: an internal draft transaction key and one
durable application key. The assistant preserves the durable key across every
page, detailed review, payload, and Save. The application row, manifest, launch
projection, and canonical identity use that durable key. Neither key is used as
a JS8Call native name or shown as normal Software Administration guidance.

The reviewed radio label now supplies the stable operator-readable JS8Call rig
name. Launch uses `--rig-name <radio>`. The same resulting application name
drives Qt-native settings and application-data paths on macOS, conventional
Linux, and Windows; Windows uses the application-specific
`AppData/Local/<application name>/<application name>.ini` ConfigLocation. Save,
forms, `DIRECTED.TXT`, `ALL.TXT`, and `inbox.db3` resolve from the same native
data identity. The old private `.freqinout/managed-instances` root is not a
normal JS8 target.

For a new distinct rig identity, the qualified writer now writes only the
rig-specific native settings file, preserves the detected/default
`JS8Call.ini` as source evidence, and places reviewed values in the active
`Configuration` group. It snapshots the exact new target before apply, verifies
readback, and removes/restores it on rollback. An alternate MultiSettings
profile requires the separately reviewed `--config` recipe and is not inferred.
The complete reviewed settings mapping, including callsign/grid when supplied,
survives preview-to-worker reconstruction.

Existing saved rows with draft-derived native identities are not silently
renamed, moved, or rewritten. They remain review/recovery cases and should be
corrected through the authoritative **Replace instance** transaction. This is
the no-damage choice because an older identity may already own native files or
a running process.

The acceptance gate now starts with the real distinct-draft allocator, runs the
production recipe resolver, persists the application and manifest, saves and
reloads the canonical record, projects Software Administration, and validates
the launch/native-writer representation. It asserts that the durable manifest
and canonical keys match, the JS8 rig and native paths use the radio label, the
writer modifies the active configuration of the rig-specific file, and no
draft key or private managed root appears in persisted paths or normal UI text.

Work-package ownership:

- Primary high-reasoning model: architecture, JS8Call source verification,
  native writer semantics, durable identity/persistence integration, delegated
  diff review, specification/work-log update, and final regression review.
- `gpt-5.6-terra`, medium reasoning: read-only production-path and canonical
  projection audit. It found the remaining manifest/canonical split and the
  normal-UI exposure of internal identities; the primary implemented and
  reviewed the fixes.
- `gpt-5.6-luna`, low reasoning: bounded JS8 recipe/UI regression assertions.
  Its initial failing tests reproduced draft-key/native-path leakage; the
  primary reviewed and extended them through real persistence and writer gates.

Automated evidence:

- focused JS8 identity, persistence, Software Administration, native writer,
  discovery, and launch suite: **157 passed**;
- complete guided/software/launch regression partition: **722 passed, 4
  skipped**;
- changed Python compilation and `git diff --check`: passed.

During the first broadened test run, an older native-writer test lacked an
injected temporary JS8 home and created the exact synthetic `Radio-A` settings
file plus empty Save/forms directories in the operator's Library. Their fresh
timestamps and synthetic test contents were verified; only those exact test
artifacts were removed. The test now supplies a temporary native home. The
focused and complete reruns left no `Radio-A` artifact outside the test
directory.

No production database, existing JS8Call identity, unrelated native file,
process, endpoint, radio, commit, or remote was changed. Live stock/Improved/
Subspace qualification on Windows and Linux remains required before closing
the wider GRS-13 external-application gate.

## 2026-09-20 — GRS-13.4 Fast Light native-bundle parity and planner handoff correction

Status: automated slice gate passed; fresh operator Add/Replace Radio and live
external-application verification remain open.

Operator screenshots proved that the prior GRS-13 implementation did not meet
the all-software invariant. A saved FT-710 Fast Light bundle displayed FLRig
and FLDigi executables but blank FLMsg/FLAmp applications and message folders;
FLDigi still used a `runtime/.../managed-instances/draft-fast_light-*` path,
the bundle was labeled Operator-managed, and Plan Builder reported no radio
context after the guided handoff.

Read-only inspection of the active development database confirmed split
persistence rather than a rendering defect. The launch recipe contained
FLMsg/FLAmp executable components, while the linked device row retained
`use_flmsg=0`, `use_flamp=0`, blank component/message paths, and draft-private
FLDigi roots. The adapter also defaulted a managed draft's manifest to
`operator`, and the canonical record did not reuse the exact saved manifest
key or all message-resource projections.

The corrected production path now:

- derives readable collision-resistant FLRig, FLDigi, and radio NBEMS roots
  from the final radio label plus durable application key under the
  application-native roots, never under the FIO private/runtime root;
- launches FLDigi and FLMsg with one persisted radio `WRAP/auto` path and
  launches FLMsg with its complete radio NBEMS root;
- persists FLMsg application/root/messages/templates/auto and FLAmp
  application/receive/outgoing claims from the reviewed component recipe even
  after the details assistant hides derived fields;
- carries the operator's explicit FLMsg/FLAmp selections independently from
  discovery, so a discovered but unselected utility is not added and a
  selected utility with incomplete discovery is retained as launch-pending
  rather than silently omitted;
- atomically projects component flags, executables, message sources, manifest,
  structured launch rows, FIO-managed ownership, and canonical identity;
- treats unqualified FLAmp native state truthfully as station-shared with
  limited attribution, retains its standard sources, and suppresses automatic
  startup instead of fabricating per-radio isolation;
- validates canonical Fast Light paths against both linked application fields
  and exact manifest resources; and
- uses the guided handoff radio ID for Plan Builder RF Guard validation before
  a new plan has an assignment row.

The end-to-end acceptance gate starts with the real distinct-draft allocator,
resolves a four-component Fast Light recipe, calls the Settings-owned Add Radio
adoption adapter, saves through the real store, reloads the radio, manifest,
launch bundle, Software Administration inputs, and canonical record, and
asserts zero projection drift. It also proves that no draft key or `.freqinout`
path survives, FLMsg arguments are structured, and FLAmp cannot autostart when
the recipe says the operator owns the shared process.

Automated evidence:

- focused Fast Light/native identity/planner partition after the final
  selection/discovery separation: **155 passed**;
- complete guided/software/launch/FreqPlanner regression partition:
  **820 passed, 4 skipped**;
- changed Python compilation and `git diff --check`: passed after final review.

The monolithic all-repository process reached the unrelated Compose GUI tests
and the Python interpreter exited with signal 11 while a live JS8 reader thread
and Qt event-loop test were both active. The exact Compose test passes alone
(`1 passed`), and the complete 820-test guided/software/launch/FreqPlanner
partition passes in one process. This is recorded as test-harness process
isolation evidence, not a product assertion failure or a waived acceptance
failure for this slice.

Work-package ownership: the primary high-reasoning model handled architecture,
native-directory rules, atomic projection, canonical parity, planner context,
tests, specification, and final integration review. No subagent was used for
this correction because the current request did not authorize a new delegated
work package. No production database, native application file, external
process, endpoint, radio, commit, or remote was changed.

## 2026-09-20 — HF Daily/HF Nets New Schedule draft semantics

Status: automated gate passed; operator visual verification remains open.

The HF Daily and HF Nets **New Schedule** actions previously detached the saved
source identity but left its visible rows in place and always erased the
editable name. That made New behave like “save the current schedule under
another name,” regardless of whether the operator named the intended draft
before or after clicking the action.

Both editors now use the same contract. New Schedule first honors the existing
unsaved-change confirmation, then detaches the saved source, clears the schedule
rows, and opens an unsaved draft. A name typed before New is retained; clicking
New first leaves a blank editable name. The name of a merely selected saved
schedule is not copied. HF Daily presents its normal empty entry row, while HF
Nets presents an empty table, matching each editor's established row-entry
behavior.

Automated evidence:

- four direct interaction-order regressions: **4 passed**;
- Plan Builder, HF Daily, HF Nets, and shell schedule regression partition:
  **133 passed, 91 deselected**;
- changed Python compilation and `git diff --check`: passed.

The primary high-reasoning model handled behavior design, implementation,
tests, specification, and integration review. No saved schedule, production
database, application configuration, process, commit, or remote was changed.

## 2026-09-20 — Fixed FIOSpotter Expect request and JS8 relay reply correction

Status: automated exit gate passed; live two-station RF relay verification
remains external qualification.

Operator evidence showed that the relayed request
`W8UFO: W8UFO: W5TTA> E? F!701C *DE* WM8Q ♢` did not trigger the saved
Expect response. Review found the fixed-form flow attached to the wrong event:
both Spotter parsers deliberately discarded `E? F!` queries, while completed
`F!` form traffic was evaluated as if it were a request. That prevented the
reported response and introduced a reply-loop hazard.

The corrected flow now parses exact fixed `E? F!<form-id>` requests before form
traffic, rejects unstructured leading text and stale/replayed requests, and
never evaluates a completed `F!` payload for reply. It accepts source-scoped
live `RX.DIRECTED` and `DIRECTED.TXT` observations, shares one durable request
identity across those adapters, and preserves the existing pause, caller/group
policy, source, reply-limit, cooldown, selected-target, guarded-send, and audit
gates.

JS8Call source review established that relay transport has no separate API
parameter. FIO must authorize the first `*DE*` callsign (`WM8Q`), reconstruct
the JS8-native reverse route (`W8UFO>WM8Q`), clear/verify stale selected-target
state, and submit the exact explicit text through `TX.SEND_MESSAGE`. FIO does
not add the local `W5TTA:` prefix or forward RF frames itself. The exact
regression transmits
`W8UFO>WM8Q F!701C 100 ST[TX] GR[EM12JV] #ISF0`; JS8Call owns forwarding and
local sender presentation.

Work-package ownership:

- primary high-reasoning model: protocol architecture, JS8Call source
  comparison, parser/dispatch implementation, safety/dedupe decisions,
  delegated-diff review, specifications, and final integration review;
- `gpt-5.6-terra` (high): independent read-only JS8Call relay-grammar/source
  audit;
- `gpt-5.6-luna` (medium): focused read-only Expect test and compatibility-risk
  audit.

Acceptance evidence:

- exact parser/relay/direct/dedupe/form-loop focused partition: **50 passed**;
- broader Spotter, Expect, JS8 send/policy, MCForm, archive/import, and message-
  ingest partition: **223 passed**;
- changed Python compilation and `git diff --check`: passed.

No schema or production-data migration was required. No production database,
JS8Call settings/profile, native application file, external process, radio,
commit, or remote was changed. The unrelated modified installation-guide DOCX
and rendered guide directory were preserved and excluded from this work.

## 2026-09-20 — MeshCore / Meshtastic transport truthfulness and local connection extension

Status: automated implementation gate passed; representative Linux/Windows
serial/TCP/BLE hardware qualification remains open.

The Local Mesh editor previously offered TCP, USB serial, BLE, HTTP, and MQTT
for both protocols even though only Meshtastic TCP/serial/BLE and MeshCore BLE
had adapter code. Meshtastic was not packaged as a runtime dependency, its TCP
adapter discarded the configured port, MeshCore TCP/serial were routed to the
BLE-only adapter, and `Allow Send` could display an enabled capability that no
adapter implemented.

The governing mesh specification now contains an explicit protocol/transport
matrix, dependency and Python-version rules, discovery ownership, one-session
lifecycle/concurrency rules, failure and teardown semantics, receive-only
truthfulness, outbound completion boundaries, and automated versus physical
acceptance gates. The implementation:

- packages the official Meshtastic 2.7 and MeshCore 2.3 client families while
  preserving lazy imports and the MeshCore Python 3.10 floor;
- gives UI, validation, and adapter selection one shared capability matrix;
- passes Meshtastic's configured TCP host and port and preserves its official
  fixed 115200 serial behavior;
- adds MeshCore Companion serial/TCP through the official Python client on one
  persistent adapter event loop, with the application handshake, automatic
  waiting-message fetching, channel/contact normalization, bounded
  cancellation/disconnect, and FIO-owned reconnect policy;
- preserves the existing qualified FIO/Bleak MeshCore BLE path unchanged;
- offers only TCP, USB serial, and BLE for new supported connections, while
  retaining saved HTTP/MQTT records visibly as unavailable evidence rather than
  coercing or deleting them; and
- makes legacy `send_enabled` values inert and consistently presents the
  current product as receive only.

The primary integration review corrected one redundant UI refresh path, added
official `meshcore_py` snake-case normalization, verified the exact Meshtastic
`portNumber` API, and kept Meshtastic BLE browse out of this slice because the
existing scan worker is correctly MeshCore/NUS-specific. Meshtastic BLE remains
usable by exact saved device id/name through the official client's bounded
service-filtered Connect discovery; a dedicated FIO browse list is a separate
follow-up.

Work-package ownership:

- `gpt-5` primary high-reasoning model: connection architecture, lifecycle and
  concurrency, dependency policy, official-client verification, MeshCore
  serial/TCP adapter, normalization, specifications, delegated-diff correction,
  and final integration review;
- `gpt-5.6-terra` high reasoning: read-only SpotterX and mesh-client transport,
  pairing, receive, send, and lifecycle comparison;
- `gpt-5.6-luna` medium reasoning: read-only FIO adapter/UI/test gap audit; and
- `gpt-5.6-terra` medium reasoning: bounded Settings capability presentation
  and focused UI regression tests.

Automated evidence:

- `uv run pytest -q tests/test_mesh_client_foundation.py tests/test_mesh_connection_ui_slice1.py tests/test_mesh_slice1_lifecycle.py tests/test_mesh_slice1_reconnect_flow.py tests/test_mesh_slice1_settings_integration.py`:
  **163 passed**;
- `uv run pytest -q tests/test_mesh_channel_admin_slice1.py tests/test_source_connection_snapshot.py`:
  **15 passed**;
- changed Python compilation and `git diff --check`: passed.

The monolithic all-repository process again reached the unrelated Compose GUI
partition with a live JS8 reader thread and the Python interpreter exited with
signal 11 in the Qt event-loop test. The exact reported Compose test passes in
isolation (**1 passed**), and the combined mesh acceptance partition passes in
one process (**178 passed**). This reproduces the already documented test-
harness process-isolation failure; it is not a mesh assertion failure and no
mesh acceptance test was waived.

No schema, settings, or production-data migration was required. No device,
native application configuration, external process, radio, commit, or remote
was changed. The unrelated modified installation-guide DOCX and rendered guide
directory were preserved and excluded from this work.

## 2026-09-20 — Tri-Mode VarAC topology recovery and writer routing

Status: automated implementation gate passed; live Windows/Linux-Wine Add
Radio qualification remains open.

Operator testing found that the safe default `Standalone VarAC node` was sent
through the cluster-only native writer. Its correct refusal instructed the
operator to choose Create Cluster or Join Cluster, but the prepared-card state
then hid the VarAC arrangement selector. The route therefore exposed a recovery
instruction without its recovery control and could not continue.

The correction makes topology authoritative before writer selection.
Standalone now prepares a distinct non-mutating FIO identity, retains any
discovered node-local paths, records native standalone configuration as
operator-owned, and returns a non-blocking `Ready with warning` state without
starting the cluster writer. Its arrangement selector stays visible so the
operator can opt into Create or Join. Only explicit Create/Join starts the
generation-fenced native cluster worker and still requires a qualified current
bundle before Next or Save. A blocked Create/Join result keeps the selector
visible and enabled beside the exact reason, and changing it invalidates and
reprepares the canonical context.

Automated evidence:

- focused real-widget topology and Tri-Mode route: **29 passed**;
- broader guided-radio, VarAC arrangement, production-shaped audit,
  final-apply, and native-preparation partition: **131 passed**;
- changed Python compilation and diff hygiene: passed.

The high-reasoning primary model owned the state-machine design, implementation,
specification, tests, and final integration review. No subagent was used because
this surgical correction did not include a new delegation request. No schema or
data migration was required. No production database, VarAC/VARA file, external
process, radio, commit, or remote was changed. The unrelated modified
installation-guide DOCX and rendered guide directory were preserved and
excluded from this work.

## 2026-09-20 — Linked VarAC topology identity and responsive discovery correction

Status: automated implementation gate passed; live Linux/Wine and Windows Add
Radio qualification remains open.

Production testing after the canonical Add Radio/Software Administration parity
slice found that `Create cluster` was visible but native preparation rejected the
same saved standalone node as missing or ambiguous. The recovery message named a
refresh action that the route did not provide, Software Administration appeared
to lose the associated paths, and the attached CPU sample showed the JS8 profile
worker still parsing a settings file after the UI discovery budget had expired.

The root cause was an authority-boundary error: topology recommendation accepted
only path-complete inventory rows. A durably radio-linked VarAC node whose paths
needed review was therefore removed from the arrangement metadata even though
the database link still identified it. The correction separates topology
identity from application qualification. A linked incomplete node remains a
named Create-cluster candidate; the native worker qualifies its paths. If UI
metadata is absent, the worker recovers only an exact single linked standalone
node outside any cluster. An explicit stale ID is never replaced, and multiple
nodes require the named arrangement choice. Recovery text now points only to the
visible VarAC arrangement.

The save/reload contract was checked with a real temporary `MultiRadioStore`:
VarAC install, INI, database, VARA runtime/INI, incoming, outbox, BBS/archive,
and launch values survive adoption and reopen in the Software Administration
state. The exact VARA executable relative path is now carried in each immutable
native member plan and included in its fingerprint so final launch persistence
cannot read that fact from the unrelated writer capability. The adjacent
receive-only Review path was also corrected so the visible `No application
launch` label is not treated as a receiver application identity.

For responsiveness, guided JS8 settings discovery now enforces a 1 MiB candidate
limit, reads in cancellation-checked 64 KiB chunks, checks cancellation while
parsing both ConfigParser and QSettings layouts, and stops before later
candidates after cancellation. This stays inside the existing background,
generation-fenced discovery coordinator; no filesystem scan or parser was moved
onto the UI thread.

Automated evidence:

- focused topology, native preparation, bounded discovery, and real-store
  projection: **104 passed**;
- real-widget Add Radio, VarAC operator route, Software Administration layout,
  and final-apply gate: **71 passed**;
- native writer/final handoff and guided-save regression gate after adjacent
  corrections: **60 passed**;
- broader guided inventory, save transaction, responsive assistant,
  production-shaped audit, canonical identity, launch recipe, native
  transaction, and Software Administration partition: **157 passed**;
- contextual operator-help registry gate: **5 passed**;
- changed Python compilation and `git diff --check`: passed.

Work packages and models:

- architecture, concurrency boundary, VarAC topology/persistence changes,
  migration review, specification/help/work-log updates, delegated-diff review,
  and final integration: **gpt-6-astra, high reasoning (primary)**;
- bounded and cancellable JS8 settings discovery plus its focused tests:
  **gpt-5.6-terra, medium reasoning**;
- focused integration tests were completed by the primary after the requested
  lower-cost test worker could not be started because the task's agent slots
  were already occupied.

No schema or production-data migration was required. The supplied production
database was inspected only through a copied/read-only audit path. No production
database, VarAC/VARA or JS8Call file, external process, radio, commit, or remote
was changed. The unrelated modified installation-guide DOCX and rendered guide
directory were preserved and excluded.

## 2026-09-20 — Collision-free managed VARA runtime retry

Status: automated implementation and integration gates passed; live
Linux/Wine and Windows Add Radio qualification remains operator-assisted.

The supplied production log showed the corrected linked-node discovery reach
native preparation, then stop because a deterministic managed VARA runtime
already existed. The native writer's refusal to replace an arbitrary runtime
was correct; Add Radio's preparation layer had no collision-free naming policy
and repeatedly proposed the occupied target.

Preparation now chooses the first absent readable sibling (`VARA`, `VARA-2`,
`VARA-3`, and so on) for both a converted standalone member and the new cluster
member. Existing directories, files, and broken symlinks are preserved and
count as occupied. Targets reserved earlier in the same immutable plan also
count as occupied, preventing normalized duplicate radio labels from sharing a
runtime. The writer's independent validation and apply-time no-replacement
checks were deliberately left unchanged.

Focused evidence: native preparation and writer safety suites **35 passed**,
including new occupied-target, stable retry, same-label, and broken-symlink
cases. No filesystem target was created by preparation and the occupied
sentinel remained unchanged.

Integration evidence:

- native preparation, writer, transaction, final apply, and launch round-trip:
  **69 passed**;
- VarAC arrangement and responsive assistant widgets: **52 passed**;
- production-shaped Add Radio and operator-route UI: **26 passed**;
- Software Administration persistence, model, and workspace: **54 passed**;
- complete guided software, inventory, recipe, and atomic-store partition:
  **214 passed**;
- contextual help registry: **5 passed**;
- changed Python compilation and `git diff --check`: passed.

Architecture, safety review, implementation, specification/help/test changes,
and integration are owned by the high-reasoning primary model. No subagent was
used because this surgical correction did not include a new delegation request.
No schema or data migration is required. No production database, VarAC/VARA
file, external process, radio, commit, or remote was changed. The unrelated
modified installation-guide DOCX and rendered guide directory remain preserved
and excluded.

## 2026-09-20 — VarAC shared-install final-save correction

Status: automated implementation gate passed; live Linux/Wine and Windows
standalone-to-cluster qualification remains operator-assisted.

Production testing reached final Add Radio save with correct generated paths,
then rolled the radio back because the old standalone manifest described the
common VarAC installation working directory as member-exclusive. A
production-shaped regression also reproduced the same error for the executable
and shared database. The UI compounded the failure by titling the recovery
message `Saved — one app needs attention`, and the handled validation exception
was absent from the log.

The persistence boundary now normalizes the explicit cluster ownership model.
Executable/install working directory, cluster database, and cluster
BBS/archive are nonexclusive shared references. VarAC INI, cloned VARA runtime
and INI, incoming/outbox, ports, and member number remain exclusive. Creating a
cluster re-scopes the existing standalone manifest, updates its verified native
paths, creates both memberships, saves the new member, and mirrors the existing
radio's canonical Software Administration identity within the same outer
transaction. Unknown claims retain their reviewed exclusivity.

Member-number validation now runs before generic manifest collision reporting.
A rejected final persistence step explicitly says `Nothing Saved`, logs the
family/radio/phase/detail, and logs both scheduling and successful completion of
native compensation; recovery failures retain their existing error path.

Focused delegated store coverage was implemented by `gpt-5.6-terra`, medium
reasoning, and reviewed/adjusted by the primary. Architecture, transaction
semantics, UI/canonical parity, diagnostics, specification/help reconciliation,
and final integration are owned by the high-reasoning primary model. The
requested Luna diagnostics package could not start because all remaining agent
slots were occupied by completed historical workers, so the primary retained
that bounded package rather than delaying the gate.

Final combined evidence: **277 passed** across manifest persistence, assistant
payloads, Add Radio/Software Administration adapters and canonical parity,
VarAC arrangement, native preparation/writer/transaction/final apply,
responsive real widgets, production-shaped operator routes, and contextual
help. Changed-file compilation and `git diff --check` passed. A repository-wide
run progressed through its first 170 tests, then the Python process exited with
a Qt segmentation fault in the unrelated Compose layout coalescing test while
a JS8 reader thread was active; that exact test passed **1/1** when rerun alone.
No assertion failure from this change was suppressed.

No schema or destructive migration is required. No production database,
VarAC/VARA file, external process, radio, commit, or remote was changed. The
unrelated modified installation-guide DOCX and rendered guide directory remain
preserved and excluded.

## 2026-09-20 — Add Radio atomic UI publication and Edit Apps correction

Status: automated implementation gate passed; operator retest remains open.

Production evidence showed FT-710 in the Radios workspace after a reviewed
Tri-Mode + FIO Spotter + CommStat save, while Software Administration contained
only FTDX-10 and the supplied database had no durable FT-710 radio or canonical
identity rows. The database retained only orphan manifests from an earlier
attempt and a one-member `FTDX-10 + FT-710 VarAC` cluster. The visible FT-710
was therefore a provisional in-transaction UI projection, not a committed
radio. Opening Edit Apps then exposed legacy component toggles that attempted
an unsafe partial reassignment and surfaced the internal
`adopt_software_instance(..., replace_existing=True)` instruction.

The guided Add/Edit owner now suppresses table refreshes, runtime projection
reloads, and public radio-inventory signals for the complete outer transaction.
After commit or rollback it performs one authoritative multi-radio reload;
only a successful commit emits the public inventory change. Radios, Software
Administration, readiness, launch, and cluster views therefore cannot publish
different generations. Edit Apps now reopens the same guided Software step as
Add Radio, and compact software flags are read-only for canonical-backed
radios.

The supplied database also contains the intended
`FTDX-10 + FT-710 VarAC` cluster with only FTDX-10 as member 1. The topology
adapter now recognizes that exact deterministic partial state and presents
`Resume FTDX-10 + FT-710 VarAC: add FT-710 as member 2 — Recommended` as an
explicit choice. It does not auto-select the mutation, infer from orphan
manifests, or alter unrelated cluster choices. GRS-14.4 and operator help record
this recovery contract.

The database's three FT-710 manifests are also orphaned: each points to an
application system key that has no application row. Manifest conflict checking
now retains such rows as diagnostics but excludes them as resource owners, so
their stale JS8 endpoint and working-directory claims cannot veto the reviewed
recovery. Linked manifests retain the existing collision rules; no orphan is
silently deleted.

GRS-13.5 records the atomic publication and Edit Apps contract. Operator help
now explains the all-or-nothing result and explicitly lists the five Software
Administration families produced by Tri-Mode + FIO Spotter + CommStat.

Acceptance evidence:

- guided final-apply, radio-scoped Settings, canonical selected-stack identity,
  VarAC arrangement, and real arrangement-widget routes: **214 passed**;
- guided software model/discovery/proposals, Software Administration adapter,
  editors/layout/model/persistence/workspace, instance assistant/manifest, and
  contextual help: **195 passed**;
- changed Python compilation and `git diff --check`: passed.

No production database, orphan metadata, application file, external process,
radio, commit, or remote was changed. The unrelated installation-guide DOCX
and rendered guide directory remain preserved and excluded.

## 2026-09-21 — Add Radio discovery completion handoff race

Status: focused and broader automated gates passed.

The attached production log proves discovery itself was healthy: applications,
Fast Light, JS8 profiles, and VarAC all finished, and the complete request
finished successfully in **101 ms**. Logging then stopped while the dialog
remained on `Discovery in progress`. The attached CPU hotspot was recorded
before Add Radio started and showed ControlFreq tooltip traversal plus MeshCore
BLE import/connect activity; it is not the discovery blocker.

The defect was a queued-event ownership race. The worker result relay and
`QThread.finished` were independently queued to SettingsTab. If thread release
arrived first, it removed both the Qt wrappers and the completion callback. The
later successful result was silently ignored, leaving the dialog permanently
in progress.

Thread release now removes only worker/thread ownership. The success or failure
handler atomically consumes its callback and logs a diagnostic if one is
unexpectedly absent. Deterministic tests deliver thread release before success
and prove the result still publishes once; the failure path also consumes its
callback once. GRS-12.4 and operator help record this lifecycle contract.

Focused final-apply, prepare-first, and production-shaped guided operator UI
coverage: **40 passed**.

Broader guided discovery/model/proposal, production-shaped audit, VarAC native
acceptance, final-apply, and real guided widget coverage: **93 passed**.

## 2026-09-21 — Fast Light radio-name roots and guided step positioning

Status: focused and broader automated gates passed.

Production screenshots showed a newly prepared FT-710 Fast Light identity using
`FT-710-99bc8c38` below the native FLDigi/NBEMS locations. The suffix was the
first eight characters of FIO's durable application key. It was collision-safe
but violated the accepted operator model: opaque FIO identities belong in the
database/manifest, while application-native configuration should be recognizable
and usable without FIO.

New Fast Light preparation now derives its native child solely from the radio
display name using a conservative cross-platform slug. `FT-710` remains
`FT-710`; `Field Radio 1` becomes `Field-Radio-1`. Existing explicit reviewed
paths remain unchanged. Duplicate exclusive roots continue through the existing
collision safety path rather than receiving a hidden hash suffix.

The Guided Add Radio step group now has fixed vertical sizing so maximizing the
dialog gives extra space to the single scrolling body instead of the header.
Every explicit wizard-page transition resets the body scroll to the top both
immediately and after the visibility/layout event, ensuring Safety begins with
antenna and supported-band controls.

Focused Fast Light recipe, managed-recipe UI, and production-shaped guided
operator widget coverage: **54 passed**.

Broader Fast Light identity, canonical Add Radio/Software Administration,
prepare-first, responsive real-widget, final-apply, and unified guided UX
coverage: **138 passed**.

## 2026-09-21 — Launch Control action semantics and multi-radio isolation

Status: focused and broader automated gates passed; production operator retest
remains open.

The Launch Control surface presented three related controls without enforcing
three independent meanings. Changing only the radio-level automatic-start
switch did not mark Settings dirty, so it could be lost. `Start Startup Apps`
used the saved automatic-start gate even though it is an explicit operator
action, causing a checked startup set to report that nothing was selected when
automatic startup was off. Row `Start` also passed the selected item through a
legacy station catalog normalizer, which discarded radio-owned instance
identity, monitor state, dependencies, working directory/readiness facts, and
execution scope before launch.

The selected-radio contract is now explicit and consistent:

- `Monitor Health` controls health reporting only and immediately displays
  `Monitoring off` when unchecked;
- `Launch at Startup` selects that radio's ordered startup set;
- `Automatically launch this radio's startup apps when FIO opens` gates only
  unattended FIO startup and is now a normal dirty, persistable setting;
- row `Start` launches only that exact selected-radio application recipe now;
- `Start Startup Apps` launches the checked selected-radio set now without
  silently enabling automatic startup.

Both explicit start actions now use the canonical selected-radio planner and
retain executable, arguments, working directory, environment, instance key,
dependencies, readiness policy, and execution scope. Automatic station startup
continues to honor every active radio's persisted automatic gate and startup
rows, deduplicating exact shared identities while keeping distinct rig
instances separate. An empty saved radio health bundle is authoritative and no
longer inherits another radio's legacy station-global monitor flags.

The governing reliability specification and operator help now state these
semantics directly. Focused radio-bundle, shell-health, and radio-scoped
Settings coverage passes **314 tests**. Broader orchestrator, identity, guided
recipe, receiver, VarAC, and multi-rig coverage passes **147 tests**. Changed
Python compilation and `git diff --check` pass.

## 2026-09-21 — Wine-native VARA paths and persisted cluster visibility

Status: focused automated gates passed; production operator retest remains
open.

Production database evidence showed that Add Radio had successfully saved the
`FTDX-10 + FT-710 VarAC` cluster and both memberships. The cluster table looked
empty because a separate stale `varac_cluster_mode_enabled=false` preference
short-circuited the table load. Saved topology is now authoritative: its rows
load, the cluster control is forced on, and it cannot be turned off until the
saved clusters are removed. Empty stations retain the existing optional setup
toggle.

The same saved plan showed that Linux/Wine preparation intentionally cloned
VARA below `.freqinout/managed-instances` and translated that host location to
Wine `Z:`. VarAC is a Windows application and expects its selected VARA
executable in the Wine filesystem. When source evidence identifies a
`drive_<letter>`, FIO now allocates distinct, readable `VARA-<radio>` runtime
siblings inside that drive and writes native values such as
`C:\VARA-ft-710\VARA.exe`. Existing targets remain untouched and the numbered
collision allocator plus transactional revalidation remain unchanged.

Production-shaped preparation coverage proves both members use Wine-native
drive paths and never generated `Z:` executable paths. Settings coverage proves
a saved cluster and membership remain visible when the stored display
preference is false. Focused preparation and cluster UI coverage passes **25
tests**. Broader native writer/transaction, Add Radio arrangement/final-save,
production-shaped audit, Software Administration, and Settings coverage passes
**299 tests** with 19 platform skips. Changed Python compilation and
`git diff --check` pass.

## 2026-09-21 — Exact manual launch gates and per-radio process attribution

Status: focused automated gates passed; production operator retest remains
open.

Production feedback confirmed that both `Monitor Health` and `Launch at
Startup` were checked, yet `Start Startup Apps` and row `Start` remained
disabled. The remaining blocker was not the row state: Settings applied the
primary operating model's unattended-start permission to explicit operator
actions, and the planner excluded an inactive selected radio. The explicit
selected-radio path now ignores only those automatic-start gates while keeping
recipe validation and the one-active-sequence guard. It does not activate the
radio or change command focus.

The running display and launch executor also used executable/family status as
their last attribution boundary. That is ambiguous when FT-DX10 and FT-710 run
the same binaries. Known direct processes now receive one bounded command-line
inspection, and both health projection and already-running suppression match
the selected radio's canonical executable plus its exact launch arguments.
Endpoint connection remains a separate result. The asynchronous status service
carries this identity through its scoped cache and shows a checking state,
rather than a station-wide family result, until the exact probe completes.

Focused launch bundle, endpoint status, refresh coordination, and radio-scoped
Settings coverage passes **204 tests** with 2 platform skips. Changed Python
compilation and `git diff --check` pass.

## 2026-09-21 — Projection warnings no longer veto valid radio launch plans

Status: implementation complete; production operator retest remains open.

Production launch evidence showed radio 9 was excluded before execution because
canonical projection review reported Fast Light policy differences, JS8Call and
VarAC endpoint differences, and a missing/duplicate VARA projection. No process
recipe had yet been attempted. The parity checker had incorrectly become a
whole-radio launch gate even though its contract describes bounded database
evidence for `Needs attention` and explicitly performs no runtime or filesystem
qualification.

Projection drift is now retained as a family-specific review warning while the
planner continues with the saved reviewed launch bundle. Launch Control exposes
the warning during and after a manual sequence. Independent launch-time checks
remain authoritative and blocking: native VarAC recovery, malformed recipes,
multi-instance/resource collisions, dependency failures, receive-only safety,
and concurrent sequences.

`Launch at Startup` and `Monitor Health` are now explicitly treated as mutable
operator preferences owned by the radio launch bundle. Changing those rows no
longer manufactures canonical software-identity drift for FLMsg, FLAmp, or any
other component. Focused identity, launch, status, and radio-scoped Settings
coverage passes **230 tests** with 2 platform skips. Broader launch, guided-save,
receiver, multi-rig, and unified-UX integration coverage passes **142 tests**
with 2 platform skips. Changed Python compilation and `git diff --check` pass.

## 2026-09-21 — Canonical recipe directory ownership contract

Status: specified, implemented, and regression-tested. No runtime data or
unrelated user files changed.

GRS-13 is now explicit that the reviewed and persisted canonical recipe is the
sole source of FIO-created directories across both `Settings > Radios > Add
Radio` and standalone `Settings > Software` / Software Administration, for
every supported application family. Final Save must create only the reviewed
directories before canonical commit or launch. Existing directories remain
in place with their contents preserved; operator-selected and adopted paths are
not created; a shared path is created only when the accepted recipe explicitly
authorizes that exact directory; and file targets (such as executables, INIs,
databases, logs, messages, archive files, launchers, and shortcuts) are never
created as directories. Missing directory authority remains `Needs choice` or
`Saved; launch setup pending` rather than being guessed.

Implementation introduces a typed `managed_directories` field on canonical
launch components. Managed JS8Call, FLRig, FLDigi, FLMsg, and reviewed FLAmp
recipes now publish their exact directory set. Add Radio and Software
Administration replace obsolete draft-root mkdir actions with that set before
native apply and persistence. Managed VarAC remains on its existing qualified
transaction, which creates only the reviewed member/cluster directories and
now records those exact targets in its persisted launch recipe. The
launch bundle persists the directory set, and launch preflight can repair
missing directories for both new and older saved FLRig/FLDigi/FLMsg/JS8Call
recipes without deriving another path. Missing targets are created recursively;
existing directories are idempotent; an existing file, blank target, filesystem
root, or home-directory target fails closed.

Delegation evidence: `directory_flow_audit` used `gpt-5.6-terra` at medium
reasoning for the read-only family/path audit; `vnc2_native_writer` used
`gpt-5.6-terra` at high reasoning for independent safety regression tests. A
pre-existing documentation delegate supplied the initial wording but did not
expose reliable runtime model metadata; the primary agent reviewed and finalized
the governing text and integration.

Acceptance evidence: the focused directory/native suite passes **79 tests**;
the broader software-identity, guided-save, launch-bundle, launch-control, and
VarAC regression suite passes **165 tests**. Changed Python compilation and
`git diff --check` pass.

## 2026-09-21 — Selected-radio launch identity versus station process names

Status: specified, implemented, and regression-tested; production operator
retest remains open.

Production evidence showed that the persisted FT-710 FLDigi command launched
successfully when run directly, but FIO did not attempt it while the FT-DX10
FLDigi process was already running. A selected-radio plan contains only the
selected radio's row, so the executor had incorrectly treated queue
cardinality plus the station-wide application process name as proof that the
selected instance was already starting. It then waited on the absent FT-710
endpoint without spawning the saved FT-710 recipe.

FLRig, FLDigi, and JS8Call now use the persisted endpoint identity even in a
one-radio queue. When only another radio's same-named process is visible and
the selected endpoint is absent, FIO launches the selected recipe. When the
exact selected executable-and-arguments process is already running, FIO polls
its endpoint and never launches a duplicate.

The companion Fast Light review preserves the distinct application contracts.
Managed FLMsg remains radio-scoped through exact `--flmsg-dir` and `--auto-dir`
arguments, and process matching rejects another radio's NBEMS root. The
currently qualified FLAmp recipe remains station-shared and operator-started;
endpoint relaunch recovery cannot create a duplicate FLAmp process.

Delegation evidence: `launch_identity_audit` used `gpt-5.6-terra` at medium
reasoning for the independent read-only FLRig/FLDigi/JS8Call and FLMsg/FLAmp
identity audit. The primary agent implemented and reviewed the change.

Acceptance evidence: the focused identity suite passes **77 tests** with 2
platform skips. The broader managed-directory, guided-save, launch-planner,
status, receiver, and native VarAC suite passes **200 tests** with 2 platform
skips. Changed Python compilation and `git diff --check` pass. A whole-suite
attempt encountered a macOS PySide segmentation fault while an unrelated JS8
reader thread was alive in the compose-layout test; that exact compose test
passes independently (**1 test**), so this is recorded as a suite-harness
limitation rather than launch-change evidence.

## 2026-09-21 — Launch bundle catalog integrity and radio-focus isolation

Status: specified, implemented, and regression-tested; Linux production retest
remains open. No production database or unrelated user artifact was changed.

Production evidence for radio 9 showed repeated FT-710 endpoint probes without
an FLRig spawn, canonical parity warnings that FLRig/FLDigi arguments, working
directories, and readiness differed, a missing/duplicate VARA component, launch
checkboxes that appeared not to persist, and custom-tool changes leaking across
radio contexts. The common causes were bounded and deterministic:

- custom-tool catalog reconciliation rebuilt every launch row from only its
  display name and two booleans, discarding its immutable instance key and
  complete canonical recipe;
- hidden VARA and SDR++ companion rows were discarded because they are not
  independent station catalog buttons; and
- after a radio-focus change, the still-painted previous-radio table was synced
  into the newly loaded radio cache by display name.

Catalog reconciliation is now lossless for existing rows and snapshots a new
custom command into the selected radio assignment. Editing a definition updates
only that selected radio's matching assignment; its identity name is immutable
after creation so a rename cannot orphan another radio's reference. Hidden
canonical companion rows
survive without becoming independent UI checkboxes. The launch table records
the radio it renders and refuses stale table-to-cache synchronization; each row
binds by immutable instance key. Canonical component dependencies are ordered
case-insensitively so stored `flrig` and displayed `FLRig` remain the same
dependency.

Already-damaged saved rows are repaired in memory from the selected radio's
committed GRS-13 identity and exact software manifest. FIO restores component
identity, executable, arguments, working directory, environment, dependency,
readiness, managed-directory, and scope fields while retaining that radio's
enabled/startup/monitor preferences. Manual and startup planning can use the
repaired recipe immediately; normal Settings Save persists it.

Delegation evidence: `flrig_startup_audit` used `gpt-5.6-terra` at medium
reasoning for tests covering FT-710 exact-argument launch plus per-radio startup
and monitoring persistence. `custom_tool_mapping_audit` used `gpt-5.6-luna` at
medium reasoning for tests covering lossless catalog operations and distinct
per-radio custom commands. The primary `gpt-6-astra` integrated the production
fixes, canonical recovery, specifications, work log, and broader validation.

Acceptance evidence: the focused launch, status, readiness, custom-tool, and
GRS-13 suite passes **116 tests** with 6 platform skips. The broader guided
recipe, native writer, receiver, Software Administration, and persistence suite
passes **275 tests**. Changed Python compilation and `git diff --check` pass.

## 2026-09-21 — FLMsg/FLAmp radio-scoped native launch correction

Status: specified, implemented, and regression-tested; Linux and Windows
production operator retest remains open. Existing application files and the
operator's unrelated documentation changes were not modified.

The prior work-log statement that the qualified FLAmp recipe remained
station-shared is superseded. Upstream source review found two precise launch
contract errors. Current FLMsg accepts `--flmsg-dir` but its advertised
`--auto-dir` parser is disabled; passing that argument can prevent startup.
Current FLAmp supports `--config-dir` plus explicit FLDigi ARQ and XML-RPC
address/port arguments, so it can have a radio-scoped process and native NBEMS
root instead of being forced into one station-shared process.

The canonical Fast Light recipe now gives each selected radio one stable NBEMS
root. FLMsg launches only with `--flmsg-dir`; FLDigi owns the matching
`--flmsg-dir` and `--auto-dir` handoff. FLAmp launches with that radio's
`--config-dir`, explicit ARQ endpoint, and FLDigi XML-RPC endpoint. The ARQ port
is allocated, collision-checked, displayed, persisted in the manifest, and
round-tripped through Add Radio and Software Administration. FLAmp receive,
transmit, scripts, and relay directories are derived below the same reviewed
radio root and are created by the existing canonical-directory transaction.
Exact executable-and-argument matching prevents another radio's FLMsg or
FLAmp process from satisfying readiness.

FLMsg's XML-RPC endpoint remains a per-root `FLMSG.prefs` fact, not a supported
command-line option. This slice does not claim an unqualified writer: the
intended FLDigi endpoint is review evidence until a platform/version exact
preferences writer passes backup/write/readback/rollback qualification. Legacy
FLAmp rows without a root or ARQ endpoint remain visible for reviewed
replacement and are not silently converted or auto-started.

Delegation evidence: `flamp_flmsg_cli_audit` used `gpt-5.6-terra` at medium
reasoning for read-only upstream/current-source launch-contract research;
`flamp_flmsg_test_audit` used `gpt-5.6-luna` at medium reasoning for independent
regression-gap analysis. The primary `gpt-6-astra` implemented and reviewed the
recipe, UI/persistence, specification, and test changes.

Acceptance evidence: focused launch-recipe, inventory, status, canonical
identity, assistant, native-directory, Software Administration, launch-bundle,
and review suites pass **182 tests**
with 2 platform skips. The slow all-guided offscreen sweep was interrupted and
the macOS PySide process faulted while handling that interruption in the GRS-74
theme test; that exact parametrized test passes independently (**12 tests**).
Changed Python compilation and `git diff --check` pass.

## 2026-09-21 — selected-radio FLAmp row-start identity recovery

Status: specified, surgically implemented, and focused regression-tested;
Linux production operator retest remains open. No application-native files or
unrelated operator documentation were modified.

Production evidence showed that the committed FT-710 Fast Light identity had a
qualified radio-scoped FLAmp recipe, but Launch Control's row-level **Start**
passed an in-memory bundle override after disabling the other rows. That
override replaced the already recovered saved bundle and therefore retained a
stale name-only FLAmp row. With no exact argument vector, process status could
collapse to executable-only matching and treat the running FTDX-10 FLAmp as the
FT-710 instance; no second process was started.

All in-memory bundle overrides now receive the same selected-radio canonical
recovery as saved bundles before review or launch planning. Immutable component
identity, executable, argument vector, working directory, dependencies,
readiness evidence, and managed-directory authority are restored while the
operator's enabled/startup/monitor choices remain unchanged. The executor
rejects a radio-scoped FLMsg/FLAmp row that still lacks an exact native launch
identity, before generic process matching. A reviewed alternate launcher can
remain eligible only with an explicit per-instance selector and exact non-empty
arguments. A canonical component restored because it was absent from an older
in-memory draft remains disabled, preventing row Start from launching an
unselected sibling. FLAmp was also added to the existing launch-preflight
directory repair map, which may recreate only the canonical recipe's persisted
managed directories.

Delegation evidence: `flamp_launch_regression_audit` used `gpt-5.6-luna` at low
reasoning for an independent, read-only trace of row Start, planner behavior,
and same-name process matching. The primary agent implemented the production
repair, specifications, tests, and final review.

Acceptance evidence: the broader launch-bundle, Launch Control isolation,
guided Fast Light recipe/UI, GRS-13 identity, managed-directory, and software
status suites pass **149 tests** with 4 platform skips. Coverage proves override
recovery, exact FT-710 FLAmp launch while an FTDX-10 instance is running,
fail-closed behavior for an unrecoverable name-only row, and canonical FLAmp
directory repair. Changed Python compilation and `git diff --check` pass.

## 2026-09-21 — automatic pre-canonical Fast Light child repair

Status: specified and surgically implemented; Linux production operator retest
remains open. No third-party application file is read, moved, or rewritten.

The legacy FT-DX10 could retain a linked Fast Light application and generic
FLMsg/FLAmp launch rows while having no newer canonical Fast Light identity.
The earlier recovery detector returned false in exactly that state, the Repair
action was hidden, and qualified FLAmp launch correctly refused the generic
row. This left a valid older station without a safe forward path.

Pre-canonical radios now derive an in-memory repair baseline from the one linked
Fast Light application, selected radio fields, and that radio's exact saved
launch rows. The repair bootstraps only FIO-owned manifest/canonical
projections, retains FLRig byte-for-byte, retains FLDigi's existing arguments
and adds only its NBEMS/ARQ companion pairs, qualifies FLMsg/FLAmp, and preserves
every Launch Control preference, custom tool, unrelated identity, and other
radio. A source fingerprint plus identity generation and manifest timestamp
reject any change between preparation and commit. Missing executables,
ambiguous ownership, and port conflicts remain explicit recovery conditions.

Launch preparation automatically commits this repair when the evidence is
unambiguous. Software Administration still exposes the component Repair action
for both canonical and pre-canonical configurations, so later operator or
filesystem changes have a consistent recovery surface. External directories
remain subject to the established launch-time managed-directory contract; the
repair itself creates nothing.

Delegation evidence: `component_repair_audit` used `gpt-5.6-luna` at low
reasoning for an independent read-only audit of repair eligibility, legacy
state, and regression boundaries. The primary `gpt-6-astra` agent implemented
the store, launch, UI, specification, help, and regression changes.

Acceptance evidence: component repair, GRS-13 canonical identity/UI, launch
bundle and selected-radio isolation, managed-directory, Software
Administration editor/persistence/workspace/model/layout, guided recipe/UI,
guided radio model, and proposal suites pass **243 tests**. Changed Python
compilation and `git diff --check` pass. Coverage includes automatic launch
preparation, a true pre-canonical FT-DX10-shaped assignment, exact FLRig and
FLDigi argument preservation, custom Launch Control rows/preferences, other-
radio isolation, no repair-time directory creation, rollback, and stale-plan
rejection.

## 2026-09-22 — FT-710 false-review and incremental runtime continuity correction

Status: GRS-15 specified, surgically implemented, and production-copy
validated. No production database or external radio-application file was
modified. Linux operator visual retest remains open.

The attached two-radio production database confirmed that FT-710's radio,
application, operating-model, frequency-plan, and VarAC-cluster configuration
was populated. Five visible review/degraded states came from one impossible
per-radio CommStat launch-path requirement even though CommStat is a
station-shared process. Schedule Control separately treated the absence of a
denormalized operating-model name as an absent assignment despite a valid
`operating_profile_id`. Rig Control and Connections also consumed the same
broad integration list, so one issue marked both tasks.

Readiness now follows ownership. CommStat no longer requires a radio-owned
launch path; the effective-assignment store projects the canonical Operating
Model name; and guided Rig Control owns only the selected control backend while
Connections owns the remaining application integrations. Standard JS8 native
layouts are compared by saved profile identity across platform configuration
and data roots rather than directory containment. Windows separators are
handled lexically. A shared VarAC database is accepted only when every affected
active radio is an enabled member of one matching persisted cluster.
Informational JS8 compatibility notices remain structured evidence but no
longer increment the review-required guardrail count.

The duplicate timer editor was removed from Configuration Main. Frequency,
FLDigi mode, JS8 offset, prompt cadence, scheduler enable, and default hold are
presented as selected-radio Schedule Control policy. The old station widgets
remain hidden solely to preserve legacy single-radio load/save compatibility;
they are no longer a competing operator-facing authority. The Help guide now
routes timer review and troubleshooting to the affected radio.

The same database carried migration version 2 while the build required version
3. Runtime construction therefore failed closed for both otherwise-active
radios and the scheduler repeatedly used profile-backed fallback clients. The
qualified additive v2-to-v3 transition now runs automatically before runtime
construction, creates only the receive-only Operating Model baseline, and
preserves existing radios, paths, assignments, launch policy, and topology.
Version-0 single-radio adoption remains explicit. The automatic transition is
an allowlisted `(2, 3)` pair rather than a general future-migration bypass, and
failure rolls back before subsequent Settings writes.

Delegation evidence: `readiness_ui_fix` and `guardrail_fix` were each dispatched
to `gpt-5.6-terra` at medium reasoning for bounded implementation and focused
tests. The primary `gpt-6-astra` reviewed both diffs, moved Operating Model name
projection into the canonical store query, added cross-platform JS8 path
handling, implemented and rollback-hardened the incremental migration, updated
the governing specifications/work log, and performed final integration.

Acceptance evidence: the integrated readiness, migration/runtime-status,
schedule guard, Settings task-ownership/timer-routing, and guardrail selections pass **49
tests** with 12 platform skips. A disposable copy of the attached database
produces zero FT-710 readiness issues, retains only the informational JS8
multi-endpoint notice, produces zero review-required guardrails, migrates from
v2 to v3, and restores active runtime IDs `(1, 9)` without changing either
radio. The broader related run reached **237 passed, 12 skipped** with two
pre-existing unrelated failures: legacy JS8Spotter linked-row mirroring and a
brittle Guided Add Radio source-string assertion. Those failures are recorded
and were not waived as evidence for this slice. Changed-file compilation and
`git diff --check` are required before handoff.

## 2026-09-22 — two-radio UI event-loop contention correction

Status: specified and surgically implemented; Linux operator retest remains
open.

The attached `freqinout (61).log` recorded 28 watchdog stalls after startup,
and the five supplied thread captures measured stale UI heartbeats from 8.3 to
81.1 seconds. This was a real event-loop starvation condition. Configuration
load reached 54.3 seconds, running-status repaint reached 16.1 seconds, passive
process inventories reached 5.0 seconds, schedule projections repeatedly
reached 1–3.6 seconds, and a VarAC guard job exceeded 35 seconds.

The captured main thread identified two direct UI feedback paths. Runtime
status text written into Launch Control column 4 emitted `itemChanged`, entered
the operator checkbox handler, dirtied Settings, and rebuilt section
navigation. Bulk Settings population also emitted contextual Auto-Fill and
VarAC helper work for each field. Both paths are now batch/signal safe: only
columns 1 and 2 qualify as operator launch edits, status painting uses
`QSignalBlocker` and skips unchanged text, and derived helper work runs once
after `_loading_settings` clears.

The captures also showed recurring ingest workers inside Settings schema and
Launch Control migration code while the GUI waited in launch-bundle reads.
`SettingsManager(runtime_worker=True)` now provides the same thread-owned
cached `get`/`set` surface without repeating startup initialization. Background
ingest exclusively requests that mode. Launch-bundle `get_bundle` and audit
listing now use read-only SQLite connections after constructor-time schema
initialization, so a command-bar or Configuration refresh cannot request a WAL
journal lock. Passive process inventory TTL is 30 seconds station-wide;
explicit post-launch/manual refresh still forces immediate discovery.

Acceptance passes the Settings worker/thread-affinity, launch-bundle
ownership/migration, background ingest, background refresh planner,
selected-radio software status, endpoint status, and Settings UI source
contracts: **272 passed, 5 skipped, 1 known deselected**. The deselected test is
the previously recorded brittle Guided Add Radio source-string assertion and is
unrelated to these paths. Changed Python compilation and `git diff --check`
pass. An isolated real-Qt navigation/resize soak also passed with a 628 ms first
usable shell, 11.6 ms maximum event-loop lag, and 12 ms shutdown. Linux
production retest remains required before push.

## 2026-09-22 — launch-at-startup duplicate-instance fail-closed correction

Status: specified and surgically implemented; Linux operator qualification is
open.

The operator observed multiple copies of every configured startup application.
The attached `freqinout (62).log` also showed that the restarted FIO process had
a cold dependency cache and that endpoint/process discovery remained
asynchronous. Code review confirmed the race: LaunchOrchestrator read the cold
shared cache as `running=False`, reached `Popen`, and only then requested an
asynchronous forced refresh. A matching process with an unavailable configured
port could separately consume the 90-second post-spawn readiness timeout even
though spawning another copy was never safe.

The same log contains a separate FIO shell-startup regression: database setup
used 10.8 seconds, Settings construction used 43.5 seconds, SOP construction
used 4.8 seconds, Ops Center construction used 9.1 seconds, and an additional
approximately 39-second interval preceded the measured widget construction.
That startup-construction problem occurs before Launch Control runs and is not
silently combined with this duplicate-process safety patch; it remains a
separate open production-performance gate.

All startup and manual launch paths now share a fail-closed preflight. FIO waits
for publication of a fresh asynchronous station process inventory before the
first spawn. FLRig, FLDigi, and JS8Call then receive a current configured-port
probe for the exact radio endpoint. A live endpoint is credited as already
running; an exact running process with a non-ready endpoint is reported as a
process/port mismatch; pending or timed-out evidence skips launch rather than
authorizing a duplicate. Sequence-local identity claims also prevent a repeated
row from racing the post-launch cache refresh. The existing planner still keeps
two radios' genuinely distinct executable/argument/endpoint identities
separate.

Delivery packages:

- `gpt-6-astra` high-reasoning primary agent: concurrency/lifecycle design,
  LaunchOrchestrator implementation, integration, specification, and exit gate.
- `gpt-5.6-luna` at medium reasoning: independent read-only audit of the cold
  cache race, endpoint identity boundaries, and missing regression cases. The
  audit made no file changes and its findings were reviewed before integration.

Focused launch, planner, dependency-status, endpoint, Fast Light repair, JS8,
VarAC, receiver, managed-directory, and multi-rig acceptance passes **187 tests
with 4 platform skips**. Coverage includes cold process evidence, pending
FLRig/FLDigi/JS8 endpoint evidence, occupied-port suppression, exact-process
verification, process/port mismatch fail-closed behavior, distinct-radio
launch, and reuse of one process inventory across endpoint probes. Two broader
adjacent runs reached **320 passed, 2 skipped** and **273 passed** respectively;
each retained one previously documented unrelated failure (the brittle Guided
Add Radio source-string assertion and legacy JS8Spotter linked-row mirroring).
Changed Python compilation and `git diff --check` pass; Ruff is unavailable in
the project virtual environment. A project-wide pytest attempt reached the
unrelated Compose acceptance area and then exited with a native Qt segmentation
fault while an existing JS8 reader thread was active, so that full-suite gate is
not claimed. The focused implementation gate passes; Linux operator
qualification remains open.

## 2026-09-22 — FLAmp title-independent duplicate suppression follow-up

Status: specified and surgically implemented; Linux operator qualification is
open.

The first launch-preflight correction still allowed two running FLAmp
instances to be launched again. The operator's process evidence showed that
both radio-owned processes already carried the correct config directory, ARQ
endpoint, and FLDigi XML-RPC endpoint. Review found that exact process matching
also included the presentation-only `-title` value. Older launch paths and
desktop wrappers may expose that title as one argv value or several values, so
a harmless title-tokenization difference made an existing process appear
absent.

The supplied `ps -ef` output exposes argv rather than Linux's native process
name. A configured `/usr/local/bin/flamp` symlink can therefore run under a
version-qualified name such as `flamp-2.2.14`, which the optimized inventory
previously excluded before reading argv. Inventory now admits only exact or
delimiter-qualified known application names for bounded argv inspection;
unrelated processes still receive no executable or command-line read, and the
full radio-specific argument match remains mandatory.

FLMsg/FLAmp duplicate detection now excludes only the trailing title segment
from process identity. It continues to require the executable and every
qualified radio selector: `--flmsg-dir` for FLMsg, and `--config-dir` plus both
FLAmp endpoint pairs for FLAmp. Regression coverage uses the operator's FTDX-10
and FT-710 paths and ports, covers both single-value and split title argv, and
confirms that changing the radio-owned ARQ port still rejects the match.

Delegation evidence: the existing `gpt-5.6-luna` launch-safety reviewer was
re-engaged for a bounded, read-only audit of the supplied Linux process lines.
It independently identified title tokenization as the remaining false-negative
boundary; the primary agent reviewed that finding, constrained the fix to
presentation metadata, and retained every radio-owned native selector.

Acceptance passes the complete launch-bundle and software-status suites with
**69 passed, 2 platform skips**. The adjacent multi-rig, launch-identity,
receiver, JS8, managed-directory, Fast Light repair, VarAC, refresh-coordination,
and status suites pass **121 tests with 2 platform skips**. Changed-file
compilation and `git diff --check` pass. Linux operator qualification remains
open.

## 2026-09-22 — launch-owned inventory and unattributed-process fail-close

Status: specified and surgically implemented; Linux operator qualification is
open.

The attached `freqinout (63).log` proved that title normalization was not the
shared production cause. At 16:16:24 the orchestrator accepted process
preflight sequence 4 after a timer inventory had completed at 16:16:08, then
launched both existing FLAmp identities at 16:16:45 and 16:16:52. Earlier runs
in the same log also launched additional VarAC and VARA processes. Endpoint
owners FLRig, FLDigi, and JS8Call were correctly protected by their second
endpoint gate; process-only rows had no equivalent barrier after exact argv
attribution returned false.

The shared failure had two parts. Dependency-status single-flight coalescing
allowed an unrelated in-flight timer/startup inventory to satisfy the launch
sequence merely because its sequence number was newer. Routine inventory also
deliberately avoided command-line reads for unrecognized native names, which
left Wine child processes and some native aliases unattributed. Exact-match
false was then treated as proof of absence and reached `Popen`.

Launch execution now accepts only the exact `launch-preflight:<trigger>`
snapshot requested by that sequence. If a routine worker is active, one
dedicated launch refresh is coalesced behind it and the orchestrator waits. The
launch-owned walk performs its complete argv attribution off the GUI thread;
routine timer/UI inventory remains low cost. Finally, visible family processes
that cannot all be attributed to configured rows fail closed for FLAmp, FLMsg,
VarAC, VARA, endpoint apps, and custom tools. A genuinely missing second radio
instance remains launchable when all existing same-family processes are
attributed to other rows.

Delegation evidence: the existing `gpt-5.6-luna` launch-safety reviewer
performed a bounded read-only audit of the new production log and current
implementation. It independently identified the unrelated-snapshot acceptance,
limited Wine/native process inventory, and fail-open exact-attribution branch.
The primary agent reviewed and integrated those findings; the reviewer made no
file changes.

Acceptance passes the complete launch-bundle and software-status suites with
**80 passed and 2 platform skips**. The broader multi-rig, launch-identity,
receiver, JS8, managed-directory, Fast Light repair, VarAC, refresh-coordination,
and status suite passes **123 tests with 2 platform skips**. Changed-file
compilation and `git diff --check` pass. Linux operator qualification remains
open.

## 2026-09-22 — canonical station-message identity and receipt-preserving dedupe

Status: specified and surgically implemented; production upgrade qualification
is open.

The supplied production database proved that the duplicated Spotter Inbox/All
rows were durable duplicate projections, not a Qt paint issue. The duplicate
rows were inserted during one historical replay after JS8 discovery changed
from an unqualified source to qualified `DIRECTED.TXT` source keys. Their RF
event times and payloads matched older rows, but the projector included source
identity and source row ID in `message_id`; the writer then moved neither the
old reference nor the old presentation. Duplicate application processes were
therefore not the direct cause of the displayed pairs, and the version-2 to
version-3 configuration migration was not itself the message duplication
mechanism.

Projection identity is now station-message identity within every message
family. JS8 API, `DIRECTED.TXT`, `inbox.db3`, FIO Spotter/import, VarAC
Incoming/mailboxes, CommStat/SitRep source references, FLMsg folders, and FLAmp
receive/Q locations retain distinct `message_sources` and
`message_external_refs`. Matching receipts share one canonical Inbox/All row,
and message detail labels each retained source rather than presenting an
ambiguous primary source. Different event times or different payloads remain
distinct. Completed FLMsg and FLAmp files use form/protocol identity plus
content digest for family-local idempotency rather than a hidden FIO parent
path or modification time.

The serialized projection writer now treats a source re-key as an atomic
relink. It preserves read/pin/archive/delete state, re-parents artifacts and
active queue/watch state, repairs the compact Ops index, and removes an old
projection only when no external references remain. Projector version 4 and
file projector version 5 trigger bounded background repair; UI queries do not
perform migration work.

Production-copy qualification projected 8,872 JS8 rows, 2,064 Spotter rows, 72
VarAC rows, 6,427 SitRep rows, and 5,683 CommStat rows without an orphan
projection. The reported KR1FLE-to-W8UFO Spotter pair became one canonical row
with two retained external receipts. Focused source/projector/writer/coordinator
coverage and adjacent store/read-model/file-arrival tests pass **95 tests**
after adding API-versus-`DIRECTED.TXT`, repeated-event, Spotter replay, VarAC
multi-mailbox, source-delete-with-peer-receipt, FLMsg/FLAmp multi-folder, and
source-relink state-preservation cases. The distinct-receipt detail rendering
case also passes independently. Changed-file compilation and `git diff
--check` pass. The existing macOS/PySide reader suite still terminates in its
native paint-event segmentation fault when run as one process after nine
passing cases; there was no Python assertion failure before that host-native
crash.

## 2026-09-22 — immutable launch evidence and maintenance action availability

Status: specified and surgically implemented; production startup qualification
is open.

The production process examples confirmed that FLAmp, VarAC, and VARA expose
enough durable identity to make startup idempotent without relying on a window
title: FLAmp supplies its config root and ARQ/XML-RPC ports, VarAC supplies its
selected INI, and VARA supplies its radio-specific executable. The remaining
duplicate-launch defect was a lifecycle race. Launch preflight correctly found
processes, but the orchestrator later queried the mutable station-wide process
cache. A timer, health, endpoint, or UI refresh could replace that cache before
the row reached its final launch gate, incorrectly turning an exact match into
absence and permitting `Popen`.

Each launch sequence now freezes the exact process records from its accepted
launch-owned preflight and uses only those records for both exact instance
matching and ambiguous-family fail-close checks. Linux native and Wine process
forms are covered directly from the supplied process examples. Equivalent
Windows native and macOS native/Wine forms use the same selectors, including
case/path normalization and application-bundle paths. An exact selector match
returns `already running`; visible ambiguous family evidence remains blocked;
only proven absence permits launch.

The disabled Preview Message Index Rebuild action had an independent UI
ownership defect. The Messages widget searched only its immediate container
parent, while the maintenance service belongs to the application host. Service
resolution now walks the Qt ownership chain, so the preview action is available
in the production nested tab layout as well as focused windows.

Acceptance passes the complete launch/status/refresh focused suite with **88
passed and 2 platform skips**, the broader multi-rig and application launch
regression suite with **231 passed and 2 platform skips**, and the focused
message-maintenance UI case. Changed-file compilation and `git diff --check`
pass. Windows and macOS selector behavior is covered by platform-representative
process records; native operator qualification remains a release check.

### Follow-up: exact FLAmp match must terminate the row

Production retesting exposed a downstream executor defect after the immutable
inventory correction. FIO conclusively matched the running radio-specific
FLAmp command, but the later family-level branch saw two configured FLAmp rows
and treated “multiple distinct instances” as permission to launch. That branch
was intended only to allow a proven-missing second radio instance; it also
overrode a proven-present current instance.

The executor now completes the current row as `already running` immediately
after an exact executable-plus-selector match. No family readiness or
multi-instance branch can subsequently reach `Popen` for that row. A focused
two-radio FLAmp regression reproduces the former control flow and makes any
spawn fail the test, while the existing missing-second-instance test confirms
that a genuinely absent, fully attributed radio instance can still launch.

## 2026-09-22 — Message Index rebuild database-contention recovery

Status: specified and surgically implemented; Linux operator qualification is
open.

The supplied `freqinout (65).log` shows the requested rebuild actively
processing 25-message bounded batches while normal source ingestion continued.
The same database had repeated writer busy retries and earlier `database is
locked` events. No corrupt source row or projection exception was recorded.
The maintenance coordinator and compact progress-checkpoint paths used 250 ms
write windows and allowed an escaping SQLite `OperationalError` to terminate
the future; its UI then displayed only the exception class, leaving no SQL
context in the log. The exact escaping statement cannot be recovered from this
log, so both bounded-cycle and checkpoint contention paths are corrected.

Transient SQLite busy/locked results are now explicit scheduling conditions.
The rebuild retries a contended coordinator cycle with bounded backoff,
checkpoint-only contention defers that metadata write without stopping source
projection, and sustained contention returns a safe resumable `deferred` state
instead of a terminal exception. The initial reset transaction reports `busy`
without partial mutation. Unexpected failures now write a contextual traceback
to the application log, while the UI explains a deferred rebuild as safely
paused with its checkpoint and queued work retained.

Acceptance covers an actual held SQLite writer lock during the rebuild request,
a transient lock escaping a coordinator cycle, and checkpoint-only contention
while projection continues. The focused maintenance suite passes **14 tests**;
the broader MIP-2/MIP-4/MIP-5 coordinator, writer, queue, store, telemetry, and
UI integration suite passes **117 tests**. Changed-file compilation and `git
diff --check` pass.

## 2026-09-22 — startup dedication and Tri-Mode guide introduction

Status: implemented and verified.

The launch splash now carries the approved dedication to the author's Dad, a
U.S. Navy Radioman and Silent Key who learned HF Digital Tri-Mode at age 86.
The support message now uses the station-benefit wording, includes “There's
more to come,” and presents `buymeacoffee.com/n1mag` without extra label text.
Its accessible description contains the complete dedication, support message,
and URL.

The guide moves the Buy Me a Coffee reference from its footer to an opening
dedication/support callout, expands SK once as Silent Key, and retains the
footer for support contact information. A new top-level **What Is HF Digital
Tri-Mode?** reference describes Fast Light, JS8Call, and VarAC/VARA as
independently useful software families, then explains FIO's coordinating role
without claiming that Tri-Mode is FIO-specific. It also distinguishes
radio-owned profiles and endpoints from station services including FIO
Spotter, FLAmp Q, CommStat, and the FIO BBS.

Focused splash, guide-anchor/content, Help rendering, image resolution, and
responsive-layout acceptance passes **23 tests**. The updated startup pixmap
was rendered offscreen and visually inspected with all approved text visible.
Changed-file compilation and `git diff --check` pass.

## 2026-09-22 — Message Index legacy-reference convergence

Status: specified and surgically implemented; production operator qualification
is open.

The post-rebuild production database proved that source replay had completed at
projector version 4 and had correctly merged the reported KR1FLE Spotter pair,
but legacy JS8 and file projection references were still attached to older
presentation rows. Those rows were not orphans, so the intentional orphan-only
cleanup left thousands of duplicate Inbox/All presentations visible. JS8 also
changed both its source-id classification and, for migrated rows, its external
key from the native row id to the durable source id.

Deep rebuild completion now includes a derived-index-only convergence pass on
the serialized projection writer. It recognizes proven JS8 and VarAC identity
migrations from authoritative native rows and identical file receipts by
family, filename, and SHA-256 digest. It moves references, artifacts,
operator-owned lifecycle state, active delete work, Spotter watches, and compact
Ops index state before removing a superseded presentation. Native rows and
files are never modified, and same-looking files with different hashes remain
separate.

Focused repair and maintenance acceptance passes **17 tests**. Qualification on
a disposable copy of the supplied 465 MB production message database planned
and repaired **10,084** legacy presentations in bounded transactions, retained
all **37,448** external receipt records, left zero unreferenced projections,
returned `PRAGMA quick_check=ok`, eliminated all exact JS8 presentation
duplicates, kept the longest repair transaction below **27 ms**, and planned
zero work on an immediate second run. Remaining
same-looking BBS, FLAmp, and VarAC file rows had different content hashes and
were correctly retained. The supplied production databases were inspected
read-only and were not modified.

The focused repair, maintenance, writer, coordinator, integration,
responsiveness, and Help-guide commands pass **71 tests**; the broader non-UI
source-projector, store, MIP-2, and MIP-5 group passes **49 tests**. The existing Qt inbox-reader
suite passed its first 11 cases and then reproducibly terminated in PySide's
native paint path (`_settle_reader_paint`) rather than producing a Python test
failure; no reader-paint code changed in this slice. Changed-file compilation
and `git diff --check` pass.

Model ownership: `gpt-6-astra` (current reasoning effort) owned database safety,
architecture, implementation, integration, and qualification. `gpt-5.6-luna`
with low reasoning performed the independent read-only duplicate-pattern and
test-boundary audit; the primary reviewed its findings and retained persistent,
transactional canonical repair rather than UI-only hiding because the governing
projection specification requires one station message with preserved receipts.

## 2026-09-23 — Message rebuild responsiveness and settled-idle CPU correction

Status: specified and surgically implemented; Linux production idle-CPU and
large-database operator qualification remain open.

The supplied rebuild capture showed the explicit Message Index rebuild holding
at 39 percent before the progress window disappeared and the host reported
`Main.py` as unresponsive. The accompanying hotspot captures also showed one
core remaining between approximately 85.8 and 96 percent while the station was
otherwise settled. The fixes preserve the existing five-second scheduler
cadence, message-source authority, serialized projection writer, radio-specific
endpoint safety, and visible cancel/resume behavior.

The explicit rebuild now coalesces worker progress notifications to at most
four per second, always publishes a terminal update, avoids unchanged progress
widget mutations, and keeps its modeless always-visible progress window. An
intermediate batch no longer causes an Inbox count/page query; one bounded
refresh runs after successful completion. Durable checkpoints occur every ten
bounded cycles and at the exact terminal state, while cancellation and
contention remain resumable.

The six hotspot corrections are implemented as one bounded concurrency slice:

1. The scheduler status worker owns one reusable lightweight
   `SettingsManager`, reloads it for saved configuration, and closes it on the
   same serial worker during shutdown instead of reconstructing settings and
   schema state every five seconds.
2. The five-second cadence and single-flight status-worker boundary are
   unchanged.
3. Local and shared PTT evidence publication is edge-triggered by the complete
   evidence signature; a changed group, owner, or reason republishes, while an
   unchanged active or clear state writes only once.
4. JS8Call, FLAmp, and VarAC background-ingest eligibility share one immutable
   linked-profile snapshot per committed settings generation. A settings save
   invalidates it immediately, and a transient read failure remains retryable
   rather than becoming a cached empty station.
5. Each dynamic FLAmp ingest job owns one worker settings view rather than one
   per radio.
6. Station command-bar radio, launch-monitor, Mesh configuration, and Mesh
   health rendering is cache-only. Lifecycle and health callbacks publish the
   immutable snapshots, Mesh freshness is evaluated per adapter, and periodic
   paint paths perform no database, schema-assurance, launch-bundle, or Mesh
   settings reads. Mesh health reads themselves are read-only and no longer run
   schema DDL.

The optimized delegation split independent bounded packages across three
`gpt-5.6-terra` agents: message rebuild, Station/Mesh presentation, and
scheduler/background-ingest. The primary `gpt-6-astra` integration review
corrected per-adapter Mesh freshness, removed the remaining launch-bundle read
from command-bar rendering, preserved changed PTT evidence, retained runtime
settings reload semantics, and added the transient profile-read retry guard.

Acceptance passes the complete scheduler family with **233 passed and 1
platform skip**, the broader background-ingest/runtime/hotspot group with **80 passed
and 1 platform skip**, Message Index responsiveness, maintenance, and MIP-5 integration with
**37 passed**, Phase 7 shell plus Mesh reconnect with **143 passed**, and the
scheduler/UI responsiveness group with **27 passed**. The scheduler run reports
two existing Qt signal-disconnect warnings but no failures. A single very large
mixed Qt run terminated in the host PySide native `NetScheduleTab` path without
a Python assertion; the same affected suites pass in the isolated partitions
above. Changed-file compilation and `git diff --check` pass before handoff.

## 2026-09-23 — Deferred Message Relay Queue and JS8/Mesh bridge authority

Status: specification complete; explicitly deferred until after FIO 2.0.

A read-only review of SuperSpotter 3.0.7 separated two useful concepts that had
previously appeared only as future-direction paragraphs: a custom JS8 held
message/pickup service and a bidirectional MeshCore-channel/JS8-group bridge.
The review also identified behaviors FIO must not copy, including optimistic
delivery state before send success, unused expiry, ambiguous list-number and
bare-ACK semantics, missing sender attribution, unguarded third-party storage,
raw JS8 socket transmission, direction-overloaded relay rows, body/time-only
dedupe, and remote `!` text acting too much like authorization.

`message_relay_queue_and_cross_transport_bridge_spec.md` is now the detailed
post-2.0 authority. It distinguishes JS8Call native `MSG`, FIO-held relay
traffic, MeshCore device `MESSAGES_WAITING`, and BBS/FLAmp publication; keeps
the canonical station message library authoritative; defines stable relay,
route, attempt, audit, lifecycle/evidence, access, expiry, retry, concurrency,
loop-prevention, UI, migration, and acceptance contracts; and makes operating
groups metadata/access-policy subjects rather than message silos. Initial JS8
scope is operator-created callsign-only held traffic. Automatic waiting notices,
remote third-party storage, group mailboxes, Mesh outbound, and cross-transport
automation remain separately gated.

The production-remediation, FIO Spotter, SuperSpotter integration,
protocol-neutral communications, and Local Mesh specifications now point to
that one authority. The current Mesh UI remains truthfully receive-only and no
schema, command parser, timer, endpoint, UI, transmission, migration, or
production data changed. Documentation links and `git diff --check` pass; no
runtime test was required for this specification-only package.

## 2026-09-23 — endpoint-scoped launch attribution and Fast Light duplicate guard

Status: specified and surgically implemented; Linux production launch
qualification remains open.

Observable reproduction: on the FT-710 Launch Control page, row **Start** for
JS8 Subspace and FLRig reached the launch orchestrator, their configured target
endpoints were not active, and both were rejected as duplicate risks because a
legacy/default same-family process for the other radio lacked FIO-attributable
selector argv. Startup showed the same JS8 suppression. This contradicted the
persisted endpoint ownership rule for FLRig, FLDigi, and JS8Call.

The launcher now records only fresh per-sequence `clear` endpoint evidence. For
FLRig, FLDigi, and JS8Call, a clear requested endpoint plus absence of the exact
configured process permits the distinct radio instance to launch. An occupied
requested endpoint and an exact process/endpoint mismatch remain terminal, and
pending, failed, stale, or timed-out endpoint evidence still fails closed. The
same path is used by startup, selected-radio start, row Start, and manual launch.

FLMsg and FLAmp remain process-only and deliberately do not receive this
endpoint exception. Exact duplicate prevention uses FLMsg's `--flmsg-dir` and
FLAmp's config root plus ARQ/XML-RPC address/port arguments; presentation titles
are excluded. Direct Windows executables and macOS app forwarding retain those
selectors. Linux family counting now also recognizes version-qualified native
names such as `flmsg-4.0.24` and `flamp-2.2.14` when argv cannot be read, so the
row fails closed rather than authorizing an unverified second process.

Primary `gpt-6-astra` (current session reasoning effort) owned the safety design,
implementation, specification reconciliation, diff review, and integration
tests. `gpt-5.6-terra` at medium reasoning performed the independent FLMsg/FLAmp
cross-platform identity audit; its version-qualified process-count finding was
incorporated. `gpt-5.6-luna` at medium reasoning reviewed the endpoint-attribution
boundary and focused test matrix; the primary retained its narrow fresh-clear
authorization and kept process-only families fail-closed.

Acceptance covers clear, occupied, pending, and exact-process endpoint states;
unattributed and version-qualified FLMsg/FLAmp processes; distinct attributed
radio instances; presentation-title normalization; Windows paths; macOS app
bundles and `open --args`; immutable launch-owned process inventories; and the
broader launch, software-status, guided-recipe, JS8, VarAC, and radio-bundle
families. The automated commands pass **222 tests** with **4 environment/platform
skips**. Changed-file compilation and `git diff --check` pass. External
qualification must confirm FT-710 FLRig and JS8 Subspace start through both
startup and row Start on the Linux station and that repeated FLMsg/FLAmp starts
do not create a second process for the same radio.

## 2026-09-23 — Repeatable README/video callsign-masked demo profile

Status: private preparation utility implemented and a current production-shaped
capture profile generated; operator visual qualification remains open.

The release-media workflow now uses
`tools/create_readme_demo_profile.py` with an explicit matched settings and
operational database pair. It copies sources read-only, shifts recognized
callsign letters by three and digits by one with wrap, retains suffixes, and
preserves the public `N1MAG` identity. Names such as Bill and Scott, grids,
schedules, timestamps, and other message text are intentionally unchanged.
The output is therefore obfuscated for presentation rather than claimed to be
fully anonymized.

The utility rejects collisions with preserved callsigns, transactionally
suspends/restores copied-database triggers during transformation, performs an
exact source-to-copy text-cell verification, and checks SQLite integrity. Its
runtime overlay disables startup/transmit behavior, clears live ingest and
saved Mesh connection paths, and assigns local RadioTools emulator endpoints.
Safe refresh uses `--replace-output`, rotates the previous capture profile, and
restores it if generation fails. The utility and its focused tests are private
engineering assets excluded by the public runtime allowlist.

Acceptance evidence:

- `./.venv/bin/python -m pytest -q tests/test_create_readme_demo_profile.py`
  passes 8 tests covering callsign rotation/wrap, digit-prefix international
  callsigns, N1MAG and suffix preservation, unchanged names/grids/text, nested
  JSON, preserved-identity collision
  rejection, trigger suppression/restoration, exact copied content, and unique
  callsign columns, plus safe existing-output rotation.
- `python3 -m py_compile tools/create_readme_demo_profile.py` and
  `git diff --check` pass.
- The September 22 matched production copies generated
  `/Users/Shared/FreqInOut-README-Demo-Current`: 3,356 callsign bases and
  282,296 rows were transformed; both SQLite integrity checks returned `ok` and
  the source-to-copy verification was exact.
- The copied profile loads at migration version 3 with two active profiles,
  zero launch-at-startup rows, an empty saved Mesh connection library, and
  emulator endpoints on the expected FLRig, FLDigi, JS8, and rig-control ports.
  All twelve endpoint probes succeeded while the three-radio emulator remained
  running. A second end-to-end build with `--replace-output` also passed and
  retained the prior capture profile at a timestamped sibling path.

The primary root agent owned data-safety design, implementation, diff review,
generation, and integration checks. Current runtime metadata did not expose an
exact model identifier/reasoning-effort label, so none is invented here. No
delegation was used for this bounded private-data preparation task. Exit gate:
implementation and automated validation pass; the maintainer must inspect the
running profile and captured frames before any media is published.

## 2026-09-23 — Release-media profile bound to GUI radio suites

Status: implemented, canonical profile refreshed, and live GUI-lab connection
qualified.

Review of `tools/start_multirig_gui_lab.sh` confirmed that its FLRig, FLDigi,
and rigctld series are `12345`, `7362`, and `4532` plus the profile index. The
GUI lab intentionally overrides JS8Call's TCP API to `2242`–`2244` and uses
profile-specific save roots below `tool-homes/js8call/fio-a`, `fio-b`, and
`fio-c`. Its operator SaveDir is distinct from the rig-named Qt application
data root that owns `DIRECTED.TXT`, `ALL.TXT`, and `inbox.db3`. The launcher
status text and lab documentation now distinguish both concepts and the lab's
port contract from JS8Call's ordinary `2442` TCP default.

`create_readme_demo_profile.py --gui-lab-root` now projects that exact contract
into both each visible `device_profiles` row and its linked Fast Light and JS8
identity. It also stores the real macOS FLRig, FLDigi, and per-version JS8Call
executables and the GUI lab's log, NBEMS check-in, SaveDir, Qt data root,
`DIRECTED.TXT`, inbox, forms, and `ALL.TXT` paths. Companion FLMsg, FLAmp,
VarAC, JS8Spotter, and CommStat remain on the safe RadioTools stubs.
Launch-at-startup and saved Mesh connections remain disabled.

The tool inspects process open-file ownership before rotation and fails closed
with a close-FIO instruction while allowing closed residual WAL files. The
canonical profile was safely regenerated at
`/Users/Shared/FreqInOut-README-Demo-Current` after its earlier FIO process had
exited. FTDX-10 maps to suite A (`4532/12345/7362/2242`) and FT-710 maps to
suite B (`4533/12346/7363/2243`). Both linked identities, distinct SaveDir/data
roots, rig-scoped storage evidence, all paths, migration version 3, N1MAG/Bill
preservation, zero startup rows, empty saved Mesh library, and both SQLite
integrity checks passed. The previous canonical profile was retained as a
timestamped sibling.

The copied single-radio compatibility keys are now aligned with suite A as the
primary runtime, including JS8 host, port, offset, SaveDir, DIRECTED.TXT, and
forms. This prevents the shared compatibility client from also connecting to
the production-default `2442` endpoint while per-radio suite B remains on
`2243`. Focused regression coverage verifies that projection.

Before live qualification, duplicate A/B Python mock FLRig/FLDigi listeners
were stopped by exact validated command line so the real GUI applications own
the test endpoints unambiguously. Rigctld, JS8Call, and the companion stubs
were retained. Direct XML-RPC/API probes passed for both suites: FLRig A/B,
FLDigi A/B, JS8Call A in compatible basic-API mode (2.5.2), and JS8Call B in
full-API mode (3.0.3). FIO was then started with the canonical config directory;
its live sockets show `12345`, `7362`, and `2242` for primary suite A plus
`12346` monitoring for suite B, with no residual connection to `2442`. Suite B
FLDigi and JS8 remain correctly saved and listening on `7363/2243` for use when
that radio context is active.

Acceptance: `bash -n tools/start_multirig_gui_lab.sh`, changed-file compilation,
`git diff --check`, and 11 focused demo-profile tests pass. The primary root
agent owned configuration safety, implementation, review, and validation; no
delegation was used. Exit gate: the canonical profile is running against the
GUI suites and is ready for maintainer visual/application qualification.

## 2026-09-23 — GUI lab FLRig CAT identity and startup ordering

Status: implemented and live-qualified against the callsign-masked README demo
profile.

The GUI launcher no longer prepares FLRig with the placeholder `NONE` model.
Each selected suite now starts a distinct, stateful pseudo-serial Kenwood
TS-2000 CAT emulator. Its FLRig profile is saved as `TS-2000`, points to a
stable suite-specific device link, retains its existing XML-RPC allocation,
and uses the TS-2000 serial defaults needed by FLRig. The launcher waits until
FLRig reports the expected model and a valid VFO before starting FLDigi and
JS8Call, avoiding the earlier race in which a dependent application could see
an unready rig endpoint.

Live A/B qualification used the canonical FIO profile at
`/Users/Shared/FreqInOut-README-Demo-Current`. FLRig on `12345` and `12346`
reports `TS-2000`, USB, and distinct valid frequencies. Both FLDigi instances
are connected to their matching FLRig port; both JS8Call profiles are running
on the intended `2242` and `2243` API ports; FIO remains connected to its
active suite while the second suite remains available for radio-context use.
No third suite was started because the current demo profile has two radios.

Acceptance: direct CAT protocol smoke testing, `bash -n`, Python compilation,
focused emulator tests, XML-RPC model/frequency/mode probes, process ownership,
and socket attribution pass. The primary root agent owned the bounded lab
implementation and validation; no delegation was used.

## 2026-09-23 — Public 2.0 README screenshot set and narrative draft

Status: screenshots staged and public README draft prepared for maintainer
review; the current testing README remains unchanged.

Six maintainer-supplied production screenshots were reviewed and copied under
stable release names in `docs/images/readme-2.0/`: Ops Center, radio profiles,
Launch Control, Plan Builder, Message Compose, and the operational map. The
frames expose no non-public callsigns, message bodies, access codes, or local
filesystem paths. The intentionally public `N1MAG` project identity remains in
the small places where it appears.

`docs/internal/README-public-2.0-draft.md` is a clean public landing-page draft
whose paths are already written for eventual root `README.md` placement. It
leads with the product outcome and Ops Center image, explains HF Digital
Tri-Mode without claiming the concept as FIO-specific, presents multi-radio
configuration, launch attribution, scheduling, messaging, offline mapping,
platform support, fresh installation, single-radio upgrade expectations,
privacy/logging, documentation, dedication, and development support. It does
not include private repository, testing-branch, engineering-tool, or internal
workflow instructions.

Exit gate: maintainer wording and screenshot-order review remains required.
The final README must not replace the testing README or enter the public export
until the broader 2.0 promotion gates authorize it.

## 2026-09-23 — Public 1.2.8 to multi-rig 2.0 semantic reconciliation

Status: the five requested audit/remediation actions are complete at the source
and focused-test level. The promotion gate remains open for native Windows,
supported-platform qualification, and unrelated pre-existing private-suite
failures.

The immutable comparison used common ancestor `e91deaa`, public 1.2.8
`2c3ba1a`, audited multi-rig head `630c45b`, and multi-rig alignment checkpoint
`b8a71c6`. Public history has 98 commits after the ancestor versus 577 on the
multi-rig side. `git cherry` identified 15 patch-equivalent commits;
high-creation-factor `range-diff` paired 64 public commits and left 34 for
explicit disposition. The private record
`docs/internal/public_1_2_8_to_2_0_reconciliation.md` maps those fixes to the
current architecture. No missing public runtime module was found.

The managed-BBS/VarAC/FLAMP parity package mapped the adapted public behaviors
to current station catalog, vault parser, helper filtering, signing, BLR/FLAMP
state machine, and management tests. Its combined acceptance run passed 156
tests with one expected skip and no failures.

Source corrections made during reconciliation:

- FIO 2.0 now consistently requires Python 3.10-3.13. Project metadata,
  requirements, uv lock, install helpers, Linux installer, user install docs,
  public README draft, and Mesh specification agree. The Python 3.9 and
  `urllib3<2` compatibility work is explicitly superseded.
- The existing PyInstaller runtime hook now removes inherited host Python/Qt
  paths before prepending bundled QML/plugins. Windows defaults to software
  Qt/Chromium rendering, UPX is disabled, windowed logging tolerates missing
  stdout, `--smoke-test` exits after a bounded startup, and fatal startup
  tracebacks are retained in `startup-error.log`.
- A new public-1.2.8-shaped rehearsal test proves preview, retained backup,
  explicit conversion, radio/app endpoint carry-forward, launch-off review,
  idempotent rerun, and rollback to the pre-migration marker/state.
- Legacy external JS8Spotter projection coverage was clarified to require the
  matching software flag, and Guided Add Radio's current FLDigi ARQ port
  refresh wiring replaced a stale single-line source assertion.

Work-package ownership:

- Architecture, Python/runtime contract, Windows frozen-runtime correction,
  migration rehearsal, diff review, documentation, and integration were owned
  by the primary `gpt-6-astra` agent. The runtime did not expose a trustworthy
  reasoning-effort label, so none is invented.
- Public commit ledger: `gpt-5.6-luna`, low reasoning, read-only.
- Windows packaging/Python declaration inventory: `gpt-5.6-luna`, medium
  reasoning, read-only.
- BBS/VarAC/FLAMP parity audit: `gpt-5.6-terra`, medium reasoning, read-only.
  The primary reviewed every report; delegates made no filesystem changes.

Acceptance evidence:

- `QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m pytest -q` across the nine
  BBS/VarAC/FLAMP files: **156 passed, 1 skipped**.
- Packaging/interpreter/install/migration focus across seven files:
  **240 passed**.
- `./.venv/bin/python tools/release_preflight.py`, changed-tree compilation,
  `bash -n` for the Linux installer/launcher/uninstaller, `uv lock --check`, and
  `git diff --check`: pass.
- The full suite in one process reaches about 3% before a reproducible PySide
  6.8/macOS native crash when one real-widget test's deferred native teardown
  overlaps a later Qt paint/layout event. The fixture evaluation below proves
  that a JS8 predecessor is not required. Running all 376 test files in
  isolated processes completed the inventory and exposed six unrelated failing
  modules plus two skipped-only modules and two Qt teardown exit-139 modules.
  The current focused reconciliation suites do not share those failures.
- Remaining assertion modules are `test_config_lab_preset.py`,
  `test_message_file_projection_pipeline_core.py`,
  `test_multi_rig_wave1_slice_b.py`, `test_performance_regression_boundaries.py`,
  `test_sdr_receiver_setup_ui.py`, and
  `test_varac_bbs_filename_normalization_1_2_3.py`. They concern stale lab path
  expectations, one changed-file tombstone expectation, older Guided Add Radio
  fixtures, an immediate-button-label race, lightweight SDR store doubles, and
  lightweight BBS reader doubles/delegate naming. They remain visible blockers
  to the final all-private-tests gate rather than being waived here.

Exit gate: focused reconciliation passes. Public promotion remains blocked
until the listed private-suite items are reconciled, a native Windows packaged
build/install/uninstall smoke passes, Python 3.10 and primary-version clean-host
installs pass, and the final operator upgrade/export review is complete.

## 2026-09-23 — Public 2.0 launcher and installer reconciliation

Status: implemented; native Windows packaging smoke remains a release gate.

The root source launcher no longer redirects ordinary upgrades into
`~/.freqinout/runtime/multi-rig` or a sibling checkout runtime. Linux and macOS
now use FIO's standard profile by default, with only an explicit
`FREQINOUT_CONFIG_DIR` selecting isolation. A matching Windows command launcher
supports both `.venv` and `venv`, forwards arguments, and follows the same
profile contract. The older `FREQINOUT_RUNTIME_ROOT` remains only as a warned
compatibility alias.

The Linux installer includes the operator launcher and public metadata in its
runtime sparse tree and no longer writes multi-rig migration state. Migration
remains owned by the in-app informed review after backup. Runtime
`requirements.txt` no longer installs the internal PyYAML tool dependency, and
the paired uninstaller now removes all icon sizes and the pixmap installed by
the installer while leaving the operator profile intact. The installer target
is now explicitly the canonical public HTTPS repository
`https://github.com/N1MAG/FreqInOut.git` on `main`; private-preview use requires
an explicit private repository and branch.

The update audit also corrected an existing-checkout defect: an explicit
`--repo` previously remained advisory while `git fetch` still used the old
origin. The installer now changes origin only for an explicit repo selection,
records the previous URL for error rollback, fetches the target into a named
remote-tracking ref, and creates a missing local branch from that ref before a
fast-forward-only pull.

The private preview instructions still name the private source explicitly.
Contract tests cover the public installer defaults, migration ownership, both
launcher layouts/profile behavior, explicit-repository update handling, and
runtime dependency separation.

## 2026-09-23 — macOS PySide fixture teardown evaluation

Status: evaluation complete; no production lifecycle change justified.

The primary `gpt-6-astra` agent owned the concurrency/lifecycle decision and
reviewed all evidence. A read-only fixture audit used `gpt-5.6-luna` with low
reasoning, and a read-only reproduction/ordering package used
`gpt-5.6-terra` with medium reasoning. Delegates made no filesystem changes.
The primary's experimental fixture edits were reverted after evaluation, so
the two target test modules have no residual diff.

The fault is reproducible without the monolithic suite. In an isolated test
profile, Compose's keystroke test followed by its layout-coalescing test exits
139 at `QTest.qWait`; each passes alone. In the Inbox reader module, the
CommStat table-layout test followed by bounded reader navigation exits 139 in
the paint-settle path; the reverse order passes. The JS8 API module can also
precede a construction-time abort, but it is not required to reproduce the
fault. The live `concurrent.futures` worker seen in traces is the shared
dependency-status executor started by the test-created launch orchestrator,
not proof of a surviving JS8 socket reader.

Fixture experiments separated production ownership from native widget
destruction. Calling `MessageViewerTab.shutdown()` before the existing
`close()`/`deleteLater()` sequence did not prevent the second-test crash.
Stubbing the launch orchestrator and using a temporary `FREQINOUT_CONFIG_DIR`
removed the shared executor and operator-profile dependency, but explicitly
draining `QEvent.DeferredDelete` then caused the first isolated test to fault
inside native PySide teardown. Retaining closed wrappers merely moved the
fault to interpreter exit. These results do not prove that a production-owned
worker outlives its owner, and changing shared Linux/Windows/macOS runtime
lifecycle would add risk without supporting evidence.

Acceptance policy for this host is therefore process isolation for the two
real-widget modules, followed by a native macOS startup/use/shutdown soak. All
**32 collected test nodes** across those modules pass and exit normally when
run in fresh isolated processes with temporary configuration roots. The
remaining six Python assertion modules remain separate release blockers. The
single-process macOS/PySide 6.8 aggregate is recorded as a harness limitation,
not waived as a passing test and not represented as an operator crash.
No production profile, database, external application, or device was modified.

## 2026-09-23 — Public runtime projection and private release-gate closure

Status: local/private readiness checks pass; external platform, operator, and
immutable-candidate gates remain open.

The release boundary is now enforced by the private
`tools/build_public_runtime_export.py` allowlist exporter. It copies only
reviewed application/runtime files, public documentation and launch/install
assets, and required licenses/data. It substitutes the public 2.0 README draft
at the exported root and excludes tests, engineering tools, internal documents,
GUI lab/emulators, the lab preset module, caches, and third-party JS8Net example
programs. The current export contains 408 validated files. Its scanner rejects
private repository/branch markers and named-user paths. A fresh temporary
profile launched and shut down cleanly from the exact exported tree using the
bounded `--smoke-test` path.

Runtime packaging now includes the net-resource, propagation-profile, and
resource-catalog data used by installed FIO. JS8Net packaging is limited to the
runtime module and license. Public installation documents target the canonical
repository and describe Windows, macOS, and Linux installation/update,
single-radio upgrade review, backups, logs, and troubleshooting without private
preview instructions. Current-operator NBEMS/CommStat discovery no longer names
developer accounts or assumes their home directories.

The earlier six stale assertion modules are closed. Lab preset paths now use the
same platform-native managed JS8 paths saved into radio profiles; the production
proposal helper no longer carries a lab-specific name; projection metadata-only
refresh and true-content-change tombstone expectations are distinct; older
Guided Add Radio/SDR/performance fixtures match current contracts; and the
managed-BBS normalization fixture creates the station catalog target now
required by runtime policy. These modules pass 42 tests together.

The isolated full inventory accounted for 378 private test modules: 374 pass,
two are intentional skip-only modules, and the Compose and Inbox reader modules
encounter the already-characterized PySide 6.8/macOS cross-test teardown crash
when multiple real-widget tests share a process. All 32 nodes across those two
modules pass and exit normally when run in fresh processes. This reinforces the
fixture-teardown finding and does not justify a cross-platform production
lifecycle change.

Work-package ownership and review:

- Primary `gpt-6-astra`: release architecture, persistence/portability safety,
  implementation integration, delegated-diff review, acceptance, documentation,
  and exit-gate decision. No reasoning-effort label was exposed by the runtime.
- `release_projection_contract`: `gpt-5.6-luna`, low reasoning; bounded
  projection-contract test review.
- `release_stale_fixtures`: `gpt-5.6-terra`, medium reasoning; bounded stale
  fixture review/correction.
- `release_public_audit`: `gpt-5.6-luna`, medium reasoning; read-only public
  boundary and documentation audit.

Acceptance evidence:

- isolated module sweep: 378 accounted for; 374 pass, 2 skip-only, 2 documented
  Qt teardown modules whose 32 individual nodes all pass;
- stale-module focus: 42 passed;
- help/packaging/export/portability/BBS focus: 55 passed;
- public 408-file projection: validation pass and clean macOS offscreen
  `--smoke-test` startup/shutdown;
- release preflight, changed Python compilation, Linux installer/launcher/
  uninstaller `bash -n`, and `git diff --check`: pass.

Exit gate: local source/projection readiness passes. Promotion remains blocked
on native Windows frozen build/install/update/uninstall, clean-host Python 3.10
and primary-version supported-platform installs, macOS native soak,
operator-assisted public-1.2.8 upgrade and companion/hardware qualification,
the immutable candidate/export fingerprint, and final public diff/inventory
approval. No public push, tag, or release was performed.

## 2026-09-24 — Maintainer acceptance of bounded 2.0 release risks

Status: accepted for a source-only release; final candidate/export review still
required.

The maintainer explicitly waived native executable qualification because no
2.0 executable or signed bundle will be offered. Python 3.11 remains the tested
and recommended release interpreter; local Python 3.10 clean-host testing is
deferred while the declared 3.10-3.13 metadata range remains accepted rather
than represented as fully platform-qualified. Stable local macOS operation is
accepted as source-runtime evidence.

The maintainer also waived a final production-shaped public-1.2.8 operator
migration as a release prerequisite. This does not remove the automated
backup/preview/explicit-apply/idempotency/rollback rehearsal. Public guidance
now makes the verified backup mandatory, preserves the recoverable `v1.2.8`
tag, and states that operators may need to review or rebuild affected companion
application configuration. These decisions are recorded as accepted residual
risk, not as passed tests.

Primary `gpt-6-astra` owns this release-scope decision and documentation update;
the runtime did not expose a trustworthy reasoning-effort label. No delegation
was used for this primary-owned release integration decision.

Exit gate: proceed to the immutable private candidate and exact public source
projection. Public push, merge, and `v2.0.0` tag remain outside this gate.

## 2026-09-24 — Frozen private and public 2.0 source candidates

Status: local candidate and projection gates pass; waiting for maintainer review
before any remote action.

The reviewed private source was frozen at
`43b869f563333a89304f69634c5f9a4c4d4114b2`. From that exact commit, the
fail-closed exporter produced 408 allowlisted files. Their durable inventory is
`docs/internal/public_2_0_runtime_manifest.sha256`, whose SHA-256 is
`c67241bd2c0367c8dfa869fd0f883221f29ba8f2d863b862d5b7833415ff1b19`.

The projection was applied mechanically to a clean worktree based directly on
public `main`/`v1.2.8` (`2c3ba1a`) and committed locally as
`2f03e5f72c8d5dc27d0666b8444849d282079809` on
`release/public-2.0-candidate`. Its Git tree is
`dda03e10aa5d728a93ade49e91f6fecafabc4e7d`, and it exactly matches the
allowlisted inventory fingerprint. No remote branch or `v2.0.0` tag exists.

Public-boundary review confirms that no tests, tools, internal documents,
packaging helpers, emulator/lab files, caches, private repository markers, or
named maintainer paths entered the candidate. Relative to public 1.2.8, the only
non-third-party deletions are contributor/developer material, generated
installer documentation, the obsolete executable-installer definition, and the
replaced single-rig launcher. The large remaining deletion set is the unused
JS8Net auxiliary/example/image bundle; the runtime module and license remain.

Acceptance:

- exact 408-file source/inventory fingerprint: pass;
- public worktree and private candidate worktree cleanliness: pass;
- source-only manifest self-containment and zero broken local Markdown links:
  pass;
- forbidden path/marker and developer-surface scans: pass;
- public branch shell syntax and `git diff --check`: pass;
- projected-tree fresh-profile offscreen startup/shutdown: pass;
- exact projected code plus private harness: 38 help, runtime-portability,
  managed-BBS, and public-1.2.8 upgrade tests pass.

Primary `gpt-6-astra` owned the release integration, candidate freeze, projection
comparison, safety review, and acceptance. No reliable reasoning-effort label
was exposed. No new delegation was used for this primary-owned release action.

Exit gate: the local public candidate is ready for the maintainer's exact-diff
and inventory approval. Pushing the branch, merging public `main`, and creating
`v2.0.0` require the maintainer's next explicit authorization.

## 2026-09-24 — FreqInOut 2.0 public source promotion

Status: complete.

After the maintainer approved the exact public comparison, the reviewed commit
was promoted with one atomic remote update. Public `main` advanced from
`2c3ba1af81d7d1ac319637a8eb77dae21254564c` (`v1.2.8`) to
`2f03e5f72c8d5dc27d0666b8444849d282079809`. The retained
`release/public-2.0-candidate` branch resolves to the same commit, and the new
annotated `v2.0.0` tag dereferences to it. The original `v1.2.8` tag remains
available as the archived single-radio source release.

Remote-ref verification passed for `main`, the candidate branch, and the tag;
the local public candidate worktree remained clean. No executable or signed
bundle was published. The first push command failed locally before contacting
GitHub because zsh parsed an undelimited SHA refspec; remote state remained
unchanged, and the corrected explicitly delimited atomic push then succeeded.

Primary `gpt-6-astra` owned final ref verification and promotion. No reliable
reasoning-effort label was exposed, and no new delegation was used.

Exit gate: public source promotion is complete. Future executable packaging,
maintenance patches, or a GitHub Release page are separate work items.

## 2026-09-24 — Custom-tool runtime identity and VarAC cluster launch follow-up

Status: specifications and VarAC cluster launch correction complete; live
Linux/Wine operator qualification remains pending.

The Custom Tool launch contract now separates the command FIO executes from
the long-lived process and endpoint that prove the tool is running. The future
slice adds optional radio-owned executable, exact argument, and TCP readiness
facts without parsing wrapper scripts or guessing identities for legacy tools.
The production-shaped `rigctld` example distinguishes FTDX-10 (`12345`/`4539`)
from FT-710 (`12346`/`4538`) while preserving the existing wrapper commands.

The VarAC native specification now records the selected-radio launch defect:
family processes were counted station-wide but attributed only against the
selected queue. The correction keeps launch scope narrow while matching
observed VarAC and VARA processes against the complete persisted station
catalog. It also makes managed cluster VarAC the sole VARA launch authority by
requiring semantic readback of
`[VARAHF_CONFIG] VarahfLaunchOnModemConnect=ON`; the hidden VARA component
remains identity/readiness evidence and is not independently spawned.

Primary `gpt-6-astra` owns both architecture specifications and the VarAC
implementation. No delegation was used. The Custom Tool runtime-identity slice
remains intentionally unimplemented for later work.

The VarAC correction now writes and validates launch-on-connect `ON` for new
managed cluster members, transactionally repairs that one key for qualified
existing members, and gives VarAC exclusive authority to start its node-local
VARA modem. The hidden VARA row remains available for exact process attribution
but is excluded from startup and manual execution plans. Selected-radio launch
continues to launch only the selected radio while duplicate preflight credits
known sibling processes from the complete station catalog; unknown processes
still fail closed. Automatic repair distinguishes exact live nodes from idle
siblings and refuses writes when family process evidence is not fully
attributable.

Automated exit gate: 197 focused VarAC writer/preparation/transaction,
Launch Control, identity, and launch-planner tests passed. No schema or runtime
data was changed, and nothing was pushed. Remaining exit gate: operator verify
the FT-710 manual start on Linux/Wine opens its distinct VarAC and VARA pair.

Production testing then exposed one remaining attribution-only regression. The
complete-station review fed two legitimate cluster members through the generic
independent-VarAC validator, which rejected their intentionally shared database
before either identity could be credited. Both launch preflight and automatic
launch-policy repair now project each radio independently and combine the
result only as a read-only process-attribution catalog. Normal station launch
planning retains its existing cross-radio collision checks. A shared-cluster-DB
fixture covers both paths; the focused gate is now 201 passing tests.

## 2026-09-24 — Terminal process exclusion and launch-child reaping

Status: implementation gate passes; live Linux/Wine requalification remains
open.

Production evidence showed `[VarAC.exe] <defunct>` with FIO as its parent. The
exited child held no port, but the immutable launch-preflight inventory counted
it as VarAC family evidence. With the valid FTDX-10 VarAC running, manual
FT-710 Start therefore failed closed before Popen with an unattributed-process
duplicate-risk result. The separate VARA files and their `8300`/`8100` and
`8310`/`8312` values remained correct; this slice does not change ports,
application configuration, or persisted launch bundles.

Process inventory now excludes positively identified zombie/dead application
and wrapper processes before publishing shared tokens or records. Cached family
counts and exact-instance matching enforce the same rule. Unknown or
permission-denied live evidence remains fail-closed. LaunchOrchestrator retains
every qualifying Popen handle and uses a one-second Qt timer to call only
nonblocking `poll()` until the child exit status is collected; no GUI-thread
`wait()` was added.

Work-package ownership:

- Primary `gpt-6-astra` owned lifecycle/concurrency design, implementation,
  integration review, specification, and exit gate. No reliable primary
  reasoning-effort label was exposed.
- `gpt-5.6-luna` at `medium` performed the required bounded read-only test and
  safety audit. It identified the missing terminal-status and nonblocking-reap
  cases and confirmed that live unmatched Wine evidence must retain the
  existing fail-closed behavior. The primary reviewed and implemented the
  resulting cases; the delegate made no file changes.

Acceptance evidence:

- `./.venv/bin/python -m pytest -q tests/test_slice0_refresh_coordination.py tests/test_software_status_endpoints.py tests/test_launch_bundles_slice4_contract.py tests/test_grs10_varac_launch_roundtrip.py tests/test_varac_native_cluster_writer.py tests/test_varac_native_preparation.py tests/test_varac_runtime_repair.py`
  — 175 passed, 2 skipped;
- `./.venv/bin/python -m pytest -q tests/test_launch*.py tests/test_software_status*.py tests/test_slice0_refresh_coordination.py tests/test_varac*.py tests/test_grs10_varac_launch_roundtrip.py`
  — 361 passed, 5 skipped;
- changed-file compilation and `git diff --check` — pass.

Exit gate: automated implementation passes. Operator qualification requires a
restart onto this code, confirmation that no historical zombie remains, and a
manual FT-710 VarAC Start while the DX10 VarAC/VARA pair is running. The result
must be two distinct VarAC/VARA pairs on their configured ports with no duplicate
processes.

## 2026-09-25 — MeshCore/Meshtastic canonical Inbox hotfix

Status: **Released in public FreqInOut 2.0.1**.

Maintainer operator testing passed on 2026-09-25. The fix is now eligible for
the next accumulated hotfix bundle, but this approval does not authorize an
individual public push or assign a point-release version.

Production reported MeshCore traffic in Ops Center but not in the Message
Inbox. The failure was not transport discovery or channel policy: local mesh
ingest correctly persisted the raw receipt and policy-shaped observation, which
Ops Center consumed. Projection-primary Inbox mode intentionally returns before
the legacy mesh-observation loader and queries only `message_projection`; no
Mesh projector populated that canonical read model.

The private hotfix adds Mesh as a bounded native projection source, uses the
policy observation's explicit `inbox` surface as the visibility gate, queues
live receipts only after policy projection finishes, and catches up retained
history through the existing durable rowid watermark. Canonical identity keeps
protocol and adapter distinct, repeated receipts coalesce, and an Ops-only
policy removes Inbox presentation without changing the native receipt or Ops
observation. No GUI merge path or synchronous Inbox database scan was added.

Work-package ownership:

- Primary `gpt-6-astra` owned diagnosis, architecture, implementation,
  integration, tests, specification, and private delivery. The primary
  reasoning-effort label was not exposed by the runtime.
- `gpt-5.6-luna` at `low` performed the bounded independent read-only audit. It
  confirmed the observation/canonical-projection boundary, recommended the
  shared writer/coordinator path, and identified surface gating, adapter-aware
  identity, retained-history backfill, state preservation, and double-rendering
  risks. The delegate changed no files; the primary reviewed and applied the
  findings.

Automated acceptance evidence:

- Mesh-focused policy, persistence, protocol, and projection suite: 123 tests
  passed;
- message projection coordinator, regression, qualification, read-model, and
  Mesh hotfix suite: 44 tests passed;
- changed-file compilation and `git diff --check`: pass.

Maintainer pass gate: restart FIO from the internal-testing branch, receive one
accepted MeshCore or Meshtastic operator message, and verify it appears once in
Inbox while remaining visible in Ops Center. Also verify an Ops-only channel
does not enter Inbox. Approval moves this item only to
`Approved—queued for next point release`; it does not authorize a public push.

## 2026-09-28 — Minimal source-install and existing-station upgrade hotfix

Status: `Released in public FreqInOut 2.0.1; native Windows qualification deferred`

Private hotfix commit: `865bd61c8be46f6eb89943bc7e22df0a636769a0` on
`wip/private-testing-multi-rig-1.2.3-not-ready`. Automated qualification is
complete. The maintainer explicitly authorized the 2.0.1 public release while
accepting native Windows installation and operator-upgrade qualification as a
documented post-release validation item because no Windows environment is
currently available.

The source-install path now uses neutral `start-freqinout` launchers, with the
old multi-rig names retained as wrappers. `install_freqinout.py` validates the
release and interpreter, preserves and replaces an incomplete `.venv`, checks
and cold-backs up an existing FIO profile, verifies copied hashes and SQLite
integrity, installs requirements, runs an isolated application/database check,
and writes the receipt required by the launcher only after all checks pass.

Existing single-radio and previously deferred profiles now reach **Upgrade
Existing Station** before post-shell runtime services start. Manufacturer,
model, display name, plan, and detected software remain part of the review.
The available actions are **Back Up and Upgrade Station** and **Exit FIO**;
closing the dialog also exits without migration writes and requires the review
again on the next launch. Fresh and already-migrated profiles bypass the gate.

Ownership:

- The primary Codex model owned installer/migration safety, UI lifecycle,
  documentation, integration, and final gate review. The runtime did not expose
  a trustworthy exact model identifier or reasoning-effort label, so neither is
  invented.
- `upgrade_test_review`: `gpt-6-luna`, low reasoning, read-only focused audit of
  launcher, installer, migration UI, test, and specification scope.

Acceptance evidence:

- installer, launcher, 1.2.8 migration, runtime-status, radio-scoped settings,
  BBS-upgrade, public-export, and startup-shell regression partition — 235
  passed;
- end-to-end macOS source installation against an isolated profile, including
  repair of a virtual environment that lacked pip, isolated runtime/database
  verification, receipt creation, neutral-launcher validation, and startup
  smoke test — pass;
- supplied tester databases were exercised from temporary copies: the primary
  settings database passed integrity checking, while the supplied nets
  database correctly failed closed before install or migration because SQLite
  reported a malformed `observation_projection` table;
- Python compilation, shell syntax, DOCX terminology checks, and
  `git diff --check` — pass;
- the 11-page current-single-radio upgrade guide was rendered and every page
  was inspected after the final edit — pass.

Accepted residual qualification: automated and macOS source-install evidence
passes. Native Windows remains unverified in the current release environment.
The first available Windows pass must run the documented PowerShell installer
and neutral `.cmd` launcher, then perform one healthy closed 1.2.8 station
upgrade and verify the two backup locations, manufacturer/model selection,
software assignments, schedules, BBS, and restart persistence. A database-
integrity failure remains a support stop, not an automatic repair or permission
to discard operator data. This deferral is visible release evidence, not a
claim that native Windows qualification occurred.

## 2026-09-28 — Visible Local Mesh outbound permission hotfix

Status: `Released in public FreqInOut 2.0.1`

The default-off **Allow Send** permission now appears directly in the selected
saved-device area under Configuration > Main > Local Mesh. Operators no longer
need to open Advanced connection details to find the primary outbound safety
control. The final 2.0.1 capability matrix exposes **Allow Send** for qualified
Meshtastic and MeshCore TCP/USB/BLE connections; unsupported legacy transports
remain visibly receive-only. MeshCore BLE qualification comes from the later
approved official-client outbound hotfix. Persistence, connection ownership,
destination policy, confirmation, and audit behavior are unchanged.

Acceptance evidence:

- Local Mesh settings layout, protocol/transport selection, saved-device
  behavior, and send-control regression partition — 14 passed;
- guarded outbound adapter/worker/audit partition — 11 passed;
- Local Mesh Compose preview, confirmation, and request emission — 1 passed;
- channel administration and settings integration partition — 12 passed;
- contextual Help regression partition — 13 passed;
- public runtime export boundary — 2 passed;
- offscreen visual inspection confirmed **Allow Send** is visible above the
  collapsed Advanced connection details panel;
- changed-file compilation and `git diff --check` — pass.

Test-harness note: the complete Compose acceptance file still encounters the
existing macOS Qt native crash in its layout-coalescing test when run as one
process. The isolated Local Mesh Compose acceptance test passes; this hotfix
does not change Compose layout or lifecycle code.

Maintainer approval: the maintainer explicitly approved the visible default-off
**Allow Send** control for the public 2.0.1 bundle. The later official-client
MeshCore BLE hotfix supersedes the original receive-only BLE expectation;
qualified MeshCore BLE now uses the same visible permission and connected-only
Compose guard. Approval does not authorize an immediate public push.
