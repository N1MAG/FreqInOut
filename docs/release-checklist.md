# FreqInOut Release Checklist

Use this checklist before pushing a release commit, tagging, or building installers.

For the one-time promotion that makes multi-rig 2.0 the public base release,
the binding publication, single-rig-upgrade, documentation/help, dependency,
installer, runtime-only export, qualification, and rollback gates are in
`docs/internal/public_2_0_runtime_release_plan.md`. Do not publish the private
WIP history or begin that promotion until the maintainer explicitly declares
the release candidate ready.

## 1) Versioning

- Update `freqinout/version.py` (`__version__`).
- Ensure matching version in:
  - `pyproject.toml` (`project.version`)
  - `installer.iss` (`AppVersion`)
  - `docs/guide.html` (`Current version`)
  - `CHANGELOG.md` (top release header)

## 2) Documentation

- `README.md` install paths and commands are current.
- `CONTRIBUTING.md` setup and release notes are current.
- `docs/Installation.md` is accurate for source installs.
- `docs/FreqInOut-linux-installer.md` and `.html` match installer behavior.
- `docs/tools-and-scripts.md` reflects current script behavior and examples.
- `SECURITY.md` contains valid private reporting contact info.

## 3) Installer / Packaging

- Linux: run installer in at least one fresh scenario and one update scenario against `git@github.com:N1MAG/FreqInOut-internal-testing.git` branch `wip/private-testing-multi-rig-1.2.3-not-ready`.
- Linux: verify desktop launcher/icon behavior and logs.
- Linux multi-rig WIP: install into a separate test directory before any in-place production upgrade test, then run one explicit in-place upgrade from the current single-rig install.
- Windows: run `python build_executable.py`.
- Windows: update `installer.iss` and compile with Inno Setup.

## 4) Preflight

Run:

```bash
python tools/release_preflight.py
python -m compileall freqinout
```

Fix any reported ERROR items before release.

## 5) Performance smoke

- Clear the dedicated perf log:

```bash
python tools/perf_benchmark.py reset-log
```

- Do one cold and one warm workflow pass that includes:
  - `FreqPlanner`
  - `HF Daily`
  - `HF Nets`
  - `SOP Builder`
  - one `SOP Builder -> Save` action that triggers SOP data fanout
  - one `HF Daily -> Resolve Conflicts` review
  - one `HF Nets -> Manage Net/SOP Policies` review

- Summarize the result:

```bash
python tools/perf_benchmark.py summarize --name "^(main_window|messages|map|operators|controlfreq|freqplanner|daily_schedule|net_schedule|sop\\.|settings|digi_ncs|js8_ncs)" --sort p95 --limit 80
```

## 6) Repo hygiene

- Confirm no local-only artifacts are staged (`dist/`, `build/`, DB files, logs).
- Confirm no placeholder/stub docs remain unintentionally.
- Confirm no mojibake text appears in docs/changelog.

## 7) Final release flow

1. Run preflight.
2. Run compile verification and SOP-focused perf smoke.
3. Build and smoke-test app.
4. Confirm the SOP workflow docs in `docs/guide.html` match the shipped behavior.
5. Commit release changes.
6. Tag release.
7. Publish release notes from `CHANGELOG.md`.

## 8) Multi-rig Test Readiness

- Confirm the active runtime profile before inspecting data. For Bill's local multi-rig lab, use `/Users/bill/RadioCode/runtime/multi-rig/config/freqinout.db` and `/Users/bill/RadioCode/runtime/multi-rig/config/freqinout_nets.db`.
- Verify FIO-A/FIO-B selected-radio settings panes, launch control, health monitoring, JS8 profile folders, Fast Light paths, VarAC paths, and CommStat connector paths all follow the selected radio.
- Run the map operator workflow with real JS8 `inbox.db`, `ALL.TXT`, `DIRECTED.TXT`, and CommStat traffic so path, regional intelligence, and message handoff behavior are exercised together.
- Capture one fresh-install bundle and one production-upgrade bundle with `tools/multirig_capture_test_session.py`.

## 9) Tools & Resources / Local Nets Qualification

- Rehearse migration against an isolated copy of a production-sized
  `freqinout_nets.db`; never point the rehearsal writer at the production source.
- Record the source and backup hashes, `PRAGMA integrity_check`, migrated row
  counts, review-required count, and a second-run zero-write/idempotence result.
- Export and preview-import a selection containing a frequency, directory net,
  published session, and Operating Group links. Confirm the target preserves
  stable keys, source/version data, relationships, and group associations.
- Exercise both HF subscription directions: Net Directory to a named HF Nets
  schedule and HF Nets to Net Directory. Confirm that review drafts do not tune,
  launch software, or replace an accepted schedule silently.
- Exercise Local Nets from a directory session and from custom creation. Verify
  local/UTC review, recurrence, Operating Group association, pause, one-occurrence
  dismissal, resource-update status, and stable return from Resources.
- From Ops Center, verify Local Net Details, Dismiss, and Open SOP. Confirm the
  reminder remains separate from HF rows and message traffic and that no action
  QSYs a radio or activates an SOP automatically.
- Run the fresh-process repository test sweep. Treat skip-only test modules as
  skips, not failures, and investigate any module that fails in its own process.
- Run `tools/gui_smoke_tabs.py` with an isolated initialized configuration and
  confirm Resources and Local Nets open without schema warnings.
- Run a 30-minute `tools/gui_slice0_soak.py` session that includes Resources and
  Local Nets. Record startup, event-loop lag, interactions, resizes, navigation
  switches, shutdown time, and Qt thread/timer errors.
- On Linux production, validate 1920x1080 Normal Text, then compact and Large
  Text. Check Light and Dark themes, keyboard focus, accessible names, elastic
  name/occurrence columns, wrapped chips, and absence of page-level horizontal
  scrolling or clipped primary actions.
- Run the 1,000-schedule Local Net outlook benchmark and require warm p95 below
  50 ms with a bounded dashboard result.

## 10) Guided Radio / Software Qualification

Release hold: the 2026-09-17 existing-station TriMode Add Radio run failed the
distinct-instance qualification. Do not mark this section complete until GRS-6
passes its automated gate and the exact operator route is repeated successfully.

GRS-6.1 authority/inventory gate passed 2026-09-17: Add Radio and Software
Administration share one saved-plus-retained collision inventory; draft keys
are stable; imports are source-locked; explicit clone creates a distinct draft;
and stale imported sources fail before persistence. GRS-6.2 recipe gate passed
2026-09-17: Add Radio and Software Administration use the configuration-owned
managed root; exact stock/Improved/Subspace JS8 identities use distinct
profile, platform data, TCP, and UDP claims; Fast Light resolves component
roots, commands, dependencies, and receive-only scope; and unsupported recipes
fail closed. GRS-6.3 through GRS-6.5 and all live checks remain open.

- Add and edit one transceiver and one receive-only SDR through all seven
  guided steps. Confirm Back, Next, Cancel, role changes, Review, and final Save
  remain responsive while software discovery or endpoint work is running.
- For the observer, confirm only receive-only Operating Models and receive-only
  Frequency Plans are assignable. Verify manual/unverified control produces
  reminders only and matching SDR++ tune/readback/restore evidence permits
  automatic receive retuning.
- Give the SDR and an active peer the same Receiver Guard antenna or front-end
  group. Confirm an automatic SDR retune is held with the exact peer/resource
  reason and recovery action, while an unrelated radio continues normally.
- Confirm Station Overview distinguishes manual tuning, applying, verified,
  shared-resource hold, receiver unavailable, and endpoint backoff without
  probing an endpoint from the UI thread.
- Exercise exact launch or operator-start review for SDR++, stock JS8Call,
  JS8Call Improved, Subspace, FLRig, FLDigi, FLMsg, FLAmp, and VarAC as
  applicable. Verify an intentional manual-start choice is not reported as an
  error or pending verification.
- On a station that already has working JS8Call, Fast Light, CommStat, and VarAC
  configuration, add another TriMode transceiver and choose `Create a distinct
  instance`. Confirm JS8 receives a new stable rig/profile/data identity and
  collision-free TCP/UDP ports; no existing profile, settings file, data root,
  endpoint, native file, manifest, or launch bundle is selected or modified.
- Confirm Fast Light uses the radio name as its visible family identity and
  resolves separate FLRig/FLDigi profile roots, endpoints, commands, dependency,
  and readiness without asking for a normal-flow custom command.
- Confirm FIO Spotter resolves its built-in MCF catalog without a per-radio
  folder question; CommStat remains one station-shared process with an explicit
  binding to the new radio's JS8 endpoint; and additional VarAC defaults to
  standalone unless the operator explicitly selects cluster create/join.
- Confirm Step 1 uses `Transceiver` or `Receive-only SDR`, Step 2 explains FIO
  behavior with no schedule-like model name or redundant receive-only suffix,
  and schedule selection remains in Step 6.
- Cancel once from Software Administration and once from Review. Verify the
  existing working application records, native profiles, files, launch bundles,
  and radio links are byte-for-byte/row-for-row unchanged.
- Record operator-assisted evidence separately for macOS, Linux, Windows, a
  physical transceiver/backend, RTL-SDR with SDR++, live Fast Light, all three
  JS8 variants, and a multi-node VarAC Cluster. Automated tests do not close
  these live gates.
