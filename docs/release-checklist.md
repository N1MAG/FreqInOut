# FreqInOut Release Checklist

Use this checklist before pushing a release commit, tagging, or building installers.

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
