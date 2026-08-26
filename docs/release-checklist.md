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

- Linux: run installer in at least one fresh scenario and one update scenario.
- Linux: verify desktop launcher/icon behavior and logs.
- Linux multi-rig WIP: install into a separate test directory before any in-place production upgrade test.
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
