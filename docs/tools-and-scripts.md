# FreqInOut Tools and Scripts Guide

This guide explains every delivered maintenance/release script in plain language, with safe starter commands.

## Before you run anything

1. Open a terminal in the project root (`FreqInOut`).
2. Activate your virtual environment.
3. Start with read-only commands first.
4. Always create backups before any truncate/delete operation.

Windows PowerShell:

```powershell
cd C:\path\to\FreqInOut
.\venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
cd ~/path/to/FreqInOut
source venv/bin/activate
```

## Script index (all delivered scripts)

### Top-level helper scripts (repo root)

- `install_freqinout.py`: creates `venv` (if missing) and installs `requirements.txt`.
- `view_logs.py`: prints full current log file to terminal.
- `build_executable.py`: Windows PyInstaller build helper.
- `release_builder.py`: release preflight + optional EXE build helper.
- `install_FreqInOut_linux.sh`: Linux installer (guided, repair, dry-run, update flows).
- `uninstall_FreqInOut_linux.sh`: Linux uninstaller.

Safe starter commands:

```bash
python install_freqinout.py
python build_executable.py
python release_builder.py
bash install_FreqInOut_linux.sh --help
bash uninstall_FreqInOut_linux.sh --help
```

Notes:

- `view_logs.py` prints the entire log; use only when you intentionally want full output.
- `install_freqinout.py` does not have `-h`; running it executes installation immediately.

### DB wrappers (recommended for beginners)

- `tools/freqinout_db.py`: cross-platform DB wrapper (status/init/truncate/table/backup/vacuum)
- `tools/freqinout-db.ps1`: PowerShell wrapper around `tools/freqinout_db.py`
- `tools/freqinout-db.sh`: Bash wrapper around `tools/freqinout_db.py`

Safe starter commands:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status
```

```bash
bash tools/freqinout-db.sh status
```

### DB admin and inspection

- `tools/db_admin.py`: schema init and truncate operations for settings/nets DBs.
- `tools/db_tools.py`: settings/table inspection and JSON export utilities.
- `tools/db_schema.py`: Python schema definitions used by admin tools (library module; not a user CLI).

Safe starter commands:

```bash
python tools/db_admin.py --init all --show
python tools/db_tools.py --list-dbs
python tools/db_tools.py --table operator_checkins --table-show
```

Destructive example (only when you are sure):

```bash
python tools/db_admin.py --truncate nets_all --yes --show
```

### Propagation data and calibration

- `tools/propagation_calibrate.py`: calibrates offline blend/gate constants from local outcome history.
- `tools/build_prop_db.py`: creates a climatology SQLite DB from CSV (`month,band,lat_idx,lon_idx,muf_score`).
- `tools/generate_prop_climatology.py`: generates climatology DB via PyIRI (requires extra dependency).

Safe starter commands:

```bash
python tools/propagation_calibrate.py --fast
python tools/propagation_calibrate.py --fast --output config/propagation/prop_calibration_recommendation.json
```

Apply calibrated settings:

```bash
python tools/propagation_calibrate.py --apply
```

Build a DB from CSV:

```bash
python tools/build_prop_db.py --input .\muf_grid.csv --output .\config\propagation\prop_climatology.db
```

Generate with PyIRI:

```bash
pip install PyIRI numpy
python tools/generate_prop_climatology.py --year 2020 --output config/propagation/prop_climatology.db
```

### Data quality and ingestion checks

- `tools/js8_links_check.py`: quick summary/sample of `js8_links` ingestion.
- `tools/dedupe_operator_groups.py`: dry-run/apply cleanup for duplicate operator group assignments.

Safe starter commands:

```bash
python tools/js8_links_check.py --limit 5
python tools/dedupe_operator_groups.py --show
```

Apply dedupe changes:

```bash
python tools/dedupe_operator_groups.py --apply --show
```

### Release and changelog helpers

- `tools/release_preflight.py`: verifies version/changelog/docs/license consistency before release.
- `release_builder.py`: runs preflight and optionally `.exe` build.
- `build_executable.py`: PyInstaller build for Windows executable from `FreqInOut.spec`.
- `tools/publish-release.ps1`: GitHub release publish/upload helper (maintainer workflow).
- `tools/update_changelog_from_security.py`: reads internal security incident YAML and reports status (stub behavior).

Safe starter commands:

```bash
python tools/release_preflight.py
python release_builder.py
python release_builder.py --build-exe
```

Direct EXE build:

```bash
python -m pip install pyinstaller
python build_executable.py
```

### GUI smoke sweep

- `tools/gui_smoke_tabs.py`: offscreen GUI smoke runner that activates all `MainWindow` tabs and performs lightweight subsection control sweeps (combos, checkboxes, checkable buttons, nested stacked widgets).

Safe starter commands:

```bash
python tools/gui_smoke_tabs.py
python tools/gui_smoke_tabs.py --json-out .benchmarks/gui-smoke-latest.json --keep-config
```

Notes:

- Runs with isolated config via `FREQINOUT_CONFIG_DIR` by default (`.benchmarks/gui-smoke/<timestamp>`).
- Exits non-zero if any tab smoke step fails.

## Common workflows

### Check DB health without modifying data

```bash
python tools/freqinout_db.py status
python tools/db_tools.py --list-dbs
python tools/db_tools.py --table operator_checkins --table-show
```

### Backup + cleanup (careful workflow)

```bash
python tools/freqinout_db.py backup
python tools/freqinout_db.py vacuum
```

### Release prep (recommended order)

```bash
python tools/release_preflight.py
python -m pytest -q
python release_builder.py --build-exe
```

## Troubleshooting

- `Python not found`: activate venv or install Python 3.9+.
- `Permission denied` on PowerShell wrapper: run with `-ExecutionPolicy Bypass` as shown above.
- `PyInstaller build failed`: install PyInstaller in the active environment.
- `DB not found`: confirm config/database path and that FreqInOut has run at least once.

## Safety notes

- Any command with `truncate`, `delete`, or `--yes` is destructive.
- Prefer wrapper commands for guardrails and consistency.
- Keep backups before schema/data maintenance.
