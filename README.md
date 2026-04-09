# FreqInOut

FreqInOut is a cross-workflow HF operations console for amateur radio. It unifies net control (FLDigi/JS8Call), schedule enforcement, SOP action reminders, operator database tooling, message management (JS8/FLMSG/FLAMP/VarAC), and map-based link intelligence in one desktop UI, with UTC-native data handling and practical safeguards for live operating conditions.

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/ControlFreq_tab.png">
    <img
      src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/ControlFreq_tab.png"
      alt="ControlFreq tab showing frequency control, operating status, message summary, and schedule outlook"
      width="980">
  </a>
</p>

## Highlights

- ControlFreq operations dashboard with Frequency Control, Schedule Outlook, Message Summary, propagation forecast, and View presets/chips for card layout control
- Offline propagation modeling (Modeled/Actual/Blended) with lower-48 targeting (Region/State/Operator) and historical-outcome blending
- SitRep status pipeline from CommStatOne and JS8Spotter forms (`F!104`, `F!301`, `F!304`) with latest-signal-wins logic across ControlFreq, Operators, and Map
- Map intelligence with link filters, propagation overlay, SitRep-only mode, and schedule-risk/QSY awareness
- UTC-native HF + Net scheduling with controlled enforcement, busy deferral, and NET override behavior
- SOP Builder v2 with `HF SOP` and `Local Comms SOP` categories, conflict-aware save-time resolution, and Daily interval support
- Net control operations for both FLDigi and JS8Call (start/track/save/end workflows)
- Settings redesign: `Fast Light Settings`, `VarAC Settings`, `Launch Control`, and `Logging & Diagnostics`
- Local operations expansion: `Local Operators` roster tab and `NCS-Local` net control tab
- Net resources catalog workflow via SitRepNet.com or custom JSON: import JSON into managed Net Resources and promote selected entries into active Net Schedule
- Launch orchestration with configurable start order, per-app startup toggles, global startup mode, and continue-on-failure handling
- Messages center for JS8/FLMSG/FLAMP/VarAC plus VarAC BBS workflows (view/archive/delete, aging visibility, auto-archive support), including GPG/PGP and Hash signature verification
- Operator History with CSV import/export, group/role standardization, trust tools, sorting/filtering, SitRep chip updates, and VarAC callsign-tag sync
- Linux guided installer with repair mode, rollback protections, desktop launcher support, and detailed logs
- Cross-platform database admin wrappers and maintenance tooling for advanced users

## What's New in v1.2.2

- Added CommStat sitrep fusion under the unified `SitRep` message family, including source/receipt metadata, brevity decode support, shared latest-status fusion with JS8Spotter, and map/operator history integration.
- Added `HF Operators -> Manage Operators -> Sync to VarAC` to maintain `VarAC_callsign_tags.conf` from known operator rows, using the unquoted `CALLSIGN,NAME / STATE / GROUP1 / GROUP2 / GROUP3 / ROLE` format.
- Improved Settings setup guidance with warning-highlighted section chips for incomplete single-radio configuration, including JS8Call companion fields, Fast Light endpoint/file-path gaps, and missing VarAC incoming-files path.
- Fixed ControlFreq so `Next Change` shows the upcoming target frequency, the hero frequency re-syncs more reliably to the actual radio state, and changing the hold preset updates an already active hold immediately.
- Fixed ControlFreq `Activity` so the selected time window reflects actual recent traffic rather than schedule-start proximity.
- Fixed Map operator activity semantics so `Last Seen` reflects latest observed activity, `Last Contact` reflects direct inbound contact only, and sitrep-focused views can show receipt/source/state summary data with a bottom-docked legend.
- Fixed `Messages` transport-form handling so `.k2s` fallback payloads decode through the same friendly path as `.b2s` where possible.
- Fixed `HF Operator History` CSV import so Excel `CSV UTF-8` files with BOM import cleanly.
- Added the Reliability Baseline updates: background ingest now runs off the GUI thread, `operator_checkins` schema repair/init is centralized in core DB code, and VarAC local mirror tables are created during cold-start initialization
- Fixed software-status validation so `JS8Call`, `FLRig`, and `FLDigi` badges are endpoint-aware, with persisted `FLDigi` XML-RPC host/port settings and Settings probes that honor the currently entered endpoint values
- Fixed runtime profile consistency so logging, updater downloads, and DB admin tooling operate on the same active profile root as the running app
- Hardened the updater by validating downloaded ZIP entry paths before extraction and rejecting unsafe archive contents
- Simplified startup single-instance locking and changed WebEngine startup prewarm to default on for Windows and off for macOS/Linux unless explicitly overridden in settings

## Quick Start

### Windows

```bash
git clone https://github.com/N1MAG/FreqInOut.git FreqInOut
cd FreqInOut
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m freqinout.main
```

### Linux (Debian/Ubuntu)

```bash
sudo apt-get install python3 python3-venv python3-pip libxcb-cursor0 libxcb-xinerama0
git clone https://github.com/N1MAG/FreqInOut.git FreqInOut
cd FreqInOut
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m freqinout.main
```

### Linux one-step installer

Run locally from a clone:

```bash
bash install_FreqInOut_linux.sh
```

When started without command-line arguments, the installer now shows an interactive mode menu (guided, dry-run, repair, etc.).

Or run directly from GitHub (review script first, then run):

```bash
curl -fsSL https://raw.githubusercontent.com/N1MAG/FreqInOut/main/install_FreqInOut_linux.sh -o install_FreqInOut_linux.sh && bash install_FreqInOut_linux.sh
```

This installer:
- Checks Python version (needs 3.9+)
- Offers to install missing system dependencies automatically
- Creates venv, installs requirements, and creates launcher + desktop icon
- If already installed, asks for existing app location and lets you choose:
  update app, install icon/launcher, or both
- Backs up detected user/config data before updates
- Runs a post-install self-test and writes logs to `~/freqinout-install.log`

Default install path is `~/FreqInOut`.

Easiest install-location override:

```bash
bash install_FreqInOut_linux.sh --dir "$HOME/Apps/FreqInOut"
```

You can also pass the folder directly:

```bash
bash install_FreqInOut_linux.sh "$HOME/Apps/FreqInOut"
```

Useful modes:

```bash
# repair existing install (rebuild venv/launcher/icon)
bash install_FreqInOut_linux.sh --repair --dir "$HOME/FreqInOut"

# preview actions without changing anything
bash install_FreqInOut_linux.sh --dry-run

# use beta channel (branch "beta")
bash install_FreqInOut_linux.sh --channel beta

# offline mode (skip internet checks/downloads)
bash install_FreqInOut_linux.sh --offline

# explicit non-interactive safety policies
bash install_FreqInOut_linux.sh --yes --on-dirty stash --on-running fail --on-non-git replace
```

Uninstall helper:

```bash
bash uninstall_FreqInOut_linux.sh --dir "$HOME/FreqInOut"
```

For QtWebEngine map support on Debian/Ubuntu:

```bash
sudo apt-get install libxcb-cursor0 libxcb-xinerama0
```

## Configuration Notes

- Set radio software paths and JS8Call DIRECTED.TXT in the Settings tab.
- Watch the Settings left-nav for warning highlights; they indicate partially configured sections that still need required companion fields.
- When `JS8Call Install Folder` is set, also configure host, TCP port, and `DIRECTED.TXT`. If `JS8Spotter Launch Path` is set, also configure `JS8Spotter forms`.
- When `VarAC Install Folder` is set, also configure `Incoming Files`.
- Populate Operator History before expecting the Map tab to show full results.
- JS8 live ingest is used when available; log parsing is used as a fallback.
- Set your operator Grid (Grid 6 recommended) in Settings to enable full propagation forecast output in ControlFreq.

## Screenshots

<details>
<summary>Additional tab screenshots (click to expand)</summary>

### FreqPlanner Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/FreqPlanner_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/FreqPlanner_tab.png" alt="FreqPlanner tab screenshot" width="900">
  </a>
</p>

### Messages Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Messages_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Messages_tab.png" alt="Messages tab screenshot" width="900">
  </a>
</p>

### Map Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Map_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Map_tab.png" alt="Map tab screenshot" width="900">
  </a>
</p>

### NCS Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/NCS_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/NCS_tab.png" alt="NCS tab screenshot" width="900">
  </a>
</p>

### HF Schedule Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/HF_Schedule_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/HF_Schedule_tab.png" alt="HF Schedule tab screenshot" width="900">
  </a>
</p>

### NetSchedule Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/NetSchedule_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/NetSchedule_tab.png" alt="Net Schedule tab screenshot" width="900">
  </a>
</p>

### SOP Builder Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/SOPBuilder_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/SOPBuilder_tab.png" alt="SOP Builder tab screenshot" width="900">
  </a>
</p>

### Settings Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Settings_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Settings_tab.png" alt="Settings tab screenshot" width="900">
  </a>
</p>

### Help Tab

<p align="center">
  <a href="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Help_tab.png">
    <img src="https://github.com/N1MAG/FreqInOut/releases/download/release-assets/Help_tab.png" alt="Help tab screenshot" width="900">
  </a>
</p>

</details>

## Documentation

- Installation: `docs/Installation.md`
- Linux installer guide: `docs/FreqInOut-linux-installer.md`
- Linux installer guide (HTML): `docs/FreqInOut-linux-installer.html`
- Database tools: `docs/db-tools.md`
- Tools and scripts guide: `docs/tools-and-scripts.md`
- User guide: `docs/guide.html`
- Changelog: `CHANGELOG.md`
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Release checklist: `docs/release-checklist.md`

## Maintainer Tools

- Preflight release checks: `python tools/release_preflight.py`
- Linux live-station benchmark capture: `bash tools/linux_fio_bench_capture.sh --duration 300`
- Linux benchmark summary (rerun on a capture folder): `python tools/linux_fio_bench_summary.py <capture_dir>`
- Optional release helper (run from active venv): `python release_builder.py --build-exe`

## License

GNU General Public License v3 (see `LICENSE.md`).
