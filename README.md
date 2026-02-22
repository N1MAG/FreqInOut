# FreqInOut

FreqInOut is a cross-workflow HF operations console for amateur radio. It unifies net control (FLDigi/JS8Call), schedule enforcement, SOP action reminders, operator database tooling, message management (JS8/FLMSG/FLAMP/VarAC), and map-based link intelligence in one desktop UI, with UTC-native data handling and practical safeguards for live operating conditions.

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
- Operator History with CSV import/export, group/role standardization, trust tools, sorting/filtering, and SitRep chip updates
- Linux guided installer with repair mode, rollback protections, desktop launcher support, and detailed logs
- Cross-platform database admin wrappers and maintenance tooling for advanced users

## What's New in v1.1.8

- Added `Local Operators` and `NCS-Local` tabs for local net workflows
- Added Net Resources catalog actions and JSON import flow for schedule resource management
- Expanded contextual action-highlighting consistency across tabs and themes
- Improved map first-open stability and reduced first-use window flash behavior
- Added local-net and local-operator data model updates including SitRep and notes workflows
- Continued performance and reliability hardening across tab activation and render paths
- Added GPG/PGP and Hash signature verification to messages center

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
- Populate Operator History before expecting the Map tab to show full results.
- JS8 live ingest is used when available; log parsing is used as a fallback.
- Set your operator Grid (Grid 6 recommended) in Settings to enable full propagation forecast output in ControlFreq.

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
- Optional release helper (run from active venv): `python release_builder.py --build-exe`

## License

GNU General Public License v3 (see `LICENSE.md`).


