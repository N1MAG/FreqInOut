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

## What's New in v1.2.5.4

- FLDigi / SSB Net Control now keeps an explicit check-in order number, supports PP/RR traffic sorting, keeps NCS/ANCS pinned at the top, and adds a Default Sort button so operators can get back to the working net order quickly.
- ANCS relay work is clearer: Compare can show stations your local roster has but the partner NCS list does not, and Copy Relays writes the selected relay set to role-specific relay files for paste or macro use.
- Station Health now explains both external app responsiveness and scheduler holds. Stale OK checks are called out, and FLDigi busy break-away events are shown as possible stale/hung external app busy states instead of leaving FIO trapped behind a stale receive indication.
- Scheduler protection was hardened so FLDigi busy holds force a fresh recheck after 3 minutes, status snapshot workers can be reset if they stall, and scheduler/control-task issues are visible in Station Health.

## What's New in v1.2.5.3

- VarAC Managed BBS FLAMP block requests are more forgiving during live operation: `BLKS 7,8 E957`, `BLK 7,8 E957`, `BLOCK 7,8 E957`, and `BLOCKS 7,8 E957` all request a generated block-fill file.
- FLAMP BBS helpers now separate the commands more clearly: use `LIST E957` to inspect a queue's blocks, and use `BLKS 7,8 E957` to generate a block file. If a station types `BLKS E957` without block numbers, FIO publishes a helper notice instead of guessing.
- `Resume Schedule` is now treated as an operator override from both the left ledge and ControlFreq even when companion apps appear RX-busy, while active PTT protection remains in place.

## What's New in v1.2.5.2

- VarAC Managed BBS FLAMP block fills are more reliable: when a station requests blocks such as `BLK 0,1 E957`, FIO keeps that block-fill file available through refreshes and recreates it if VarAC consumes the live file during download handling.

## What's New in v1.2.5.1

- VarAC Managed BBS now uses the VarAC traffic log as the command authority and tracks a durable log cursor so valid commands are not skipped by stale runtime timestamps.
- Managed BBS listings stay published until the remote station sends another command or disconnects, which supports long BBS refreshes and multiple file downloads from the same listing.
- Public-visible code-protected BBS folders now show in the root menu, while the access code is still enforced when a station opens the folder.
- FLAMP BBS helper views are clean standalone views instead of being mixed with the current managed folder.
- Access codes are more forgiving for operators: `HUBS MRHUB`, `hubs mrhub`, and `HUBS [MRHUB]` are handled as the same request.
- Startup and background timer handling was tightened so worker-thread completions marshal Qt timer updates back to the Qt thread, reducing `QObject::startTimer/killTimer` warnings.
- Diagnostics are easier to share: the Help tab now includes `Recent Issues`, and release preflight checks runtime dependencies against installation requirements.

## What's New in v1.2.5

- VarAC Managed BBS command handling is more reliable in field use: FIO now reads the visible VarAC traffic log first, handles `/P` portable callsigns, local-vs-remote session direction, `<BLR>` refreshes, and noisy file-transfer lines more safely.
- Managed BBS refreshes now update the current BBS or FLAMP view from traffic-log activity instead of leaving stale listings visible after a remote station refreshes.
- FIO reduces reliance on VarAC database command scanning during live BBS operation, using it as fallback when no traffic-log command events are available.

## What's New in v1.2.4

- FLDigi / SSB Net Control now lists scheduled nets that are active or coming up soon, so operators who start a little late can still select the intended scheduled net.
- The FLDigi / SSB Net Roster now uses role-aware `Directed By` and `Acked By` chips, scoped live actions (`NCS`, `ANCS`, `Shared`, `All`), `ACK Needed`, `Next TFC`, traffic progress chips, and role-first macro files such as `NCS_ACK_Pending.txt` and `ANCS_Next_TFC.txt`.
- The FLDigi macro area is collapsible and quieter during a net, with setup controls shown only when a macro is missing or needs mapping; the expanded Help guide now explains copy actions, macro files, and NCS/ANCS workflows in operator-friendly wording.
- Station Health adds a dedicated view for external dependency responsiveness so operators can see when JS8Call, FLDigi, FLRig, background ingest, or other companion services are unreachable or backing off without confusing traffic-busy states with app health.
- Messages filtering/export and FLAMP/CommStat handling were tightened for field use, including cleaner CSV export behavior and clearer incomplete-FLAMP/CommStat message handling.
- Performance isolation work reduces the chance that slow or unreachable companion applications can make FIO feel like the culprit, with guarded background work, cooldowns, and clearer status reporting.

## What's New in v1.2.3

- Messages Compose is now a fuller outbound staging workflow: it can create standard blank or custom FLMsg files, stage to FLMsg, FLAmp, VarAC Outbox, or VarAC BBS, choose ICS/Messages subfolders up to two levels deep, and reset cleanly between new drafts.
- FLAmp compose signing now fails closed instead of quietly staging unsigned fallback files, stages outbound FLAmp copies to the transmit-side folder, and verifies signed FLAmp output after signing.
- GPG signing support now handles passphrase-protected keys through the operating system credential store, validates passphrase entry in Settings, and gives clearer Windows guidance when Kleopatra or another GUI key manager is selected instead of `gpg.exe`.
- CommStat and JS8Spotter SitRep data now roll into the same ControlFreq, Messages, Operator History, and Map status model, including operator-callsign group filtering and clearer CommStat labels in map tooltips.
- VarAC BBS support expanded with Managed BBS Vault services, alias-driven virtual folders, allowed-callsign management, VarAC.ini sync tools, VGuard-style inbound file protection, and safer BBS filename handling.
- The in-app Help guide and README were refreshed so major tabs, setup sections, Compose, Message Auth, BBS/Vault workflows, and release/install expectations are explained in operator-friendly language.
- Performance and stability work reduces redundant Messages, Map, Settings, SOP, and hidden-tab refresh churn while preserving active workflow updates.
- FLDigi Net Control gained the unified editable local roster table, macro-profile discovery/mapping for `.mdf` files, and log-assisted intake refinements developed during the 1.2.3 cycle.

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

This path runs FreqInOut from a source checkout.

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

Recommended for end users. The installer keeps the installed app checkout runtime-focused and excludes `tests/` and other developer-only paths from the installed working tree, so later `git pull` updates stay lean.

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
- Checks Python version (supported: 3.9 through 3.13; 3.11 recommended; 3.14 is not yet supported)
- Offers to install missing system dependencies automatically
- Creates venv, installs requirements, and creates launcher + desktop icon
- If already installed, asks for existing app location and lets you choose:
  update app, install icon/launcher, or both
- Backs up detected user/config data before updates
- Runs a post-install self-test and writes logs to `~/freqinout-install.log`

`keyring` is included in `requirements.txt` and is installed automatically by the installer or by `pip install -r requirements.txt`. FIO uses it only for secure GPG signing passphrase storage through the OS credential store. On Linux, the desktop keyring service must also be available, such as Secret Service or KWallet.

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
- When `Managed BBS Vault` is enabled under `Settings -> VarAC Settings`, also configure a live `BBS Directory`, initialize the vault, and keep at least one valid `Default Location`. Access codes are operational controls, not strong secrets.
- In the current release, the vault root is automatic: if your live VarAC BBS is `/path/to/VarAC_files/BBS`, FreqInOut creates the managed vault next to it as `/path/to/VarAC_files/FIO_BBS_Vault`.
- New vault location content belongs under `FIO_BBS_Vault/locations/<Location Name>`. That is where you place files from your computer when you want a vault location to publish them into the live BBS.
- Remote Vault workflow is: caller refreshes the BBS root, reads the helper entry, sends the location alias like `TEST_A` or `TEST_A <code>`, then refreshes again to see that location's files. `ROOT`, `BACK`, `EXIT`, or `LOCK` returns to the main menu.
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
- User guide: `docs/guide.html`
- Changelog: `CHANGELOG.md`
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`

## License

GNU General Public License v3 (see `LICENSE.md`).
