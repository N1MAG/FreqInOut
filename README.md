# FreqInOut 2.0 Multi-Rig — Testing Preview

FreqInOut is a desktop operations console for amateur radio. It brings radio-profile control, scheduling, net operations, message review, offline map awareness, operator history, SOP reminders, FIO Spotter/JS8 Expect automation, and VarAC BBS handling into one application without replacing the radio programs an operator already uses.

> **Testing-community build:** this branch is not a public release. Install it beside any production FreqInOut setup and use a dedicated profile root. Access to the private `FreqInOut-internal-testing` repository is required. Do not perform an in-place production upgrade unless you have been assigned an upgrade test.

## Testing target

| Item | Required value |
|---|---|
| Repository | `https://github.com/N1MAG/FreqInOut-internal-testing.git` |
| Branch | `wip/private-testing-multi-rig-1.2.3-not-ready` |
| Application version | `2.0.2` |
| Supported Python | 3.10–3.13 (3.11 recommended) |

The WIP branch name is an opaque testing-channel name. It is intentionally unchanged while the application version advances. These temporary repository instructions will be switched to the public repository when the multi-rig work is approved for merge.

## What is in this preview

- Named radio profiles with profile-specific companion-app paths, endpoints, runtime state, launch control, and health monitoring.
- Unified Message Inbox for JS8, Spotter, CommStat, FLMsg, FLAmp, VarAC, Mesh, and BBS traffic, with source-aware actions and shared watches.
- FIO Spotter configuration for watches, Expect responses, reusable access policies, forms, and imports.
- Compose and full Compose Workbench flows for FLMsg/FLAmp, JS8Call, FIO Spotter, and CommStat RF, including Spotter form handling and `Send as MSG` where supported.
- A separate, responsive Map window with local vector geography and operational overlays. It does not download map tiles or require an API key or Internet connection.
- UTC-native HF and net scheduling, controlled enforcement, busy deferral, NET override behavior, and multi-endpoint scheduler routing.
- FLDigi/SSB and JS8Call net-control workflows, operator history, offline propagation modeling, and schedule-aware operational context.
- Managed VarAC BBS libraries, per-radio live folders, inbound access controls, and archive handling.

## Safe quick start

Use a new source folder and profile root. The environment variable points FIO at the test profile; the databases are created below its `config` directory.

### Windows 10/11 (PowerShell)

Install Git and 64-bit Python 3.11 first, then:

```powershell
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" `
  https://github.com/N1MAG/FreqInOut-internal-testing.git `
  "$HOME\FreqInOut-multi-rig-test"
Set-Location "$HOME\FreqInOut-multi-rig-test"
$env:FREQINOUT_CONFIG_DIR = "$env:LOCALAPPDATA\FreqInOut-MultiRig-Test"
py -3.11 install_freqinout.py
.\start-freqinout.cmd
```

Keep that PowerShell window open while testing so the isolated profile setting remains in effect.

### macOS (Terminal)

Install Git and Python 3.11 first (the Python.org installer or Homebrew are both suitable), then:

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  https://github.com/N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
export FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test"
python3.11 install_freqinout.py
./start-freqinout.sh
```

macOS may ask for permission when FIO first opens files or launches companion applications. Grant only the access needed by the configured paths.

### Linux

For a side-by-side test under the same OS account, use the source launch so the production desktop/menu entry is untouched:

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  https://github.com/N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
export FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test"
python3.11 install_freqinout.py
./start-freqinout.sh
```

The guided installer supports Debian/Ubuntu/Mint, Fedora/RHEL-family, Arch-family, and openSUSE-family package managers. It installs required system packages and preserves an isolated `--config-root`, but it also refreshes the account's standard `freqinout` launcher/menu entry. Use it on a dedicated test account, or only when replacing that launcher target is acceptable. See the full installation guide for its command and dry-run option.

## First-run checks

Before connecting FIO to live radio applications:

1. Confirm the title/version and open **Settings**.
2. Confirm the test databases are under the dedicated profile root, not the normal production root.
3. Create or review the first radio profile and its station-default assignment.
4. Configure only the companion paths and endpoints needed for the test.
5. Open each major tab once, then open and close the separate Map window.
6. Restart FIO and confirm settings, radio profiles, theme, and window placement persist.

The two primary databases are:

- `<profile-root>/config/freqinout.db` for settings and configuration.
- `<profile-root>/config/freqinout_nets.db` for operational and message data.

The main logs are `<profile-root>/freqinout.log` and `<profile-root>/perf_metrics.log`.

## Updating the preview

Close FIO and its companion applications before updating.

Windows/macOS source checkout:

```bash
git pull --ff-only origin wip/private-testing-multi-rig-1.2.3-not-ready
```

Then rerun `install_freqinout.py`. On Linux, rerun the original installer command with the same `--dir` and `--config-root` values. Never change the profile root during an update unless the test plan explicitly calls for it.

## Reporting a test issue

Include:

- operating system and version;
- FIO version and git commit (`git rev-parse --short HEAD`);
- Python version (`python --version`);
- radio profile and companion-app versions involved;
- exact steps, expected behavior, and observed behavior;
- `freqinout.log`, `perf_metrics.log`, and any generated hang/hotspot file for the affected run;
- a screenshot or short recording for visual or window-management problems.

Remove callsigns, message content, access codes, filesystem details, or other sensitive data you do not want to share. Do not send the SQLite databases or a full configuration copy unless support specifically requests them.

The cross-platform capture helper creates a structured folder for notes and evidence:

```bash
python tools/multirig_capture_test_session.py --session-label "short-issue-name"
```

Copy the relevant logs into the generated `logs` folder and screenshots into its `screenshots` folder. See [Installation](docs/Installation.md) for platform-specific log locations and [Tools and Scripts](docs/tools-and-scripts.md) for advanced capture options.

## Important operating notes

- FIO's Map uses bundled vector geography and local operational data; map use must remain fully functional offline.
- Populate Operator History and operator grids before expecting complete station placement and propagation context.
- JS8 live API ingest is used when available; local JS8 files and databases provide supported fallback/history sources.
- `keyring` is installed from `requirements.txt`. Secure passphrase storage also requires an available OS credential backend; FIO does not fall back to plaintext.
- Access codes used by Managed BBS are operational controls, not strong secrets.
- Backups are recovery points, not working directories. Validate restores under a separate profile root before changing production data.

## Documentation

- [Cross-platform installation and testing guide](docs/Installation.md)
- [Linux guided installer reference](docs/FreqInOut-linux-installer.md)
- [Linux isolated fresh-install guide](docs/multi-rig-isolated-fresh-install-linux.md)
- [Linux single-rig upgrade guide](docs/single-rig-to-multi-rig-upgrade-linux.md) — assigned upgrade tests only
- [Tools and support scripts](docs/tools-and-scripts.md)
- [User guide](docs/guide.html)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)

## Packaging status

Community testing currently uses source checkouts from the private WIP branch. `FreqInOut.spec`, `build_executable.py`, and `installer.iss` support maintainer-side Windows packaging, but no public 2.0 installer should be inferred from this README. macOS application bundling and signed/notarized distribution are not yet published workflows.

The native offline Map depends on the Qt Location, Qt Positioning, and Qt Quick QML runtime modules. Packaged builds must include those modules and FIO's bundled QML/vector assets; the Map does not use a browser engine, an online tile provider, or an API-key service.

## License

GNU General Public License v3; see [LICENSE.md](LICENSE.md).
