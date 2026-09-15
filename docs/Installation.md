# FreqInOut 2.0 Testing Installation Guide

This guide installs the multi-rig testing preview on Windows, macOS, or Linux without reusing normal production data. The preview will eventually merge into the public single-rig repository, but **all testing-community installs currently use only the private WIP repository and branch below**.

## Required testing source

| Item | Value |
|---|---|
| Repository | `https://github.com/N1MAG/FreqInOut-internal-testing.git` |
| Branch | `wip/private-testing-multi-rig-1.2.3-not-ready` |
| Python | 3.9–3.13; 3.11 recommended |

Your GitHub account must have access to the private repository. GitHub may request browser sign-in, a credential-manager login, or an SSH key depending on your Git configuration.

## Before installing

- Close FreqInOut and companion radio applications before an update or migration test.
- Use a new application folder and a dedicated test profile root.
- Do not copy production databases into the test root for an ordinary fresh-install test.
- Do not run the in-place upgrade guide unless the test coordinator specifically assigns that scenario.

`FREQINOUT_CONFIG_DIR` identifies the profile root. FIO creates a `config` directory beneath it containing `freqinout.db` and, after operational use, `freqinout_nets.db`.

## Windows 10/11

### Prerequisites

- 64-bit Python 3.11 from python.org (enable the Python launcher during setup).
- Git for Windows.
- Optional companion applications such as FLRig, FLDigi, FLMsg, FLAmp, JS8Call, CommStat, and VarAC.

### Install and run from PowerShell

```powershell
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" `
  https://github.com/N1MAG/FreqInOut-internal-testing.git `
  "$HOME\FreqInOut-multi-rig-test"
Set-Location "$HOME\FreqInOut-multi-rig-test"
py -3.11 install_freqinout.py
$env:FREQINOUT_CONFIG_DIR = "$env:LOCALAPPDATA\FreqInOut-MultiRig-Test"
.\.venv\Scripts\python.exe -m freqinout.main
```

If PowerShell blocks activation scripts, activation is not required: invoke `.venv\Scripts\python.exe` directly as shown. The profile override lasts for that PowerShell process. Use the same assignment before every test launch.

Default production root (do not use for this fresh test): `%LOCALAPPDATA%\FreqInOut`, falling back to `%APPDATA%\FreqInOut`.

### Windows packaging note

The repository includes PyInstaller/Inno Setup maintainer tooling. Testing-community installation currently uses the source workflow above; a public 2.0 Windows installer has not been published.

## macOS

### Prerequisites

- Git (installed by Xcode Command Line Tools or Homebrew).
- Python 3.11 from python.org or Homebrew. Do not use an obsolete system Python.

### Install and run from Terminal

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  https://github.com/N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
python3.11 install_freqinout.py
FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test" \
  ./.venv/bin/python -m freqinout.main
```

The preview is not distributed as a signed/notarized macOS application. Launch it from Terminal. macOS may request file or automation permissions when configured companion applications are first accessed.

Default production root (do not use for this fresh test): `~/.freqinout`.

## Linux

### Side-by-side source install

Use this path when a production FreqInOut launcher must remain unchanged:

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  https://github.com/N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
python3.11 install_freqinout.py
FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test" \
  ./.venv/bin/python -m freqinout.main
```

Install the Qt/PySide system libraries required by your distribution if they are not already present. On Debian/Ubuntu-family systems, `libxcb-cursor0` and `libxcb-xinerama0` are commonly required for the native Map window.

### Guided isolated-profile install

The installer recognizes `apt`, `dnf`, `yum`, `pacman`, and `zypper`, validates Python 3.9–3.13, creates a `venv`, installs Python dependencies, and creates a desktop/menu launcher.

The profile data is isolated by `--config-root`, but the installer refreshes the account's standard `freqinout` launcher and menu entry. Use this path on a dedicated test account, or only when replacing that launcher target is acceptable. Use the source path above when production and testing must coexist under one account without changing the production launcher.

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  https://github.com/N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
bash install_FreqInOut_linux.sh \
  --dir "$HOME/FreqInOut-multi-rig-test" \
  --config-root "$HOME/.freqinout-multi-rig-test" \
  --branch "wip/private-testing-multi-rig-1.2.3-not-ready"
```

The generated `freqinout` launcher exports the selected profile root. Installer output and failures are written to `~/freqinout-install.log`.

Preview without changes:

```bash
bash install_FreqInOut_linux.sh \
  --dry-run \
  --dir "$HOME/FreqInOut-multi-rig-test" \
  --config-root "$HOME/.freqinout-multi-rig-test" \
  --branch "wip/private-testing-multi-rig-1.2.3-not-ready"
```

The Map's native Qt window may require `libxcb-cursor0` and `libxcb-xinerama0` on Debian/Ubuntu-family desktops. The installer offers the appropriate platform packages when they are missing.

See [FreqInOut Linux Installer Guide](FreqInOut-linux-installer.md) for repair, rollback, offline, and policy options.

## First launch and configuration

1. Verify the test profile contains `config/freqinout.db` only after the test launch.
2. Open **Settings** and create/review the first radio profile.
3. Set the Station Default radio intentionally.
4. Configure executable, host/port, and data-file paths per radio. A configured JS8Call instance needs its matching API endpoint and data paths.
5. Configure the FIO Spotter forms folder for built-in Spotter compose/decode. The external JS8Spotter executable is optional.
6. Open Station Health and resolve only actionable configuration warnings.
7. Restart and confirm profile settings persist in the isolated root.

FIO vendors its supported JS8 networking integration in the repository; do **not** install `pyjs8call` separately.

## Updating

Close FIO and companion applications first. From the test checkout:

```bash
git pull --ff-only origin wip/private-testing-multi-rig-1.2.3-not-ready
```

Then run `py -3.11 install_freqinout.py` on Windows or `python3.11 install_freqinout.py` on macOS. Linux guided-installer users should rerun the same installer command, preserving both `--dir` and `--config-root`.

## Logs and support evidence

| OS | Test profile root example |
|---|---|
| Windows | `%LOCALAPPDATA%\FreqInOut-MultiRig-Test` |
| macOS | `~/.freqinout-multi-rig-test` |
| Linux | `~/.freqinout-multi-rig-test` |

Look in the profile root for:

- `freqinout.log` — application log;
- `perf_metrics.log` — bounded performance observations;
- `fio_cpu_hotspot_*.txt` — CPU hotspot evidence when generated;
- UI hang-dump files when generated by the watchdog.

Create a support-session folder from the checkout:

```bash
python tools/multirig_capture_test_session.py --session-label "short-issue-name"
```

Add relevant files to the generated `logs` and `screenshots` folders and complete `operator_notes.md`. Do not include databases or use `--config-dir` unless support specifically requests a configuration copy. Review all evidence for callsigns, message content, access codes, paths, and secrets before sharing.

## Troubleshooting

- **Python rejected:** use Python 3.9 through 3.13; Python 3.14 is not yet supported.
- **Private repository clone fails:** confirm the GitHub account has testing-repository access and authenticate with Git Credential Manager or a configured SSH key.
- **Module/import error:** rerun `install_freqinout.py` (Windows/macOS) or the Linux installer with `--repair` and the original paths.
- **Wrong settings appear:** stop immediately and confirm `FREQINOUT_CONFIG_DIR` points at the dedicated test root before relaunching.
- **Companion app unavailable:** verify that the selected radio profile has the correct executable, host, port, profile, and data-file paths for that instance.
- **Linux launcher is missing:** rerun the installer with the same `--dir`, `--config-root`, and branch values.
- **Map is blank:** do not add a tile-server API key. Capture logs; the map is designed to use bundled geography and remain functional offline.

## Removing the test install

Deleting a checkout or profile is destructive. Confirm the exact test-only paths before removal. The Linux uninstaller can remove the application folder and shared launcher but intentionally leaves profile data in place:

```bash
bash uninstall_FreqInOut_linux.sh --dir "$HOME/FreqInOut-multi-rig-test"
```

Archive or remove the dedicated profile root separately only after its test evidence is no longer needed.
