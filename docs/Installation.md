# Installing and Updating FreqInOut 2.0.4

The [latest GitHub Release](https://github.com/N1MAG/FreqInOut/releases/latest)
is the primary download location for FreqInOut. It provides release notes,
source archives, SHA-256 checksums, and native packages for Windows x86-64,
Linux amd64, macOS Intel, and macOS Apple Silicon. Source installation remains
supported on Windows 10/11, current macOS releases, and common Linux desktop
distributions.

Windows and macOS packages are currently unsigned and include `-unsigned` in
their download filenames. Windows may display **Unknown publisher** or a
SmartScreen prompt. macOS may require **Open Anyway** under Privacy & Security.
The installed application name remains **FreqInOut**. Verify downloads against
`SHA256SUMS.txt` on the release page.

Python 3.10 through 3.13 is accepted for source installation; Python 3.11 is
the tested and recommended release interpreter. Companion radio applications
are installed separately.

## Source installation rule

New installations and upgrades use the same `install_freqinout.py` check. Do
not start FIO until it prints exactly:

```text
Installation verified. Run: ...
```

The installer validates the release files and Python version, repairs an
incomplete `.venv` without deleting it, installs the requirements, and runs an
isolated application/database check. When it finds an existing station, it
also requires FIO to be closed, checks both FIO databases, creates a cold
profile backup, verifies every copied file and database, and records the backup
location. If any step fails, it prints `Installation failed. Do not launch
FreqInOut.` and the launcher remains blocked.

On Windows, security or indexing software may prevent the verified backup
directory from receiving its preferred timestamped name. FIO retries that
rename. If Windows continues to deny only the rename, the installer retains the
already verified backup under the exact temporary name it reports and continues;
the installation receipt records that actual path. This warning is safe only
when it is followed by `Verified pre-install station backup:` and the final
`Installation verified` message. Database, copy, hash, and manifest failures
still stop the installation.

If database validation fails, do not delete, replace, or edit the named
database. The installer made no changes; preserve the database and contact
FreqInOut support before attempting the upgrade again.

## Upgrade an existing station

1. Close FreqInOut, FLRig, FLDigi, JS8Call, FIO Spotter, VarAC, CommStat, and
   other companion radio applications.
2. Download the appropriate package from the latest GitHub Release, or update
   the existing public source checkout using the manual procedure below.
3. For a package, complete the operating-system installation and start
   **FreqInOut**. For a source checkout, rerun `install_freqinout.py`, require
   the final `Installation verified` line, and use the neutral
   `start-freqinout` launcher.
4. FIO automatically uses the established profile unless
   `FREQINOUT_CONFIG_DIR` intentionally selects another one.
5. In **Upgrade Existing Station**, review or select the manufacturer, model,
   display name, plan name, and detected software. Select **Back Up and Upgrade
   Station**.
6. Review the radio's software, schedule assignment, and Launch Control
   settings. Launch Control remains off until the operator enables it.

The only other choice in the upgrade window is **Exit FIO**. Closing the window
also exits FIO. No migration writes occur, and the same upgrade is required on
the next launch.

For rollback, close FIO, retain the failed/new folder, restore the `config`
folder from the reported `pre-install-...` backup, and launch the retained
1.2.8 application folder. Do not copy a database while FIO is running.

## Windows 10/11

Download and run
`FreqInOut-2.0.4-windows-x86_64-setup-unsigned.exe` from the latest GitHub
Release. Windows may require **More info** followed by **Run anyway** while the
installer remains unsigned.

For a manual source installation, install 64-bit Python 3.11 from Python.org,
open PowerShell in the public checkout, and run:

```powershell
py -3.11 install_freqinout.py
.\start-freqinout.cmd
```

The launcher accepts normal FIO command-line options. An explicit
`FREQINOUT_CONFIG_DIR` is honored when a separate profile is intentionally
required.

Maintainers and technical testers updating an approved source checkout use:

```powershell
Set-Location "$HOME\FreqInOut"
git pull --ff-only origin main
py -3.11 install_freqinout.py
```

Use only installers attached to an official FreqInOut GitHub Release.

## macOS

Download the disk image matching the Mac processor from the latest GitHub
Release, open it, and copy **FreqInOut** to **Applications**. While the disk
images remain unsigned, macOS may require **Open Anyway** under Privacy &
Security on the first launch.

For a manual source installation, install Python 3.11 from Python.org or
Homebrew. Do not use an obsolete system Python. Open Terminal in the public
checkout and run:

```bash
python3.11 install_freqinout.py
./start-freqinout.sh
```

For an update:

```bash
cd "$HOME/FreqInOut"
git pull --ff-only origin main
python3.11 install_freqinout.py
```

macOS may ask for file, Bluetooth, or automation access when configured
companion applications or hardware are first used; grant only the access
needed by those configured paths.

## Linux

On Debian/Ubuntu-family amd64 systems, download the `.deb` from the latest
GitHub Release and install it with:

```bash
sudo apt install ./FreqInOut-2.0.4-linux-amd64.deb
freqinout
```

The manual source path remains supported on Linux:

```bash
python3.11 install_freqinout.py
./start-freqinout.sh
```

The guided Linux installer remains available for users who also need system
packages, a desktop icon, and the account-wide `freqinout` command.

The guided installer recognizes `apt`, `dnf`, `yum`, `pacman`, and `zypper`,
installs supported system dependencies, creates a virtual environment, and
prepares the launcher and desktop entry.

```bash
git clone https://github.com/N1MAG/FreqInOut.git "$HOME/FreqInOut"
cd "$HOME/FreqInOut"
bash install_FreqInOut_linux.sh
```

Rerun the same installer to update. Preserve any explicit `--dir` and
`--config-root` values used for the original installation. See the
[Linux installer reference](FreqInOut-linux-installer.md) for repair, offline,
unattended, rollback, and uninstall options.

The native Map may require `libxcb-cursor0` and `libxcb-xinerama0` on
Debian/Ubuntu-family desktops. The guided installer offers applicable platform
packages when they are missing.

## First launch for a new station

1. Enter the station callsign, location, and time preferences under
   **Configuration**.
2. Define the operating groups used by schedules, filtering, access policies,
   and publication rules.
3. Add or review the first radio and select only the software it uses.
4. Review Software Administration and Launch Control for that radio.
5. Assign a plan and schedule, then verify Ops Center and Station Health.
6. Restart FIO and confirm that the selected profile and radio settings persist.

FIO vendors its supported JS8 networking integration. Do not install
`pyjs8call` as a replacement for that integration.

## Profile and log locations

Unless `FREQINOUT_CONFIG_DIR` selects another profile, the normal roots are:

| OS | Default profile root |
|---|---|
| Windows | `%LOCALAPPDATA%\FreqInOut` (or `%APPDATA%\FreqInOut`) |
| macOS | `~/.freqinout` |
| Linux | `~/.freqinout` |

The settings and operational databases are under the profile's `config`
folder. Support evidence in the profile root includes:

- `freqinout.log` — primary application log;
- `perf_metrics.log` — bounded performance observations;
- `fio_cpu_hotspot_*.txt` — CPU hotspot evidence when generated;
- UI hang evidence generated by the watchdog, when present.

Review attachments for callsigns, message content, access codes, local paths,
and other private information. Database files are not normally needed unless
support specifically requests them.

## Troubleshooting

- **Python rejected:** use Python 3.10 through 3.13. Python 3.14 is not yet
  supported.
- **Module or import error:** rerun `install_freqinout.py`, or use the Linux
  installer's `--repair` option with the original paths.
- **Wrong configuration appears:** close FIO and check
  `FREQINOUT_CONFIG_DIR` before changing any settings.
- **A companion app is unavailable:** verify that the selected radio owns the
  correct executable, endpoint, profile, and data paths.
- **Map is blank:** capture the logs; the Map is designed to use bundled
  geography and does not require an online tile API key.

The in-app FreqInOut Guide contains configuration, recovery, and feature-level
reference material.
