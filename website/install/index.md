---
title: Install or Upgrade
description: A reliable installation path for new FreqInOut stations and upgrades from earlier releases.
---

# Install or upgrade FreqInOut

<div class="doc-badge-row">
  <span class="doc-badge">Packages preferred</span>
  <span class="doc-badge">Source installs supported</span>
  <span class="doc-badge">Verified backup on upgrade</span>
</div>

Use the [latest FreqInOut release](https://github.com/N1MAG/FreqInOut/releases/latest) for the clearest installation path. A release provides notes, source archives, SHA-256 checksums, and the available native packages.

## Choose the installation path

| Platform | Recommended download | Source option |
|---|---|---|
| Windows 10/11 x86-64 | `FreqInOut-<version>-windows-x86_64-setup-unsigned.exe` | Python 3.11 and `install_freqinout.py` |
| Debian/Ubuntu-family amd64 | `FreqInOut-<version>-linux-amd64.deb` | Python installer or guided Linux installer |
| macOS Apple Silicon | `FreqInOut-<version>-macos-arm64-unsigned.dmg` | Python 3.11 and `install_freqinout.py` |
| macOS Intel | `FreqInOut-<version>-macos-x86_64-unsigned.dmg` | Python 3.11 and `install_freqinout.py` |

::: warning Unsigned packages
Windows and macOS packages are currently unsigned. Windows may show **Unknown publisher** or SmartScreen; macOS may require **Open Anyway** under Privacy & Security. Verify the package against `SHA256SUMS.txt` on the release page. The installed application is still named **FreqInOut**.
:::

## Upgrade an existing station

1. Close FreqInOut, FLRig, FLDigi, JS8Call, FIO Spotter, VarAC, CommStat, and other affected companion applications.
2. Download the current package from the latest GitHub Release, or update the established source checkout.
3. Complete the operating-system installation. For a source checkout, rerun `install_freqinout.py` and require the final `Installation verified` line.
4. Start FreqInOut. FIO uses the established profile unless `FREQINOUT_CONFIG_DIR` intentionally selects another one.
5. If **Upgrade Existing Station** appears, review the manufacturer, model, display name, plan, and detected software.
6. Select **Back Up and Upgrade Station**.
7. Review the radio's software, schedule assignment, and Launch Control settings. Launch Control remains off until you enable it.

<div class="expectation">
  <strong>Expected result</strong>
  The installer reports a verified pre-install station backup and finishes with <code>Installation verified</code>. After the reviewed station upgrade, the established station appears as the first runtime radio.
</div>

::: danger Stop when verification fails
If the installer prints `Installation failed. Do not launch FreqInOut.`, do not delete, replace, or edit the named database. Preserve it and collect the installer output before trying again.
:::

## Windows

For the package installation, download and run the Windows setup executable from the latest release.

For a source installation, install 64-bit Python 3.11, open PowerShell in the established FIO checkout, and run:

```powershell
py -3.11 install_freqinout.py
.\start-freqinout.cmd
```

To update an existing public source checkout:

```powershell
Set-Location "C:\path\to\your\existing\FreqInOut"
git fetch origin
git switch main
git pull --ff-only origin main
py -3.11 install_freqinout.py
```

::: info Use the original installation folder
`C:\path\to\your\existing\FreqInOut` means the folder where that user originally cloned or installed the FIO source. `$HOME\FreqInOut` is only a common example. Do not create a second checkout merely because an example uses `$HOME`.
:::

## Linux

Install the downloaded Debian package from the directory that contains it:

```bash
sudo apt install ./FreqInOut-<version>-linux-amd64.deb
freqinout
```

For a manual source checkout:

```bash
cd "/path/to/your/existing/FreqInOut"
git fetch origin
git switch main
git pull --ff-only origin main
python3.11 install_freqinout.py
./start-freqinout.sh
```

The guided Linux installer remains available when the station also needs supported system packages, a desktop icon, and the account-wide `freqinout` command:

```bash
bash install_FreqInOut_linux.sh
```

Preserve any explicit `--dir` and `--config-root` values used for the original installation.

## macOS

Download the disk image matching the Mac processor, open it, and copy **FreqInOut** to **Applications**. While the image is unsigned, the first launch may require **Open Anyway** under Privacy & Security.

For an existing source checkout:

```bash
cd "/path/to/your/existing/FreqInOut"
git fetch origin
git switch main
git pull --ff-only origin main
python3.11 install_freqinout.py
./start-freqinout.sh
```

## Source installation rules

- Python 3.10 through 3.13 is accepted; Python 3.11 is the tested and recommended release interpreter.
- A Git pull by itself is not a complete update. Rerun the installer.
- Do not launch FIO until the installer reports `Installation verified. Run: ...`.
- Use the neutral `start-freqinout` launcher after a source installation.
- Companion radio applications remain separate installations under the operator's control.

## Roll back an unsuccessful upgrade

1. Close FIO and companion applications.
2. Retain the failed or newly installed folder for diagnosis.
3. Restore the complete `config` folder from the verified `pre-install-...` backup.
4. Launch the retained earlier FIO application folder.

Do not copy a database while FIO is running. If the backup contains SQLite `-wal` or `-shm` files, treat the backup as one unit rather than selecting individual database files.

## Next step

[Configure or review the first radio →](/guide/first-radio)
