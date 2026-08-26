# FreqInOut Linux Installer Guide

This guide is written for:
- **new Linux users** who want a safe guided install, and
- **advanced users** who want deterministic command-line control.

The installer script is:
- `install_FreqInOut_linux.sh`

---

## 1) Fast path (recommended)

Open a terminal and run:

```bash
curl -fsSL https://raw.githubusercontent.com/N1MAG/FreqInOut/main/install_FreqInOut_linux.sh -o install_FreqInOut_linux.sh
bash install_FreqInOut_linux.sh
```

If you already cloned the repo:

```bash
bash install_FreqInOut_linux.sh
```

When started with no arguments, the installer opens a guided prompt flow.

---

## 2) What the installer does

It automatically:
- checks required tools and Python support
- installs missing dependencies when possible
- clones or updates FreqInOut from GitHub
- creates/repairs a virtual environment and installs `requirements.txt`
- creates launcher + desktop entry/icon
- runs a post-install self-test
- writes detailed logs to `~/freqinout-install.log`
- cleans up deprecated files from old versions during install/update/repair

Default install location:
- `~/FreqInOut`

`requirements.txt` includes `keyring`, so the installer installs it during fresh installs, repairs, and updates. FIO uses `keyring` for secure GPG signing passphrase storage. Linux systems still need an OS credential backend such as Secret Service or KWallet; if no secure backend is available, FIO reports that passphrase storage is unavailable instead of storing plaintext.

---

## 3) Guided prompts (what to expect)

The installer may ask:
- Is this an existing installation?
- If yes: update app, desktop icon/launcher, or both?
- For package installs: continue with elevated package manager commands?
- For local git changes: stash/skip/fail behavior
- For running app detection: close app, skip update, or fail

If anything fails, recovery tips are printed and the log location is shown.

---

## 4) Common commands

Install to a custom folder:

```bash
bash install_FreqInOut_linux.sh --dir "$HOME/Apps/FreqInOut"
```

Or positional path:

```bash
bash install_FreqInOut_linux.sh "$HOME/Apps/FreqInOut"
```

Repair an existing install:

```bash
bash install_FreqInOut_linux.sh --repair --dir "$HOME/FreqInOut"
```

Preview actions without changes:

```bash
bash install_FreqInOut_linux.sh --dry-run
```

---

## 5) Advanced options

Use a specific repo:

```bash
bash install_FreqInOut_linux.sh --repo "https://github.com/ORG/FreqInOut.git"
```

Use a specific branch:

```bash
bash install_FreqInOut_linux.sh --branch "main"
```

Use channel shortcut:

```bash
bash install_FreqInOut_linux.sh --channel beta
```

Install a private multi-rig WIP build for isolated testing:

```bash
bash install_FreqInOut_linux.sh \
  --dir "$HOME/FreqInOut-multi-rig-test" \
  --repo "git@github.com:N1MAG/FreqInOut-internal-testing.git" \
  --branch "wip/private-testing-multi-rig-1.2.3-not-ready"
```

Use a separate install directory for this WIP branch until upgrade testing is complete. The branch name is intentionally treated as an opaque test-channel name even when the app version advances to 2.0.0.

Offline mode:

```bash
bash install_FreqInOut_linux.sh --offline
```

Set an explicit log file:

```bash
bash install_FreqInOut_linux.sh --log-file "$HOME/freqinout-install-custom.log"
```

Non-interactive policy-driven run:

```bash
bash install_FreqInOut_linux.sh --yes --on-dirty stash --on-running fail --on-non-git replace
```

Show help:

```bash
bash install_FreqInOut_linux.sh --help
```

---

## 6) Run and uninstall

Run FreqInOut:
- App menu: search **FreqInOut**
- Terminal: `freqinout`

Uninstall:

```bash
curl -fsSL https://raw.githubusercontent.com/N1MAG/FreqInOut/main/uninstall_FreqInOut_linux.sh -o uninstall_FreqInOut_linux.sh
bash uninstall_FreqInOut_linux.sh --dir "$HOME/FreqInOut"
```

---

## 7) Troubleshooting (quick)

If desktop icon or launcher does not appear:
- log out/in (or reboot)
- rerun icon step via guided existing-install mode

If install fails:
- open `~/freqinout-install.log`
- rerun repair:

```bash
bash install_FreqInOut_linux.sh --repair --dir "$HOME/FreqInOut"
```

If internet checks fail:
- use `--offline`
- or configure proxy environment variables and retry

---

## 8) Safety and reliability features

The installer includes:
- single-instance lock (prevents concurrent runs)
- running-app detection before in-place update
- dirty git worktree protection (prompt/policy)
- non-git folder handling (replace/skip/fail)
- rollback points for launcher/desktop/icon/venv on failure
- desktop cache refresh attempts across multiple desktop environments
- deprecated-file cleanup from prior releases

---

## 9) Suggested usage patterns

For **new users**:
- run `bash install_FreqInOut_linux.sh`
- follow guided prompts
- keep default install path unless you have a reason to change it

For **advanced users / automation**:
- prefer explicit flags (`--dir`, `--repo`, `--branch`, `--yes`, policy flags)
- use `--dry-run` before first unattended run
- capture logs with `--log-file`
