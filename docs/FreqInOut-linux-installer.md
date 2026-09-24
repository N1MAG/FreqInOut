# FreqInOut 2.0 Linux Installer Reference

`install_FreqInOut_linux.sh` provides guided and command-line installation,
update, and repair for common Linux desktop distributions.

## Normal installation

```bash
git clone https://github.com/N1MAG/FreqInOut.git "$HOME/FreqInOut"
cd "$HOME/FreqInOut"
bash install_FreqInOut_linux.sh
```

The installer:

- validates Python 3.10 through 3.13;
- recognizes `apt`, `dnf`, `yum`, `pacman`, and `zypper`;
- installs applicable system dependencies when approved;
- clones or updates the public FreqInOut repository;
- creates or repairs the virtual environment;
- installs the runtime requirements;
- creates the launcher, desktop entry, and icons;
- performs a post-install self-test; and
- writes detailed output to `~/freqinout-install.log` by default.

The installer prepares the application but does not silently finalize the
single-radio-to-2.0 configuration migration. FIO owns the informed backup,
review, and confirmation flow on first launch.

## Common commands

Install in the default location, `~/FreqInOut`:

```bash
bash install_FreqInOut_linux.sh
```

Install in another application folder:

```bash
bash install_FreqInOut_linux.sh --dir "$HOME/Apps/FreqInOut"
```

Bind the launcher to an intentionally separate profile:

```bash
bash install_FreqInOut_linux.sh \
  --dir "$HOME/Apps/FreqInOut" \
  --config-root "$HOME/.freqinout-field"
```

Preview without changing the system:

```bash
bash install_FreqInOut_linux.sh --dry-run
```

Repair the environment, launcher, and icons without recloning:

```bash
bash install_FreqInOut_linux.sh --repair --dir "$HOME/FreqInOut"
```

Use local files without network checks or downloads:

```bash
bash install_FreqInOut_linux.sh --offline --dir "$HOME/FreqInOut"
```

## Update behavior

Close FIO and companion radio applications before updating, then rerun the
installer with the same `--dir` and `--config-root` values used originally.
The public repository and `main` are the defaults.

For unattended policy control:

```bash
bash install_FreqInOut_linux.sh \
  --yes \
  --on-dirty fail \
  --on-running fail \
  --on-non-git fail
```

Available policies are:

- `--on-dirty prompt|stash|skip|fail`
- `--on-running prompt|skip|fail`
- `--on-non-git prompt|replace|skip|fail`

An explicit repository or branch can be selected with `--repo` and `--branch`.
Ordinary operators should retain the public defaults unless support supplies a
specific recovery instruction.

## Profile ownership and backups

Without `--config-root`, FIO uses its standard Linux profile under
`~/.freqinout`. When `--config-root` is supplied, the generated launcher
preserves that selection on later starts. Never change the profile root during
an update merely to work around an error.

The installer checks for a running FIO process, protects dirty Git worktrees,
and creates rollback state before replacing managed launcher, environment, or
icon files. Installer backups and FIO migration backups are recovery points,
not working directories.

Before restoring:

1. close FIO and companion applications;
2. retain the failed installation and logs;
3. copy the current profile to a separate holding location;
4. inspect the backup manifest or archive; and
5. validate restored data under a separate `FREQINOUT_CONFIG_DIR` before
   replacing a live profile.

The `--repair` option repairs the application environment. It is not permission
to overwrite production configuration.

## Running FIO

After installation:

- use the desktop application menu and search for **FreqInOut**; or
- run `freqinout` in a terminal.

If a desktop entry or icon does not appear, log out and back in, then rerun the
installer with the same paths if necessary.

## Logs

The installer log defaults to `~/freqinout-install.log`. Select another path
with `--log-file`.

Application logs are in the active FIO profile root:

- `freqinout.log`
- `perf_metrics.log`
- generated `fio_cpu_hotspot_*.txt` files
- generated UI-hang evidence, when present

## Uninstall

Preview uninstall actions:

```bash
bash uninstall_FreqInOut_linux.sh --dry-run --dir "$HOME/FreqInOut"
```

Run the uninstaller:

```bash
bash uninstall_FreqInOut_linux.sh --dir "$HOME/FreqInOut"
```

The uninstaller can remove the application folder, launcher, desktop entry,
and installed icons. It intentionally leaves the operator profile and radio
data in place. Archive or remove profile data separately only after confirming
that it is no longer needed.

## Troubleshooting

- Use `--repair` after dependency or virtual-environment damage.
- Use `--dry-run` before the first unattended or custom-path invocation.
- Use `--offline` only when the required repository and dependencies are
  already present locally.
- If FIO is reported as running, close it rather than forcing an in-place
  update.
- On Debian/Ubuntu-family desktops, native Qt Map support may require
  `libxcb-cursor0` and `libxcb-xinerama0`.
