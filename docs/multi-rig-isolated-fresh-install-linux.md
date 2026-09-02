# FIO Multi-Rig Isolated Fresh Install Guide

Purpose: install the current multi-rig WIP on Linux without touching the normal single-rig configuration. Use this before any in-place upgrade test.

## Target Paths

| Item | Path |
|---|---|
| App install folder | `$HOME/FreqInOut-multi-rig-test` |
| Dedicated config/runtime root | `$HOME/.freqinout-multi-rig-test` |
| Settings database | `$HOME/.freqinout-multi-rig-test/config/freqinout.db` |
| Nets/runtime database | `$HOME/.freqinout-multi-rig-test/config/freqinout_nets.db` |
| Optional launcher command | `$HOME/.local/bin/freqinout-multi-rig-test` |

## 1. Clone the Multi-Rig WIP Repo

```bash
git clone --branch "wip/private-testing-multi-rig-1.2.3-not-ready" \
  git@github.com:N1MAG/FreqInOut-internal-testing.git \
  "$HOME/FreqInOut-multi-rig-test"

cd "$HOME/FreqInOut-multi-rig-test"
```

## 2. Create the Dedicated Config Root

```bash
mkdir -p "$HOME/.freqinout-multi-rig-test"
```

This keeps the fresh multi-rig test data separate from the normal single-rig `~/.freqinout` location.

## 3. Run the Linux Installer

```bash
bash install_FreqInOut_linux.sh \
  --dir "$HOME/FreqInOut-multi-rig-test"
```

The installer in this WIP branch defaults to:

```text
Repository: git@github.com:N1MAG/FreqInOut-internal-testing.git
Branch: wip/private-testing-multi-rig-1.2.3-not-ready
```

## 4. Start FIO with the Isolated Config

```bash
FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test" \
"$HOME/FreqInOut-multi-rig-test/venv/bin/python" -m freqinout.main
```

On first launch, FIO should create a fresh multi-rig blank slate. It should not read or migrate the regular single-rig config.

## 5. Confirm the Isolated Database Location

```bash
ls -la "$HOME/.freqinout-multi-rig-test/config"
```

Expected after first launch:

```text
freqinout.db
```

After app use, this may also appear:

```text
freqinout_nets.db
```

## 6. Optional Dedicated Launcher

Create a separate command for repeated test launches:

```bash
mkdir -p "$HOME/.local/bin"

cat > "$HOME/.local/bin/freqinout-multi-rig-test" <<'EOF'
#!/usr/bin/env bash
export FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test"
cd "$HOME/FreqInOut-multi-rig-test"
exec "$HOME/FreqInOut-multi-rig-test/venv/bin/python" -m freqinout.main "$@"
EOF

chmod +x "$HOME/.local/bin/freqinout-multi-rig-test"
```

Run it with:

```bash
freqinout-multi-rig-test
```

## 7. Update the Repo Launcher Script

If you want `/Users/bill/RadioCode/FreqInOut-multi-rig/start-multi-rig.sh` to launch this custom install instead of the local development worktree, update the path block near the top to:

```bash
WORKTREE="${FREQINOUT_INSTALL_DIR:-$HOME/FreqInOut-multi-rig-test}"
VENV="$WORKTREE/venv"
DEFAULT_RUNTIME_ROOT="${FREQINOUT_RUNTIME_ROOT:-$HOME/.freqinout-multi-rig-test}"

RUNTIME_ROOT="$DEFAULT_RUNTIME_ROOT"
CONFIG_ROOT="$RUNTIME_ROOT/config"
```

The rest of the script can continue to create the runtime folders, export `FREQINOUT_CONFIG_DIR`, change into `WORKTREE`, and execute `freqinout.main`.

You can override either location when needed:

```bash
FREQINOUT_INSTALL_DIR="$HOME/Other-FIO" \
FREQINOUT_RUNTIME_ROOT="$HOME/.other-fio-runtime" \
/Users/bill/RadioCode/FreqInOut-multi-rig/start-multi-rig.sh
```

## 8. Fresh Install Acceptance Checks

Before moving to upgrade testing, confirm:

- `freqinout.db` exists only under `$HOME/.freqinout-multi-rig-test/config` for this run.
- The app starts without using the normal `$HOME/.freqinout/config/freqinout.db`.
- Settings opens with no existing single-rig data preloaded.
- Multi-rig setup can create the first radio intentionally from the fresh blank slate.
- Station Health does not show no-error waiting states as warnings.

## 9. Next Step After This Passes

After the isolated fresh install is clean, run an isolated update test in the same install folder. Then run the in-place upgrade test from the current single-rig release.
