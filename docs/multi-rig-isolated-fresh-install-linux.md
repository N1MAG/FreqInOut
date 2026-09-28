# FIO Multi-Rig Isolated Fresh Install Guide

Use this Linux procedure for ordinary testing-community installs. It keeps the 2.0 WIP checkout and profile separate from a normal single-rig installation. Do not use the production `~/.freqinout` root.

## Target paths

| Item | Test path |
|---|---|
| Application | `$HOME/FreqInOut-multi-rig-test` |
| Profile root | `$HOME/.freqinout-multi-rig-test` |
| Settings DB | `$HOME/.freqinout-multi-rig-test/config/freqinout.db` |
| Operational DB | `$HOME/.freqinout-multi-rig-test/config/freqinout_nets.db` |

## Install

The private repository requires testing-community access.

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

`--config-root` is part of the safety contract: it is written into the generated
`freqinout` launcher and used by FIO at first launch. The installer itself does
not finalize configuration migration. Keep this argument unchanged for every
update or repair.

## Launch and verify isolation

Run `freqinout` from a terminal or use the desktop/menu entry. Then verify:

```bash
ls -la "$HOME/.freqinout-multi-rig-test/config"
```

Expected after first launch:

```text
freqinout.db
```

`freqinout_nets.db` appears after operational features initialize. The test must not create, modify, or migrate `$HOME/.freqinout/config/freqinout.db`.

For a direct launch that does not use the generated launcher:

```bash
FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test" \
  "$HOME/FreqInOut-multi-rig-test/venv/bin/python" -m freqinout.main
```

The repository helper can also launch a source install and accepts both locations without editing the script:

```bash
FREQINOUT_INSTALL_DIR="$HOME/FreqInOut-multi-rig-test" \
FREQINOUT_CONFIG_DIR="$HOME/.freqinout-multi-rig-test" \
  "$HOME/FreqInOut-multi-rig-test/start-freqinout.sh"
```

## Acceptance checks

- The test databases exist only beneath the dedicated profile root for this run.
- Settings begins as a fresh profile without production radio data; the first radio and Station Default assignment are created intentionally.
- Settings, theme, radio profiles, and window placement persist after restart.
- Message Inbox, FIO Spotter, Compose, and the separate Map window open without blocking the main window.
- The Map remains usable offline and never requests an API key or tile download.
- Station Health distinguishes actionable configuration problems from normal waiting states.

## Update or repair

Close FIO and companion applications, then rerun the same installer command. For repair, add `--repair`; keep `--dir`, `--config-root`, and `--branch` unchanged. After the checks pass, continue with isolated update/restart testing. An in-place single-rig upgrade is a separate, higher-risk scenario and should be performed only when assigned, using the dedicated [upgrade guide](single-rig-to-multi-rig-upgrade-linux.md).
