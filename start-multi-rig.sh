#!/usr/bin/env bash
set -euo pipefail

SCRIPT_WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE="${FREQINOUT_INSTALL_DIR:-$SCRIPT_WORKTREE}"
if [[ -x "$WORKTREE/.venv/bin/python" ]]; then
  PYTHON="$WORKTREE/.venv/bin/python"
elif [[ -x "$WORKTREE/venv/bin/python" ]]; then
  PYTHON="$WORKTREE/venv/bin/python"
else
  echo "Missing virtual environment at $WORKTREE/.venv or $WORKTREE/venv" >&2
  exit 1
fi

# FIO owns the platform-specific default profile location.  Do not silently
# create a second multi-rig profile when an operator upgrades from single-rig.
# FREQINOUT_CONFIG_DIR remains the supported way to select an isolated profile.
if [[ -n "${FREQINOUT_RUNTIME_ROOT:-}" && -z "${FREQINOUT_CONFIG_DIR:-}" ]]; then
  export FREQINOUT_CONFIG_DIR="$FREQINOUT_RUNTIME_ROOT"
  echo "Warning: FREQINOUT_RUNTIME_ROOT is deprecated; use FREQINOUT_CONFIG_DIR." >&2
fi

cd "$WORKTREE"
exec "$PYTHON" -m freqinout.main "$@"
