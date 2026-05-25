#!/usr/bin/env bash
set -euo pipefail

WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$WORKTREE/.venv"
WORKTREE_PARENT="$(cd "$WORKTREE/.." && pwd)"
LEGACY_RUNTIME_ROOT="$WORKTREE_PARENT/runtime/multi-rig"
DEFAULT_RUNTIME_ROOT="$HOME/.freqinout/runtime/multi-rig"
if [[ -n "${FREQINOUT_RUNTIME_ROOT:-}" ]]; then
  RUNTIME_ROOT="$FREQINOUT_RUNTIME_ROOT"
elif [[ -f "$LEGACY_RUNTIME_ROOT/config/freqinout.db" ]]; then
  RUNTIME_ROOT="$LEGACY_RUNTIME_ROOT"
else
  RUNTIME_ROOT="$DEFAULT_RUNTIME_ROOT"
fi
CONFIG_ROOT="$RUNTIME_ROOT/config"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "Missing virtual environment at $VENV" >&2
  exit 1
fi

mkdir -p "$RUNTIME_ROOT" "$CONFIG_ROOT"
export FREQINOUT_CONFIG_DIR="$RUNTIME_ROOT"

cd "$WORKTREE"
exec "$VENV/bin/python" -m freqinout.main "$@"
