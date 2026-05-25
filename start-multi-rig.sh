#!/usr/bin/env bash
set -euo pipefail

WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$WORKTREE/.venv"
RUNTIME_ROOT="${FREQINOUT_RUNTIME_ROOT:-$HOME/.freqinout/runtime/multi-rig}"
CONFIG_ROOT="$RUNTIME_ROOT/config"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "Missing virtual environment at $VENV" >&2
  exit 1
fi

mkdir -p "$RUNTIME_ROOT" "$CONFIG_ROOT"
export FREQINOUT_CONFIG_DIR="$RUNTIME_ROOT"

cd "$WORKTREE"
exec "$VENV/bin/python" -m freqinout.main "$@"
