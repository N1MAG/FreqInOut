#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRAPPER_PY="$ROOT_DIR/tools/freqinout_db.py"
PYTHON_BIN=""

if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "ERROR: Python not found." >&2
  exit 1
fi

cd "$ROOT_DIR"
exec "$PYTHON_BIN" "$WRAPPER_PY" "$@"
