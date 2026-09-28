#!/usr/bin/env bash
set -euo pipefail

SCRIPT_WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE="${FREQINOUT_INSTALL_DIR:-$SCRIPT_WORKTREE}"

if [[ -d "$WORKTREE/.venv" ]]; then
  if [[ ! -x "$WORKTREE/.venv/bin/python" ]]; then
    echo "FreqInOut installation is incomplete: .venv has no usable Python." >&2
    echo "Run: python3.11 install_freqinout.py" >&2
    exit 1
  fi
  PYTHON="$WORKTREE/.venv/bin/python"
elif [[ -x "$WORKTREE/venv/bin/python" ]]; then
  PYTHON="$WORKTREE/venv/bin/python"
else
  echo "FreqInOut is not installed in $WORKTREE." >&2
  echo "Run: python3.11 install_freqinout.py" >&2
  exit 1
fi

# FIO owns the platform-specific default profile location. Do not silently
# create another profile when an operator upgrades an existing station.
if [[ -n "${FREQINOUT_RUNTIME_ROOT:-}" && -z "${FREQINOUT_CONFIG_DIR:-}" ]]; then
  export FREQINOUT_CONFIG_DIR="$FREQINOUT_RUNTIME_ROOT"
  echo "Warning: FREQINOUT_RUNTIME_ROOT is deprecated; use FREQINOUT_CONFIG_DIR." >&2
fi

cd "$WORKTREE"
export FIO_LAUNCH_WORKTREE="$WORKTREE"
if ! "$PYTHON" -c "import hashlib, json, os, pathlib, sys; from freqinout.version import __version__; import freqinout.main, PySide6; root=pathlib.Path(os.environ['FIO_LAUNCH_WORKTREE']); r=json.loads((root/'.freqinout-install-verified.json').read_text()); assert r.get('version') == __version__; assert pathlib.Path(r.get('python','')).resolve() == pathlib.Path(sys.executable).resolve(); assert r.get('requirements_sha256') == hashlib.sha256((root/'requirements.txt').read_bytes()).hexdigest()" >/dev/null 2>&1; then
  echo "FreqInOut installation validation failed." >&2
  echo "Run: python3.11 install_freqinout.py" >&2
  exit 1
fi
exec "$PYTHON" -m freqinout.main "$@"
