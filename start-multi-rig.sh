#!/usr/bin/env bash
set -euo pipefail

SCRIPT_WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_WORKTREE/start-freqinout.sh" "$@"
