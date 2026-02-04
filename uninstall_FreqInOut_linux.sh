#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-$HOME/FreqInOut}"
LOG_FILE="${LOG_FILE:-$HOME/freqinout-uninstall.log}"
ASSUME_YES=0
DRY_RUN=0

LAUNCHER_PATH="$HOME/.local/bin/freqinout"
DESKTOP_FILE_NAME="freqinout.desktop"
DESKTOP_ENTRY_PATH="$HOME/.local/share/applications/$DESKTOP_FILE_NAME"
ICON_DEST="$HOME/.local/share/icons/hicolor/256x256/apps/freqinout.png"

usage() {
  cat <<'EOF'
FreqInOut Linux Uninstaller

Usage:
  bash uninstall_FreqInOut_linux.sh
  bash uninstall_FreqInOut_linux.sh --dir /path/to/FreqInOut
  INSTALL_DIR=/path/to/FreqInOut bash uninstall_FreqInOut_linux.sh

Options:
  -d, --dir <path>      Install location (default: ~/FreqInOut)
  -y, --yes             Auto-approve prompts
      --dry-run         Show what would be removed without deleting
      --log-file <path> Log output path (default: ~/freqinout-uninstall.log)
  -h, --help            Show help
EOF
}

log() { echo "[FreqInOut] $*"; }
warn() { echo "[FreqInOut] WARNING: $*" >&2; }
die() { echo "[FreqInOut] ERROR: $*" >&2; exit 1; }
expand_path() { local raw="$1"; echo "${raw/#\~/$HOME}"; }
command_exists() { command -v "$1" >/dev/null 2>&1; }

run_cmd() {
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: $*"
    return 0
  fi
  "$@"
}

prompt_yes_no() {
  local prompt="$1"
  local answer=""
  if [[ $ASSUME_YES -eq 1 ]]; then
    return 0
  fi
  while true; do
    read -r -p "$prompt [y/N]: " answer || true
    case "${answer,,}" in
      y|yes) return 0 ;;
      n|no|"") return 1 ;;
      *) echo "Please enter y or n." ;;
    esac
  done
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -d|--dir)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        INSTALL_DIR="$2"
        shift 2
        ;;
      -y|--yes)
        ASSUME_YES=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --log-file)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        LOG_FILE="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1 (run with --help)"
        ;;
    esac
  done
}

setup_logging() {
  LOG_FILE="$(expand_path "$LOG_FILE")"
  mkdir -p "$(dirname "$LOG_FILE")"
  if command_exists tee; then
    exec > >(tee -a "$LOG_FILE") 2>&1
  fi
  log "Logging to $LOG_FILE"
  [[ $DRY_RUN -eq 1 ]] && log "Dry-run mode enabled; nothing will be deleted."
}

remove_desktop_items() {
  local desktop_dir="$HOME/Desktop"
  if command_exists xdg-user-dir; then
    local candidate
    candidate="$(xdg-user-dir DESKTOP 2>/dev/null || true)"
    [[ -n "$candidate" ]] && desktop_dir="$candidate"
  fi

  [[ -f "$DESKTOP_ENTRY_PATH" ]] && run_cmd rm -f "$DESKTOP_ENTRY_PATH"
  [[ -f "$LAUNCHER_PATH" ]] && run_cmd rm -f "$LAUNCHER_PATH"
  [[ -f "$ICON_DEST" ]] && run_cmd rm -f "$ICON_DEST"
  [[ -f "$desktop_dir/$DESKTOP_FILE_NAME" ]] && run_cmd rm -f "$desktop_dir/$DESKTOP_FILE_NAME"

  if command_exists update-desktop-database; then
    run_cmd update-desktop-database "$HOME/.local/share/applications"
  fi
}

main() {
  parse_args "$@"
  INSTALL_DIR="$(expand_path "$INSTALL_DIR")"
  setup_logging

  log "Preparing to uninstall FreqInOut."
  log "Install folder: $INSTALL_DIR"

  if ! prompt_yes_no "Remove launcher, desktop icon, and menu entry?"; then
    warn "Skipped desktop item removal."
  else
    remove_desktop_items
  fi

  if [[ -d "$INSTALL_DIR" ]]; then
    if prompt_yes_no "Remove application folder at '$INSTALL_DIR'?"; then
      run_cmd rm -rf "$INSTALL_DIR"
    else
      warn "Kept application folder."
    fi
  else
    warn "Install folder not found; skipping app folder removal."
  fi

  log "Uninstall complete."
}

main "$@"

