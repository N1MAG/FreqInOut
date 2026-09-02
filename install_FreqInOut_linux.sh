#!/usr/bin/env bash
set -eEuo pipefail

MIN_PYTHON_MAJOR=3
MIN_PYTHON_MINOR=9
MAX_PYTHON_MAJOR=3
MAX_PYTHON_MINOR=13

DEFAULT_REPO_URL="git@github.com:N1MAG/FreqInOut-internal-testing.git"
DEFAULT_BRANCH="wip/private-testing-multi-rig-1.2.3-not-ready"
REPO_URL="${REPO_URL:-$DEFAULT_REPO_URL}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/FreqInOut}"
LOG_FILE="${LOG_FILE:-$HOME/freqinout-install.log}"

ASSUME_YES=0
DRY_RUN=0
REPAIR_MODE=0
CLI_ARGS_PROVIDED=0
OFFLINE_MODE=0
SKIP_UPDATE_WORK=0
ON_DIRTY_POLICY="${ON_DIRTY_POLICY:-prompt}"
ON_RUNNING_POLICY="${ON_RUNNING_POLICY:-prompt}"
ON_NON_GIT_POLICY="${ON_NON_GIT_POLICY:-prompt}"

SETUP_MODE="full"
DO_UPDATE=1
DO_ICON=1

CHANNEL="stable"
BRANCH=""
PKG_MGR=""
VENV_DIR=""
BACKUP_ARCHIVE=""
DISTRO_ID=""
DISTRO_ID_LIKE=""
NETWORK_HINT_SHOWN=0
ROLLBACK_DIR=""
ROLLBACK_STAMP=""
ROLLBACK_LAUNCHER_BACKUP=""
ROLLBACK_DESKTOP_BACKUP=""
ROLLBACK_ICON_BACKUP_DIR=""
ROLLBACK_VENV_BACKUP=""
CREATED_INSTALL_DIR=0
REPLACED_NON_GIT_BACKUP=""
ROLLBACK_IN_PROGRESS=0

LAUNCHER_PATH="$HOME/.local/bin/freqinout"
DESKTOP_FILE_NAME="freqinout.desktop"
DESKTOP_ENTRY_PATH="$HOME/.local/share/applications/$DESKTOP_FILE_NAME"
ICON_THEME_ROOT="$HOME/.local/share/icons/hicolor"
ICON_PRIMARY_PATH="$ICON_THEME_ROOT/1024x1024/apps/freqinout.png"
ICON_CACHE_DIR="$HOME/.local/state/freqinout/cache"
BACKUP_ROOT="$HOME/.local/state/freqinout/backups"
ICON_ZOOM_PERCENT="${ICON_ZOOM_PERCENT:-320}"
PRIMARY_ICON_NAME="FreqInOut-desktop.png"
LOCK_DIR="${XDG_RUNTIME_DIR:-/tmp}/freqinout-installer.lock"
LOCK_HELD=0

usage() {
  cat <<'EOF'
FreqInOut Linux Installer

Usage:
  bash install_FreqInOut_linux.sh
  bash install_FreqInOut_linux.sh /path/to/install/folder
  bash install_FreqInOut_linux.sh --dir /path/to/install/folder
  INSTALL_DIR=/path/to/install/folder bash install_FreqInOut_linux.sh

Options:
  -d, --dir <path>      Install location (default: ~/FreqInOut)
  -r, --repo <url>      Git repository URL (default: multi-rig WIP repo)
  -c, --channel <name>  Update channel: stable or beta (default: stable/WIP branch)
  -b, --branch <name>   Git branch override (takes priority over --channel)
      --repair          Rebuild venv + launcher + icon without recloning
      --dry-run         Show what would be done without changing anything
      --offline         Skip network checks/downloads and use local files only
      --on-dirty <p>    Policy for dirty git tree: prompt|stash|skip|fail
      --on-running <p>  Policy if app is running: prompt|skip|fail
      --on-non-git <p>  Policy for non-git install folder: prompt|replace|skip|fail
  -y, --yes             Auto-approve prompts
      --log-file <path> Log output path (default: ~/freqinout-install.log)
  -h, --help            Show this help
EOF
}

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] [FreqInOut] $*"; }
warn() { echo "[$(timestamp)] [FreqInOut] WARNING: $*" >&2; }
die() { echo "[$(timestamp)] [FreqInOut] ERROR: $*" >&2; exit 1; }

command_exists() { command -v "$1" >/dev/null 2>&1; }
expand_path() { local raw="$1"; echo "${raw/#\~/$HOME}"; }

run_cmd() {
  local cmd=("$@")
  local cmd_string=""
  local token
  for token in "${cmd[@]}"; do
    cmd_string+="$(printf "%q " "$token")"
  done
  log "CMD: ${cmd_string% }"

  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: command skipped"
    return 0
  fi
  "${cmd[@]}"
}

run_step() {
  local step_name="$1"
  shift
  local start_ts
  local end_ts
  start_ts="$(date +%s)"
  log "STEP START: $step_name"
  if "$@"; then
    end_ts="$(date +%s)"
    log "STEP SUCCESS: $step_name ($((end_ts - start_ts))s)"
    return 0
  fi
  end_ts="$(date +%s)"
  warn "STEP FAILED: $step_name ($((end_ts - start_ts))s)"
  return 1
}

init_rollback_dir() {
  if [[ -n "$ROLLBACK_DIR" ]]; then
    return 0
  fi
  ROLLBACK_STAMP="$(date +%Y%m%d-%H%M%S)"
  ROLLBACK_DIR="$HOME/.local/state/freqinout/backups/installer-rollback-$ROLLBACK_STAMP"
  mkdir -p "$ROLLBACK_DIR"
}

backup_file_for_rollback() {
  local src="$1"
  local dst="$2"
  [[ -f "$src" ]] || return 0
  init_rollback_dir
  mkdir -p "$(dirname "$dst")"
  cp -f "$src" "$dst"
}

backup_icon_targets_for_rollback() {
  local sizes=(64 128 256 512 1024)
  local size=""
  local src=""

  init_rollback_dir
  ROLLBACK_ICON_BACKUP_DIR="$ROLLBACK_DIR/icons"
  mkdir -p "$ROLLBACK_ICON_BACKUP_DIR"
  for size in "${sizes[@]}"; do
    src="$ICON_THEME_ROOT/${size}x${size}/apps/freqinout.png"
    if [[ -f "$src" ]]; then
      mkdir -p "$ROLLBACK_ICON_BACKUP_DIR/${size}x${size}/apps"
      cp -f "$src" "$ROLLBACK_ICON_BACKUP_DIR/${size}x${size}/apps/freqinout.png"
    fi
  done
}

restore_rollback_state() {
  local size=""
  local dst=""
  local src=""
  local sizes=(64 128 256 512 1024)

  if [[ $ROLLBACK_IN_PROGRESS -eq 1 ]]; then
    return
  fi
  ROLLBACK_IN_PROGRESS=1
  set +e

  if [[ -n "$ROLLBACK_VENV_BACKUP" && -d "$ROLLBACK_VENV_BACKUP" ]]; then
    rm -rf "$VENV_DIR"
    mv "$ROLLBACK_VENV_BACKUP" "$VENV_DIR"
    warn "Rollback: restored previous virtual environment."
  fi

  if [[ -n "$ROLLBACK_LAUNCHER_BACKUP" && -f "$ROLLBACK_LAUNCHER_BACKUP" ]]; then
    mkdir -p "$(dirname "$LAUNCHER_PATH")"
    cp -f "$ROLLBACK_LAUNCHER_BACKUP" "$LAUNCHER_PATH"
    warn "Rollback: restored launcher file."
  fi

  if [[ -n "$ROLLBACK_DESKTOP_BACKUP" && -f "$ROLLBACK_DESKTOP_BACKUP" ]]; then
    mkdir -p "$(dirname "$DESKTOP_ENTRY_PATH")"
    cp -f "$ROLLBACK_DESKTOP_BACKUP" "$DESKTOP_ENTRY_PATH"
    warn "Rollback: restored desktop entry."
  fi

  if [[ -n "$ROLLBACK_ICON_BACKUP_DIR" && -d "$ROLLBACK_ICON_BACKUP_DIR" ]]; then
    for size in "${sizes[@]}"; do
      src="$ROLLBACK_ICON_BACKUP_DIR/${size}x${size}/apps/freqinout.png"
      dst="$ICON_THEME_ROOT/${size}x${size}/apps/freqinout.png"
      if [[ -f "$src" ]]; then
        mkdir -p "$(dirname "$dst")"
        cp -f "$src" "$dst"
      fi
    done
    warn "Rollback: restored prior icon theme files."
  fi

  if [[ $CREATED_INSTALL_DIR -eq 1 && -d "$INSTALL_DIR" ]]; then
    rm -rf "$INSTALL_DIR"
    warn "Rollback: removed partially created install directory."
  fi

  if [[ -n "$REPLACED_NON_GIT_BACKUP" && -d "$REPLACED_NON_GIT_BACKUP" && ! -e "$INSTALL_DIR" ]]; then
    mv "$REPLACED_NON_GIT_BACKUP" "$INSTALL_DIR"
    warn "Rollback: restored original non-git install folder."
  fi

  set -e
  ROLLBACK_IN_PROGRESS=0
}

on_error() {
  local line="$1"
  local cmd="${2:-unknown}"
  echo
  warn "Install failed near line $line."
  warn "Failed command: $cmd"
  restore_rollback_state
  if [[ -n "$ROLLBACK_DIR" ]]; then
    warn "Rollback backup folder: $ROLLBACK_DIR"
  fi
  warn "Recovery tips:"
  warn "1) Open the log at: $LOG_FILE"
  warn "2) Retry with: bash install_FreqInOut_linux.sh --repair --dir \"$INSTALL_DIR\""
  warn "3) If package install failed, run again with sudo access."
}
trap 'on_error $LINENO "$BASH_COMMAND"' ERR

acquire_lock() {
  local pid_file="$LOCK_DIR/pid"
  local holder_pid=""

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" >"$pid_file" 2>/dev/null || true
    LOCK_HELD=1
    return 0
  fi

  if [[ -f "$pid_file" ]]; then
    holder_pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$holder_pid" ]] && kill -0 "$holder_pid" 2>/dev/null; then
      die "Another FreqInOut installer is already running (PID $holder_pid)."
    fi
  fi

  warn "Removing stale installer lock at $LOCK_DIR"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" >"$pid_file" 2>/dev/null || true
    LOCK_HELD=1
    return 0
  fi

  die "Could not acquire installer lock at $LOCK_DIR"
}

release_lock() {
  if [[ $LOCK_HELD -eq 1 ]]; then
    rm -rf "$LOCK_DIR" 2>/dev/null || true
    LOCK_HELD=0
  fi
}
trap 'release_lock' EXIT

parse_args() {
  local positional=()
  if [[ $# -gt 0 ]]; then
    CLI_ARGS_PROVIDED=1
  fi
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -d|--dir)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        INSTALL_DIR="$2"
        shift 2
        ;;
      -r|--repo)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        REPO_URL="$2"
        shift 2
        ;;
      -c|--channel)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        CHANNEL="$2"
        shift 2
        ;;
      -b|--branch)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        BRANCH="$2"
        shift 2
        ;;
      --repair)
        REPAIR_MODE=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --offline)
        OFFLINE_MODE=1
        shift
        ;;
      --on-dirty)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        ON_DIRTY_POLICY="${2,,}"
        shift 2
        ;;
      --on-running)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        ON_RUNNING_POLICY="${2,,}"
        shift 2
        ;;
      --on-non-git)
        [[ $# -ge 2 ]] || die "Missing value for $1"
        ON_NON_GIT_POLICY="${2,,}"
        shift 2
        ;;
      -y|--yes)
        ASSUME_YES=1
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
      -*)
        die "Unknown option: $1 (run with --help)"
        ;;
      *)
        positional+=("$1")
        shift
        ;;
    esac
  done

  if [[ ${#positional[@]} -gt 1 ]]; then
    die "Too many positional arguments (run with --help)"
  fi
  if [[ ${#positional[@]} -eq 1 ]]; then
    INSTALL_DIR="${positional[0]}"
  fi

  case "$ON_DIRTY_POLICY" in
    prompt|stash|skip|fail) ;;
    *) die "--on-dirty must be one of: prompt|stash|skip|fail" ;;
  esac
  case "$ON_RUNNING_POLICY" in
    prompt|skip|fail) ;;
    *) die "--on-running must be one of: prompt|skip|fail" ;;
  esac
  case "$ON_NON_GIT_POLICY" in
    prompt|replace|skip|fail) ;;
    *) die "--on-non-git must be one of: prompt|replace|skip|fail" ;;
  esac
}

prompt_startup_options() {
  local choice=""
  if [[ $ASSUME_YES -eq 1 || $CLI_ARGS_PROVIDED -eq 1 ]]; then
    return
  fi

  while true; do
    cat <<'EOF'

Choose installer mode:
  1) Guided install/update (default)
  2) Dry run (no changes)
  3) Repair mode
  4) Repair mode + dry run
  5) Show help and exit
EOF
    read -r -p "Enter 1, 2, 3, 4, or 5 [1]: " choice || true
    case "${choice:-1}" in
      1) break ;;
      2) DRY_RUN=1; break ;;
      3) REPAIR_MODE=1; break ;;
      4) REPAIR_MODE=1; DRY_RUN=1; break ;;
      5) usage; exit 0 ;;
      *) echo "Please enter 1, 2, 3, 4, or 5." ;;
    esac
  done
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

setup_logging() {
  LOG_FILE="$(expand_path "$LOG_FILE")"
  mkdir -p "$(dirname "$LOG_FILE")"
  if [[ $DRY_RUN -eq 0 && -f "$LOG_FILE" ]]; then
    cp "$LOG_FILE" "$LOG_FILE.previous" 2>/dev/null || true
  fi
  if command_exists tee; then
    exec > >(tee -a "$LOG_FILE") 2>&1
  fi
  log "Logging to $LOG_FILE"
  if [[ $DRY_RUN -eq 1 ]]; then
    log "Dry-run mode enabled; no files will be changed."
  fi
}

detect_package_manager() {
  if command_exists apt-get; then PKG_MGR="apt"
  elif command_exists dnf; then PKG_MGR="dnf"
  elif command_exists yum; then PKG_MGR="yum"
  elif command_exists pacman; then PKG_MGR="pacman"
  elif command_exists zypper; then PKG_MGR="zypper"
  else PKG_MGR=""
  fi
}

detect_distro_info() {
  DISTRO_ID=""
  DISTRO_ID_LIKE=""
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    DISTRO_ID="${ID:-}"
    DISTRO_ID_LIKE="${ID_LIKE:-}"
  fi
}

show_network_troubleshooting_hint() {
  if [[ $NETWORK_HINT_SHOWN -eq 1 ]]; then
    return
  fi
  NETWORK_HINT_SHOWN=1
  warn "Network checks failed. If you use a proxy, set http_proxy/https_proxy and retry."
  warn "Example: export https_proxy=http://proxy.example:8080"
  warn "Or run installer with --offline to skip network-dependent checks."
}

repo_raw_base_url() {
  local remote="$REPO_URL"
  local owner_repo=""

  if [[ "$remote" =~ ^https://github\.com/([^/]+/[^/.]+)(\.git)?$ ]]; then
    owner_repo="${BASH_REMATCH[1]}"
  elif [[ "$remote" =~ ^git@github\.com:([^/]+/[^/.]+)(\.git)?$ ]]; then
    owner_repo="${BASH_REMATCH[1]}"
  fi

  if [[ -n "$owner_repo" ]]; then
    echo "https://raw.githubusercontent.com/$owner_repo"
    return 0
  fi
  return 1
}

get_branch_candidates() {
  local candidates=()
  if [[ -n "$BRANCH" ]]; then
    candidates+=("$BRANCH")
  fi
  candidates+=("main" "master")
  printf "%s\n" "${candidates[@]}"
}

has_http_fetch_tool() {
  if command_exists curl || command_exists wget; then
    return 0
  fi
  return 1
}

has_internet_connectivity() {
  if [[ $OFFLINE_MODE -eq 1 ]]; then
    log "Offline mode enabled; skipping internet connectivity checks."
    return 1
  fi
  if ! has_http_fetch_tool; then
    warn "Neither curl nor wget is available; network checks/downloads will be skipped."
    return 1
  fi
  if command_exists curl; then
    if run_cmd curl -fsSL --max-time 5 --connect-timeout 3 https://raw.githubusercontent.com >/dev/null; then
      return 0
    fi
    show_network_troubleshooting_hint
    return 1
  fi
  if command_exists wget; then
    if run_cmd wget -q --spider --timeout=5 https://raw.githubusercontent.com; then
      return 0
    fi
    show_network_troubleshooting_hint
    return 1
  fi
  return 1
}

read_version_from_version_py() {
  local file_path="$1"
  [[ -f "$file_path" ]] || return 1
  awk -F'"' '/^__version__[[:space:]]*=[[:space:]]*"/ { print $2; exit }' "$file_path"
}

read_version_from_pyproject() {
  local file_path="$1"
  [[ -f "$file_path" ]] || return 1
  awk -F'"' '/^version[[:space:]]*=[[:space:]]*"/ { print $2; exit }' "$file_path"
}

fetch_remote_version() {
  local raw_base=""
  local candidate=""
  local url=""
  local content=""
  local remote_version=""

  raw_base="$(repo_raw_base_url 2>/dev/null || true)"
  [[ -n "$raw_base" ]] || return 1
  has_http_fetch_tool || return 1

  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] || continue
    url="$raw_base/$candidate/freqinout/version.py"
    log "Checking remote version from: $url"
    if command_exists curl; then
      if content="$(run_cmd curl -fsSL "$url" 2>/dev/null)"; then
        remote_version="$(printf "%s\n" "$content" | awk -F'"' '/^__version__[[:space:]]*=[[:space:]]*"/ { print $2; exit }')"
        if [[ -n "$remote_version" ]]; then
          printf "%s\n" "$remote_version"
          return 0
        fi
      fi
    elif command_exists wget; then
      if content="$(run_cmd wget -qO- "$url" 2>/dev/null)"; then
        remote_version="$(printf "%s\n" "$content" | awk -F'"' '/^__version__[[:space:]]*=[[:space:]]*"/ { print $2; exit }')"
        if [[ -n "$remote_version" ]]; then
          printf "%s\n" "$remote_version"
          return 0
        fi
      fi
    else
      return 1
    fi
  done < <(get_branch_candidates)

  return 1
}

is_version_newer() {
  local local_version="$1"
  local remote_version="$2"
  local newest=""

  newest="$(printf "%s\n%s\n" "$local_version" "$remote_version" | sort -V | tail -n1)"
  if [[ "$newest" == "$remote_version" && "$remote_version" != "$local_version" ]]; then
    return 0
  fi
  return 1
}

check_update_availability() {
  local local_version=""
  local remote_version=""

  if [[ ! -d "$INSTALL_DIR" ]]; then
    log "Local install folder not found yet; skipping update availability check."
    return 0
  fi

  local_version="$(read_version_from_version_py "$INSTALL_DIR/freqinout/version.py" 2>/dev/null || true)"
  if [[ -z "$local_version" ]]; then
    # Backward compatibility for older installs.
    local_version="$(read_version_from_pyproject "$INSTALL_DIR/pyproject.toml" 2>/dev/null || true)"
  fi
  if [[ -z "$local_version" ]]; then
    warn "Could not determine local version from version metadata."
    return 0
  fi

  if ! has_internet_connectivity; then
    log "No internet connectivity detected; skipping update availability check."
    return 0
  fi

  remote_version="$(fetch_remote_version 2>/dev/null || true)"
  if [[ -z "$remote_version" ]]; then
    warn "Could not determine version available on GitHub."
    return 0
  fi

  if is_version_newer "$local_version" "$remote_version"; then
    log "Current version is $local_version. Version $remote_version is available."
    if prompt_yes_no "Current version is $local_version. Version $remote_version is available - would you like to update?"; then
      DO_UPDATE=1
      log "User accepted update prompt."
    else
      log "User declined update prompt."
    fi
  else
    log "Current version is $local_version. You are up to date."
  fi
}

download_icon_from_github() {
  local target_path="$1"
  local raw_base=""
  local candidate
  local url=""

  if [[ $OFFLINE_MODE -eq 1 ]]; then
    log "Offline mode enabled; skipping icon download from GitHub."
    return 1
  fi
  if ! has_http_fetch_tool; then
    warn "Cannot download icon: neither curl nor wget is available."
    return 1
  fi

  raw_base="$(repo_raw_base_url 2>/dev/null || true)"
  [[ -n "$raw_base" ]] || return 1

  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] || continue
    url="$raw_base/$candidate/assets/$PRIMARY_ICON_NAME"
    run_cmd mkdir -p "$(dirname "$target_path")"
    if command_exists curl; then
      log "Trying icon download: $url"
      if run_cmd curl -fsSL "$url" -o "$target_path"; then
        return 0
      fi
    elif command_exists wget; then
      log "Trying icon download: $url"
      if run_cmd wget -qO "$target_path" "$url"; then
        return 0
      fi
    else
      return 1
    fi
  done < <(get_branch_candidates)

  return 1
}

manual_install_hint() {
  detect_package_manager
  case "$PKG_MGR" in
    apt) echo "sudo apt-get update && sudo apt-get install git python3 python3-pip python3-venv libxcb-cursor0 libxcb-xinerama0" ;;
    dnf) echo "sudo dnf install git python3 python3-pip python3-virtualenv" ;;
    yum) echo "sudo yum install git python3 python3-pip python3-virtualenv" ;;
    pacman) echo "sudo pacman -Sy git python python-pip" ;;
    zypper) echo "sudo zypper install git python3 python3-pip python3-virtualenv" ;;
    *) echo "Install git + Python 3.9-3.13 + pip + venv with your distro package manager." ;;
  esac
}

run_pkg_install() {
  local pkgs=("$@")
  local runner=()
  if [[ ${#pkgs[@]} -eq 0 ]]; then
    return 0
  fi

  if [[ ${EUID:-$(id -u)} -eq 0 ]]; then
    runner=()
  elif command_exists sudo; then
    runner=(sudo)
  else
    die "Need root access. Re-run as root or install sudo."
  fi

  case "$PKG_MGR" in
    apt)
      run_cmd "${runner[@]}" apt-get update
      run_cmd "${runner[@]}" apt-get install -y "${pkgs[@]}"
      ;;
    dnf) run_cmd "${runner[@]}" dnf install -y "${pkgs[@]}" ;;
    yum) run_cmd "${runner[@]}" yum install -y "${pkgs[@]}" ;;
    pacman) run_cmd "${runner[@]}" pacman -Sy --noconfirm "${pkgs[@]}" ;;
    zypper) run_cmd "${runner[@]}" zypper --non-interactive install "${pkgs[@]}" ;;
    *) die "No supported package manager detected." ;;
  esac
}

install_required_system_packages() {
  detect_package_manager
  detect_distro_info
  [[ -n "$PKG_MGR" ]] || die "No supported package manager detected."
  log "Detected package manager: $PKG_MGR (distro: ${DISTRO_ID:-unknown}, like: ${DISTRO_ID_LIKE:-unknown})"

  local required=()
  local optional=()
  case "$PKG_MGR" in
    apt)
      required=(git python3 python3-pip python3-venv)
      case " $DISTRO_ID $DISTRO_ID_LIKE " in
        *" ubuntu "*|*" debian "*|*" linuxmint "*|*" pop "*|*" kali "*)
          required+=(libxcb-cursor0 libxcb-xinerama0)
          ;;
        *)
          optional+=(libxcb-cursor0 libxcb-xinerama0)
          ;;
      esac
      optional+=(xdg-utils desktop-file-utils)
      ;;
    dnf|yum)
      required=(git python3 python3-pip python3-virtualenv)
      optional=(xcb-util-cursor xdg-utils desktop-file-utils)
      ;;
    pacman)
      required=(git python python-pip)
      optional=(xcb-util-cursor libxinerama xdg-utils desktop-file-utils)
      ;;
    zypper)
      required=(git python3 python3-pip python3-virtualenv)
      optional=(libxcb-cursor0 xdg-utils desktop-file-utils)
      ;;
  esac

  log "Installing required system packages..."
  run_pkg_install "${required[@]}"

  if [[ ${#optional[@]} -gt 0 ]]; then
    log "Installing optional desktop/map packages..."
    run_pkg_install "${optional[@]}" || warn "Some optional packages were not installed."
  fi
}

python_version_supported() {
  command_exists python3 || return 1
  python3 - "$MIN_PYTHON_MAJOR" "$MIN_PYTHON_MINOR" "$MAX_PYTHON_MAJOR" "$MAX_PYTHON_MINOR" <<'PY'
import sys
need_major = int(sys.argv[1])
need_minor = int(sys.argv[2])
max_major = int(sys.argv[3])
max_minor = int(sys.argv[4])
version = sys.version_info[:2]
raise SystemExit(0 if (need_major, need_minor) <= version <= (max_major, max_minor) else 1)
PY
}

ensure_python_and_tools() {
  local need_packages=0
  command_exists git || need_packages=1
  python_version_supported || need_packages=1
  command_exists python3 || need_packages=1
  python3 -m venv --help >/dev/null 2>&1 || need_packages=1

  if [[ $need_packages -eq 1 ]]; then
    warn "Missing required tools (git/python3/venv)."
    warn "Manual install command: $(manual_install_hint)"
    if [[ $DRY_RUN -eq 1 ]]; then
      warn "Dry-run mode: continuing preview without installing system packages."
      return 0
    fi
    if prompt_yes_no "Install missing packages automatically now?"; then
      install_required_system_packages
    else
      die "Please install dependencies and run again."
    fi
  fi

  if ! python_version_supported; then
    warn "Detected python version: $(python3 --version 2>/dev/null || echo unknown)"
    die "Python $MIN_PYTHON_MAJOR.$MIN_PYTHON_MINOR through $MAX_PYTHON_MAJOR.$MAX_PYTHON_MINOR is supported."
  fi
}

resolve_channel_branch() {
  case "${CHANNEL,,}" in
    stable)
      if [[ -z "$BRANCH" ]]; then
        BRANCH="$DEFAULT_BRANCH"
      fi
      ;;
    beta)
      if [[ -z "$BRANCH" ]]; then
        BRANCH="beta"
      fi
      ;;
    *) die "Unsupported channel '$CHANNEL'. Use stable or beta." ;;
  esac
}

prompt_existing_install_mode() {
  local answer=""
  local existing_path=""
  local default_choice="3"
  if [[ $ASSUME_YES -eq 1 || $REPAIR_MODE -eq 1 ]]; then
    return
  fi

  log "Prompting for existing-install mode selection."
  if prompt_yes_no "Have you already installed FreqInOut on this computer?"; then
    SETUP_MODE="existing"
    DO_UPDATE=0
    DO_ICON=0

    while true; do
      read -r -p "Enter the existing install folder path: " existing_path || true
      existing_path="$(expand_path "${existing_path:-}")"
      if [[ -n "$existing_path" ]]; then
        INSTALL_DIR="$existing_path"
        log "User selected existing install path: $INSTALL_DIR"
        break
      fi
      echo "Please enter a folder path."
    done

    check_update_availability
    if [[ $DO_UPDATE -eq 1 ]]; then
      default_choice="3"
    else
      default_choice="2"
    fi

    while true; do
      cat <<'EOF'
Choose what you want to do:
  1) Update the app
  2) Install desktop icon/launcher
  3) Both
EOF
      read -r -p "Enter 1, 2, or 3 [$default_choice]: " answer || true
      case "${answer:-$default_choice}" in
        1) DO_UPDATE=1; DO_ICON=0; log "Selected action: update app"; break ;;
        2) DO_UPDATE=0; DO_ICON=1; log "Selected action: install desktop icon/launcher"; break ;;
        3) DO_UPDATE=1; DO_ICON=1; log "Selected action: both update and icon/launcher"; break ;;
        *) echo "Please enter 1, 2, or 3." ;;
      esac
    done
  else
    log "Selected action: full install flow."
  fi
}

backup_user_data() {
  local candidates=(
    "$INSTALL_DIR/config"
    "$INSTALL_DIR/runtime"
    "$HOME/.freqinout"
    "$HOME/.freqinout/config"
    "$HOME/.freqinout/runtime/single-rig/config"
    "$HOME/.freqinout/runtime/multi-rig/config"
    "$HOME/.config/FreqInOut"
    "$HOME/.local/share/FreqInOut"
  )
  local existing=()
  local item
  for item in "${candidates[@]}"; do
    if [[ -e "$item" ]] && ! path_covered_by_existing_backup "$item" "${existing[@]}"; then
      existing+=("$item")
    fi
  done
  if [[ ${#existing[@]} -eq 0 ]]; then
    return 0
  fi

  mkdir -p "$BACKUP_ROOT"
  BACKUP_ARCHIVE="$BACKUP_ROOT/freqinout-backup-$(date +%Y%m%d-%H%M%S).tar.gz"
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: would back up user data to $BACKUP_ARCHIVE"
    return 0
  fi

  tar -czf "$BACKUP_ARCHIVE" --absolute-names "${existing[@]}"
  log "Backed up user data to $BACKUP_ARCHIVE"
}

path_covered_by_existing_backup() {
  local candidate="$1"
  shift || true
  local parent
  for parent in "$@"; do
    if [[ "$candidate" == "$parent" || "$candidate" == "$parent/"* ]]; then
      return 0
    fi
  done
  return 1
}

git_current_branch() {
  git -C "$INSTALL_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true
}

git_worktree_is_dirty() {
  [[ -d "$INSTALL_DIR/.git" ]] || return 1
  [[ -n "$(git -C "$INSTALL_DIR" status --porcelain 2>/dev/null)" ]]
}

handle_dirty_worktree() {
  local policy="$ON_DIRTY_POLICY"
  if ! git_worktree_is_dirty; then
    return 0
  fi

  warn "Local git changes detected in $INSTALL_DIR:"
  git -C "$INSTALL_DIR" status --short | head -n 20

  if [[ "$policy" == "prompt" && $ASSUME_YES -eq 1 ]]; then
    policy="skip"
  fi

  case "$policy" in
    stash)
      run_cmd git -C "$INSTALL_DIR" stash push -u -m "freqinout-installer-$(date +%Y%m%d-%H%M%S)"
      log "Applied dirty-tree policy: stash"
      return 0
      ;;
    skip)
      warn "Dirty-tree policy is skip; continuing without app update."
      SKIP_UPDATE_WORK=1
      return 0
      ;;
    fail)
      die "Dirty-tree policy is fail; aborting update."
      ;;
  esac

  if prompt_yes_no "Local changes detected. Stash changes and continue update?"; then
    run_cmd git -C "$INSTALL_DIR" stash push -u -m "freqinout-installer-$(date +%Y%m%d-%H%M%S)"
    return 0
  fi

  if prompt_yes_no "Skip app update and continue with launcher/icon steps only?"; then
    SKIP_UPDATE_WORK=1
    return 0
  fi

  die "Aborted update due to local git changes."
}

is_freqinout_running() {
  if command_exists pgrep; then
    if pgrep -f "freqinout.main|freqinout" >/dev/null 2>&1; then
      return 0
    fi
    return 1
  fi

  if command_exists ps; then
    if ps aux 2>/dev/null | grep -E "freqinout\.main|freqinout" | grep -v grep >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

ensure_app_not_running_for_update() {
  local policy="$ON_RUNNING_POLICY"
  if ! is_freqinout_running; then
    return 0
  fi

  warn "FreqInOut appears to be running."
  if [[ "$policy" == "prompt" && $ASSUME_YES -eq 1 ]]; then
    policy="skip"
  fi

  case "$policy" in
    skip)
      warn "Running-app policy is skip; continuing without app update."
      SKIP_UPDATE_WORK=1
      return 0
      ;;
    fail)
      die "Running-app policy is fail; close FreqInOut before updating."
      ;;
  esac

  if prompt_yes_no "Close FreqInOut and continue update now?"; then
    if is_freqinout_running; then
      warn "FreqInOut still appears to be running."
      if prompt_yes_no "Skip app update and continue with launcher/icon only?"; then
        SKIP_UPDATE_WORK=1
        return 0
      fi
      die "Please close FreqInOut before updating."
    fi
    return 0
  fi

  if prompt_yes_no "Skip app update and continue with launcher/icon only?"; then
    SKIP_UPDATE_WORK=1
    return 0
  fi

  die "Update canceled because FreqInOut is running."
}

clone_repository_into_install_dir() {
  CREATED_INSTALL_DIR=1
  log "Cloning repository into $INSTALL_DIR"
  if [[ -n "$BRANCH" ]]; then
    run_cmd git clone --branch "$BRANCH" --single-branch "$REPO_URL" "$INSTALL_DIR"
  else
    run_cmd git clone "$REPO_URL" "$INSTALL_DIR"
  fi
  configure_runtime_sparse_checkout
}

configure_runtime_sparse_checkout() {
  [[ -d "$INSTALL_DIR/.git" ]] || return 0
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: would configure runtime sparse checkout for $INSTALL_DIR"
    return 0
  fi
  if ! git -C "$INSTALL_DIR" sparse-checkout -h >/dev/null 2>&1; then
    warn "Installed git does not support sparse-checkout; leaving full source tree in place."
    return 0
  fi

  log "Configuring runtime sparse checkout (excludes tests/dev-only paths)"
  run_cmd git -C "$INSTALL_DIR" sparse-checkout init --cone
  run_cmd git -C "$INSTALL_DIR" sparse-checkout set \
    assets \
    config \
    docs \
    freqinout \
    third_party \
    requirements.txt \
    README.md \
    CHANGELOG.md \
    LICENSE.md \
    install_FreqInOut_linux.sh \
    uninstall_FreqInOut_linux.sh
}

handle_non_git_install_dir() {
  local policy="$ON_NON_GIT_POLICY"
  local backup_path=""
  local stamp=""

  warn "$INSTALL_DIR exists but is not a git checkout."
  if [[ "$policy" == "prompt" && $ASSUME_YES -eq 1 ]]; then
    policy="skip"
  fi

  case "$policy" in
    replace)
      stamp="$(date +%Y%m%d-%H%M%S)"
      backup_path="$HOME/.local/state/freqinout/backups/non-git-install-$stamp"
      run_cmd mkdir -p "$(dirname "$backup_path")"
      run_cmd mv "$INSTALL_DIR" "$backup_path"
      REPLACED_NON_GIT_BACKUP="$backup_path"
      log "Moved existing non-git folder to $backup_path"
      clone_repository_into_install_dir
      return 0
      ;;
    skip)
      warn "Non-git policy is skip; continuing without app update."
      SKIP_UPDATE_WORK=1
      return 0
      ;;
    fail)
      die "Non-git policy is fail; aborting update."
      ;;
  esac

  if prompt_yes_no "Replace this folder with a fresh git clone (recommended for updates)?"; then
    stamp="$(date +%Y%m%d-%H%M%S)"
    backup_path="$HOME/.local/state/freqinout/backups/non-git-install-$stamp"
    run_cmd mkdir -p "$(dirname "$backup_path")"
    run_cmd mv "$INSTALL_DIR" "$backup_path"
    REPLACED_NON_GIT_BACKUP="$backup_path"
    log "Moved existing non-git folder to $backup_path"
    clone_repository_into_install_dir
    return 0
  fi

  if prompt_yes_no "Skip app update and continue with launcher/icon only?"; then
    SKIP_UPDATE_WORK=1
    return 0
  fi

  die "Update canceled for non-git install path."
}

clone_fresh() {
  mkdir -p "$(dirname "$INSTALL_DIR")"
  if [[ -e "$INSTALL_DIR" && ! -d "$INSTALL_DIR/.git" ]]; then
    handle_non_git_install_dir
    if [[ $SKIP_UPDATE_WORK -eq 1 ]]; then
      return 0
    fi
    return 0
  fi

  if [[ -d "$INSTALL_DIR/.git" ]]; then
    log "Existing git checkout found; updating it."
    update_existing_install
    return
  fi

  clone_repository_into_install_dir
}

update_existing_install() {
  SKIP_UPDATE_WORK=0
  ensure_app_not_running_for_update
  if [[ $SKIP_UPDATE_WORK -eq 1 ]]; then
    log "Skipping update because app is running."
    return 0
  fi
  if [[ ! -d "$INSTALL_DIR/.git" ]]; then
    handle_non_git_install_dir
    if [[ $SKIP_UPDATE_WORK -eq 1 ]]; then
      log "Skipping update due to non-git install path."
    fi
    return 0
  fi

  handle_dirty_worktree
  if [[ $SKIP_UPDATE_WORK -eq 1 ]]; then
    log "Skipping update due to local worktree state."
    return 0
  fi

  backup_user_data
  log "Updating install at $INSTALL_DIR"
  if [[ -n "$BRANCH" ]]; then
    local current_branch=""
    local target_branch="$BRANCH"
    current_branch="$(git_current_branch)"
    if [[ -n "$current_branch" && "$current_branch" != "$target_branch" ]]; then
      warn "Current branch is '$current_branch' but target branch is '$target_branch'."
      if prompt_yes_no "Switch from '$current_branch' to '$target_branch' for update?"; then
        :
      else
        target_branch="$current_branch"
        log "Keeping current branch: $target_branch"
      fi
    fi

    run_cmd git -C "$INSTALL_DIR" fetch origin "$target_branch"
    run_cmd git -C "$INSTALL_DIR" checkout "$target_branch"
    run_cmd git -C "$INSTALL_DIR" pull --ff-only origin "$target_branch"
  else
    run_cmd git -C "$INSTALL_DIR" pull --ff-only
  fi
  configure_runtime_sparse_checkout
}

create_venv_and_install_python_deps() {
  if [[ $DRY_RUN -eq 1 && ! -f "$INSTALL_DIR/requirements.txt" ]]; then
    log "DRY RUN: would install Python dependencies from $INSTALL_DIR/requirements.txt after source checkout."
    return 0
  fi
  [[ -f "$INSTALL_DIR/requirements.txt" ]] || die "requirements.txt not found in $INSTALL_DIR"

  if [[ -d "$VENV_DIR" && -z "$ROLLBACK_VENV_BACKUP" ]]; then
    init_rollback_dir
    ROLLBACK_VENV_BACKUP="$ROLLBACK_DIR/venv-backup"
    run_cmd rm -rf "$ROLLBACK_VENV_BACKUP"
    run_cmd cp -a "$VENV_DIR" "$ROLLBACK_VENV_BACKUP"
    log "Saved rollback backup of virtual environment."
  fi

  if [[ $REPAIR_MODE -eq 1 && -d "$VENV_DIR" ]]; then
    log "Repair mode: rebuilding virtual environment."
    run_cmd rm -rf "$VENV_DIR"
  fi

  if [[ -d "$VENV_DIR" ]]; then
    log "Using existing virtual environment at $VENV_DIR"
  else
    log "Creating virtual environment..."
    run_cmd python3 -m venv "$VENV_DIR"
  fi

  log "Installing Python dependencies..."
  run_cmd "$VENV_DIR/bin/python" -m ensurepip --upgrade
  run_cmd "$VENV_DIR/bin/python" -m pip install --upgrade pip
  run_cmd "$VENV_DIR/bin/pip" install -r "$INSTALL_DIR/requirements.txt"
}

cleanup_deprecated_files() {
  [[ -d "$INSTALL_DIR" ]] || return 0

  local removed_count=0
  local path=""
  local rel=""
  local -a deprecated_paths=(
    "freqinout/core/scheduler_engine_orig.py"
    "freqinout/__init__orig.py"
    "freqinout/__init__updated.py"
    "freqinout/utils/__init__orig.py"
    "docs/appimage.md"
    "install_linux.sh"
    "uninstall_linux.sh"
  )

  log "Checking for deprecated files to clean up..."

  for rel in "${deprecated_paths[@]}"; do
    path="$INSTALL_DIR/$rel"
    if [[ -f "$path" ]]; then
      log "Removing deprecated file: $path"
      run_cmd rm -f "$path"
      removed_count=$((removed_count + 1))
    fi
  done

  if [[ -d "$INSTALL_DIR/freqinout" ]]; then
    while IFS= read -r path; do
      [[ -n "$path" ]] || continue
      log "Removing legacy Python artifact: $path"
      run_cmd rm -f "$path"
      removed_count=$((removed_count + 1))
    done < <(
      find "$INSTALL_DIR/freqinout" -type f \( -name '*_orig.py' -o -name '*_updated.py' \) 2>/dev/null | sort
    )
  fi

  log "Deprecated file cleanup complete. Removed $removed_count file(s)."
}

create_launcher() {
  if [[ $DRY_RUN -eq 1 && ! -d "$INSTALL_DIR" ]]; then
    log "DRY RUN: would write launcher to $LAUNCHER_PATH after source checkout."
    return 0
  fi
  [[ -d "$INSTALL_DIR" ]] || die "Install folder not found: $INSTALL_DIR"
  log "Preparing launcher at $LAUNCHER_PATH"
  if [[ -z "$ROLLBACK_LAUNCHER_BACKUP" ]]; then
    init_rollback_dir
    ROLLBACK_LAUNCHER_BACKUP="$ROLLBACK_DIR/launcher.bak"
    backup_file_for_rollback "$LAUNCHER_PATH" "$ROLLBACK_LAUNCHER_BACKUP"
  fi
  mkdir -p "$(dirname "$LAUNCHER_PATH")"
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: would write launcher to $LAUNCHER_PATH"
    return 0
  fi
  cat >"$LAUNCHER_PATH" <<EOF
#!/usr/bin/env bash
cd "$INSTALL_DIR"
exec "$VENV_DIR/bin/python" -m freqinout.main "\$@"
EOF
  chmod +x "$LAUNCHER_PATH"
  log "Launcher written: $LAUNCHER_PATH"
}

detect_desktop_environment() {
  local env_name="${XDG_CURRENT_DESKTOP:-${DESKTOP_SESSION:-unknown}}"
  echo "${env_name,,}"
}

trust_desktop_launcher() {
  local target="$1"
  if [[ ! -f "$target" ]]; then
    return 0
  fi

  run_cmd chmod +x "$target"

  # Cinnamon/Nemo (Linux Mint) often requires this trust metadata for double-click launch.
  if command_exists gio; then
    run_cmd gio set "$target" metadata::trusted true || true
  fi
}

refresh_desktop_caches() {
  local de="$1"
  if command_exists desktop-file-validate; then
    run_cmd desktop-file-validate "$DESKTOP_ENTRY_PATH" || true
  fi
  if command_exists update-desktop-database; then
    log "Refreshing desktop application database"
    run_cmd update-desktop-database "$HOME/.local/share/applications"
  fi
  if command_exists gtk-update-icon-cache; then
    log "Refreshing icon cache"
    run_cmd gtk-update-icon-cache -f "$ICON_THEME_ROOT" || true
  fi
  if command_exists xdg-desktop-menu; then
    log "Refreshing xdg desktop menu cache"
    run_cmd xdg-desktop-menu forceupdate || true
  fi
  if [[ "$de" == *kde* || "$de" == *plasma* ]]; then
    if command_exists kbuildsycoca6; then
      log "Refreshing KDE menu cache (kbuildsycoca6)"
      run_cmd kbuildsycoca6 || true
    elif command_exists kbuildsycoca5; then
      log "Refreshing KDE menu cache (kbuildsycoca5)"
      run_cmd kbuildsycoca5 || true
    fi
  fi
}

prepare_icon_source() {
  local primary_icon="$INSTALL_DIR/assets/$PRIMARY_ICON_NAME"
  local cache_icon="$ICON_CACHE_DIR/$PRIMARY_ICON_NAME"
  local input_icon=""
  local output_icon="$INSTALL_DIR/.freqinout_icon_prepared.png"

  if [[ -f "$primary_icon" ]]; then
    log "Using icon from install assets: $primary_icon"
    input_icon="$primary_icon"
    run_cmd mkdir -p "$ICON_CACHE_DIR"
    run_cmd cp -f "$primary_icon" "$cache_icon"
  else
    log "Icon file not found in install assets; attempting cached download of $PRIMARY_ICON_NAME"
    run_cmd mkdir -p "$ICON_CACHE_DIR"
    if download_icon_from_github "$cache_icon"; then
      log "Downloaded icon to cache: $cache_icon"
      input_icon="$cache_icon"
    else
      return 1
    fi
  fi

  # If ImageMagick is available, trim transparent padding so the desktop icon appears larger.
  if command_exists convert; then
    log "Preparing zoomed desktop icon with ImageMagick (ICON_ZOOM_PERCENT=${ICON_ZOOM_PERCENT}%)"
    run_cmd convert "$input_icon" -alpha set -fuzz 20% -trim +repage -background none -gravity center -resize 1024x1024 -extent 1024x1024 -resize "${ICON_ZOOM_PERCENT}%" -gravity center -extent 1024x1024 "$output_icon"
    echo "$output_icon"
  else
    warn "ImageMagick not found; using raw PNG icon without zoom processing."
    echo "$input_icon"
  fi
  return 0
}

refresh_icon_asset() {
  local primary_icon="$INSTALL_DIR/assets/$PRIMARY_ICON_NAME"
  local branch=""

  if [[ $OFFLINE_MODE -eq 1 ]]; then
    log "Offline mode enabled; skipping icon refresh."
    return 0
  fi
  if [[ -d "$INSTALL_DIR/.git" ]]; then
    branch="$(git_current_branch)"
    if [[ -z "$branch" ]]; then
      warn "Could not determine current git branch; skipping icon refresh."
      return 0
    fi
    if git -C "$INSTALL_DIR" status --porcelain -- "$primary_icon" | grep -q .; then
      warn "Local changes detected in icon asset; skipping icon refresh."
      return 0
    fi
    log "Refreshing icon asset from origin/$branch"
    run_cmd git -C "$INSTALL_DIR" fetch origin "$branch"
    run_cmd git -C "$INSTALL_DIR" checkout "origin/$branch" -- "assets/$PRIMARY_ICON_NAME"
    return 0
  fi

  if [[ -f "$primary_icon" ]]; then
    log "Non-git install detected; attempting to refresh icon from GitHub."
    run_cmd mkdir -p "$ICON_CACHE_DIR"
    if download_icon_from_github "$ICON_CACHE_DIR/$PRIMARY_ICON_NAME"; then
      run_cmd mkdir -p "$(dirname "$primary_icon")"
      run_cmd cp -f "$ICON_CACHE_DIR/$PRIMARY_ICON_NAME" "$primary_icon"
      log "Replaced local icon asset from GitHub."
    fi
  fi
  return 0
}

install_icon_files() {
  local source_icon="$1"
  local size
  local icon_target
  local sizes=(64 128 256 512 1024)

  for size in "${sizes[@]}"; do
    icon_target="$ICON_THEME_ROOT/${size}x${size}/apps/freqinout.png"
    run_cmd mkdir -p "$(dirname "$icon_target")"
    run_cmd rm -f "$icon_target"
    run_cmd cp -f "$source_icon" "$icon_target"
  done
}

install_pixmaps_icon() {
  local source_icon="$1"
  local pixmaps_target="$HOME/.local/share/pixmaps/freqinout.png"
  run_cmd mkdir -p "$(dirname "$pixmaps_target")"
  run_cmd rm -f "$pixmaps_target"
  run_cmd cp -f "$source_icon" "$pixmaps_target"
}

create_desktop_icon() {
  if [[ $DRY_RUN -eq 1 && ! -d "$INSTALL_DIR" ]]; then
    log "DRY RUN: would write desktop entry to $DESKTOP_ENTRY_PATH after source checkout."
    return 0
  fi
  [[ -d "$INSTALL_DIR" ]] || die "Install folder not found: $INSTALL_DIR"

  local icon_value="applications-utilities"
  local desktop_shortcut=""
  local prepared_icon=""
  if [[ -f "$DESKTOP_ENTRY_PATH" ]]; then
    log "Existing desktop entry detected at $DESKTOP_ENTRY_PATH; it will be replaced."
  fi
  refresh_icon_asset
  if [[ -z "$ROLLBACK_DESKTOP_BACKUP" ]]; then
    init_rollback_dir
    ROLLBACK_DESKTOP_BACKUP="$ROLLBACK_DIR/desktop-entry.bak"
    backup_file_for_rollback "$DESKTOP_ENTRY_PATH" "$ROLLBACK_DESKTOP_BACKUP"
  fi
  if [[ -z "$ROLLBACK_ICON_BACKUP_DIR" ]]; then
    backup_icon_targets_for_rollback
  fi
  log "Preparing desktop entry at $DESKTOP_ENTRY_PATH"
  if prepared_icon="$(prepare_icon_source)"; then
    log "Installing desktop icon assets in $ICON_THEME_ROOT"
    install_icon_files "$prepared_icon"
    install_pixmaps_icon "$prepared_icon"
    # Use the install asset path for reliable desktop icon updates.
    icon_value="$INSTALL_DIR/assets/$PRIMARY_ICON_NAME"
    if [[ "$prepared_icon" == "$INSTALL_DIR/.freqinout_icon_prepared.png" ]]; then
      run_cmd rm -f "$prepared_icon"
    fi
  else
    warn "App icon source not found, using generic icon."
  fi

  mkdir -p "$(dirname "$DESKTOP_ENTRY_PATH")"
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: would write desktop entry to $DESKTOP_ENTRY_PATH"
  else
    cat >"$DESKTOP_ENTRY_PATH" <<EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=FreqInOut
Comment=HF Radio Frequency and Net Control Utility
Exec=$LAUNCHER_PATH
Icon=$icon_value
Terminal=false
Categories=Utility;HamRadio;
StartupNotify=true
Path=$INSTALL_DIR
EOF
    trust_desktop_launcher "$DESKTOP_ENTRY_PATH"
    log "Desktop entry written: $DESKTOP_ENTRY_PATH"
  fi

  local desktop_dir="$HOME/Desktop"
  if command_exists xdg-user-dir; then
    local candidate
    candidate="$(xdg-user-dir DESKTOP 2>/dev/null || true)"
    if [[ -n "$candidate" ]]; then
      desktop_dir="$candidate"
    fi
  fi
  if [[ -d "$desktop_dir" ]]; then
    desktop_shortcut="$desktop_dir/$DESKTOP_FILE_NAME"
    log "Copying desktop shortcut to $desktop_shortcut"
    run_cmd cp -f "$DESKTOP_ENTRY_PATH" "$desktop_shortcut"
    trust_desktop_launcher "$desktop_shortcut"
    log "Desktop shortcut ready: $desktop_shortcut"
  else
    warn "Desktop folder not found; skipped desktop shortcut copy."
  fi

  local de
  de="$(detect_desktop_environment)"
  refresh_desktop_caches "$de"
  log "Detected desktop environment: $de"
  if [[ "$de" == *cinnamon* || "$de" == *mint* ]]; then
    log "Mint/Cinnamon note: if double-click still prompts, right-click launcher and choose 'Allow Launching' once."
  fi
  log "If icon does not appear immediately, log out/in or run: update-desktop-database ~/.local/share/applications"
}

run_self_test() {
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY RUN: would run post-install self-test."
    return 0
  fi
  log "Running post-install self-test..."
  if "$VENV_DIR/bin/python" - <<'PY'
import importlib
import os
import tempfile
from pathlib import Path

temp_root = Path(tempfile.mkdtemp(prefix="freqinout-installer-selftest-"))
os.environ["FREQINOUT_CONFIG_DIR"] = str(temp_root)
importlib.import_module("freqinout.main")
from freqinout.core.settings_manager import SettingsManager

SettingsManager()
assert (temp_root / "config" / "freqinout.db").exists()
print("Self-test passed: freqinout.main import and settings DB check OK")
PY
  then
    log "Self-test passed."
  else
    warn "Self-test failed. Try: bash install_FreqInOut_linux.sh --repair --dir \"$INSTALL_DIR\""
  fi
}

print_finish_message() {
  local completed="no changes requested"
  if [[ $DO_UPDATE -eq 1 && $DO_ICON -eq 1 ]]; then
    completed="app updated/dependencies installed; launcher and desktop icon installed"
  elif [[ $DO_UPDATE -eq 1 ]]; then
    completed="app updated/dependencies installed"
  elif [[ $DO_ICON -eq 1 ]]; then
    completed="launcher and desktop icon installed"
  fi

  cat <<EOF

Install complete.

Completed:
  - $completed

How to run:
  - App launcher/menu: search for "FreqInOut"
  - Terminal command: freqinout

Installed to:
  - App folder: $INSTALL_DIR
  - Launcher:   $LAUNCHER_PATH
  - Desktop:    $DESKTOP_ENTRY_PATH
  - Log file:   $LOG_FILE
EOF
  if [[ -n "$BACKUP_ARCHIVE" ]]; then
    echo "  - Backup:     $BACKUP_ARCHIVE"
  fi
  cat <<'EOF'

Helpful commands:
  - Repair install: bash install_FreqInOut_linux.sh --repair --dir "$HOME/FreqInOut"
  - Dry run:        bash install_FreqInOut_linux.sh --dry-run
  - Uninstall:      bash uninstall_FreqInOut_linux.sh --dir "$HOME/FreqInOut"
EOF
}

main() {
  parse_args "$@"
  prompt_startup_options
  acquire_lock
  LOG_FILE="$(expand_path "$LOG_FILE")"
  setup_logging
  prompt_existing_install_mode

  INSTALL_DIR="$(expand_path "$INSTALL_DIR")"
  resolve_channel_branch
  VENV_DIR="$INSTALL_DIR/venv"

  if [[ $REPAIR_MODE -eq 1 ]]; then
    SETUP_MODE="repair"
    DO_UPDATE=1
    DO_ICON=1
  fi

  log "Starting installer."
  log "Install folder: $INSTALL_DIR"
  log "Repository: $REPO_URL"
  log "Desktop icon zoom percent: $ICON_ZOOM_PERCENT"
  log "Policies: on-dirty=$ON_DIRTY_POLICY, on-running=$ON_RUNNING_POLICY, on-non-git=$ON_NON_GIT_POLICY"
  if [[ $OFFLINE_MODE -eq 1 ]]; then
    log "Offline mode: enabled"
  fi
  if [[ -n "$BRANCH" ]]; then
    log "Branch: $BRANCH"
  else
    log "Branch: repo default"
  fi

  if [[ "$SETUP_MODE" == "repair" ]]; then
    [[ -d "$INSTALL_DIR" ]] || die "Repair mode needs an existing install directory."
    run_step "Check Python and required tools" ensure_python_and_tools
    run_step "Cleanup deprecated files" cleanup_deprecated_files
    run_step "Create virtual environment and install dependencies" create_venv_and_install_python_deps
    run_step "Create launcher script" create_launcher
    run_step "Create desktop icon and menu entry" create_desktop_icon
    run_step "Run post-install self-test" run_self_test
    print_finish_message
    return
  fi

  if [[ "$SETUP_MODE" == "existing" ]]; then
    [[ -d "$INSTALL_DIR" ]] || die "Install folder not found: $INSTALL_DIR"
    run_step "Cleanup deprecated files" cleanup_deprecated_files
    if [[ $DO_UPDATE -eq 1 ]]; then
      run_step "Check Python and required tools" ensure_python_and_tools
      run_step "Update existing install from git" update_existing_install
      if [[ $SKIP_UPDATE_WORK -eq 0 ]]; then
        run_step "Create virtual environment and install dependencies" create_venv_and_install_python_deps
        run_step "Run post-install self-test" run_self_test
      else
        log "Skipping dependency refresh and self-test because app update was skipped."
      fi
    fi
    if [[ $DO_ICON -eq 1 ]]; then
      run_step "Create launcher script" create_launcher
      run_step "Create desktop icon and menu entry" create_desktop_icon
    fi
    print_finish_message
    return
  fi

  run_step "Check Python and required tools" ensure_python_and_tools
  run_step "Clone or update application source" clone_fresh
  if [[ -d "$INSTALL_DIR" ]]; then
    run_step "Cleanup deprecated files" cleanup_deprecated_files
  fi
  if [[ $SKIP_UPDATE_WORK -eq 0 ]]; then
    run_step "Create virtual environment and install dependencies" create_venv_and_install_python_deps
    run_step "Create launcher script" create_launcher
    run_step "Create desktop icon and menu entry" create_desktop_icon
    run_step "Run post-install self-test" run_self_test
  else
    log "Skipping install/update actions after clone step due to user choice."
  fi
  print_finish_message
}

main "$@"
