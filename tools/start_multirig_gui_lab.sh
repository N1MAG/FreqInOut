#!/usr/bin/env bash
#
# FIO multi-rig GUI lab launcher.
#
# Common use:
#   cd /Users/bill/RadioCode/FreqInOut-multi-rig
#   tools/start_multirig_gui_lab.sh start
#
# Preview without launching:
#   tools/start_multirig_gui_lab.sh start --dry-run
#
# Start only selected profiles:
#   tools/start_multirig_gui_lab.sh start --profiles a,b
#
# Check or stop tracked lab processes:
#   tools/start_multirig_gui_lab.sh status
#   tools/start_multirig_gui_lab.sh stop
#
# Default lab mapping:
#   fio-a: FLRig 12345, FLDigi 7362, JS8Call 2242, JS8Call 2.5.2
#   fio-b: FLRig 12346, FLDigi 7363, JS8Call 2243, JS8Call 3.0.3
#   fio-c: FLRig 12347, FLDigi 7364, JS8Call 2244, JS8Call 3.0.3
#
# JS8Call app overrides:
#   tools/start_multirig_gui_lab.sh start \
#     --js8call-bin-a /Applications/RadioApps/JS8Call.app/Contents/MacOS/JS8Call \
#     --js8call-bin-b "/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call" \
#     --js8call-bin-c "/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call"
#
# If you edit JS8Call settings during testing, quit JS8Call from its own UI
# before stopping the lab so JS8Call can persist preferences gracefully.
set -euo pipefail

ACTION="${1:-start}"
shift || true

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RADIO_TOOLS="${RADIO_TOOLS:-/Users/bill/RadioTools}"
LAB_ROOT="${FIO_MULTIRIG_LAB_ROOT:-/Users/bill/RadioCode/WORK/MultiRig/TestLab}"
RUN_DIR="${LAB_ROOT}/run-gui"
LOG_DIR="${LAB_ROOT}/logs-gui"
TOOL_PROFILES="${LAB_ROOT}/tool-profiles"
JS8_HOME_ROOT="${LAB_ROOT}/tool-homes/js8call"
FLDIGI_HOME_ROOT="${LAB_ROOT}/tool-homes/fldigi"

FLRIG_BIN="${FLRIG_BIN:-/Applications/RadioApps/flrig-2.0.10.app/Contents/MacOS/flrig}"
FLDIGI_BIN="${FLDIGI_BIN:-/Applications/RadioApps/fldigi-4.2.11.app/Contents/MacOS/fldigi}"
JS8CALL_BIN_A="${JS8CALL_BIN_A:-/Applications/RadioApps/JS8Call.app/Contents/MacOS/JS8Call}"
JS8CALL_BIN_B="${JS8CALL_BIN_B:-/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call}"
JS8CALL_BIN_C="${JS8CALL_BIN_C:-/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call}"
JS8CALL_BIN_D="${JS8CALL_BIN_D:-${JS8CALL_BIN_C}}"
if [[ -n "${JS8CALL_BIN:-}" ]]; then
  JS8CALL_BIN_A="$JS8CALL_BIN"
  JS8CALL_BIN_B="$JS8CALL_BIN"
  JS8CALL_BIN_C="$JS8CALL_BIN"
  JS8CALL_BIN_D="$JS8CALL_BIN"
fi
JS8CALL_INI="${JS8CALL_INI:-${HOME}/Library/Preferences/JS8Call.ini}"
if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

PROFILES=(a b c)
DRY_RUN=0
FORCE=0

usage() {
  cat <<EOF
Usage:
  tools/start_multirig_gui_lab.sh start [options]
  tools/start_multirig_gui_lab.sh status [options]
  tools/start_multirig_gui_lab.sh stop [options]

Starts the three-radio GUI lab:
  - rigctld radio emulators: A/B/C on 4532/4533/4534
  - real FLRig apps: A/B/C on XML-RPC 12345/12346/12347
  - real FLDigi apps: A/B/C on XML-RPC 7362/7363/7364
  - real JS8Call instances: fio-a on 2.5.2, fio-b/fio-c on 3.0.3

Options:
  --profiles a,b,c       Comma-separated profile list. Default: a,b,c
  --dry-run              Print commands without launching apps
  --force                With stop, kill lingering processes after TERM
  --flrig-bin PATH       Override FLRig executable
  --fldigi-bin PATH      Override FLDigi executable
  --js8call-bin PATH     Override JS8Call executable for all profiles
  --js8call-bin-a PATH   Override JS8Call executable for profile a
  --js8call-bin-b PATH   Override JS8Call executable for profile b
  --js8call-bin-c PATH   Override JS8Call executable for profile c
  --lab-root PATH        Override lab root
  -h, --help             Show this help

Environment overrides:
  RADIO_TOOLS, FIO_MULTIRIG_LAB_ROOT, FLRIG_BIN, FLDIGI_BIN,
  JS8CALL_BIN, JS8CALL_BIN_A, JS8CALL_BIN_B, JS8CALL_BIN_C, JS8CALL_INI

Notes:
  If you change JS8Call settings during a test, quit each JS8Call window from
  the JS8Call UI to give it the best chance to persist settings. The stop action
  sends TERM by default and only force-kills with --force.
EOF
}

parse_options() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profiles)
        IFS=',' read -r -a PROFILES <<<"${2:-}"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --force)
        FORCE=1
        shift
        ;;
      --flrig-bin)
        FLRIG_BIN="${2:-}"
        shift 2
        ;;
      --fldigi-bin)
        FLDIGI_BIN="${2:-}"
        shift 2
        ;;
      --js8call-bin)
        JS8CALL_BIN_A="${2:-}"
        JS8CALL_BIN_B="${2:-}"
        JS8CALL_BIN_C="${2:-}"
        JS8CALL_BIN_D="${2:-}"
        shift 2
        ;;
      --js8call-bin-a)
        JS8CALL_BIN_A="${2:-}"
        shift 2
        ;;
      --js8call-bin-b)
        JS8CALL_BIN_B="${2:-}"
        shift 2
        ;;
      --js8call-bin-c)
        JS8CALL_BIN_C="${2:-}"
        shift 2
        ;;
      --lab-root)
        LAB_ROOT="${2:-}"
        RUN_DIR="${LAB_ROOT}/run-gui"
        LOG_DIR="${LAB_ROOT}/logs-gui"
        TOOL_PROFILES="${LAB_ROOT}/tool-profiles"
        JS8_HOME_ROOT="${LAB_ROOT}/tool-homes/js8call"
        FLDIGI_HOME_ROOT="${LAB_ROOT}/tool-homes/fldigi"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

label_for_profile() {
  case "$1" in
    a) echo "fio-a" ;;
    b) echo "fio-b" ;;
    c) echo "fio-c" ;;
    d) echo "fio-d" ;;
    *) echo "fio-$1" ;;
  esac
}

flrig_port_for_profile() {
  case "$1" in
    a) echo "12345" ;;
    b) echo "12346" ;;
    c) echo "12347" ;;
    d) echo "12348" ;;
    *) return 1 ;;
  esac
}

fldigi_port_for_profile() {
  case "$1" in
    a) echo "7362" ;;
    b) echo "7363" ;;
    c) echo "7364" ;;
    d) echo "7365" ;;
    *) return 1 ;;
  esac
}

js8_port_for_profile() {
  case "$1" in
    a) echo "2242" ;;
    b) echo "2243" ;;
    c) echo "2244" ;;
    d) echo "2245" ;;
    *) return 1 ;;
  esac
}

js8call_bin_for_profile() {
  case "$1" in
    a) echo "$JS8CALL_BIN_A" ;;
    b) echo "$JS8CALL_BIN_B" ;;
    c) echo "$JS8CALL_BIN_C" ;;
    d) echo "$JS8CALL_BIN_D" ;;
    *) echo "$JS8CALL_BIN_A" ;;
  esac
}

rigctld_port_for_profile() {
  case "$1" in
    a) echo "4532" ;;
    b) echo "4533" ;;
    c) echo "4534" ;;
    d) echo "4535" ;;
    *) return 1 ;;
  esac
}

require_file() {
  if [[ ! -x "$1" ]]; then
    echo "Missing executable: $1" >&2
    exit 1
  fi
}

run_or_print() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'DRY RUN:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

find_existing_pid() {
  local required
  local line
  while IFS= read -r line; do
    case "$line" in
      *"start_multirig_gui_lab.sh"*|*" rg "*|*"ps axww -o pid=,command="*)
        continue
        ;;
    esac
    local matched=1
    for required in "$@"; do
      if [[ "$line" != *"$required"* ]]; then
        matched=0
        break
      fi
    done
    if [[ "$matched" -eq 1 ]]; then
      awk '{print $1}' <<<"$line"
      return 0
    fi
  done < <(ps axww -o pid=,command=)
  return 1
}

start_bg() {
  local name="$1"
  local match_count="$2"
  shift 2
  local match_parts=()
  local i
  for ((i = 0; i < match_count; i++)); do
    match_parts+=("$1")
    shift
  done
  shift
  mkdir -p "$RUN_DIR" "$LOG_DIR"
  local pid_file="${RUN_DIR}/${name}.pid"
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "$name already running pid=$(cat "$pid_file")"
    return 0
  fi
  rm -f "$pid_file"
  if [[ "$DRY_RUN" -eq 0 && "${#match_parts[@]}" -gt 0 ]]; then
    local existing_pid
    if existing_pid="$(find_existing_pid "${match_parts[@]}")"; then
      echo "$existing_pid" >"$pid_file"
      echo "$name already running pid=$existing_pid"
      return 0
    fi
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    run_or_print "$@"
    return 0
  fi
  nohup "$@" >"${LOG_DIR}/${name}.log" 2>&1 &
  echo $! >"$pid_file"
  echo "started $name pid=$(cat "$pid_file")"
}

set_kv_file_value() {
  local file="$1"
  local key="$2"
  local value="$3"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY RUN: set ${file} ${key}:${value}"
    return 0
  fi
  mkdir -p "$(dirname "$file")"
  touch "$file"
  if grep -q "^${key}:" "$file"; then
    perl -0pi -e "s|^\\Q${key}\\E:.*$|${key}:${value}|m" "$file"
  else
    printf '%s:%s\n' "$key" "$value" >>"$file"
  fi
}

set_xml_value() {
  local file="$1"
  local tag="$2"
  local value="$3"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY RUN: set ${file} <${tag}>${value}</${tag}>"
    return 0
  fi
  [[ -f "$file" ]] || return 0
  perl -0pi -e "s|<\\Q${tag}\\E>.*?</\\Q${tag}\\E>|<${tag}>${value}</${tag}>|gs" "$file"
}

prepare_flrig_profile() {
  local profile="$1"
  local label port config_dir prefs rig_prefs
  label="$(label_for_profile "$profile")"
  port="$(flrig_port_for_profile "$profile")"
  config_dir="${TOOL_PROFILES}/flrig/${label}"
  prefs="${config_dir}/flrig.prefs"
  rig_prefs="${config_dir}/NONE.prefs"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY RUN: ensure FLRig profile ${label} config ${config_dir} XML-RPC ${port}"
    return 0
  fi
  mkdir -p "$config_dir"
  [[ -f "$prefs" ]] || printf '; FLTK preferences file format 1.0\n; vendor: w1hkj.com\n; application: flrig\n\n[.]\n\nxcvr_name:NONE\n' >"$prefs"
  [[ -f "$rig_prefs" ]] || touch "$rig_prefs"
  set_kv_file_value "$prefs" "xcvr_name" "NONE"
  set_kv_file_value "$rig_prefs" "xmlport" "$port"
  set_kv_file_value "$rig_prefs" "xmlrig_port" "$port"
}

prepare_fldigi_profile() {
  local profile="$1"
  local label fldigi_port flrig_port config_dir prefs def_xml
  label="$(label_for_profile "$profile")"
  fldigi_port="$(fldigi_port_for_profile "$profile")"
  flrig_port="$(flrig_port_for_profile "$profile")"
  config_dir="${TOOL_PROFILES}/fldigi/${label}"
  prefs="${config_dir}/fldigi.prefs"
  def_xml="${config_dir}/fldigi_def.xml"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY RUN: ensure FLDigi profile ${label} config ${config_dir} XML-RPC ${fldigi_port}, FLRig ${flrig_port}"
    return 0
  fi
  mkdir -p "$config_dir"
  mkdir -p "${FLDIGI_HOME_ROOT}/${label}"
  [[ -f "$prefs" ]] || touch "$prefs"
  set_kv_file_value "$prefs" "xmlrpc_address" "127.0.0.1"
  set_kv_file_value "$prefs" "xmlrpc_port" "$fldigi_port"
  set_xml_value "$def_xml" "FLRIG_IP_ADDRESS" "127.0.0.1"
  set_xml_value "$def_xml" "FLRIG_IP_PORT" "$flrig_port"
}

prepare_js8_profile() {
  local profile="$1"
  local label js8_port flrig_port save_dir js8_bin existing_pid
  label="$(label_for_profile "$profile")"
  js8_port="$(js8_port_for_profile "$profile")"
  flrig_port="$(flrig_port_for_profile "$profile")"
  js8_bin="$(js8call_bin_for_profile "$profile")"
  save_dir="${JS8_HOME_ROOT}/${label}/save"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY RUN: ensure JS8Call profile ${label} uses TCP ${js8_port}, FLRig 127.0.0.1:${flrig_port}, SaveDir ${save_dir}"
    return 0
  fi
  if existing_pid="$(find_existing_pid "$js8_bin" "-r ${label}")"; then
    echo "JS8Call profile ${label} already running pid=${existing_pid}; leaving preference files untouched"
    return 0
  fi
  mkdir -p "$save_dir"
  mkdir -p "${save_dir}/messages" "${save_dir}/samples"
  touch "${save_dir}/DIRECTED.TXT"
  "$PYTHON_BIN" - "$JS8CALL_INI" "$label" "$js8_port" "$flrig_port" "$save_dir" <<'PY'
from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path

ini = Path(sys.argv[1]).expanduser()
label = sys.argv[2]
js8_port = sys.argv[3]
flrig_port = sys.argv[4]
save_dir = sys.argv[5]

def read_ini(path: Path, *, default_lines: list[str]) -> list[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        backup = path.with_suffix(path.suffix + f".fio-lab-backup-{datetime.now():%Y%m%d%H%M%S}")
        shutil.copy2(path, backup)
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    return list(default_lines)

def find_section(name: str) -> tuple[int, int] | None:
    global lines
    start = None
    for index, line in enumerate(lines):
        if line.strip() == name:
            start = index
            break
    if start is None:
        return None
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("[") and lines[index].endswith("]"):
            end = index
            break
    return start, end

def set_key(block: list[str], key: str, value: str) -> list[str]:
    prefix = f"{key}="
    for index, line in enumerate(block):
        if line.startswith(prefix):
            block[index] = f"{key}={value}"
            return block
    block.append(f"{key}={value}")
    return block

def update_section(path: Path, section: str, default_lines: list[str], values: dict[str, str]) -> None:
    global lines
    lines = read_ini(path, default_lines=default_lines)
    found = find_section(section)
    if found is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(section)
        found = (len(lines) - 1, len(lines))

    start, end = found
    block = lines[start + 1:end]
    for key, value in values.items():
        block = set_key(block, key, value)
    lines[start + 1:end] = block
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

values = {
    "AcceptTCPRequests": "true",
    "CATNetworkPort": f"127.0.0.1:{flrig_port}",
    "PTTport": f"127.0.0.1:{flrig_port}",
    "Rig": "FLRig FLRig",
    "SaveDir": save_dir,
    "TCPEnabled": "true",
    "TCPMaxConnections": "2",
    "TCPServer": "127.0.0.1",
    "TCPServerPort": js8_port,
    "UDPServer": "127.0.0.1",
    "UDPServerPort": js8_port,
    "WriteLogs": "true",
}

update_section(
    ini,
    f"[MultiSettings/{label}]",
    default_lines=["[Common]", "", "[Configuration]"],
    values=values,
)
update_section(
    ini.with_name(f"JS8Call - {label}.ini"),
    "[Configuration]",
    default_lines=["[Common]", "", "[Configuration]"],
    values=values,
)
PY
}

prepare_profiles() {
  require_file "$FLRIG_BIN"
  require_file "$FLDIGI_BIN"
  for profile in "${PROFILES[@]}"; do
    require_file "$(js8call_bin_for_profile "$profile")"
    prepare_flrig_profile "$profile"
    prepare_fldigi_profile "$profile"
    prepare_js8_profile "$profile"
  done
}

start_rigctld() {
  local profile="$1"
  local port name
  port="$(rigctld_port_for_profile "$profile")"
  name="rigctld-$(label_for_profile "$profile")"
  start_bg "$name" 3 "rigctld" "-T 127.0.0.1" "-t ${port}" -- "${RADIO_TOOLS}/bin/run-rigctld.sh" "$profile"
  echo "radio emulator $(label_for_profile "$profile"): rigctld 127.0.0.1:${port}"
}

start_profile_apps() {
  local profile="$1"
  local label flrig_port fldigi_port js8_port js8_bin
  label="$(label_for_profile "$profile")"
  flrig_port="$(flrig_port_for_profile "$profile")"
  fldigi_port="$(fldigi_port_for_profile "$profile")"
  js8_port="$(js8_port_for_profile "$profile")"
  js8_bin="$(js8call_bin_for_profile "$profile")"

  start_bg "flrig-${label}" 2 "$FLRIG_BIN" "--config-dir ${TOOL_PROFILES}/flrig/${label}" -- \
    "$FLRIG_BIN" --config-dir "${TOOL_PROFILES}/flrig/${label}"
  sleep 0.5
  start_bg "fldigi-${label}" 2 "$FLDIGI_BIN" "--config-dir ${TOOL_PROFILES}/fldigi/${label}" -- \
    "$FLDIGI_BIN" \
    --config-dir "${TOOL_PROFILES}/fldigi/${label}" \
    --home-dir "${FLDIGI_HOME_ROOT}/${label}" \
    --xmlrpc-server-address 127.0.0.1 \
    --xmlrpc-server-port "$fldigi_port"
  sleep 0.5
  start_bg "js8call-${label}" 2 "$js8_bin" "-r ${label}" -- "$js8_bin" -r "$label"

  echo "${label}: FLRig ${flrig_port}, FLDigi ${fldigi_port}, JS8Call ${js8_port}, JS8 app ${js8_bin}, DIRECTED.TXT ${JS8_HOME_ROOT}/${label}/save/DIRECTED.TXT"
}

start_all() {
  prepare_profiles
  for profile in "${PROFILES[@]}"; do
    start_rigctld "$profile"
  done
  for profile in "${PROFILES[@]}"; do
    start_profile_apps "$profile"
  done
  echo "GUI lab startup requested. Logs: ${LOG_DIR}"
}

status_all() {
  mkdir -p "$RUN_DIR"
  shopt -s nullglob
  local any=0
  for pidfile in "$RUN_DIR"/*.pid; do
    any=1
    local name pid
    name="$(basename "$pidfile" .pid)"
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "$name running pid=$pid"
    else
      echo "$name stale pid=$pid"
    fi
  done
  if [[ "$any" -eq 0 ]]; then
    echo "No GUI lab pid files found in ${RUN_DIR}"
  fi
}

stop_all() {
  mkdir -p "$RUN_DIR"
  shopt -s nullglob
  for pidfile in "$RUN_DIR"/*.pid; do
    local name pid
    name="$(basename "$pidfile" .pid)"
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "DRY RUN: kill TERM $pid # ${name}"
      else
        kill "$pid" || true
        echo "sent TERM to $name pid=$pid"
      fi
    else
      echo "$name not running"
    fi
    [[ "$DRY_RUN" -eq 1 ]] || rm -f "$pidfile"
  done
  if [[ "$FORCE" -eq 1 && "$DRY_RUN" -eq 0 ]]; then
    pkill -f "${RADIO_TOOLS}/bin/run-rigctld.sh" 2>/dev/null || true
    pkill -f "$FLRIG_BIN.*${TOOL_PROFILES}/flrig/fio-" 2>/dev/null || true
    pkill -f "$FLDIGI_BIN.*${TOOL_PROFILES}/fldigi/fio-" 2>/dev/null || true
    pkill -f "JS8Call.*-r fio-" 2>/dev/null || true
    echo "force cleanup requested"
  fi
}

parse_options "$@"

case "$ACTION" in
  start)
    start_all
    ;;
  status)
    status_all
    ;;
  stop)
    stop_all
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown action: $ACTION" >&2
    usage >&2
    exit 2
    ;;
esac
