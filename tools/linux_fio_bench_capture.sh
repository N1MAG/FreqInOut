#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
DEFAULT_PATTERNS_FILE="$TOOLS_DIR/linux_fio_bench_process_patterns.tsv"
SUMMARY_SCRIPT="$TOOLS_DIR/linux_fio_bench_summary.py"

DURATION_SECONDS=900
INTERVAL_SECONDS=1
OUT_ROOT="${HOME}/fio-bench"
SESSION_LABEL=""
PATTERNS_FILE=""
FIO_LOG_PATH="auto"
RUN_SUMMARY=1
MAKE_ARCHIVE=1

SESSION_DIR=""
RUN_LOG=""
WARNINGS_FILE=""
MANIFEST_FILE=""
COMMANDS_FILE=""
SUMMARY_RAN=0
SUMMARY_STATUS="not-run"
ARCHIVE_PATH=""
ARCHIVE_STATUS="not-requested"
PYTHON_BIN=""
STOP_REQUESTED=0
STOP_REASON="duration_elapsed"

declare -a WARNINGS=()
declare -a COLLECTOR_PIDS=()
declare -a COLLECTOR_NAMES=()
declare -a COLLECTOR_LOGS=()

usage() {
  cat <<'EOF'
FreqInOut Linux station benchmark capture (low-overhead telemetry for live ops)

Usage:
  bash tools/linux_fio_bench_capture.sh [options]

Options:
  --duration SECONDS       Capture duration (0 = run until Ctrl+C). Default: 900
  --interval SECONDS       Sample interval for collectors. Default: 1
  --out DIR                Output root directory. Default: ~/fio-bench
  --label NAME             Optional session label suffix (safe chars preferred)
  --patterns FILE          TSV process pattern file (default shipped patterns)
  --fio-log PATH|auto|off  Copy FIO log into bundle. Default: auto
  --no-summary             Skip auto-running Python summary
  --no-archive             Skip tar.gz archive creation
  --list-patterns          Print active default patterns and exit
  --help                   Show help

Examples:
  bash tools/linux_fio_bench_capture.sh --duration 1800
  bash tools/linux_fio_bench_capture.sh --duration 0 --label live-net
  bash tools/linux_fio_bench_capture.sh --interval 2 --fio-log /home/user/.freqinout/freqinout.log
EOF
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_msg() {
  local msg="$1"
  local line="[linux_fio_bench_capture] $(timestamp_utc) ${msg}"
  if [[ -n "${RUN_LOG:-}" ]]; then
    printf '%s\n' "$line" | tee -a "$RUN_LOG" >&2
  else
    printf '%s\n' "$line" >&2
  fi
}

warn_msg() {
  local msg="$1"
  WARNINGS+=("$msg")
  log_msg "WARNING: $msg"
}

die() {
  local msg="$1"
  log_msg "ERROR: $msg"
  exit 1
}

print_default_patterns() {
  if [[ ! -f "$DEFAULT_PATTERNS_FILE" ]]; then
    echo "Default pattern file not found: $DEFAULT_PATTERNS_FILE" >&2
    return 1
  fi
  cat "$DEFAULT_PATTERNS_FILE"
}

is_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

sanitize_label() {
  local raw="$1"
  if [[ -z "$raw" ]]; then
    echo ""
    return 0
  fi
  # Keep filenames predictable and shell-safe.
  raw="${raw// /_}"
  raw="$(printf '%s' "$raw" | tr -cd 'A-Za-z0-9._-')"
  printf '%s' "$raw"
}

find_python() {
  if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
    echo "$ROOT_DIR/venv/bin/python"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi
  return 1
}

write_notes_template() {
  cat >"$SESSION_DIR/operator_notes.txt" <<EOF
# FreqInOut live-station benchmark notes
# Capture ID: $(basename "$SESSION_DIR")
# Start UTC: ${CAPTURE_START_UTC}
#
# Add operator/event timeline entries in UTC (one per line), for example:
# 2026-02-23T14:03:12Z opened Schedule tab
# 2026-02-23T14:05:40Z JS8 traffic burst
# 2026-02-23T14:06:02Z FLRig reconnect
EOF
}

collect_command_versions() {
  {
    echo "date_utc=$(timestamp_utc)"
    echo "host=$(hostname 2>/dev/null || echo unknown)"
    echo "kernel=$(uname -a 2>/dev/null || echo unknown)"
    echo
    for cmd in pidstat sar iostat vmstat ps pgrep tar gzip python3 python lscpu lsblk free df; do
      if command -v "$cmd" >/dev/null 2>&1; then
        printf '[available] %s -> %s\n' "$cmd" "$(command -v "$cmd")"
      else
        printf '[missing] %s\n' "$cmd"
      fi
    done
  } >"$SESSION_DIR/command_inventory.txt"

  {
    for cmd in pidstat sar iostat vmstat; do
      if command -v "$cmd" >/dev/null 2>&1; then
        echo "### $cmd"
        "$cmd" -V 2>&1 | head -n 3 || true
        echo
      fi
    done
  } >"$SESSION_DIR/collector_versions.txt"
}

collect_system_info() {
  {
    echo "Capture start UTC: $CAPTURE_START_UTC"
    echo "Capture ID: $(basename "$SESSION_DIR")"
    echo "Host: $(hostname 2>/dev/null || echo unknown)"
    echo
    echo "== uname -a =="
    uname -a 2>&1 || true
    echo
    echo "== /etc/os-release =="
    cat /etc/os-release 2>/dev/null || true
    echo
    echo "== lsb_release -a =="
    lsb_release -a 2>&1 || true
    echo
    echo "== lscpu =="
    lscpu 2>&1 || true
    echo
    echo "== free -h =="
    free -h 2>&1 || true
    echo
    echo "== lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE =="
    lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE 2>&1 || true
    echo
    echo "== df -h =="
    df -h 2>&1 || true
    echo
    echo "== Python =="
    if [[ -n "${PYTHON_BIN:-}" ]]; then
      "$PYTHON_BIN" --version 2>&1 || true
    else
      python3 --version 2>&1 || python --version 2>&1 || true
    fi
  } >"$SESSION_DIR/system_info.txt"
}

record_process_snapshot() {
  local out_file="$1"
  {
    echo "Snapshot UTC: $(timestamp_utc)"
    ps -eo pid,ppid,ni,pri,stat,%cpu,%mem,rss,comm,args --sort=-%cpu 2>&1 || true
  } >"$out_file"
}

record_target_snapshot() {
  local out_file="$1"
  {
    echo "Snapshot UTC: $(timestamp_utc)"
    echo "Targets (broad regex): freqinout|flrig|fldigi|flamp|flmsg|varac|js8call|js8spotter|commstat|littlegucci|wine"
    ps -eo pid,ppid,comm,args 2>/dev/null | grep -Ei 'freqinout|flrig|fldigi|flamp|flmsg|varac|js8call|js8spotter|commstat|littlegucci|wine' || true
  } >"$out_file"
}

copy_patterns_file() {
  local src="$1"
  cp "$src" "$SESSION_DIR/target_process_patterns.tsv"
}

start_bg_collector() {
  local name="$1"
  local log_file="$2"
  shift 2
  (
    export LC_ALL=C
    export S_TIME_FORMAT=ISO
    exec "$@" >"$log_file" 2>&1
  ) &
  local pid=$!
  COLLECTOR_PIDS+=("$pid")
  COLLECTOR_NAMES+=("$name")
  COLLECTOR_LOGS+=("$log_file")
  printf '%s\t%s\t%s\n' "$name" "$pid" "$log_file" >>"$COMMANDS_FILE"
  log_msg "Started collector '$name' (pid=$pid)"
}

ps_children() {
  local parent_pid="$1"
  ps -o pid= --ppid "$parent_pid" 2>/dev/null | awk '{print $1}' || true
}

terminate_pid_tree() {
  local pid="$1"
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  local child
  for child in $(ps_children "$pid"); do
    terminate_pid_tree "$child" || true
  done
  kill -TERM "$pid" >/dev/null 2>&1 || true
}

stop_collectors() {
  local i
  if [[ ${#COLLECTOR_PIDS[@]} -eq 0 ]]; then
    return 0
  fi
  log_msg "Stopping collectors..."
  for i in "${!COLLECTOR_PIDS[@]}"; do
    terminate_pid_tree "${COLLECTOR_PIDS[$i]}" || true
  done
  sleep 1 || true
  for i in "${!COLLECTOR_PIDS[@]}"; do
    local pid="${COLLECTOR_PIDS[$i]}"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
  done
  for i in "${!COLLECTOR_PIDS[@]}"; do
    wait "${COLLECTOR_PIDS[$i]}" 2>/dev/null || true
  done
}

copy_fio_log() {
  local copied_path=""
  if [[ "$FIO_LOG_PATH" == "off" ]]; then
    echo ""
    return 0
  fi

  local candidates=()
  if [[ "$FIO_LOG_PATH" != "auto" ]]; then
    candidates+=("$FIO_LOG_PATH")
  else
    candidates+=(
      "$HOME/.freqinout/freqinout.log"
      "$HOME/.config/FreqInOut/freqinout.log"
      "$HOME/.local/share/FreqInOut/freqinout.log"
      "$HOME/FreqInOut/freqinout.log"
      "$HOME/FreqInOut/output/freqinout.log"
    )
  fi

  local path
  for path in "${candidates[@]}"; do
    if [[ -f "$path" ]]; then
      cp "$path" "$SESSION_DIR/freqinout.log"
      copied_path="$SESSION_DIR/freqinout.log"
      log_msg "Copied FIO log from $path"
      break
    fi
  done

  if [[ -z "$copied_path" && "$FIO_LOG_PATH" != "off" ]]; then
    warn_msg "FIO log not found (requested: $FIO_LOG_PATH)"
  fi
  echo "$copied_path"
}

signal_handler() {
  local sig="$1"
  STOP_REQUESTED=1
  STOP_REASON="$sig"
  log_msg "Received $sig, ending capture gracefully..."
}

write_manifest() {
  local fio_log_copied="$1"
  local end_utc="$2"
  local duration_actual="$3"
  {
    printf 'schema_version\t1\n'
    printf 'capture_id\t%s\n' "$(basename "$SESSION_DIR")"
    printf 'session_dir\t%s\n' "$SESSION_DIR"
    printf 'capture_start_utc\t%s\n' "$CAPTURE_START_UTC"
    printf 'capture_end_utc\t%s\n' "$end_utc"
    printf 'duration_requested_s\t%s\n' "$DURATION_SECONDS"
    printf 'duration_actual_s\t%s\n' "$duration_actual"
    printf 'interval_s\t%s\n' "$INTERVAL_SECONDS"
    printf 'stop_reason\t%s\n' "$STOP_REASON"
    printf 'host\t%s\n' "$(hostname 2>/dev/null || echo unknown)"
    printf 'kernel\t%s\n' "$(uname -r 2>/dev/null || echo unknown)"
    printf 'label\t%s\n' "$SESSION_LABEL"
    printf 'patterns_source\t%s\n' "$PATTERNS_SOURCE_USED"
    printf 'patterns_file\t%s\n' "$SESSION_DIR/target_process_patterns.tsv"
    printf 'fio_log_request\t%s\n' "$FIO_LOG_PATH"
    printf 'fio_log_copied\t%s\n' "$fio_log_copied"
    printf 'summary_requested\t%s\n' "$RUN_SUMMARY"
    printf 'summary_ran\t%s\n' "$SUMMARY_RAN"
    printf 'summary_status\t%s\n' "$SUMMARY_STATUS"
    printf 'archive_requested\t%s\n' "$MAKE_ARCHIVE"
    printf 'archive_status\t%s\n' "$ARCHIVE_STATUS"
    printf 'archive_path\t%s\n' "$ARCHIVE_PATH"
    printf 'python_bin\t%s\n' "$PYTHON_BIN"
  } >"$MANIFEST_FILE"

  if [[ ${#WARNINGS[@]} -gt 0 ]]; then
    printf '%s\n' "${WARNINGS[@]}" >"$WARNINGS_FILE"
  else
    : >"$WARNINGS_FILE"
  fi
}

run_summary_if_requested() {
  if [[ "$RUN_SUMMARY" -ne 1 ]]; then
    SUMMARY_STATUS="skipped"
    return 0
  fi
  if [[ ! -f "$SUMMARY_SCRIPT" ]]; then
    warn_msg "Summary script not found: $SUMMARY_SCRIPT"
    SUMMARY_STATUS="missing-script"
    return 0
  fi
  if [[ -z "$PYTHON_BIN" ]]; then
    warn_msg "Python not found; skipping summary step"
    SUMMARY_STATUS="missing-python"
    return 0
  fi
  log_msg "Running summary tool..."
  if "$PYTHON_BIN" "$SUMMARY_SCRIPT" "$SESSION_DIR" >>"$RUN_LOG" 2>&1; then
    SUMMARY_RAN=1
    SUMMARY_STATUS="ok"
    log_msg "Summary tool completed"
  else
    SUMMARY_RAN=1
    SUMMARY_STATUS="failed"
    warn_msg "Summary tool failed; see $RUN_LOG"
  fi
}

create_archive_if_requested() {
  if [[ "$MAKE_ARCHIVE" -ne 1 ]]; then
    ARCHIVE_STATUS="skipped"
    return 0
  fi
  if ! command -v tar >/dev/null 2>&1; then
    warn_msg "tar not found; skipping archive creation"
    ARCHIVE_STATUS="missing-tar"
    return 0
  fi
  local parent_dir base_name
  parent_dir="$(dirname "$SESSION_DIR")"
  base_name="$(basename "$SESSION_DIR")"
  ARCHIVE_PATH="${parent_dir}/${base_name}.tar.gz"
  if tar -czf "$ARCHIVE_PATH" -C "$parent_dir" "$base_name"; then
    ARCHIVE_STATUS="ok"
    log_msg "Wrote archive: $ARCHIVE_PATH"
  else
    ARCHIVE_STATUS="failed"
    warn_msg "Archive creation failed"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --duration)
        [[ $# -ge 2 ]] || die "Missing value for --duration"
        DURATION_SECONDS="$2"
        shift 2
        ;;
      --interval)
        [[ $# -ge 2 ]] || die "Missing value for --interval"
        INTERVAL_SECONDS="$2"
        shift 2
        ;;
      --out)
        [[ $# -ge 2 ]] || die "Missing value for --out"
        OUT_ROOT="$2"
        shift 2
        ;;
      --label)
        [[ $# -ge 2 ]] || die "Missing value for --label"
        SESSION_LABEL="$2"
        shift 2
        ;;
      --patterns)
        [[ $# -ge 2 ]] || die "Missing value for --patterns"
        PATTERNS_FILE="$2"
        shift 2
        ;;
      --fio-log)
        [[ $# -ge 2 ]] || die "Missing value for --fio-log"
        FIO_LOG_PATH="$2"
        shift 2
        ;;
      --no-summary)
        RUN_SUMMARY=0
        shift
        ;;
      --no-archive)
        MAKE_ARCHIVE=0
        shift
        ;;
      --list-patterns)
        print_default_patterns
        exit 0
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1 (use --help)"
        ;;
    esac
  done

  is_integer "$DURATION_SECONDS" || die "--duration must be an integer (seconds)"
  is_integer "$INTERVAL_SECONDS" || die "--interval must be an integer (seconds)"
  (( INTERVAL_SECONDS > 0 )) || die "--interval must be > 0"
  (( DURATION_SECONDS >= 0 )) || die "--duration must be >= 0"
}

start_collectors() {
  local pidstat_cmd sar_cmd iostat_cmd vmstat_cmd

  if command -v pidstat >/dev/null 2>&1; then
    start_bg_collector "pidstat_cpu" "$SESSION_DIR/pidstat_cpu.log" \
      pidstat -u -h -l -p ALL "$INTERVAL_SECONDS"
    start_bg_collector "pidstat_mem" "$SESSION_DIR/pidstat_mem.log" \
      pidstat -r -h -l -p ALL "$INTERVAL_SECONDS"
    start_bg_collector "pidstat_io" "$SESSION_DIR/pidstat_io.log" \
      pidstat -d -h -l -p ALL "$INTERVAL_SECONDS"
    start_bg_collector "pidstat_ctx" "$SESSION_DIR/pidstat_ctx.log" \
      pidstat -w -h -l -p ALL "$INTERVAL_SECONDS"
  else
    warn_msg "pidstat not found (install sysstat); per-process metrics will be unavailable"
  fi

  if command -v sar >/dev/null 2>&1; then
    start_bg_collector "sar_system" "$SESSION_DIR/sar_system.log" \
      sar -u -r -n DEV "$INTERVAL_SECONDS"
  else
    warn_msg "sar not found (install sysstat); network/system trend capture reduced"
  fi

  if command -v iostat >/dev/null 2>&1; then
    start_bg_collector "iostat" "$SESSION_DIR/iostat.log" \
      iostat -xzt "$INTERVAL_SECONDS"
  else
    warn_msg "iostat not found (install sysstat); disk contention capture reduced"
  fi

  if command -v vmstat >/dev/null 2>&1; then
    start_bg_collector "vmstat" "$SESSION_DIR/vmstat.log" \
      vmstat -t "$INTERVAL_SECONDS"
  else
    warn_msg "vmstat not found (install procps/procps-ng); VM/system capture reduced"
  fi
}

main() {
  parse_args "$@"

  [[ "$(uname -s)" == "Linux" ]] || die "This capture tool is intended for Linux"
  [[ -f "$DEFAULT_PATTERNS_FILE" ]] || die "Default pattern file missing: $DEFAULT_PATTERNS_FILE"

  if [[ -n "$PATTERNS_FILE" ]]; then
    [[ -f "$PATTERNS_FILE" ]] || die "Pattern file not found: $PATTERNS_FILE"
    PATTERNS_SOURCE_USED="$(cd "$(dirname "$PATTERNS_FILE")" && pwd)/$(basename "$PATTERNS_FILE")"
  else
    PATTERNS_FILE="$DEFAULT_PATTERNS_FILE"
    PATTERNS_SOURCE_USED="$DEFAULT_PATTERNS_FILE"
  fi

  PYTHON_BIN="$(find_python || true)"

  local capture_id label_suffix
  capture_id="$(date -u +%Y%m%dT%H%M%SZ)"
  label_suffix="$(sanitize_label "$SESSION_LABEL")"
  if [[ -n "$label_suffix" ]]; then
    capture_id="${capture_id}_${label_suffix}"
  fi

  SESSION_DIR="${OUT_ROOT%/}/${capture_id}"
  mkdir -p "$SESSION_DIR"
  RUN_LOG="$SESSION_DIR/capture.log"
  WARNINGS_FILE="$SESSION_DIR/warnings.txt"
  MANIFEST_FILE="$SESSION_DIR/manifest.tsv"
  COMMANDS_FILE="$SESSION_DIR/collector_processes.tsv"
  : >"$COMMANDS_FILE"

  CAPTURE_START_UTC="$(timestamp_utc)"
  CAPTURE_START_EPOCH="$(date +%s)"

  trap 'signal_handler SIGINT' INT
  trap 'signal_handler SIGTERM' TERM

  log_msg "Capture session initialized at $SESSION_DIR"
  log_msg "Duration=${DURATION_SECONDS}s Interval=${INTERVAL_SECONDS}s"

  copy_patterns_file "$PATTERNS_FILE"
  write_notes_template
  collect_command_versions
  collect_system_info
  record_process_snapshot "$SESSION_DIR/process_snapshot_start.txt"
  record_target_snapshot "$SESSION_DIR/target_processes_start.txt"
  start_collectors

  if [[ "$DURATION_SECONDS" -eq 0 ]]; then
    STOP_REASON="manual_stop"
    log_msg "Running until interrupted (Ctrl+C)..."
    while [[ "$STOP_REQUESTED" -eq 0 ]]; do
      sleep 1 || true
    done
  else
    log_msg "Capturing for ${DURATION_SECONDS} seconds..."
    local elapsed=0
    while [[ "$STOP_REQUESTED" -eq 0 && "$elapsed" -lt "$DURATION_SECONDS" ]]; do
      sleep 1 || true
      elapsed=$((elapsed + 1))
    done
    if [[ "$STOP_REQUESTED" -eq 0 ]]; then
      STOP_REASON="duration_elapsed"
    fi
  fi

  stop_collectors

  local fio_log_copied capture_end_utc capture_end_epoch duration_actual
  record_process_snapshot "$SESSION_DIR/process_snapshot_end.txt"
  record_target_snapshot "$SESSION_DIR/target_processes_end.txt"
  fio_log_copied="$(copy_fio_log)"
  capture_end_utc="$(timestamp_utc)"
  capture_end_epoch="$(date +%s)"
  duration_actual=$((capture_end_epoch - CAPTURE_START_EPOCH))

  run_summary_if_requested
  create_archive_if_requested
  write_manifest "$fio_log_copied" "$capture_end_utc" "$duration_actual"

  log_msg "Capture complete"
  log_msg "Stop reason: $STOP_REASON"
  log_msg "Manifest: $MANIFEST_FILE"
  if [[ "$SUMMARY_STATUS" == "ok" ]]; then
    log_msg "Summary: $SESSION_DIR/summary"
  fi
  if [[ "$ARCHIVE_STATUS" == "ok" ]]; then
    log_msg "Archive: $ARCHIVE_PATH"
  fi

  printf '\n'
  printf 'Capture directory: %s\n' "$SESSION_DIR"
  printf 'Manifest: %s\n' "$MANIFEST_FILE"
  printf 'Summary status: %s\n' "$SUMMARY_STATUS"
  printf 'Archive status: %s\n' "$ARCHIVE_STATUS"
  if [[ -s "$WARNINGS_FILE" ]]; then
    printf 'Warnings: %s\n' "$WARNINGS_FILE"
  fi
}

main "$@"
