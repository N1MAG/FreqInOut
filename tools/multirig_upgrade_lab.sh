#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${REPO_ROOT}/.venv/bin/python"
if [[ ! -x "${PYTHON}" ]]; then
  PYTHON="python3"
fi
LAB="${REPO_ROOT}/tools/multirig_test_lab.py"

usage() {
  cat <<'EOF'
Usage:
  tools/multirig_upgrade_lab.sh copy
  tools/multirig_upgrade_lab.sh start-single
  tools/multirig_upgrade_lab.sh check
  tools/multirig_upgrade_lab.sh run
  tools/multirig_upgrade_lab.sh start-multi
  tools/multirig_upgrade_lab.sh seed-extra
  tools/multirig_upgrade_lab.sh stop
  tools/multirig_upgrade_lab.sh status

Typical flow:
  copy -> start-single -> check -> run
  complete Multi-Rig Setup in FIO
  start-multi -> seed-extra -> run
EOF
}

case "${1:-}" in
  copy)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" copy-production --reset
    ;;
  start-single)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" lab start --mode single
    ;;
  check)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" check prod-upgrade
    ;;
  run)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" run prod-upgrade
    ;;
  start-multi)
    cd "${REPO_ROOT}"
    "${PYTHON}" "${LAB}" lab stop || true
    exec "${PYTHON}" "${LAB}" lab start --mode multi
    ;;
  seed-extra)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" seed-extra-radios prod-upgrade
    ;;
  stop)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" lab stop
    ;;
  status)
    cd "${REPO_ROOT}"
    exec "${PYTHON}" "${LAB}" lab status
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
