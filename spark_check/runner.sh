#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if ! command -v spark-submit >/dev/null 2>&1 && [[ ! -x /opt/spark/bin/spark-submit ]]; then
  exec docker exec ubuntu-vm bash /home/student/study/store_hse/spark_check/runner.sh "$@"
fi
DATES=("$@")
if [[ ${#DATES[@]} -eq 0 ]]; then
  DATES=(2026-03-05 2026-03-06)
fi
FAILED=()
for LOAD_DATE in "${DATES[@]}"; do
  if bash "$SCRIPT_DIR/run_check.sh" --load-date "$LOAD_DATE" --log-file "$SCRIPT_DIR/checks.log"; then
    printf 'PASSED date=%s\n' "$LOAD_DATE"
  else
    FAILED+=("$LOAD_DATE")
    printf 'FAILED date=%s\n' "$LOAD_DATE" >&2
  fi
done
if [[ ${#FAILED[@]} -gt 0 ]]; then
  printf 'Failed dates: %s\n' "${FAILED[*]}" >&2
  exit 1
fi
printf 'All runs passed: %s\n' "${DATES[*]}"
