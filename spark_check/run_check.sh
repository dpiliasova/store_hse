#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOAD_DATE=""
LOG_FILE="$SCRIPT_DIR/checks.log"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --load-date) LOAD_DATE="${2:?Missing load date}"; shift 2 ;;
    --log-file) LOG_FILE="${2:?Missing log file}"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
python3 -c 'import sys; from datetime import date; date.fromisoformat(sys.argv[1])' "$LOAD_DATE"
export PATH="/opt/spark/bin:$PATH"
RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
STATE_FILE="$SCRIPT_DIR/.state/before-$RUN_ID.json"
mkdir -p "$SCRIPT_DIR/.state" "$(dirname -- "$LOG_FILE")"

on_error() {
  local code="$?"
  python3 - "$LOG_FILE" "$LOAD_DATE" "$RUN_ID" "$code" <<'PYLOG'
import json,sys
from datetime import datetime,timezone
with open(sys.argv[1], 'a', encoding='utf-8') as stream:
    stream.write(json.dumps({'ts':datetime.now(timezone.utc).isoformat(),
        'load_date':sys.argv[2], 'run_id':sys.argv[3], 'check':'pipeline',
        'status':'ERROR', 'message':'Pipeline exited with code '+sys.argv[4]})+'\n')
PYLOG
  exit "$code"
}
trap on_error ERR

check() {
  spark-submit \
    --master spark://spark-master:7077 \
    --name "check-$1" \
    --deploy-mode client \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.driver.host="${SPARK_DRIVER_HOST:-ubuntu-vm}" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --py-files "$PROJECT_ROOT/spark/common.py" \
    "$SCRIPT_DIR/check.py" --check "$1" --load-date "$LOAD_DATE" \
    --run-id "$RUN_ID" --log-file "$LOG_FILE" --state-file "$STATE_FILE"
}

printf 'START run_id=%s load_date=%s\n' "$RUN_ID" "$LOAD_DATE"
check snapshot
for name in customers products product_categories order_items; do
  bash "$PROJECT_ROOT/spark/raw/$name/$name.sh"
done
for name in orders payments events; do
  bash "$PROJECT_ROOT/spark/raw/$name/$name.sh" --load-date "$LOAD_DATE"
done
for name in stg_orders stg_payments; do
  bash "$PROJECT_ROOT/spark/staging/$name/$name.sh" --load-date "$LOAD_DATE"
done
bash "$PROJECT_ROOT/spark/intermediate/int_orders_with_payments/int_orders_with_payments.sh" --load-date "$LOAD_DATE"
bash "$PROJECT_ROOT/spark/marts/fct_orders/fct_orders.sh" --load-date "$LOAD_DATE"
check all
rm -f -- "$STATE_FILE"
printf 'PASSED run_id=%s load_date=%s\n' "$RUN_ID" "$LOAD_DATE"
