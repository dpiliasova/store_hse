#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SPARK_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export PATH="/opt/spark/bin:$PATH"

exec spark-submit \
  --master spark://spark-master:7077 \
  --name orders \
  --deploy-mode client \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.driver.host="${SPARK_DRIVER_HOST:-ubuntu-vm}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --py-files "$SPARK_ROOT/common.py" \
  "$SCRIPT_DIR/orders.py" "$@"
