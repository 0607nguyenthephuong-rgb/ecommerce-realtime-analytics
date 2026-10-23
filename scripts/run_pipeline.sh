#!/usr/bin/env bash
set -Eeuo pipefail

# =====================================================
# Project configuration
# =====================================================
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

BRONZE_SCRIPT="$PROJECT_ROOT/streaming/bronze_ingestion.py"
SILVER_SCRIPT="$PROJECT_ROOT/streaming/silver_ingestion.py"
GOLD_SCRIPT="$PROJECT_ROOT/streaming/gold_ingestion.py"

LOG_DIR="${ECOMMERCE_LOG_DIR:-$PROJECT_ROOT/logs}"
LAKEHOUSE_ROOT="${ECOMMERCE_LAKEHOUSE_ROOT:-$PROJECT_ROOT/lakehouse}"
TOPIC="${ECOMMERCE_TOPIC:-ecommerce.events.replay}"
PAYMENTS_TOPIC="${ECOMMERCE_PAYMENTS_TOPIC:-ecommerce.payments}"
BOOTSTRAP="${ECOMMERCE_BOOTSTRAP_SERVERS:-localhost:9092}"
BRONZE_TRIGGER_SECONDS="${ECOMMERCE_BRONZE_TRIGGER_SECONDS:-10}"
SILVER_TRIGGER_SECONDS="${ECOMMERCE_SILVER_TRIGGER_SECONDS:-20}"
GOLD_TRIGGER_SECONDS="${ECOMMERCE_GOLD_TRIGGER_SECONDS:-30}"
SPARK_MASTER="${ECOMMERCE_SPARK_MASTER:-local[1]}"
SPARK_DRIVER_MEMORY="${ECOMMERCE_SPARK_DRIVER_MEMORY:-1g}"

# =====================================================
# Validate environment
# =====================================================

for filename in "$BRONZE_SCRIPT" "$SILVER_SCRIPT" "$GOLD_SCRIPT"; do
    if [[ ! -f "$filename" ]]; then
        echo "ERROR: Missing file: $filename"
        exit 1
    fi
done

if ! command -v spark-submit >/dev/null; then
    echo "ERROR: spark-submit not found"
    exit 1
fi

if [[ "$TOPIC" == "$PAYMENTS_TOPIC" ]]; then
    echo "ERROR: Analytics and payments topics must be different" >&2
    exit 1
fi

docker compose up -d kafka
bash scripts/init_oracle_payments.sh

for topic in "$TOPIC" "$PAYMENTS_TOPIC"; do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19092 --create --if-not-exists \
        --topic "$topic" --partitions 3 --replication-factor 1
done

mkdir -p \
    "$LOG_DIR" \
    "$LAKEHOUSE_ROOT/bronze/ecommerce_events"

BRONZE_PID=""
SILVER_PID=""
GOLD_PID=""
ORACLE_OUTBOX_PID=""

# =====================================================
# Graceful shutdown
# =====================================================

stop_jobs() {
    trap - EXIT INT TERM

    echo
    echo "Stopping Oracle outbox, Bronze, Silver and Gold..."

    for pid in "$BRONZE_PID" "$SILVER_PID" "$GOLD_PID" "$ORACLE_OUTBOX_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done

    for pid in "$BRONZE_PID" "$SILVER_PID" "$GOLD_PID" "$ORACLE_OUTBOX_PID"; do
        if [[ -n "$pid" ]]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
}

trap stop_jobs EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# =====================================================
# Job 1: Kafka -> Bronze
# =====================================================

echo "Starting Bronze Streaming..."

spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    "$BRONZE_SCRIPT" \
    --bootstrap-servers "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --trigger-seconds "$BRONZE_TRIGGER_SECONDS" \
    --max-offsets-per-trigger 50000 \
    > "$LOG_DIR/bronze.log" 2>&1 &

BRONZE_PID=$!

# =====================================================
# Job 2: Bronze -> Silver
# =====================================================

echo "Starting Silver Streaming..."

spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --packages io.delta:delta-spark_2.12:3.3.2 \
    "$SILVER_SCRIPT" \
    --trigger-seconds "$SILVER_TRIGGER_SECONDS" \
    --max-files-per-trigger 24 \
    > "$LOG_DIR/silver.log" 2>&1 &

SILVER_PID=$!

# =====================================================
# Job 3: Silver Delta -> Gold Delta
# =====================================================

echo "Starting Gold Streaming..."

spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --packages io.delta:delta-spark_2.12:3.3.2 \
    "$GOLD_SCRIPT" \
    --trigger-seconds "$GOLD_TRIGGER_SECONDS" \
    > "$LOG_DIR/gold.log" 2>&1 &

GOLD_PID=$!

# =====================================================
# Committed Oracle outbox relay
# =====================================================

echo "Starting Oracle Outbox Relay..."
python -m producer.oracle_producer \
    --bootstrap-servers "$BOOTSTRAP" \
    > "$LOG_DIR/oracle_outbox.log" 2>&1 &
ORACLE_OUTBOX_PID=$!

echo
echo "========================================="
echo "ECOMMERCE STREAMING PIPELINE RUNNING"
echo "========================================="
echo "Bronze PID: $BRONZE_PID"
echo "Silver PID: $SILVER_PID"
echo "Gold PID: $GOLD_PID"
echo "Oracle Outbox PID: $ORACLE_OUTBOX_PID"
echo
echo "Bronze log: $LOG_DIR/bronze.log"
echo "Silver log: $LOG_DIR/silver.log"
echo "Gold log: $LOG_DIR/gold.log"
echo "Oracle outbox log: $LOG_DIR/oracle_outbox.log"
echo
echo "Press Ctrl+C to stop all jobs."
echo "========================================="

# If any job exits, stop the remaining jobs.
set +e
wait -n "$BRONZE_PID" "$SILVER_PID" "$GOLD_PID" "$ORACLE_OUTBOX_PID"
STATUS=$?
set -e

echo "A pipeline process exited (status $STATUS)."
exit "$STATUS"
