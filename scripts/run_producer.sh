#!/usr/bin/env bash
set -euo pipefail

# Project root (the parent of scripts/).
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Configuration
DATA_DIR="${ECOMMERCE_DATA_DIR:-$PROJECT_ROOT/data}"
ANALYTICS_TOPIC="${ECOMMERCE_TOPIC:-ecommerce.events.replay}"
PAYMENTS_TOPIC="${ECOMMERCE_PAYMENTS_TOPIC:-ecommerce.payments}"
SPEED="${ECOMMERCE_REPLAY_SPEED:-600}"

# Check CSV directory
if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: CSV directory not found: $DATA_DIR"
    exit 1
fi

echo "========================================"
echo "ECOMMERCE CSV -> ORACLE STREAM"
echo "========================================"
echo "Data directory: $DATA_DIR"
echo "Oracle:         ${ORACLE_DSN:-localhost:1521/FREEPDB1}"
echo "Analytics topic: $ANALYTICS_TOPIC"
echo "Replay speed:   ${SPEED}x"
echo "========================================"

exec python -m producer.producer \
    --data-dir "$DATA_DIR" \
    --analytics-topic "$ANALYTICS_TOPIC" \
    --payments-topic "$PAYMENTS_TOPIC" \
    --speed "$SPEED" \
    "$@"
