#!/bin/bash
set -euo pipefail

ANALYTICS_TOPIC="${ECOMMERCE_TOPIC:-ecommerce.events.replay}"
PAYMENTS_TOPIC="${ECOMMERCE_PAYMENTS_TOPIC:-ecommerce.payments}"
BOOTSTRAP="kafka:19092"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAKEHOUSE_ROOT="${ECOMMERCE_LAKEHOUSE_ROOT:-$PROJECT_ROOT/lakehouse}"

if [[ "$ANALYTICS_TOPIC" == "$PAYMENTS_TOPIC" ]]; then
    echo "ERROR: Analytics and payments topics must be different" >&2
    exit 1
fi

# Recreating a topic resets its offsets, but Spark checkpoints still refer to
# the previous topic. Refuse a partial reset that can skip or duplicate data.
if [[ -d "$LAKEHOUSE_ROOT" ]] && \
    [[ -n "$(find "$LAKEHOUSE_ROOT" -type f \
        \( -name metadata -o -name '*.parquet' \) -print -quit)" ]]; then
    echo "ERROR: Lakehouse data/checkpoints already exist in $LAKEHOUSE_ROOT" >&2
    echo "Stop the pipeline and archive the entire lakehouse directory before resetting Kafka." >&2
    echo "Oracle data is kept; previously processed CSV rows will remain deduplicated." >&2
    exit 1
fi

# Check Kafka container
if [ "$(docker inspect -f '{{.State.Running}}' kafka)" != "true" ]; then
    echo "Kafka is not running."
    exit 1
fi

for topic in "$ANALYTICS_TOPIC" "$PAYMENTS_TOPIC"; do
    if docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --list --bootstrap-server "$BOOTSTRAP" | grep -Fxq "$topic"; then
        echo "Deleting topic: $topic"
        docker exec kafka /opt/kafka/bin/kafka-topics.sh \
            --delete --topic "$topic" --bootstrap-server "$BOOTSTRAP"
        until ! docker exec kafka /opt/kafka/bin/kafka-topics.sh \
            --list --bootstrap-server "$BOOTSTRAP" | grep -Fxq "$topic"; do
            sleep 1
        done
    fi
    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --create --topic "$topic" --bootstrap-server "$BOOTSTRAP" \
        --partitions 3 --replication-factor 1
done

echo "Kafka topics reset complete"
