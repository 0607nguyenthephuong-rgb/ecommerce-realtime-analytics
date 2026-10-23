#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
docker compose up -d oracle

for attempt in {1..60}; do
    if [[ "$(docker inspect -f '{{.State.Health.Status}}' ecommerce-oracle)" == "healthy" ]]; then
        exec python -c 'from ingestion.oracle_payments import initialize_schema; initialize_schema()'
    fi
    sleep 5
done

echo "ERROR: Oracle did not become healthy within 5 minutes" >&2
exit 1
