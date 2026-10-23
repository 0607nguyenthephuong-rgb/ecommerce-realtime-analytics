#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Do not clear Oracle while local writers or streaming jobs are running.
running_jobs="$(pgrep -af 'producer[.]oracle_producer|producer[.]producer|producer/(oracle_)?producer[.]py|streaming/(bronze|silver|gold)_ingestion[.]py' || true)"
if [[ -n "$running_jobs" ]]; then
    echo "ERROR: Stop the producer and pipeline before resetting Oracle:" >&2
    echo "$running_jobs" >&2
    exit 1
fi

python - <<'PY'
from ingestion.oracle_payments import connect_app

# Delete children before parents to satisfy the payment table foreign keys.
tables = (
    "PAYMENT_OUTBOX",
    "PAYMENT_TRANSACTIONS",
    "PAYMENT_ORDERS",
    "PAYMENT_CATALOG",
    "PAYMENT_INGESTED_EVENTS",
)

with connect_app() as connection:
    try:
        with connection.cursor() as cursor:
            deleted = []
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
                deleted.append((table, cursor.rowcount))
        connection.commit()
    except Exception:
        connection.rollback()
        raise

for table, count in deleted:
    print(f"{table}: deleted {count:,} rows")
print("Oracle payment data reset complete; schema and other databases are unchanged.")
PY
