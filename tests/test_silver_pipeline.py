"""Small end-to-end check for Silver parsing and retry-safe writes.

Run with spark-submit and the Delta package listed in README.md.
"""

import json
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from streaming import silver_ingestion as silver


def bronze_row(payload, offset):
    timestamp = datetime(2020, 1, 1, tzinfo=timezone.utc)
    return (
        "42",
        payload,
        "ecommerce.events.replay",
        0,
        offset,
        timestamp,
        0,
        timestamp,
    )


def event(event_id, price):
    return json.dumps({
        "event_id": event_id,
        "event_time": "2020-01-01T00:00:00+00:00",
        "event_type": "purchase",
        "product_id": 1,
        "price": price,
        "user_id": 42,
    })


def main():
    spark = silver.create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    old_silver_path = silver.SILVER_PATH
    old_quarantine_path = silver.QUARANTINE_PATH

    try:
        with tempfile.TemporaryDirectory(prefix="silver-test-") as temp_dir:
            root = Path(temp_dir)
            silver.SILVER_PATH = root / "silver"
            silver.QUARANTINE_PATH = root / "quarantine"

            source = spark.createDataFrame([
                bronze_row(event("a", "0.29"), 1),
                bronze_row(event("a", "0.29"), 2),
                bronze_row(event("b", 0.59), 3),
                bronze_row("{invalid json", 4),
            ], silver.BRONZE_SCHEMA)

            validated = silver.parse_and_validate(source)
            silver.write_batch(validated, 0)
            silver.write_batch(validated, 0)  # checkpoint retry / replay

            valid = spark.read.format("delta").load(str(silver.SILVER_PATH))
            invalid = spark.read.format("delta").load(
                str(silver.QUARANTINE_PATH)
            )

            prices = {
                row.event_id: row.price
                for row in valid.select("event_id", "price").collect()
            }
            assert prices == {"a": Decimal("0.29"), "b": Decimal("0.59")}
            assert valid.count() == 2
            assert invalid.count() == 1
            assert "INVALID_JSON" in invalid.select(
                F.col("validation_errors")
            ).first().validation_errors

            print("Silver integration check passed", flush=True)
    finally:
        silver.SILVER_PATH = old_silver_path
        silver.QUARANTINE_PATH = old_quarantine_path
        spark.stop()


if __name__ == "__main__":
    main()
