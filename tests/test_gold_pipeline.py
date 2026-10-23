"""Check Gold aggregation, change processing, and retry behavior."""

import json
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from streaming import gold_ingestion as gold
from streaming import silver_ingestion as silver


def bronze_row(event_id, event_type, price, user_id, offset):
    timestamp = datetime(2020, 1, 1, tzinfo=timezone.utc)
    payload = json.dumps({
        "event_id": event_id,
        "event_time": "2020-01-01T00:00:00+00:00",
        "event_type": event_type,
        "product_id": 1,
        "price": price,
        "user_id": user_id,
    })
    return ("1", payload, "gold.test", 0, offset, timestamp, 0, timestamp)


def run():
    spark = silver.create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    original = (
        silver.SILVER_PATH, silver.QUARANTINE_PATH,
        gold.SILVER_PATH, gold.GOLD_PATH,
    )
    try:
        with tempfile.TemporaryDirectory(prefix="gold-test-") as temp_dir:
            root = Path(temp_dir)
            silver.SILVER_PATH = gold.SILVER_PATH = root / "silver"
            silver.QUARANTINE_PATH = root / "quarantine"
            gold.GOLD_PATH = root / "gold"

            first = spark.createDataFrame([
                bronze_row("view-1", "view", "1.00", 10, 1),
                bronze_row("purchase-1", "purchase", "0.29", 10, 2),
            ], silver.BRONZE_SCHEMA)
            silver.write_batch(silver.parse_and_validate(first), 0)

            changes = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", 0)
                .load(str(gold.SILVER_PATH))
                .filter(F.col("_change_type") == "insert")
            )
            gold.write_batch(changes, 0)
            gold.write_batch(changes, 0)

            second = spark.createDataFrame([
                bronze_row("purchase-2", "purchase", "0.59", 11, 3),
            ], silver.BRONZE_SCHEMA)
            silver.write_batch(silver.parse_and_validate(second), 1)
            gold.write_batch(changes, 1)

            rows = spark.read.format("delta").load(str(gold.GOLD_PATH)).collect()
            assert len(rows) == 1, rows
            row = rows[0]
            assert row.view_count == 1, row
            assert row.purchase_count == 2, row
            assert row.purchase_value == Decimal("0.88"), row
            assert row.unique_users == 2, row
            print("Gold integration check passed", flush=True)
    finally:
        (silver.SILVER_PATH, silver.QUARANTINE_PATH,
         gold.SILVER_PATH, gold.GOLD_PATH) = original
        spark.stop()


if __name__ == "__main__":
    run()
