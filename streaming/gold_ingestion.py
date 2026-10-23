"""Incrementally maintain daily product metrics from the Silver Delta table."""

import argparse
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE = Path(
    os.environ.get("ECOMMERCE_LAKEHOUSE_ROOT", PROJECT_ROOT / "lakehouse")
).resolve()
SILVER_PATH = LAKEHOUSE / "silver" / "ecommerce_events_delta"
GOLD_PATH = LAKEHOUSE / "gold" / "daily_product_metrics"
CHECKPOINT_PATH = LAKEHOUSE / "checkpoints" / "gold_daily_product_metrics"


def parse_args():
    parser = argparse.ArgumentParser(description="Silver -> Gold Delta")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--wait-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.trigger_seconds < 1 or args.wait_seconds < 1:
        parser.error("trigger and wait seconds must be positive")
    return args


def create_spark():
    spark = (
        SparkSession.builder
        .appName("EcommerceGoldIngestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def wait_for_silver(spark, seconds):
    deadline = time.monotonic() + seconds
    while not (SILVER_PATH / "_delta_log").is_dir():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Silver Delta table did not appear: {SILVER_PATH}")
        time.sleep(2)

    details = spark.sql(f"DESCRIBE DETAIL delta.`{SILVER_PATH}`").first()
    if details.properties.get("delta.enableChangeDataFeed") != "true":
        spark.sql(
            f"ALTER TABLE delta.`{SILVER_PATH}` "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )


def aggregate_daily(df):
    count_type = lambda event_type: F.sum(
        F.when(F.col("event_type") == event_type, 1).otherwise(0)
    ).cast("long")

    return (
        df.groupBy("event_date", "product_id")
        .agg(
            count_type("view").alias("view_count"),
            count_type("cart").alias("cart_count"),
            count_type("remove_from_cart").alias("remove_from_cart_count"),
            count_type("purchase").alias("purchase_count"),
            F.coalesce(
                F.sum(F.when(
                    F.col("event_type") == "purchase", F.col("price")
                )),
                F.lit(0).cast(DecimalType(28, 2))
            ).cast(DecimalType(28, 2)).alias("purchase_value"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def ensure_gold_table(df):
    if (GOLD_PATH / "_delta_log").is_dir():
        return
    if GOLD_PATH.exists():
        raise RuntimeError(f"Gold path exists but is not Delta: {GOLD_PATH}")
    df.limit(0).write.format("delta").mode("errorifexists").partitionBy(
        "event_date"
    ).save(str(GOLD_PATH))


def write_batch(batch_df, batch_id):
    # Recompute every key touched by this CDF batch from the full Silver
    # snapshot. Repeating a batch sets the same totals instead of adding twice.
    affected = batch_df.select("event_date", "product_id").distinct().cache()
    try:
        key_count = affected.count()
        if not key_count:
            print(f"[Gold batch {batch_id}] No new keys", flush=True)
            return

        spark = batch_df.sparkSession
        silver = spark.read.format("delta").load(str(SILVER_PATH))
        selected = silver.join(
            F.broadcast(affected), ["event_date", "product_id"], "inner"
        )
        metrics = aggregate_daily(selected)
        ensure_gold_table(metrics)
        metrics.createOrReplaceTempView("gold_daily_batch")
        spark.sql(f"""
            MERGE INTO delta.`{GOLD_PATH}` AS target
            USING gold_daily_batch AS source
            ON target.event_date = source.event_date
               AND target.product_id = source.product_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"[Gold batch {batch_id}] Updated keys={key_count:,}", flush=True)
    finally:
        affected.unpersist()


def validate_resume_state():
    commits = CHECKPOINT_PATH / "commits"
    if commits.is_dir() and any(path.name.isdigit() for path in commits.iterdir()):
        if not (GOLD_PATH / "_delta_log").is_dir():
            raise RuntimeError(
                f"Gold checkpoint has commits but output is missing: {GOLD_PATH}"
            )


def main():
    args = parse_args()
    GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    validate_resume_state()

    spark = create_spark()
    query = None
    try:
        wait_for_silver(spark, args.wait_seconds)
        changes = (
            spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .load(str(SILVER_PATH))
            .filter(F.col("_change_type") == "insert")
        )
        query = (
            changes.writeStream.foreachBatch(write_batch)
            .outputMode("append")
            .queryName("ecommerce_gold_daily_product_metrics")
            .option("checkpointLocation", str(CHECKPOINT_PATH))
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        print(f"Gold is watching {SILVER_PATH}", flush=True)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Gold...", flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
