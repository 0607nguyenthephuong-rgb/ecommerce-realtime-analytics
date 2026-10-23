
#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType,
)


# =========================================================
# 1. Project paths
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE = Path(
    os.environ.get("ECOMMERCE_LAKEHOUSE_ROOT", PROJECT_ROOT / "lakehouse")
).resolve()

BRONZE_PATH = LAKEHOUSE / "bronze" / "ecommerce_events"
SILVER_PATH = LAKEHOUSE / "silver" / "ecommerce_events_delta"
QUARANTINE_PATH = LAKEHOUSE / "quarantine" / "ecommerce_events_delta"

# A new checkpoint is required because the previous two Parquet queries had
# different plans and independent offsets. The old data remains untouched.
SILVER_CHECKPOINT = LAKEHOUSE / "checkpoints" / "silver_delta"


# =========================================================
# 2. Bronze and Producer schemas
# =========================================================

BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("kafka_timestamp_type", IntegerType(), True),
    StructField("bronze_ingested_at", TimestampType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    # Spark also coerces legacy JSON numbers to strings before Decimal casting.
    StructField("price", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
    StructField("ingestion_time", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True),
])


# =========================================================
# 3. Configuration
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Online Bronze -> Silver Streaming"
    )

    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=20
    )

    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=24
    )

    args = parser.parse_args()

    if args.trigger_seconds < 1:
        parser.error("--trigger-seconds must be >= 1")

    if args.max_files_per_trigger < 1:
        parser.error("--max-files-per-trigger must be >= 1")

    return args


# =========================================================
# 4. Spark Session
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("EcommerceSilverIngestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================================================
# 5. Normalization
# =========================================================

def clean_optional_string(column):
    value = F.trim(column)
    return F.when(F.length(value) > 0, value)


def parse_and_validate(bronze_df):
    df = bronze_df.withColumn(
        "payload",
        F.from_json(
            F.col("raw_json"),
            EVENT_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
        )
    )

    # Producer mới: 2019-10-01T00:00:07+00:00
    # Format cũ:     2019-10-01 00:00:07 UTC
    raw_time = F.col("payload.event_time")

    parsed_time = F.coalesce(
        F.try_to_timestamp(
            raw_time,
            F.lit("yyyy-MM-dd'T'HH:mm:ssXXX")
        ),
        F.try_to_timestamp(
            raw_time,
            F.lit("yyyy-MM-dd HH:mm:ss 'UTC'")
        )
    )

    df = (
        df
        .withColumn(
            "event_id",
            clean_optional_string(F.col("payload.event_id"))
        )
        .withColumn("event_time", parsed_time)
        .withColumn(
            "event_type",
            F.lower(F.trim(F.col("payload.event_type")))
        )
        .withColumn("product_id", F.col("payload.product_id"))
        .withColumn(
            "category_id",
            clean_optional_string(F.col("payload.category_id"))
        )
        .withColumn(
            "category_code",
            clean_optional_string(F.col("payload.category_code"))
        )
        .withColumn(
            "brand",
            clean_optional_string(F.col("payload.brand"))
        )
        .withColumn(
            "price",
            F.col("payload.price").cast(DecimalType(18, 2))
        )
        .withColumn("user_id", F.col("payload.user_id"))
        .withColumn(
            "user_session",
            clean_optional_string(F.col("payload.user_session"))
        )
        .withColumn(
            "producer_ingestion_unix",
            F.col("payload.ingestion_time")
        )
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "event_month",
            F.date_format("event_time", "yyyy-MM")
        )
        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )

    allowed_types = [
        "view",
        "cart",
        "remove_from_cart",
        "purchase"
    ]

    errors = F.array_compact(
        F.array(
            F.when(
                F.col("payload").isNull()
                | F.col("payload._corrupt_record").isNotNull(),
                F.lit("INVALID_JSON")
            ),
            F.when(
                F.col("event_id").isNull(),
                F.lit("MISSING_EVENT_ID")
            ),
            F.when(
                F.col("event_time").isNull(),
                F.lit("INVALID_EVENT_TIME")
            ),
            F.when(
                ~F.coalesce(
                    F.col("event_type").isin(allowed_types),
                    F.lit(False)
                ),
                F.lit("INVALID_EVENT_TYPE")
            ),
            F.when(
                F.col("product_id").isNull()
                | (F.col("product_id") <= 0),
                F.lit("INVALID_PRODUCT_ID")
            ),
            F.when(
                F.col("user_id").isNull()
                | (F.col("user_id") <= 0),
                F.lit("INVALID_USER_ID")
            ),
            F.when(
                F.col("price").isNull()
                | (F.col("price") < 0),
                F.lit("INVALID_PRICE")
            )
        )
    )

    return (
        df
        .withColumn("validation_errors", errors)
        .withColumn(
            "is_valid",
            F.size("validation_errors") == 0
        )
    )


# =========================================================
# 6. Valid Silver records
# =========================================================

def valid_events(df):
    return (
        df
        .filter(F.col("is_valid"))
        .select(
            "event_id",
            "event_time",
            "event_date",
            "event_month",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
            "producer_ingestion_unix",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingested_at",
            "silver_processed_at"
        )
    )


# =========================================================
# 7. Invalid records -> Quarantine
# =========================================================

def invalid_events(df):
    return (
        df
        .filter(~F.col("is_valid"))
        .select(
            "raw_json",
            "event_id",
            "validation_errors",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingested_at",
            "silver_processed_at"
        )
    )


# =========================================================
# 8. Idempotent output for both destinations
# =========================================================

def ensure_delta_table(df, path, partition_by=None):
    if (path / "_delta_log").exists():
        return

    if path.exists():
        raise RuntimeError(f"Output path exists but is not a Delta table: {path}")

    writer = df.limit(0).write.format("delta").mode("errorifexists")
    if path == SILVER_PATH:
        writer = writer.option("delta.enableChangeDataFeed", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def write_batch(batch_df, batch_id):
    # The same Bronze batch feeds both outputs. If either MERGE fails, Spark
    # retries this batch; matching keys make both writes safe to repeat.
    validated = batch_df.cache()
    try:
        valid = valid_events(validated).dropDuplicates(["event_id"])
        invalid = invalid_events(validated).dropDuplicates(
            ["kafka_topic", "kafka_partition", "kafka_offset"]
        )

        ensure_delta_table(valid, SILVER_PATH, ["event_month"])
        ensure_delta_table(invalid, QUARANTINE_PATH)

        valid_count = valid.count()
        invalid_count = invalid.count()
        spark = batch_df.sparkSession

        if valid_count:
            valid.createOrReplaceTempView("silver_valid_batch")
            spark.sql(f"""
                MERGE INTO delta.`{SILVER_PATH}` AS target
                USING silver_valid_batch AS source
                ON target.event_id = source.event_id
                WHEN NOT MATCHED THEN INSERT *
            """)

        if invalid_count:
            invalid.createOrReplaceTempView("silver_invalid_batch")
            spark.sql(f"""
                MERGE INTO delta.`{QUARANTINE_PATH}` AS target
                USING silver_invalid_batch AS source
                ON target.kafka_topic <=> source.kafka_topic
                   AND target.kafka_partition <=> source.kafka_partition
                   AND target.kafka_offset <=> source.kafka_offset
                WHEN NOT MATCHED THEN INSERT *
            """)

        print(
            f"[Batch {batch_id}] Valid={valid_count:,} | "
            f"Quarantine={invalid_count:,}",
            flush=True
        )
    finally:
        validated.unpersist()


def start_streaming(validated_df, trigger_seconds):
    return (
        validated_df.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .queryName("ecommerce_silver_delta")
        .option("checkpointLocation", str(SILVER_CHECKPOINT))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def validate_resume_state():
    commits = SILVER_CHECKPOINT / "commits"
    if not commits.is_dir() or not any(
        path.name.isdigit() for path in commits.iterdir()
    ):
        return

    for path in (SILVER_PATH, QUARANTINE_PATH):
        if not (path / "_delta_log").is_dir():
            raise RuntimeError(
                f"Checkpoint has committed batches but output is missing: {path}"
            )


# =========================================================
# 9. Monitor streaming
# =========================================================

def monitor_stream(query):
    while query.isActive:
        query.awaitTermination(5)

    if query.exception() is not None:
        raise RuntimeError(f"Silver stopped: {query.exception()}")


# =========================================================
# 10. Main
# =========================================================

def main():
    args = parse_args()

    for path in [
        BRONZE_PATH,
        SILVER_PATH.parent,
        QUARANTINE_PATH.parent,
        SILVER_CHECKPOINT.parent
    ]:
        path.mkdir(parents=True, exist_ok=True)

    validate_resume_state()

    print("=" * 60)
    print("ECOMMERCE SILVER STREAMING")
    print("=" * 60)
    print(f"Bronze:      {BRONZE_PATH}")
    print(f"Silver:      {SILVER_PATH}")
    print(f"Quarantine:  {QUARANTINE_PATH}")
    print(f"Checkpoint:  {SILVER_CHECKPOINT}")
    print("=" * 60, flush=True)

    spark = create_spark()
    query = None

    try:
        bronze_stream = (
            spark.readStream
            .format("parquet")
            .schema(BRONZE_SCHEMA)
            .option(
                "maxFilesPerTrigger",
                args.max_files_per_trigger
            )
            .load(str(BRONZE_PATH))
        )

        validated = parse_and_validate(bronze_stream)

        query = start_streaming(validated, args.trigger_seconds)

        print(
            "Silver is online and watching Bronze files.",
            flush=True
        )

        monitor_stream(query)

    except KeyboardInterrupt:
        print("Stopping Silver...", flush=True)

    finally:
        if query is not None and query.isActive:
            query.stop()

        spark.stop()


if __name__ == "__main__":
    main()
