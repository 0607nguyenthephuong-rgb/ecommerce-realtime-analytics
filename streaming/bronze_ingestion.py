
import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. Project paths
# =========================================================

# streaming/bronze_ingestion.py -> ecommerce/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

LAKEHOUSE_ROOT = Path(
    os.environ.get("ECOMMERCE_LAKEHOUSE_ROOT", PROJECT_ROOT / "lakehouse")
).resolve()

BRONZE_PATH = (
    LAKEHOUSE_ROOT / "bronze" / "ecommerce_events"
)

CHECKPOINT_PATH = (
    LAKEHOUSE_ROOT / "checkpoints" / "bronze_ingestion"
)


# =========================================================
# 2. Configuration
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka -> Spark -> Bronze Parquet"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092"
    )

    parser.add_argument(
        "--topic",
        default="ecommerce.events.replay"
    )

    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="earliest"
    )

    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=10
    )

    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=50000
    )

    args = parser.parse_args()

    if args.trigger_seconds <= 0:
        parser.error("--trigger-seconds must be positive")

    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be positive")

    return args


# =========================================================
# 3. Spark Session
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("EcommerceBronzeIngestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 4. Kafka Source
# =========================================================

def read_kafka_stream(spark, args):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            args.bootstrap_servers
        )
        .option("subscribe", args.topic)
        .option(
            "startingOffsets",
            args.starting_offsets
        )
        .option(
            "maxOffsetsPerTrigger",
            args.max_offsets_per_trigger
        )
        .option("failOnDataLoss", "true")
        .load()
    )


# =========================================================
# 5. Bronze Transformation
# =========================================================

def transform_to_bronze(kafka_df):
    return kafka_df.select(
        F.col("key")
            .cast("string")
            .alias("kafka_key"),

        F.col("value")
            .cast("string")
            .alias("raw_json"),

        F.col("topic")
            .alias("kafka_topic"),

        F.col("partition")
            .alias("kafka_partition"),

        F.col("offset")
            .alias("kafka_offset"),

        F.col("timestamp")
            .alias("kafka_timestamp"),

        F.col("timestampType")
            .alias("kafka_timestamp_type"),

        F.current_timestamp()
            .alias("bronze_ingested_at")
    )


# =========================================================
# 6. Start Streaming
# =========================================================

def start_streaming(bronze_df, args):
    return (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .queryName("ecommerce_bronze_ingestion")
        .option(
            "checkpointLocation",
            str(CHECKPOINT_PATH)
        )
        .trigger(
            processingTime=f"{args.trigger_seconds} seconds"
        )
        .start(str(BRONZE_PATH))
    )


# =========================================================
# 7. Monitor Progress
# =========================================================

def monitor_streaming(query):
    last_batch_id = None

    while query.isActive:
        query.awaitTermination(10)

        progress = query.lastProgress

        if not progress:
            continue

        batch_id = progress.get("batchId")

        if batch_id == last_batch_id:
            continue

        last_batch_id = batch_id

        rows = progress.get("numInputRows", 0)
        rate = progress.get("processedRowsPerSecond", 0)

        print(
            f"[Batch {batch_id}] "
            f"Rows: {rows:,} | "
            f"Processed: {rate:,.1f} rows/s",
            flush=True
        )


# =========================================================
# 8. Main
# =========================================================

def main():
    args = parse_args()

    BRONZE_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    CHECKPOINT_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 60)
    print("ECOMMERCE BRONZE STREAMING INGESTION")
    print("=" * 60)
    print(f"Project:     {PROJECT_ROOT}")
    print(f"Kafka:       {args.bootstrap_servers}")
    print(f"Topic:       {args.topic}")
    print(f"Bronze:      {BRONZE_PATH}")
    print(f"Checkpoint:  {CHECKPOINT_PATH}")
    print(f"Trigger:     {args.trigger_seconds}s")
    print("=" * 60, flush=True)

    spark = create_spark()
    query = None

    try:
        kafka_df = read_kafka_stream(
            spark,
            args
        )

        bronze_df = transform_to_bronze(
            kafka_df
        )

        query = start_streaming(
            bronze_df,
            args
        )

        print("\nStreaming started.", flush=True)

        monitor_streaming(query)

    except KeyboardInterrupt:
        print("\nStopping streaming...", flush=True)

    finally:
        if query is not None and query.isActive:
            query.stop()

        spark.stop()


if __name__ == "__main__":
    main()
