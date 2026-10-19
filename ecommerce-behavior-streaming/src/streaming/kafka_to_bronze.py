"""Raw ingestion example: Kafka payload and metadata to Parquet.

Requires a configured Spark runtime and a matching Spark Kafka connector.
Cleaning, business deduplication, analytical windows, and serving writes are
separate team tasks; this job intentionally preserves the raw payload.
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    args = parser.parse_args()
    if args.output.rstrip("/") == args.checkpoint.rstrip("/"):
        parser.error("output and checkpoint must be different directories")
    if args.max_offsets_per_trigger < 1:
        parser.error("max-offsets-per-trigger must be positive")

    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.appName("ecommerce-raw-ingestion").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    query = None
    try:
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        raw = source.select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_json"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        ).withColumn("ingestion_date", F.to_date("ingested_at"))
        query = (
            raw.writeStream.format("parquet")
            .outputMode("append")
            .option("path", args.output)
            .option("checkpointLocation", args.checkpoint)
            .partitionBy("ingestion_date")
            .trigger(processingTime="10 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

