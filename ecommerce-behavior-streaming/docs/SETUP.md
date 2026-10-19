# Setup and running the starter

## Local validation

Use Python 3.10+ and run commands from the repository root. The standard-library checks and dry-run do not require installing dependencies.

```bash
python -m unittest discover -s tests -v
python -m src.producer.replay --dry-run --max-events 8 --rate 0
```

The example configuration points to `data/sample/events_sample.csv`, a synthetic fixture. Relative CSV paths in a JSON config are resolved from the repository root, not from the config directory. CLI paths are resolved from the current working directory.

## Optional virtual environment and Kafka client

```bash
python -m venv .venv
```

Activate it using `source .venv/bin/activate` on Linux/macOS or `.venv\Scripts\Activate.ps1` in Windows PowerShell. Then install the publishing dependency:

```bash
python -m pip install -r requirements.txt
```

Start the team-provided Kafka deployment and create `ecommerce.events.raw` before publishing. The starter does not provision services. Set `--bootstrap-servers` to an address reachable from the producer process.

```bash
python -m src.producer.replay --config config/project.example.json
```

## Replay a real input slice

After downloading the source CSV, run:

```bash
python -m src.producer.replay --csv data/raw/2019-Oct.csv --dataset-version cosmetics-download-v1 --dry-run --max-events 10 --rate 0
python -m src.producer.replay --csv data/raw/2019-Oct.csv --dataset-version cosmetics-download-v1 --bootstrap-servers localhost:9092 --topic ecommerce.events.raw --rate 100 --max-events 10000
```

Replace the snapshot label with the label recorded in your input manifest. `--max-events` defaults to 1000; use zero to read the full file. `--rate 0` disables throttling. Requested rate is a ceiling target; delivery and validation overhead may reduce actual throughput. Invalid input stops the run with a nonzero exit code. Valid records sent before the failure remain in Kafka.

The producer keeps at most 256 outstanding acknowledgements. It checks each delivery future and fails on delivery errors. It is an educational replay client, not a production load generator; there is no producer-side resume checkpoint, so use event identity and the downstream policy when replaying after interruption.

## Raw Spark ingestion example

Install and configure the Spark version selected by the team, including its Java runtime. The Kafka connector version must match Spark, and its Scala suffix must match the Spark distribution. Use your actual versions in this Bash example:

```bash
export SPARK_VERSION='YOUR_INSTALLED_SPARK_VERSION'
export SCALA_BINARY_VERSION='YOUR_SPARK_SCALA_BINARY_VERSION'
spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" src/streaming/kafka_to_bronze.py --bootstrap-servers localhost:9092 --topic ecommerce.events.raw --output artifacts/bronze --checkpoint artifacts/checkpoints/bronze
```

Replace both version placeholders before executing. For HDFS, replace output and checkpoint with valid `hdfs://<namenode>:<port>/...` URIs accessible to the Spark driver and executors. Hadoop client configuration and permissions must also be available. Do not interpret a successful local-filesystem run as an HDFS integration test.

The example stores raw JSON text with Kafka metadata. It intentionally performs no cleaning, deduplication, business aggregation, or dashboard update. `startingOffsets=earliest` applies only when starting without an existing checkpoint.

## What still needs integration testing

The local suite checks Python contracts and replay behavior with synthetic inputs and a fake Kafka client. It does not verify a live Kafka broker, Spark runtime, HDFS cluster, serving store, or dashboard. Add those checks when the deployment exists.

