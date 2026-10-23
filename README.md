# Ecommerce Realtime Analytics

A local ecommerce pipeline that replays CSV events into Oracle, processes simulated orders and payments, and streams committed results through Kafka and Spark into a lakehouse.

## Data flow

```text
CSV files
   │  replayed in event-time order
   ▼
Oracle ── orders, payment results, transactional outbox
   │
   └── outbox relay ──► Kafka (one broker)
                         ├── ecommerce.events.replay ──► Bronze ──► Silver ──► Gold
                         └── ecommerce.payments      ──► declined payments
```

The CSV producer sends events directly to Oracle. Oracle records orders and simulated payment outcomes, then stores publishable events in an outbox in the same transaction. The relay sends committed outbox events to Kafka. Successful purchases and shopping activity feed the analytics topic; declined payments use the payments topic.

Spark reads the analytics topic into **Bronze** (raw Parquet), validates and deduplicates records in **Silver** (Delta), and aggregates daily product metrics in **Gold** (Delta). Invalid records are written to a quarantine Delta table. Payment processing is a simulation; no external payment gateway is involved.

## Project structure

```text
ecommerce/
├── data/                 # Local input CSV files (not tracked by Git)
├── examples/             # Small sample CSV for a quick run
├── ingestion/            # Oracle schema and order/payment processing
├── producer/             # CSV replay and Oracle-to-Kafka outbox relay
├── streaming/            # Spark Bronze, Silver, and Gold jobs
├── scripts/              # Pipeline, producer, Oracle setup, Kafka reset
├── tests/                # Pipeline and payment checks
├── lakehouse/            # Generated tables and streaming checkpoints
├── logs/                 # Generated job logs
├── docker-compose.yml    # Local Oracle, Kafka, and PostgreSQL services
└── requirements.txt      # Python dependencies
```

The `lakehouse/` and `logs/` directories are created at runtime and are not tracked by Git.

## Run locally

Use WSL or Linux with Docker Compose, Java, Spark 3.5.8 (`spark-submit` on `PATH`), and the `ecommerce` conda environment. Install Python dependencies with `pip install -r requirements.txt`. Spark downloads its Kafka and Delta packages on the first run.

In each terminal, activate the environment and point Spark to its Python interpreter:

```bash
conda activate ecommerce
export PYSPARK_PYTHON="$CONDA_PREFIX/bin/python"
export PYSPARK_DRIVER_PYTHON="$CONDA_PREFIX/bin/python"
```

Start Oracle, Kafka, the outbox relay, and the Spark jobs in one terminal:

```bash
bash scripts/run_pipeline.sh
```

Replay the sample CSV into Oracle from another terminal:

```bash
ECOMMERCE_DATA_DIR=examples bash scripts/run_producer.sh --max-events 3 --speed 1000000
```

By default, the producer reads CSV files from `data/`. Set `ECOMMERCE_DATA_DIR` to another directory to change the source. Pipeline logs appear in `logs/`; Bronze, Silver, Gold, and quarantine tables appear under `lakehouse/`.
