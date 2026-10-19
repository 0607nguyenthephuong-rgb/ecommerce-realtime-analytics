# Architecture and module boundaries

## Design

Kafka provides the event transport. Spark owns stream parsing and analytical transformations. HDFS stores durable raw and cleaned history. A proposed PostgreSQL serving layer exposes small aggregate tables to a Streamlit dashboard. The deployment and analytical layers are planned; the supplied Spark example is raw ingestion only.

| Boundary | Producer | Consumer | Contract |
| --- | --- | --- | --- |
| CSV to normalized event | Thái | Anh, Quyền | Nine source fields plus replay metadata |
| Kafka raw topic | Anh | Quyền | JSON value; session key, or user key when session is missing |
| Raw HDFS history | Quyền | Quyết | Payload plus Kafka topic, partition, offset, and timestamps |
| Clean HDFS events | Quyền | Quyết | Parsed, accepted records with a documented deduplication policy |
| Serving aggregates | Quyền | Phương | Window keys, metric values, update time, and finalization status |
| Evaluation manifest | All owners | Phương | Input identity, configuration, run boundaries, and measurements |

## Proposed paths

- `/ecommerce/bronze/events/`: append-only raw Kafka records, partitioned by ingestion date.
- `/ecommerce/silver/events/`: validated events, partitioned by historical event date.
- `/ecommerce/gold/`: batch analytical outputs.
- `/ecommerce/checkpoints/<query-name>/`: a separate checkpoint for each streaming query.

For local experiments, use the ignored `artifacts/` directory instead of HDFS. This local mode does not demonstrate distributed storage.

## Time and identity

Use `event_time` for historical analytical windows. Use `emitted_at` and later processing/display timestamps for operational latency. Advance watermarks using observed event time, not the wall clock. When replay stops, some windows may remain provisional until the watermark advances sufficiently; make that state visible.

The producer's `event_id` identifies a source record across replays. Raw storage preserves replayed records. The clean layer must explicitly handle replay duplicates; finite-state deduplication cannot promise suppression of arbitrarily old replays.

## Failure behavior

The raw example preserves the payload without validating or deduplicating it. Kafka offsets and Spark checkpoints support recovery, but deleting a checkpoint or creating a new query against an existing output can replay data. Use a separate output and checkpoint for each independent experiment.

The future serving sink should use deterministic aggregate keys and retry-safe writes. Do not infer end-to-end exactly-once behavior from the presence of a checkpoint alone. Test failure boundaries before making reliability claims.

## Deployment ownership

Anh and Quyết will add the concrete Kafka/HDFS deployment under `infra/`. Record exact versions, memory limits, ports, volume persistence, and host/container addresses. Quyền selects a Spark Kafka connector matching both the Spark version and Scala binary version of that deployment.

