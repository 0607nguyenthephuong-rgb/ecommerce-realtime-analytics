# Team assignments

The allocation follows module ownership. Each member implements, tests, and documents their own module and participates in integration. No team leader or GitHub username is assumed.

| Member | Primary module | Main collaborators |
| --- | --- | --- |
| Thái | Dataset and producer | Anh, Quyền, Phương |
| Anh | Kafka and deployment | Thái, Quyền, Quyết |
| Quyền | Streaming transformations | Thái, Quyết, Phương |
| Quyết | HDFS and batch verification | Anh, Quyền |
| Phương | Dashboard and evaluation | Quyền, Thái, Anh |

## Thái — dataset and event replay

- Profile the five monthly files and document missing values and event distributions.
- Extend the event contract while preserving `user_session` and all four event types.
- Adapt the starter producer for controlled experiments and bounded-memory input reading.
- Prepare normal, duplicate-replay, late-event, and malformed-event fixtures with expected results.
- Record input checksums, snapshot labels, replay rates, and row ranges.

**Deliverables:** profiling report, producer, dataset manifest, fixtures, dataset report section.

**Acceptance:** reproducible source-record identity, explicit delivery failures, controlled input rate, and no silent row loss.

## Anh — Kafka and deployment

- Select compatible Kafka, Spark, Hadoop, Java, and Python versions with the team.
- Create version-pinned deployment configuration and document internal/external addresses.
- Configure topics, partition keys, retention, volumes, and service health checks.
- Integrate the producer and Spark consumer; record broker offsets and consumer backlog.
- Support restart and load experiments and document hardware limitations.

**Deliverables:** deployment files, topic setup, version manifest, runbook, infrastructure report section.

**Acceptance:** another member can start the environment using the runbook and reproduce a small end-to-end ingestion run.

## Quyền — Spark Structured Streaming

- Extend raw ingestion into a parsed, validated stream with an explicit rejection path.
- Implement bounded-state replay deduplication and document the retention boundary.
- Implement event-time windows, watermark behavior, and all four event-type counters.
- Produce clean HDFS events and agreed serving-table aggregates.
- Verify restart behavior, late arrivals, and sink retry handling; distinguish finalized and provisional windows.

**Deliverables:** streaming jobs, configuration, output schemas, correctness/recovery checks, processing report section.

**Acceptance:** agreed fixtures match expected results; raw, rejected, duplicate, late, and clean counts reconcile under the documented policy.

## Quyết — HDFS and historical analysis

- Configure HDFS with Anh and define raw, clean, aggregate, and checkpoint paths.
- Specify Parquet schemas and date partitions; track small-file growth.
- Build batch reference calculations over the same accepted event population as streaming.
- Compare counts, distinct users, product rankings, and purchase-value proxies across both paths.
- Measure historical query cost and any file-compaction improvements.

**Deliverables:** storage layout, HDFS configuration, batch jobs, comparison report, storage report section.

**Acceptance:** stored data is readable after restart and reconciliation explains every intentional exclusion or discrepancy.

## Phương — dashboard and evaluation

- Agree metric definitions and serving schemas with Quyền.
- Build a Streamlit dashboard with time filters, freshness indicators, event counts, active users, and product rankings.
- Implement session conversion views only after ordered-event and observation-boundary rules are agreed.
- Coordinate at least three achievable load levels with Thái and Anh; measure throughput, p50/p95 latency, backlog, and resource use.
- Assemble the results, slide deck, and demo; collect report sections from all owners.

**Deliverables:** dashboard, serving integration, benchmark runner/results, slides, evaluation report section.

**Acceptance:** displayed results reconcile with reference calculations and each performance result records its configuration and measurement boundary.

## Shared responsibilities

Review interfaces in week 1, integrate a small vertical slice in week 2, and review progress at least twice weekly. Every pull request should describe its changed contract and provide evidence appropriate to that change. Report writing is distributed across all five members.

