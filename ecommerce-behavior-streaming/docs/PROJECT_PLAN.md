# Four-week project plan

Weeks are relative to the agreed project start date. This is a proposed schedule; record calendar dates in GitHub milestones once the group confirms them.

| Week | Thái | Anh | Quyền | Quyết | Phương | Integration gate |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Profile input and freeze schema | Select versions and configure Kafka | Define streaming contracts | Design HDFS paths | Define metrics and wireframes | Everyone accepts source schema, event identity, and interfaces |
| 2 | Replay a small real-data slice | Connect all services | Implement raw and initial clean ingestion | Read stored Parquet | Display initial counts | CSV → Kafka → Spark → HDFS and one visible metric |
| 3 | Build edge-case fixtures | Run restart scenarios | Complete windows and recovery | Reconcile batch results | Complete dashboard | Correctness and recovery evidence collected |
| 4 | Prepare load manifests | Record resource configuration | Tune measured bottlenecks | Evaluate historical queries | Run benchmarks and assemble demo | Reproducible demo, report, and limitations |

## Initial GitHub tasks

| ID | Task | Owner | Depends on |
| --- | --- | --- | --- |
| DATA-01 | Profile the source CSV files | Thái | Dataset download |
| DATA-02 | Freeze replay identity and fixtures | Thái | DATA-01 |
| INFRA-01 | Choose and pin the service versions | Anh | Team environment agreement |
| INFRA-02 | Start Kafka and create topics | Anh | INFRA-01 |
| STORE-01 | Configure HDFS and path conventions | Quyết | INFRA-01 |
| STREAM-01 | Verify raw Kafka-to-HDFS ingestion | Quyền | DATA-02, INFRA-02, STORE-01 |
| STREAM-02 | Implement clean windows and serving output | Quyền | STREAM-01 |
| STORE-02 | Implement batch reconciliation | Quyết | STREAM-02 |
| DASH-01 | Connect the dashboard to serving tables | Phương | STREAM-02 |
| EVAL-01 | Run correctness, recovery, and load scenarios | Phương | STORE-02, DASH-01; support from all members |

## Definition of done

- Implementation and configuration are committed through a reviewed pull request.
- The module has documented inputs, outputs, failure behavior, and run commands.
- Relevant fixture checks pass and integration evidence is attached to the task.
- Dataset files, credentials, runtime output, and generated checkpoints are not committed.
- The owner has updated their report section and declared unresolved limitations.

