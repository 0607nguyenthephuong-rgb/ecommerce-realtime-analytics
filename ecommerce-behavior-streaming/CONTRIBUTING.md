# Contributing

## Workflow

1. Select a task and its owner from `docs/PROJECT_PLAN.md`.
2. Create a focused branch such as `feature/kafka-topics` or `docs/dataset-profile`.
3. Make a bounded change and update the relevant documentation.
4. Run `python -m unittest discover -s tests -v` for changes affecting the event contract or producer.
5. Open a pull request with the purpose, changed behavior, validation evidence, and remaining limitations.
6. Ask another member to review before merging.

Use English for code comments, documentation, task descriptions, and commit messages. Keep the member names as supplied.

## Shared contracts

Coordinate changes to field names, schema versions, topic names, event identity, HDFS paths, and serving-table keys with both the producing and consuming owners. Do not change `user_session` to an undocumented alias or remove `remove_from_cart` from the accepted actions.

## Data and results

Commit small synthetic fixtures and compact result summaries. Keep source CSV files, credentials, checkpoints, service volumes, logs, and large outputs out of Git. Every benchmark summary must identify the actual data subset and configuration. Do not label configured rates as measured throughput.

## Software licensing

The team has not selected a software license for this starter. Add an agreed license before offering reuse permissions. Dataset usage terms and attribution remain separate from the project code.

