# Dataset setup

Source: [Kaggle dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop). Credit: Michael Kechinov and [REES46](https://rees46.com/).

The source lists five monthly files: `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, and `2020-Feb.csv`. Each row is a product-related user event. Keep the publisher attribution when describing this dataset.

## Obtain and record the input

1. Download the dataset from its Kaggle page and extract it locally.
2. Place the monthly CSV files in `data/raw/`. This directory is ignored by Git.
3. Record the download date, dataset version or snapshot label, file sizes, and SHA-256 checksums in your experiment manifest.
4. Inspect headers and observed values before the first large replay.
5. Choose an immutable `dataset_version` label for that exact input snapshot.

The starter does not download or redistribute the real dataset. `data/sample/events_sample.csv` is hand-authored synthetic input.

## Profiling tasks for Thái

Measure actual row counts, event-type counts, time range, missing values, negative prices, and ordering violations for each downloaded file. Do not assume exact row counts or quality rates from this documentation. Validate the event types against the data, because the source card's column description and event-type list are inconsistent.

## Replay policy

- Preserve `event_time` as the historical UTC timestamp.
- Add a separate `emitted_at` timestamp immediately before a replay send attempt.
- Process one file per command. Replay monthly files chronologically; alphabetic filename sorting is not chronological.
- The producer preserves CSV row order; it does not sort, rewrite timestamps, or simulate historical inter-event gaps.
- A constant rate is a load-test input, not a reproduction of original customer traffic.
- For repeated independent experiments, use a fresh topic/output/checkpoint namespace. For restart tests, reuse the intended namespace and record that choice.

Identical source rows at different row positions receive different replay identities. Potential duplicates already present in the dataset require a separate, justified cleaning policy.

