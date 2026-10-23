"""Replay chronological CSV events directly into Oracle transactions."""

import argparse
import csv
import hashlib
import heapq
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path

from ingestion.oracle_payments import connect_app, process_event


def parse_event_time(value):
    return datetime.strptime(
        value.strip(), "%Y-%m-%d %H:%M:%S UTC"
    ).replace(tzinfo=timezone.utc)


def discover_csv_files(data_dir, pattern):
    files = sorted(path for path in data_dir.glob(pattern) if path.is_file())
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    for path in files:
        print(f"CSV: {path.relative_to(data_dir)}", flush=True)
    return files


def make_event_id(file_path, line_number, data_dir):
    source = f"{file_path.relative_to(data_dir).as_posix()}:{line_number}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def merge_csv_files(files):
    """Merge files sorted by event_time, using memory proportional to file count."""
    with ExitStack() as stack:
        readers = []
        last_times = [None] * len(files)
        heap = []

        def read_next(index):
            reader = readers[index]
            try:
                row = next(reader)
            except StopIteration:
                return
            event_time = parse_event_time(row["event_time"])
            if last_times[index] is not None and event_time < last_times[index]:
                raise ValueError(
                    f"CSV is not sorted by event_time: {files[index]} "
                    f"at line {reader.line_num}"
                )
            last_times[index] = event_time
            heapq.heappush(heap, (event_time, index, reader.line_num, row))

        for file_path in files:
            source = stack.enter_context(open(
                file_path, "r", encoding="utf-8-sig", newline=""
            ))
            readers.append(csv.DictReader(source))

        for index in range(len(files)):
            read_next(index)

        while heap:
            event_time, index, line_number, row = heapq.heappop(heap)
            yield files[index], line_number, row, event_time
            read_next(index)


def build_event(row, event_time, event_id):
    event = {
        "event_id": event_id,
        "event_time": event_time.isoformat(),
        "event_type": row["event_type"],
        "product_id": int(row["product_id"]),
        "category_id": row.get("category_id"),
        "category_code": row.get("category_code"),
        "brand": row.get("brand"),
        "price": row["price"],
        "user_id": int(row["user_id"]),
        "user_session": row.get("user_session"),
        "ingestion_time": time.time(),
    }
    if row.get("quantity") not in (None, ""):
        event["quantity"] = int(row["quantity"])
    if row.get("payment_outcome") not in (None, ""):
        event["payment_outcome"] = row["payment_outcome"]
    return event


def replay(files, data_dir, analytics_topic, payments_topic, speed,
           max_events, log_every):
    counts = {"activity": 0, "paid": 0, "declined": 0, "duplicate": 0}
    first_event_time = None
    replay_start = None
    scanned = 0

    with connect_app() as connection:
        for file_path, line_number, row, event_time in merge_csv_files(files):
            if max_events and scanned >= max_events:
                break
            if first_event_time is None:
                first_event_time = event_time
                replay_start = time.monotonic()
            target_time = replay_start + (
                event_time - first_event_time
            ).total_seconds() / speed
            remaining = target_time - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

            event_id = make_event_id(file_path, line_number, data_dir)
            try:
                event = build_event(row, event_time, event_id)
                result = process_event(
                    connection, event, analytics_topic, payments_topic
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Could not process {file_path}:{line_number} "
                    f"(event_id={event_id})"
                ) from exc
            counts[result] += 1
            scanned += 1
            if scanned % log_every == 0:
                print(f"Oracle processed={scanned:,} results={counts}", flush=True)

    print(f"Oracle replay complete: scanned={scanned:,} results={counts}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Stream CSV events into Oracle")
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--pattern", default="*.csv")
    parser.add_argument("--analytics-topic", default="ecommerce.events.replay")
    parser.add_argument("--payments-topic", default="ecommerce.payments")
    parser.add_argument("--speed", type=float, default=600)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--log-every", type=int, default=10000)
    args = parser.parse_args()
    if args.speed <= 0 or args.max_events < 0 or args.log_every <= 0:
        parser.error("speed/log-every must be positive; max-events must be >= 0")
    if args.analytics_topic == args.payments_topic:
        parser.error("analytics and payments topics must differ")
    data_dir = args.data_dir.resolve()
    if not data_dir.is_dir():
        parser.error(f"CSV directory not found: {data_dir}")
    files = discover_csv_files(data_dir, args.pattern)
    print(f"Streaming CSV -> Oracle at {args.speed}x", flush=True)
    replay(files, data_dir, args.analytics_topic, args.payments_topic,
           args.speed, args.max_events, args.log_every)


if __name__ == "__main__":
    main()
