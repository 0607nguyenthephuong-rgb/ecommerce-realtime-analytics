"""Replay one source CSV as JSON lines or Kafka messages.

Run from the repository root: python -m src.producer.replay --dry-run
"""

import argparse
import json
import math
import sys
import time
from pathlib import Path

from src.common.events import read_events, utc_now

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPOSITORY_ROOT / "config" / "project.example.json"
ALLOWED_CONFIG_KEYS = {
    "csv_path", "dataset_version", "bootstrap_servers", "topic",
    "events_per_second", "max_events",
}


def replay_events(events, rate: float, producer=None, topic: str = "", output=None) -> dict:
    """Bound outstanding sends and verify every delivery acknowledgement.

    A missing producer selects JSON-lines dry-run mode. This function does not
    close a supplied producer, so the caller owns its connection lifecycle.
    """
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("rate must be finite and nonnegative")
    if producer is not None and not topic:
        raise ValueError("a topic is required for Kafka publishing")
    output = output if output is not None else sys.stdout
    started = time.monotonic()
    next_send = started
    processed = acknowledged = 0
    pending = []

    def drain():
        nonlocal acknowledged
        for future in pending:
            future.get(timeout=30)
            acknowledged += 1
        pending.clear()

    for original in events:
        if rate:
            delay = next_send - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        event = dict(original)
        event["emitted_at"] = utc_now()
        if producer is None:
            output.write(json.dumps(event, ensure_ascii=False) + "\n")
        else:
            session = event["user_session"]
            key = f'{event["user_id"]}:{session}' if session else event["user_id"]
            pending.append(producer.send(topic, key=key, value=event))
        processed += 1
        if rate:
            next_send = max(next_send + 1.0 / rate, time.monotonic())
        if len(pending) >= 256:
            drain()
    drain()
    elapsed = time.monotonic() - started
    return {
        "mode": "kafka" if producer is not None else "dry-run",
        "events_processed": processed,
        "acknowledged": acknowledged if producer is not None else None,
        "elapsed_seconds": round(elapsed, 6),
        "observed_events_per_second": round(processed / elapsed, 2) if elapsed else None,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--dataset-version")
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--topic")
    parser.add_argument("--rate", type=float, help="Target events per second; zero disables throttling")
    parser.add_argument("--max-events", type=int, help="Maximum records; zero reads the whole file")
    parser.add_argument("--dry-run", action="store_true", help="Print JSON instead of sending to Kafka")
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        if not isinstance(config, dict) or set(config) != ALLOWED_CONFIG_KEYS:
            raise ValueError("config must contain exactly the keys shown in project.example.json")
        csv_value = config["csv_path"]
        if not isinstance(csv_value, str) or not csv_value:
            raise ValueError("csv_path must be a nonempty string")
        config_path = Path(csv_value)
        args.csv = args.csv if args.csv is not None else REPOSITORY_ROOT / config_path
        args.dataset_version = args.dataset_version if args.dataset_version is not None else config["dataset_version"]
        args.bootstrap_servers = args.bootstrap_servers if args.bootstrap_servers is not None else config["bootstrap_servers"]
        args.topic = args.topic if args.topic is not None else config["topic"]
        args.rate = args.rate if args.rate is not None else config["events_per_second"]
        args.max_events = args.max_events if args.max_events is not None else config["max_events"]
        if type(args.rate) not in (int, float) or not math.isfinite(args.rate) or args.rate < 0:
            raise ValueError("rate must be finite and nonnegative")
        if type(args.max_events) is not int or args.max_events < 0:
            raise ValueError("max_events must be a nonnegative integer")
        for field in ("dataset_version", "bootstrap_servers", "topic"):
            if not isinstance(getattr(args, field), str) or not getattr(args, field).strip():
                raise ValueError(f"{field} must be a nonempty string")
        if not args.csv.is_file():
            raise ValueError(f"CSV file not found: {args.csv}")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    producer = None
    try:
        if not args.dry_run:
            try:
                from kafka import KafkaProducer
            except ImportError as exc:
                raise RuntimeError("Install requirements.txt for Kafka mode, or pass --dry-run") from exc
            producer = KafkaProducer(
                bootstrap_servers=[server.strip() for server in args.bootstrap_servers.split(",")],
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )
        events = read_events(args.csv, args.dataset_version, args.max_events)
        summary = replay_events(events, args.rate, producer=producer, topic=args.topic)
        print(json.dumps(summary), file=sys.stderr)
        return 0
    except KeyboardInterrupt:
        print("Replay interrupted; earlier records may already have been delivered.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    raise SystemExit(main())

