"""Validate source records and attach deterministic replay metadata.

No external packages are required. Source IDs are kept as strings and prices
as decimal strings to avoid accidental precision loss in downstream clients.
"""

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

SOURCE_COLUMNS = (
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
)
EVENT_TYPES = frozenset({"view", "cart", "remove_from_cart", "purchase"})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def optional_text(value: str) -> str | None:
    return value.strip() or None


def identifier(value: str, name: str, optional: bool = False) -> str | None:
    value = value.strip()
    if not value and optional:
        return None
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError(f"{name} must be a nonempty digit string")
    return value


def normalize_event(
    row: dict, source_file: str, row_number: int, dataset_version: str,
) -> dict:
    if not isinstance(dataset_version, str) or not dataset_version.strip():
        raise ValueError("dataset_version must be a nonempty snapshot label")
    if not isinstance(source_file, str) or not source_file.strip():
        raise ValueError("source_file must be nonempty")
    if type(row_number) is not int or row_number < 1:
        raise ValueError("row_number must be a positive integer")
    if set(row) != set(SOURCE_COLUMNS) or any(not isinstance(row[k], str) for k in SOURCE_COLUMNS):
        raise ValueError("record must contain exactly the nine source fields as strings")
    event_type = row["event_type"].strip()
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type!r}")
    try:
        event_time = datetime.strptime(row["event_time"].strip(), "%Y-%m-%d %H:%M:%S UTC")
    except ValueError as exc:
        raise ValueError("event_time must use YYYY-MM-DD HH:MM:SS UTC") from exc
    try:
        price = Decimal(row["price"].strip())
    except InvalidOperation as exc:
        raise ValueError("price must be a finite decimal") from exc
    if not price.is_finite():
        raise ValueError("price must be a finite decimal")

    # Including the source position preserves legitimate equal-valued events.
    # emitted_at is deliberately excluded, so replay preserves source identity.
    identity = [dataset_version, source_file, row_number, [row[k] for k in SOURCE_COLUMNS]]
    event_id = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": "1.0",
        "dataset_version": dataset_version,
        "event_id": event_id,
        "source_file": source_file,
        "source_row_number": row_number,
        "event_time": event_time.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        "emitted_at": utc_now(),
        "event_type": event_type,
        "product_id": identifier(row["product_id"], "product_id"),
        "category_id": identifier(row["category_id"], "category_id", optional=True),
        "category_code": optional_text(row["category_code"]),
        "brand": optional_text(row["brand"]),
        "price": format(price, "f"),
        "user_id": identifier(row["user_id"], "user_id"),
        "user_session": optional_text(row["user_session"]),
        "quality_flags": ["negative_price"] if price < 0 else [],
    }


def read_events(path: str | Path, dataset_version: str, max_events: int = 0):
    """Yield validated events without loading the whole CSV into memory.

    max_events=0 means the whole file. The data-record index excludes the header.
    Invalid headers or records stop iteration; no rejected records are hidden.
    """
    if type(max_events) is not int or max_events < 0:
        raise ValueError("max_events must be a nonnegative integer")
    source = Path(path)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        names = reader.fieldnames or []
        if len(names) != len(SOURCE_COLUMNS) or set(names) != set(SOURCE_COLUMNS):
            raise ValueError(f"{source.name}: expected exactly the nine source column names")
        for index, row in enumerate(reader, 1):
            if max_events and index > max_events:
                break
            try:
                yield normalize_event(row, source.name, index, dataset_version)
            except ValueError as exc:
                raise ValueError(f"{source.name}: data record {index}: {exc}") from exc

