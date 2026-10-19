import csv
import tempfile
import unittest
from collections import Counter
from decimal import Decimal
from pathlib import Path

from src.common.events import SOURCE_COLUMNS, normalize_event, read_events

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data/sample/events_sample.csv"


class SourceContractTests(unittest.TestCase):
    def setUp(self):
        with FIXTURE.open(newline="", encoding="utf-8") as handle:
            self.row = next(csv.DictReader(handle))

    def test_all_four_actions_and_hand_calculated_fixture_totals(self):
        events = list(read_events(FIXTURE, "fixture-v1"))
        self.assertEqual(Counter(e["event_type"] for e in events), {
            "view": 3, "cart": 3, "remove_from_cart": 1, "purchase": 1,
        })
        total = sum(Decimal(e["price"]) for e in events if e["event_type"] == "purchase")
        self.assertEqual(total, Decimal("12.50"))
        self.assertEqual(len({e["event_id"] for e in events}), 8)

    def test_replay_preserves_identity_but_separate_rows_are_not_collapsed(self):
        first = normalize_event(self.row, "fixture.csv", 1, "v1")
        replay = normalize_event(self.row, "fixture.csv", 1, "v1")
        separate_row = normalize_event(self.row, "fixture.csv", 2, "v1")
        changed = normalize_event({**self.row, "price": "20.00"}, "fixture.csv", 1, "v1")
        self.assertEqual(first["event_id"], replay["event_id"])
        self.assertNotEqual(first["event_id"], separate_row["event_id"])
        self.assertNotEqual(first["event_id"], changed["event_id"])

    def test_large_identifier_precision_and_historical_utc(self):
        event = normalize_event(self.row, "fixture.csv", 1, "v1")
        self.assertEqual(event["category_id"], "210000000000000001")
        self.assertEqual(event["event_time"], "2019-10-01T10:00:00Z")
        self.assertEqual(event["price"], "12.50")

    def test_nullable_fields_remain_null(self):
        event = list(read_events(FIXTURE, "v1"))[-1]
        for field in ["category_id", "category_code", "user_session"]:
            self.assertIsNone(event[field])

    def test_negative_price_is_preserved_and_flagged(self):
        event = normalize_event({**self.row, "price": "-1.25"}, "fixture.csv", 1, "v1")
        self.assertEqual(event["price"], "-1.25")
        self.assertEqual(event["quality_flags"], ["negative_price"])

    def test_invalid_values_are_not_silently_accepted(self):
        for change in [{"price": "NaN"}, {"price": "Infinity"}, {"user_id": ""},
                       {"event_time": "yesterday"}, {"event_type": "checkout"}]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                normalize_event({**self.row, **change}, "fixture.csv", 1, "v1")

    def test_duplicate_header_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            path.write_text(",".join(SOURCE_COLUMNS + ("brand",)) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                list(read_events(path, "v1"))

    def test_record_limit_does_not_change_identity(self):
        full = list(read_events(FIXTURE, "v1"))
        limited = list(read_events(FIXTURE, "v1", max_events=2))
        self.assertEqual([e["event_id"] for e in limited], [e["event_id"] for e in full[:2]])


if __name__ == "__main__":
    unittest.main()

