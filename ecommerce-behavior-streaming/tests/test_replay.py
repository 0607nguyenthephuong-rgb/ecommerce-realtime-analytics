import io
import json
import subprocess
import sys
import unittest
from pathlib import Path

from src.common.events import read_events
from src.producer.replay import replay_events

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data/sample/events_sample.csv"


class FakeFuture:
    def __init__(self, fail=False):
        self.fail = fail
        self.checked = False

    def get(self, timeout):
        self.checked = True
        if self.fail:
            raise RuntimeError("simulated broker rejection")
        return object()


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []
        self.futures = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        future = FakeFuture(self.fail)
        self.futures.append(future)
        return future


class ReplayTests(unittest.TestCase):
    def test_dry_run_emits_json_and_keeps_event_time(self):
        output = io.StringIO()
        result = replay_events(read_events(FIXTURE, "v1"), 0, output=output)
        emitted = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(emitted), 8)
        self.assertEqual(result["events_processed"], 8)
        self.assertEqual(emitted[0]["event_time"], "2019-10-01T10:00:00Z")
        self.assertTrue(emitted[0]["emitted_at"].endswith("Z"))

    def test_all_futures_checked_and_missing_session_uses_user_key(self):
        producer = FakeProducer()
        result = replay_events(read_events(FIXTURE, "v1"), 0, producer, "test-topic")
        self.assertEqual(result["acknowledged"], 8)
        self.assertTrue(all(f.checked for f in producer.futures))
        self.assertEqual(producer.sent[0][1], "1001:synthetic-session-01")
        self.assertEqual(producer.sent[-1][1], "1003")

    def test_failed_acknowledgement_stops_the_run(self):
        with self.assertRaisesRegex(RuntimeError, "broker rejection"):
            replay_events(read_events(FIXTURE, "v1", 1), 0, FakeProducer(True), "test-topic")

    def test_cli_rejects_invalid_rate(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.producer.replay", "--dry-run", "--rate", "-1"],
            cwd=ROOT, text=True, capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("rate must be finite and nonnegative", result.stderr)


if __name__ == "__main__":
    unittest.main()

