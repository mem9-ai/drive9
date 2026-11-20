"""Offline readiness protocol tests; only a temporary loopback HTTP stub runs."""
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("mount_wait", Path(__file__).with_name("fixed-view-mount-smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)

UNAVAILABLE = {"error": "fixed-view content accounting is not available"}
PLAN = {"upload_id": "upload-a", "content_layout": "extent", "total_parts": 1, "part_size": 4194304}


class AccountingWaitTest(unittest.TestCase):
    def start(self, replies, abort="ABORTED"):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                calls.append((self.path, dict(self.headers), json.loads(body) if body else None))
                operation = self.headers.get("Idempotency-Key")
                if self.path.endswith("/abort"):
                    status, value = 200, {"operation_id": operation, "upload_id": "upload-a", "status": abort}
                else:
                    status, value = replies[min(len(calls) - 1, len(replies) - 1)]
                raw = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Idempotency-Key", operation)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close():
            server.shutdown()
            server.server_close()
            thread.join()

        self.addCleanup(close)
        probe = smoke.http.Probe(f"http://127.0.0.1:{server.server_port}", "unit-test-placeholder")
        return probe, {"layer_id": "layer-a", "work_id": "work-a", "fence": 2}, calls

    def test_admission_retries_same_identity_and_cancels_without_upload(self):
        probe, work, calls = self.start([(503, UNAVAILABLE), (202, PLAN)])
        with patch.object(smoke.time, "sleep"):
            smoke.wait_retained_accounting(probe, work)
        self.assertEqual([x[0] for x in calls], ["/v1/views/layer-a/v2/uploads/initiate"] * 2 +
                         ["/v1/views/layer-a/v2/uploads/upload-a/abort"])
        self.assertEqual(calls[0][2], calls[1][2])
        self.assertEqual(calls[0][2], {"path": probe.root + "/accounting-readiness", "total_size": 1,
                                      "expected_revision": 0, "layout": "extent"})
        self.assertEqual(calls[0][1]["Idempotency-Key"], calls[1][1]["Idempotency-Key"])
        self.assertNotEqual(calls[1][1]["Idempotency-Key"], calls[2][1]["Idempotency-Key"])
        for _, headers, _ in calls:
            self.assertEqual(headers["Authorization"], "Bearer unit-test-placeholder")
            self.assertEqual(headers["X-Drive9-Work-Id"], "work-a")
            self.assertEqual(headers["X-Drive9-Work-Fence"], "2")

    def test_already_ready_is_cancelled_without_sleep(self):
        probe, work, calls = self.start([(202, PLAN)])
        with patch.object(smoke.time, "sleep") as sleep:
            smoke.wait_retained_accounting(probe, work)
        self.assertEqual(len(calls), 2)
        sleep.assert_not_called()

    def test_unknown_error_or_wrong_status_never_retries(self):
        for status, value in [(503, {"error": "sensitive unexpected failure"}), (500, UNAVAILABLE), (200, PLAN)]:
            with self.subTest(status=status, value=value):
                probe, work, calls = self.start([(status, value)])
                with self.assertRaises(smoke.http.ProbeFailure) as error:
                    smoke.wait_retained_accounting(probe, work)
                self.assertNotIn("sensitive", str(error.exception))
                self.assertEqual(len(calls), 1)

    def test_incomplete_or_unsafe_plan_never_cancels_guessed_upload(self):
        for value in [None, {}, dict(PLAN, upload_id="../secret"), dict(PLAN, content_layout="single"),
                      dict(PLAN, part_size=0), dict(PLAN, total_parts=2)]:
            with self.subTest(value=value):
                probe, work, calls = self.start([(202, value)])
                with self.assertRaises(smoke.http.ProbeFailure):
                    smoke.wait_retained_accounting(probe, work)
                self.assertEqual(len(calls), 1)

    def test_cancellation_failure_is_not_readiness_success(self):
        probe, work, calls = self.start([(202, PLAN)], abort="COMPLETED")
        with self.assertRaisesRegex(smoke.http.ProbeFailure, "not cancelled"):
            smoke.wait_retained_accounting(probe, work)
        self.assertEqual(len(calls), 2)

    def test_persistent_unavailability_stops_at_total_deadline(self):
        probe, work, calls = self.start([(503, UNAVAILABLE)])
        with patch.object(smoke.time, "monotonic", side_effect=[0, 0, 1, 1, 4]), patch.object(smoke.time, "sleep"):
            with self.assertRaisesRegex(smoke.http.ProbeFailure, "timed out"):
                smoke.wait_retained_accounting(probe, work, timeout=3)
        self.assertEqual(len(calls), 1)

    def test_late_success_cannot_pass_deadline(self):
        probe, work, calls = self.start([(202, PLAN)])
        with patch.object(smoke.time, "monotonic", side_effect=[0, 0, 4]):
            with self.assertRaisesRegex(smoke.http.ProbeFailure, "timed out"):
                smoke.wait_retained_accounting(probe, work, timeout=3)
        self.assertEqual(len(calls), 1)

    def test_request_budget_and_transport_failure_have_no_retry(self):
        probe, work, _ = self.start([])
        with patch.object(probe, "request", side_effect=smoke.http.ProbeFailure("transport failed")) as request:
            with self.assertRaises(smoke.http.ProbeFailure):
                smoke.wait_retained_accounting(probe, work, timeout=2)
        self.assertEqual(request.call_count, 1)
        self.assertGreater(request.call_args.kwargs["timeout"], 0)
        self.assertLessEqual(request.call_args.kwargs["timeout"], 2)

    def test_missing_identity_or_malformed_json_stops_without_retry(self):
        for raw, headers in [(b'{}', {}), (b'private', {"Idempotency-Key": "original"})]:
            probe = smoke.http.Probe("http://unused.invalid", "")
            with patch.object(probe, "op", return_value="original"), patch.object(probe, "request", return_value=(raw, headers, 202)) as request:
                with self.assertRaises(smoke.http.ProbeFailure) as error:
                    smoke.wait_retained_accounting(probe, {"layer_id": "layer-a"})
                self.assertNotIn("private", str(error.exception))
                self.assertEqual(request.call_count, 1)

    def test_invalid_deadline_never_requests(self):
        probe = smoke.http.Probe("http://unused.invalid", "")
        for timeout in (0, -1, 241):
            with patch.object(probe, "request") as request:
                with self.assertRaises(smoke.http.ProbeFailure):
                    smoke.wait_retained_accounting(probe, {}, timeout)
                request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
