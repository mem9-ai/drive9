"""Offline HTTP-driver regressions; no filesystem service or database."""

import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
import unittest

spec = importlib.util.spec_from_file_location("fixed_view_smoke", Path(__file__).with_name("fixed-view-smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class SmokeDriverTest(unittest.TestCase):
    def start(self, respond):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                respond(self)

            def do_POST(self):
                respond(self)

            def do_PUT(self):
                respond(self)

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
        return smoke.Probe(f"http://127.0.0.1:{server.server_port}", "unit-test-placeholder")

    def test_mutation_query_and_receipt_keep_original_identity(self):
        requests = []

        def respond(handler):
            requests.append((handler.command, handler.path, dict(handler.headers)))
            if handler.command == "GET":
                payload = {"state": "DONE", "operation_id": handler.path.rsplit("/", 1)[1]}
            else:
                payload = {"operation_id": handler.headers["Idempotency-Key"], "view": {"generation": 1}}
            body = json.dumps(payload).encode()
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body)

        probe = self.start(respond)
        probe.mutate("POST", "/test/private?hardlink=1", {"layer_id": "a", "work_id": "w", "fence": 2},
                     headers={"X-Dat9-Hardlink-Source": "/test/source"})
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[0][1], "/v1/views/a/v1/fs/test/private?hardlink=1")
        self.assertEqual(requests[0], requests[1])
        self.assertTrue(requests[2][1].endswith("/operations/" + requests[0][2]["Idempotency-Key"]))

    def test_truncated_json_and_redirect_cannot_pass(self):
        requests = []

        def respond(handler):
            requests.append(handler.path)
            handler.send_response(307 if handler.path == "/redirect" else 200)
            handler.send_header("Location", "/never-follow?secret=not-for-logs")
            handler.send_header("Content-Length", "200")
            handler.end_headers()
            handler.wfile.write(b'{"state":"DONE"}')
            handler.close_connection = True

        probe = self.start(respond)
        for path in ("/truncated", "/redirect"):
            with self.assertRaises(smoke.ProbeFailure) as failure:
                probe.api("GET", path)
            self.assertNotIn("not-for-logs", str(failure.exception))
        self.assertEqual(requests, ["/truncated", "/redirect"])

    def test_seal_and_validation_replay_and_query_keep_original_ids(self):
        requests = []
        work = {"layer_id": "a", "work_id": "w", "fence": 1}
        validation = {"layer_id": "a", "work_id": "v", "fence": 2,
                      "validation": True, "lease_until": "2099-01-01T00:00:00Z"}
        sealed = {"state": "DONE", "layer": {"layer_id": "a", "phase": "READY",
                                             "fence": 1, "admission_closed": True}}
        receipts = {}

        def respond(handler):
            body = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
            req = json.loads(body) if body else None
            requests.append((handler.command, handler.path, req))
            if handler.command == "GET":
                operation = handler.path.rsplit("/", 1)[1]
                payload = receipts[operation]
            else:
                operation = req["operation_id"]
                if handler.path.endswith("/seal"):
                    self.assertEqual(req["work"], work)
                    payload = sealed
                    kind = "seal"
                else:
                    self.assertIs(req["validation"], True)
                    payload = {"launch": operation not in receipts, "work": validation}
                    kind = "begin-work"
                result = sealed if kind == "seal" else {"launch": False, "work": validation}
                receipts[operation] = {"operation_id": operation, "layer_id": "a", "kind": kind,
                                       "state": "DONE", "result": result}
            raw = json.dumps(payload).encode()
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(raw)))
            handler.end_headers()
            handler.wfile.write(raw)

        probe = self.start(respond)
        self.assertEqual(probe.seal_and_validate(work), validation)
        self.assertEqual(len(requests), 6)
        self.assertEqual(requests[0], requests[1])
        self.assertEqual(requests[3], requests[4])
        self.assertTrue(requests[2][1].endswith(requests[0][2]["operation_id"]))
        self.assertTrue(requests[5][1].endswith(requests[3][2]["operation_id"]))
        self.assertNotEqual(requests[0][2]["operation_id"], requests[3][2]["operation_id"])

    def test_unfinished_or_mismatched_seal_never_claims_validation(self):
        for kind in ("running", "upload-pending", "wrong-fence", "wrong-layer", "open"):
            with self.subTest(kind=kind):
                requests = []
                sealed = {"state": "DONE", "layer": {"layer_id": "a", "phase": "READY",
                                                     "fence": 1, "admission_closed": True}}
                if kind == "running":
                    sealed["state"] = "RUNNING"
                elif kind == "upload-pending":
                    sealed.update(state="FAILED", code="UPLOAD_PENDING")
                elif kind == "wrong-fence":
                    sealed["layer"]["fence"] = 2
                elif kind == "wrong-layer":
                    sealed["layer"]["layer_id"] = "other"
                else:
                    sealed["layer"]["phase"] = "OPEN"

                def respond(handler):
                    requests.append(handler.path)
                    handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
                    raw = json.dumps(sealed).encode()
                    handler.send_response(200)
                    handler.send_header("Content-Length", str(len(raw)))
                    handler.end_headers()
                    handler.wfile.write(raw)

                probe = self.start(respond)
                with self.assertRaises(smoke.ProbeFailure):
                    probe.seal_and_validate({"layer_id": "a", "work_id": "w", "fence": 1})
                self.assertEqual(requests, ["/v1/views/a/seal"])

    def test_object_request_does_not_inherit_api_or_work_credentials(self):
        requests = []

        def respond(handler):
            requests.append(dict(handler.headers))
            handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
            handler.send_response(200)
            handler.send_header("Content-Length", "0")
            handler.end_headers()

        probe = self.start(respond)
        probe.request("GET", "/api", work={"layer_id": "a", "work_id": "w", "fence": 1})
        probe.request("PUT", probe.base + "/object?signature=test", object_url=True,
                      data=b"x", headers={"If-None-Match": "*"})
        self.assertEqual(requests[0]["Authorization"], "Bearer unit-test-placeholder")
        self.assertFalse(any(k.lower() == "authorization" or k.lower().startswith("x-drive9-")
                             for k in requests[1]))
        with self.assertRaises(smoke.ProbeFailure):
            probe.request("PUT", "/object", object_url=True, headers={"Authorization": "forbidden"})
        self.assertEqual(len(requests), 2)


if __name__ == "__main__":
    unittest.main()
