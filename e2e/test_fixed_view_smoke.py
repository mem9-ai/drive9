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
