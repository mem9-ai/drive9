"""Network-level transport checks, including a lost mutation acknowledgement."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import subprocess
import sys
import threading
import time


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    dropped = 0
    redirected = 0

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.path == "/drop":
            type(self).dropped += 1
            self.close_connection = True

    def do_GET(self):
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/must-not-follow")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/must-not-follow":
            type(self).redirected += 1
        if self.path == "/hold":
            time.sleep(0.3)
        data = b"x" * 100 if self.path == "/large" else b"ok"
        if self.path == "/slow":
            data = b"x" * 5
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            if self.path == "/slow":
                for c in data:
                    self.wfile.write(bytes([c]))
                    self.wfile.flush()
                    time.sleep(0.1)
            else:
                self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass


with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    result = subprocess.run([sys.argv[1], f"http://127.0.0.1:{server.server_port}"], timeout=30)
    server.shutdown()
    thread.join()
    assert Handler.dropped == 1, "transport replayed a mutation after losing its acknowledgement"
    assert Handler.redirected == 0, "transport followed a credential-bearing redirect"
    sys.exit(result.returncode)
