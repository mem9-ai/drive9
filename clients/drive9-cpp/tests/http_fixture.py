"""Network-level transport checks, including a lost mutation acknowledgement."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import subprocess
import ssl
import sys
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    dropped = 0
    redirected = 0
    tls_applied = 0
    tls_closed = threading.Condition()

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError):
            pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.path == "/drop":
            type(self).dropped += 1
            self.close_connection = True
        elif self.path == "/tls-drop":
            # Nonblocking unwrap emits close_notify before asking for its peer's
            # alert. Signal only after the alert is queued and the socket closes.
            self.connection.setblocking(False)
            try:
                self.connection.unwrap()
            except (ssl.SSLWantReadError, ssl.SSLError, OSError):
                pass
            self.connection.close()
            self.close_connection = True
            with type(self).tls_closed:
                type(self).tls_applied += 1
                type(self).tls_closed.notify_all()

    def do_GET(self):
        if self.path.startswith("/error/"):
            # Error documents can be far larger than a one-byte immutable
            # block. Cover both header and streaming response-size limits.
            _, _, status, framing = urlsplit(self.path).path.split("/")
            self.send_response(int(status))
            self.send_header("X-Fixture-Error", "object")
            data = b"<Error><Code>NoSuchKey</Code>" + b"x" * 65536 + b"</Error>"
            if framing == "length":
                self.send_header("Content-Length", str(len(data)))
            else:
                self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            if framing == "length":
                self.wfile.write(data)
            else:
                self.wfile.write(b"1\r\n<\r\n")
                self.wfile.write(f"{len(data) - 1:x}\r\n".encode() + data[1:] + b"\r\n0\r\n\r\n")
            return
        if self.path in ("/byte", "/old-byte"):
            self.send_response(200)
            self.send_header("Content-Length", "1")
            self.end_headers()
            self.wfile.write(b"A" if self.path == "/old-byte" else b"B")
            return
        if self.path.startswith("/await-tls-drop?count="):
            count = int(self.path.split("=", 1)[1])
            with type(self).tls_closed:
                applied = type(self).tls_closed.wait_for(lambda: type(self).tls_applied >= count, timeout=2)
            self.send_response(200 if applied else 504)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
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
        if self.path == "/block":
            data = bytes(range(256)) * 1024
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


with tempfile.TemporaryDirectory(prefix="drive9-cpp-tls-") as tmp:
    cert, key = Path(tmp, "cert.pem"), Path(tmp, "key.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
                    "-keyout", str(key), "-out", str(cert)],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server, ThreadingHTTPServer(("127.0.0.1", 0), Handler) as tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (server, tls)]
        for thread in threads:
            thread.start()
        result = subprocess.run([sys.argv[1], f"http://127.0.0.1:{server.server_port}",
                                 f"https://127.0.0.1:{tls.server_port}", str(cert)], timeout=30)
        for s in (server, tls):
            s.shutdown()
        for thread in threads:
            thread.join()
        assert Handler.dropped == 1, "transport replayed a mutation after losing its acknowledgement"
        assert Handler.redirected == 0, "transport followed a credential-bearing redirect"
        assert Handler.tls_applied == 1, f"TLS mutation applied {Handler.tls_applied} times"
        sys.exit(result.returncode)
