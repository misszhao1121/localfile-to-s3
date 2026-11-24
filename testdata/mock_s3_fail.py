#!/usr/bin/env python3
"""Mock S3 that fails the first N PUTs with 503 to test retry/resume."""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import re

BUCKET = "dev-industai-app"
OBJECTS = {}
FAIL_EVERY = {"n": 0, "every": 0}  # fail every Nth PUT with 503 when every>0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _split(self):
        path = self.path.split("?", 1)[0]
        m = re.match(r"^/([^/]+)(?:/(.*))?$", path)
        if not m:
            return None, None
        return m.group(1), m.group(2) or ""

    def _drain(self):
        n = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(n) if n else b""

    def _empty(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        bucket, key = self._split()
        if bucket != BUCKET or key not in OBJECTS:
            self._empty(404)
            return
        body = OBJECTS[key]
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def do_PUT(self):
        bucket, key = self._split()
        body = self._drain()
        if bucket != BUCKET:
            self._empty(400)
            return
        if not key:
            # CreateBucket
            self.send_response(200)
            self.send_header("Location", f"/{BUCKET}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if FAIL_EVERY["every"] > 0:
            FAIL_EVERY["n"] += 1
            if FAIL_EVERY["n"] % FAIL_EVERY["every"] == 0:
                self._empty(503)
                return
        OBJECTS[key] = body
        self.send_response(200)
        self.send_header("ETag", '"mock"')
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        bucket, key = self._split()
        self._drain()
        if bucket == BUCKET and key in OBJECTS:
            body = OBJECTS[key]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._empty(404)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9333
    every = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    FAIL_EVERY["every"] = every
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"mock s3 (503 every {every}th PUT) listening on {port}", flush=True)
    srv.serve_forever()
