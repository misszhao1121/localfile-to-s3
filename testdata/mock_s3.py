#!/usr/bin/env python3
"""Minimal S3-compatible mock server for smoke-testing rustfs-migrate.

Ignores Authorization headers entirely; stores objects in-memory with sizes
tracked separately, answering HEAD/PUT/GET on /<bucket>/<key>.
"""
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BUCKET = "dev-industai-app"
OBJECTS = {}  # key -> bytes


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # keep quiet

    def _split(self):
        path = self.path.split("?", 1)[0]
        m = re.match(r"^/([^/]+)(?:/(.*))?$", path)
        if not m:
            return None, None
        bucket = m.group(1)
        key = m.group(2) or ""
        return bucket, key

    def do_HEAD(self):
        bucket, key = self._split()
        if bucket != BUCKET:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if key not in OBJECTS:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = OBJECTS[key]
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"mock"')
        self.end_headers()

    def do_PUT(self):
        bucket, key = self._split()
        if bucket != BUCKET:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        n = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(n) if n else b""
        if not key:
            # CreateBucket: PUT /<bucket>
            self.send_response(200)
            self.send_header("Location", f"/{BUCKET}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        OBJECTS[key] = body
        self.send_response(200)
        self.send_header("ETag", '"mock"')
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        # generic drain; nothing legit posts here in this mock
        n = int(self.headers.get("Content-Length", "0") or 0)
        if n:
            self.rfile.read(n)
        self.send_response(400)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        bucket, key = self._split()
        if bucket == BUCKET and key in OBJECTS:
            body = OBJECTS[key]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif bucket == BUCKET and key == "":
            # list: not implemented
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            body = b"<?xml version='1.0'?><ListBucketResult/>"
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9333
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"mock s3 listening on {port}", flush=True)
    srv.serve_forever()
