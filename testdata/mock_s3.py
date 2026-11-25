#!/usr/bin/env python3
"""Minimal S3-compatible mock server for smoke-testing rustfs-migrate.

Ignores Authorization headers entirely; stores objects in-memory with sizes
tracked separately, answering HEAD/PUT/GET on /<bucket>/<key>. PUT with
no key = CreateBucket. GET on /<bucket> (no key) = ListObjectsV2.
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
        self.send_header("ETag", '"mock"')
        self.end_headers()

    def do_PUT(self):
        bucket, key = self._split()
        if bucket != BUCKET:
            self._empty(400)
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
        n = int(self.headers.get("Content-Length", "0") or 0)
        if n:
            self.rfile.read(n)
        self._empty(400)

    def do_GET(self):
        bucket, key = self._split()
        if bucket == BUCKET and key in OBJECTS:
            body = OBJECTS[key]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif bucket == BUCKET and key == "":
            # ListObjectsV2: prefix/delimiter/max-keys/continuation-token
            from urllib.parse import urlparse, parse_qs
            from xml.sax.saxutils import escape
            q = parse_qs(urlparse(self.path).query)
            prefix = q.get("prefix", [""])[0]
            delim = q.get("delimiter", [""])[0]
            max_keys = int(q.get("max-keys", ["1000"])[0])
            token = q.get("continuation-token", [""])[0]

            keys = sorted(k for k in OBJECTS if k.startswith(prefix))
            # delimiter mode: group keys into common prefixes / plain keys
            # FIRST (real S3 semantics), then paginate over that stream.
            if delim:
                stream = []  # ('cp', prefix) or ('key', key) in lex order
                seen_cp = set()
                for k in keys:
                    rest = k[len(prefix):]
                    if delim in rest:
                        cp = prefix + rest.split(delim)[0] + delim
                        if cp not in seen_cp:
                            seen_cp.add(cp)
                            stream.append(('cp', cp))
                    else:
                        stream.append(('key', k))
                stream.sort(key=lambda x: x[1])
            else:
                stream = [('key', k) for k in keys]

            start = 0
            if token:
                start = sum(1 for _, v in stream if v <= token)
            page = stream[start:start + max_keys]
            truncated = (start + max_keys) < len(stream)

            parts = ["<?xml version='1.0'?><ListBucketResult>",
                     f"<Name>{BUCKET}</Name>",
                     f"<Prefix>{escape(prefix)}</Prefix>",
                     f"<MaxKeys>{max_keys}</MaxKeys>",
                     f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"]
            if truncated and page:
                parts.append(f"<NextContinuationToken>{escape(page[-1][1])}</NextContinuationToken>")
            for kind, v in page:
                if kind == 'cp':
                    parts.append(f"<CommonPrefixes><Prefix>{escape(v)}</Prefix></CommonPrefixes>")
                else:
                    parts.append(f"<Contents><Key>{escape(v)}</Key><Size>{len(OBJECTS[v])}</Size></Contents>")
            parts.append("</ListBucketResult>")
            body = "".join(parts).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._empty(404)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9333
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"mock s3 listening on {port}", flush=True)
    srv.serve_forever()
