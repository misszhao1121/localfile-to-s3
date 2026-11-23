#!/usr/bin/env python3
"""Pretend some files were already uploaded by 'other means'.

Reads the smoke source tree, uploads the first N files (deterministic order)
directly into the mock S3 OBJECTS store file, bypassing rustfs-migrate.
Simplest: run a tiny HTTP client that PUTs into the running mock server.
"""
import os
import sys
import re
import urllib.request

BUCKET = "dev-industai-app"
ENDPOINT = "http://127.0.0.1:9333"
SRC = sys.argv[1] if len(sys.argv) > 1 else "."
TAKE = int(sys.argv[2]) if len(sys.argv) > 2 else 2

files = []
for base, dirs, names in os.walk(SRC):
    dirs.sort()
    for n in sorted(names):
        p = os.path.join(base, n)
        rel = os.path.relpath(p, SRC).replace("\\", "/")
        files.append((rel, p))

put = 0
for rel, p in files[:TAKE]:
    with open(p, "rb") as f:
        body = f.read()
    url = f"{ENDPOINT}/{BUCKET}/{rel}"
    req = urllib.request.Request(url, data=body, method="PUT")
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
    put += 1
    print("pre-uploaded:", rel, len(body), "bytes")

print(f"total pre-uploaded: {put}")
