"""Regenerate only DNS partition data from the pinned official AWS model."""
import hashlib
import json
from pathlib import Path
import urllib.request

VERSION = "v1.41.5"
URL = f"https://raw.githubusercontent.com/aws/aws-sdk-go-v2/{VERSION}/internal/endpoints/awsrulesfn/partitions.json"
SHA256 = "0caf9dfe139339c47a64e10e61f7ae436e7fb1c94353a4f862b1409d17b3545c"
with urllib.request.urlopen(URL, timeout=30) as response:
    raw = response.read()
if hashlib.sha256(raw).hexdigest() != SHA256:
    raise RuntimeError("AWS partition source checksum mismatch")
parts = [{"id": p["id"], "dnsSuffix": p["outputs"]["dnsSuffix"], "regionRegex": p["regionRegex"],
          "regions": list(p["regions"])} for p in json.loads(raw)["partitions"]]
header = (f"// Generated from AWS SDK Go v2 {VERSION} partitions.json; do not edit.\n"
          "// Copyright Amazon.com, Inc. or its affiliates. SPDX-License-Identifier: Apache-2.0\n"
          f"// Source SHA-256: {SHA256}\n")
output = Path(__file__).resolve().parents[1] / "src/aws_partitions.inc"
output.write_text(header + 'R"drive9aws(' + json.dumps(parts, separators=(",", ":")) + ')drive9aws"\n')
