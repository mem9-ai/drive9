"""Distinguish route-level vs node-level 404 on POST ?chmod=1.

Creates a fresh small file, chmods it, renames it, chmods the renamed node, and
prints the status and body of each step so the failing layer is unambiguous.
"""

import http.client
import json
import os
import pathlib
import time

CRED_PATH = pathlib.Path(os.environ.get("DRIVE9_BENCH_CRED", "/home/ubuntu/.drive9-create.json"))
CRED = json.loads(CRED_PATH.read_text())
HOST = os.environ.get("DRIVE9_BENCH_HOST") or CRED["server"].split("//", 1)[-1].split("/", 1)[0]
PORT = int(os.environ.get("DRIVE9_BENCH_PORT", "80"))
ROOT = os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark") + "/diag-chmod/run-" + str(int(time.time()))


def main():
    conn = http.client.HTTPConnection(HOST, PORT, timeout=60)
    auth = {"Authorization": "Bearer " + CRED["api_key"]}

    def call(method, path, body=None, extra=None):
        headers = dict(auth)
        if extra:
            headers.update(extra)
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        payload = resp.read()
        return resp.status, payload[:300].decode("utf-8", "replace")

    steps = []
    steps.append(("mkdir root", call("POST", f"/v1/fs{ROOT}?mkdir")))
    steps.append(("put a.bin", call("PUT", f"/v1/fs{ROOT}/a.bin", b"x" * 212)))
    steps.append(("head a.bin", call("HEAD", f"/v1/fs{ROOT}/a.bin")))
    steps.append(("list root (after put)", call("GET", f"/v1/fs{ROOT}?list=1")))
    def chmod(path, mode):
        return call("POST", f"/v1/fs{path}?chmod=1", json.dumps({"mode": mode}).encode(),
                    {"Content-Type": "application/json"})

    steps.append(("chmod a.bin 0644 (same as current, expect no-op)", chmod(f"{ROOT}/a.bin", 420)))
    steps.append(("chmod a.bin 0600 (real change)", chmod(f"{ROOT}/a.bin", 384)))
    steps.append(("chmod a.bin 0600 again (same as current, expect no-op)", chmod(f"{ROOT}/a.bin", 384)))
    steps.append(("list root (after chmods)", call("GET", f"/v1/fs{ROOT}?list=1")))
    steps.append(("rename a->b", call(
        "POST", f"/v1/fs{ROOT}/b.bin?rename", None,
        {"X-Dat9-Rename-Source": f"{ROOT}/a.bin"})))
    steps.append(("head b.bin", call("HEAD", f"/v1/fs{ROOT}/b.bin")))
    steps.append(("chmod b.bin 0600 (after rename)", chmod(f"{ROOT}/b.bin", 384)))
    steps.append(("chmod missing.bin", chmod(f"{ROOT}/missing.bin", 384)))

    for name, (status, body) in steps:
        print(json.dumps({"step": name, "status": status, "body": body}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
