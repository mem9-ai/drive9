#!/bin/sh
# drive9/api/fork-and-sql: `[core]` Read-only SQL inspection (`POST /v1/sql`) and
# tenant fork (`POST /v1/fork`, `DELETE /v1/fork`) bidirectional Copy-on-Write isolation (--no-mount).
# Providers without tenant-level cluster branching (e.g. `local` / `shared`) return 409
# `"not supported"` on `POST /v1/fork` and record `unsupported` for the fork check while still
# verifying `POST /v1/sql`.
set -eu

HELPER="$RUN_DIR/fork_sql_contract.py"
export HELPER

cat > "$HELPER" <<'PYEOF'
import base64
import hashlib
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ["SERVER"].rstrip("/")
PARENT_KEY = os.environ["API_KEY"]
ROOT = f"/fork-sql-{os.getpid()}-{int(time.time())}"


def crc32c_b64(data: bytes) -> str:
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if (crc & 1) else 0)
    return base64.b64encode(struct.pack(">I", crc ^ 0xFFFFFFFF)).decode("ascii")


def call(method, path, key=PARENT_KEY, body=None, headers=None):
    hdrs = {}
    if key is not None:
        hdrs["Authorization"] = f"Bearer {key}"
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            hdrs["Content-Type"] = "application/json"
        elif isinstance(body, str):
            data = body.encode()
        else:
            data = body
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, raw
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, {k.lower(): v for k, v in e.headers.items()}, raw


def get_bytes(path, key=PARENT_KEY):
    st, h, raw = call("GET", path, key=key)
    if st in (301, 302, 303, 307, 308):
        loc = h.get("location", "")
        with urllib.request.urlopen(loc, timeout=30) as resp:
            return resp.status, resp.read()
    return st, raw


def put_large_v2(path, data, key=PARENT_KEY):
    sha = hashlib.sha256(data).hexdigest()
    sha_b64 = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
    st, _, raw = call("POST", "/v2/uploads/initiate", key=key, body={"path": path, "total_size": len(data)})
    assert st == 202, (st, raw)
    uid = json.loads(raw)["upload_id"]
    st, _, praw = call("POST", f"/v2/uploads/{uid}/presign", key=key, body={"part_number": 1, "checksum": {"algorithm": "sha256", "value": sha_b64}})
    assert st == 200, (st, praw)
    pv = json.loads(praw)
    r = urllib.request.Request(pv["url"], data=data, method="PUT")
    for hk, hv in (pv.get("headers") or {}).items():
        r.add_header(hk, str(hv))
    with urllib.request.urlopen(r, timeout=60) as resp:
        etag = (resp.headers.get("ETag") or sha).strip('"') or sha
    st, _, craw = call("POST", f"/v2/uploads/{uid}/complete", key=key, body={"parts": [{"number": 1, "etag": etag}], "checksum_sha256": sha})
    assert st == 200, (st, craw)


mode = sys.argv[1]

if mode == "sql-inspection":
    try:
        assert call("POST", f"/v1/fs{ROOT}/sql?mkdir=1")[0] == 200
        assert call("PUT", f"/v1/fs{ROOT}/sql/alpha.txt", body=b"alpha")[0] == 200
        assert call("PUT", f"/v1/fs{ROOT}/sql/beta.txt", body=b"beta-data")[0] == 200

        st, _, qraw = call(
            "POST",
            "/v1/sql",
            key=PARENT_KEY,
            body={"query": f"SELECT path FROM file_nodes WHERE path LIKE '{ROOT}/sql/%' ORDER BY path"},
        )
        assert st == 200, (st, qraw)
        rows = json.loads(qraw)
        paths = [r["path"] for r in rows]
        assert f"{ROOT}/sql/alpha.txt" in paths and f"{ROOT}/sql/beta.txt" in paths, paths

        for bad_q in [
            "DELETE FROM file_nodes",
            "UPDATE file_nodes SET path = '/x'",
            "SELECT 1; SELECT 2",
            "",
        ]:
            bst, _, _ = call("POST", "/v1/sql", key=PARENT_KEY, body={"query": bad_q})
            assert bst == 400, (bad_q, bst)
    finally:
        call("DELETE", f"/v1/fs{ROOT}?recursive=1", key=PARENT_KEY)

elif mode == "fork-isolation":
    inline_v1 = b"parent-inline-v1"
    big_v1 = bytes((i % 251) for i in range(65536))
    fork_key = None
    try:
        assert call("POST", f"/v1/fs{ROOT}/sub?mkdir=1")[0] == 200
        assert call("PUT", f"/v1/fs{ROOT}/sub/inline.txt", body=inline_v1, headers={"X-Dat9-Tag": "env=seed"})[0] == 200
        put_large_v2(f"{ROOT}/sub/big.bin", big_v1, key=PARENT_KEY)

        st, _, raw = call("POST", "/v1/fork")
        if st == 409 and "not supported" in raw.decode("utf-8", "replace").lower():
            print("POST /v1/fork not supported by current tenant provider (409)")
            sys.exit(2)
        assert st in (200, 202), (st, raw)
        fv = json.loads(raw)
        fork_id, fork_key = fv["tenant_id"], fv["api_key"]
        assert fork_id and fork_key and fork_key != PARENT_KEY

        deadline = time.time() + 60
        while time.time() < deadline:
            s_code, _, s_raw = call("GET", "/v1/status", key=fork_key)
            if s_code == 200 and json.loads(s_raw).get("status") == "active":
                break
            time.sleep(0.5)
        else:
            raise AssertionError("forked tenant never became active")

        st, f_in = get_bytes(f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key)
        assert st == 200 and f_in == inline_v1
        st, f_big = get_bytes(f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)
        assert st == 200 and hashlib.sha256(f_big).hexdigest() == hashlib.sha256(big_v1).hexdigest()

        assert call("PUT", f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key, body=b"fork-mutated-v2")[0] == 200
        assert call("PUT", f"/v1/fs{ROOT}/sub/fork-only.txt", key=fork_key, body=b"only-in-fork")[0] == 200
        assert call("PUT", f"/v1/fs{ROOT}/sub/parent-only.txt", key=PARENT_KEY, body=b"only-in-parent")[0] == 200
        assert call("DELETE", f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)[0] == 200

        assert get_bytes(f"/v1/fs{ROOT}/sub/inline.txt", key=PARENT_KEY)[1] == inline_v1
        assert get_bytes(f"/v1/fs{ROOT}/sub/big.bin", key=PARENT_KEY)[0] == 200
        assert call("GET", f"/v1/fs{ROOT}/sub/fork-only.txt", key=PARENT_KEY)[0] == 404

        assert get_bytes(f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key)[1] == b"fork-mutated-v2"
        assert call("GET", f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)[0] == 404
        assert call("GET", f"/v1/fs{ROOT}/sub/parent-only.txt", key=fork_key)[0] == 404

        dst, _, _ = call("DELETE", "/v1/fork", key=fork_key)
        assert dst in (200, 202), dst
        fork_key = None
        assert call("GET", "/v1/status", key=fv["api_key"])[0] == 401
        assert call("GET", "/v1/status", key=PARENT_KEY)[0] == 200
    finally:
        if fork_key:
            call("DELETE", "/v1/fork", key=fork_key)
        call("DELETE", f"/v1/fs{ROOT}?recursive=1", key=PARENT_KEY)

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-api-sql-readonly' --claim 'sql-readonly-inspection-guardrails' -- python3 "$HELPER" sql-inspection
check shell --accept pass,unsupported --req 'drive9-api-fork-cow' --claim 'fork-create-bidirectional-cow-isolation-and-delete' -- python3 "$HELPER" fork-isolation
