#!/bin/sh
# drive9/api/s3-express-append-log: `[prod-ext]` S3 Express `?append-log` HTTP contract (`--no-mount`).
# Requires `DRIVE9_E2E_S3_EXPRESS_ENABLED=1` and `storage_capabilities.append_log_v1=true`;
# exits 2 (`unsupported`) otherwise.
set -eu

require --req 'drive9-s3x-api-capability' --claim 's3-express-append-log-api-supported' -- sh -c '
    [ "${DRIVE9_E2E_S3_EXPRESS_ENABLED:-1}" != "0" ] || exit 2
    python3 - "$SERVER" "$API_KEY" <<'\''PY'\''
import json, sys, urllib.request
req = urllib.request.Request(sys.argv[1].rstrip("/") + "/v1/status", headers={"Authorization": "Bearer " + sys.argv[2]})
st = json.load(urllib.request.urlopen(req, timeout=20))
sys.exit(0 if st.get("storage_capabilities", {}).get("append_log_v1") is True else 2)
PY
'

check shell --req 'drive9-s3x-api-contract' --claim 'append-log-create-append-range-conflict-limit-and-reset' -- sh -c '
    python3 - "$SERVER" "$API_KEY" <<'\''PY'\''
import json, os, sys, time, urllib.error, urllib.request

BASE = sys.argv[1].rstrip("/")
KEY = sys.argv[2]
PATH = f"/s3-express-api-{os.getpid()}-{int(time.time())}-wal"

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

OPENER = urllib.request.build_opener(NoRedirect)

def req(method, path, body=b"", headers=None):
    if isinstance(body, str):
        body = body.encode()
    h = {"Authorization": f"Bearer {KEY}"}
    if headers:
        h.update(headers)
    r = urllib.request.Request(BASE + path, data=body if method in ("POST", "PUT") else None, method=method, headers=h)
    try:
        with OPENER.open(r, timeout=30) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()

try:
    st, _, raw = req("POST", f"/v1/fs{PATH}?append-log", b"wal-header",
                     {"X-Dat9-Expected-Revision": "0", "X-Dat9-Expected-Size": "0"})
    assert st == 200, (st, raw)
    v1 = json.loads(raw)
    rev, sz = int(v1["revision"]), int(v1["size_bytes"])

    st, _, raw = req("POST", f"/v1/fs{PATH}?append-log", b"frame-1",
                     {"X-Dat9-Expected-Revision": str(rev), "X-Dat9-Expected-Size": str(sz)})
    assert st == 200, (st, raw)
    v2 = json.loads(raw)
    rev, sz = int(v2["revision"]), int(v2["size_bytes"])

    st, h, body = req("GET", f"/v1/fs{PATH}")
    assert st == 200 and body == b"wal-headerframe-1" and "location" not in h, (st, h, body)

    st, h, body = req("GET", f"/v1/fs{PATH}", headers={"Range": "bytes=4-9"})
    assert st == 206 and body == b"header" and h.get("content-range") == "bytes 4-9/17", (st, h, body)

    st, _, raw = req("POST", f"/v1/fs{PATH}?append-log", b"stale",
                     {"X-Dat9-Expected-Revision": str(rev - 1), "X-Dat9-Expected-Size": str(sz)})
    assert st == 409 and json.loads(raw).get("code") == "append_log_conflict", (st, raw)

    st, _, raw = req("POST", f"/v1/fs{PATH}?append-log", b"x",
                     {"X-Dat9-Expected-Revision": str(rev), "X-Dat9-Expected-Size": "5000000000"})
    assert st == 413 and json.loads(raw).get("code") == "append_log_too_large", (st, raw)

    st, _, raw = req("PUT", f"/v1/fs{PATH}", b"reset", {"X-Dat9-Expected-Revision": str(rev)})
    assert st == 200 and int(json.loads(raw)["size_bytes"]) == 5
    st, _, body = req("GET", f"/v1/fs{PATH}")
    assert st == 200 and body == b"reset"
finally:
    req("DELETE", f"/v1/fs{PATH}")
PY
'
