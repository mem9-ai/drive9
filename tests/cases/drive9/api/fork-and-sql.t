#!/bin/sh
# drive9/api/fork-and-sql: `[core]` Tenant fork (`POST /v1/fork`, `DELETE /v1/fork`)
# bidirectional Copy-on-Write isolation and read-only SQL inspection (`POST /v1/sql`) (--no-mount).
set -eu

HELPER="$RUN_DIR/fork_sql_contract.py"
export HELPER

cat > "$HELPER" <<'PYEOF'
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ["SERVER"].rstrip("/")
PARENT_KEY = os.environ["API_KEY"]
ROOT = f"/fork-sql-{os.getpid()}-{int(time.time())}"


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


mode = sys.argv[1]
state_file = os.path.join(os.environ["RUN_DIR"], "fork_state.json")

if mode == "fork-isolation":
    # 1. Seed inline + S3-backed files + directory + tags on parent
    inline_v1 = b"parent-inline-v1"
    big_v1 = bytes((i % 251) for i in range(65536))
    assert call("POST", f"/v1/fs{ROOT}/sub?mkdir=1")[0] == 200
    assert call("PUT", f"/v1/fs{ROOT}/sub/inline.txt", body=inline_v1, headers={"X-Dat9-Tag": "env=seed"})[0] == 200
    assert call("PUT", f"/v1/fs{ROOT}/sub/big.bin", body=big_v1)[0] == 200

    # 2. Fork tenant
    st, _, raw = call("POST", "/v1/fork")
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

    # 3. Verify forked snapshot matches parent seed
    st, _, f_in = call("GET", f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key)
    assert st == 200 and f_in == inline_v1
    st, _, f_big = call("GET", f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)
    assert st == 200 and hashlib.sha256(f_big).hexdigest() == hashlib.sha256(big_v1).hexdigest()

    # 4. Mutate fork AND parent independently -> strict bidirectional CoW isolation
    assert call("PUT", f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key, body=b"fork-mutated-v2")[0] == 200
    assert call("PUT", f"/v1/fs{ROOT}/sub/fork-only.txt", key=fork_key, body=b"only-in-fork")[0] == 200
    assert call("PUT", f"/v1/fs{ROOT}/sub/parent-only.txt", key=PARENT_KEY, body=b"only-in-parent")[0] == 200
    assert call("DELETE", f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)[0] == 200

    # Parent still has original inline.txt & big.bin, and lacks fork-only.txt
    assert call("GET", f"/v1/fs{ROOT}/sub/inline.txt", key=PARENT_KEY)[2] == inline_v1
    assert call("GET", f"/v1/fs{ROOT}/sub/big.bin", key=PARENT_KEY)[0] == 200
    assert call("GET", f"/v1/fs{ROOT}/sub/fork-only.txt", key=PARENT_KEY)[0] == 404

    # Fork has mutated inline.txt & fork-only.txt, lacks big.bin & parent-only.txt
    assert call("GET", f"/v1/fs{ROOT}/sub/inline.txt", key=fork_key)[2] == b"fork-mutated-v2"
    assert call("GET", f"/v1/fs{ROOT}/sub/big.bin", key=fork_key)[0] == 404
    assert call("GET", f"/v1/fs{ROOT}/sub/parent-only.txt", key=fork_key)[0] == 404

    with open(state_file, "w") as f:
        json.dump({"root": ROOT, "fork_id": fork_id, "fork_key": fork_key}, f)

elif mode == "sql-and-teardown":
    with open(state_file) as f:
        st_data = json.load(f)
    root = st_data["root"]
    fork_key = st_data["fork_key"]

    # 5. Read-only POST /v1/sql SELECT works and mutating / multi-statement SQL is rejected (400)
    st, _, qraw = call(
        "POST",
        "/v1/sql",
        key=PARENT_KEY,
        body={"query": f"SELECT path, kind, size FROM file_nodes WHERE path LIKE '{root}/sub/%' ORDER BY path"},
    )
    assert st == 200, (st, qraw)
    rows = json.loads(qraw)
    paths = [r["path"] for r in rows]
    assert f"{root}/sub/inline.txt" in paths and f"{root}/sub/parent-only.txt" in paths, paths

    for bad_q in [
        "DELETE FROM file_nodes",
        "UPDATE file_nodes SET size = 0",
        "SELECT 1; SELECT 2",
        "",
    ]:
        bst, _, _ = call("POST", "/v1/sql", key=PARENT_KEY, body={"query": bad_q})
        assert bst == 400, (bad_q, bst)

    # 6. DELETE /v1/fork invalidates fork credentials while parent remains healthy
    dst, _, _ = call("DELETE", "/v1/fork", key=fork_key)
    assert dst == 200, dst
    assert call("GET", "/v1/status", key=fork_key)[0] == 401
    assert call("GET", "/v1/status", key=PARENT_KEY)[0] == 200
    call("DELETE", f"/v1/fs{root}?recursive=1", key=PARENT_KEY)

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-api-fork-cow' --claim 'fork-create-and-bidirectional-cow-isolation' -- python3 "$HELPER" fork-isolation
check shell --req 'drive9-api-sql-and-fork-delete' --claim 'sql-readonly-inspection-guardrails-and-fork-delete' -- python3 "$HELPER" sql-and-teardown
