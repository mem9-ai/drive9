#!/bin/sh
# drive9/api/journal: append-only journal HTTP contract + CLI journal round-trip (--no-mount).
set -eu

TS="$(date +%s)-$$"
JID="jrn_smoke_${TS}"
AID="app_smoke_${TS}"
HELPER="$RUN_DIR/journal_helper.py"
export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export TS JID AID HELPER

cat > "$HELPER" <<'PYEOF'
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

SERVER = os.environ["SERVER"].rstrip("/")
API_KEY = os.environ["API_KEY"]
JID = os.environ["JID"]
AID = os.environ["AID"]


def req(method, path, body=None, headers=None):
    hdrs = {"Authorization": f"Bearer {API_KEY}"}
    if headers:
        hdrs.update(headers)
    data = None
    if body is not None:
        data = body.encode("utf-8") if isinstance(body, str) else json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    r = urllib.request.Request(SERVER + path, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


mode = sys.argv[1]
if mode == "create-and-append":
    st, raw = req(
        "POST",
        "/v1/journals",
        {
            "journal_id": JID,
            "kind": "agent",
            "labels": [
                {"key": "env", "value": "prod"},
                {"key": "env", "value": "us-east"},
                {"key": "suite", "value": "journal-smoke"},
            ],
        },
    )
    assert st == 200, f"POST /v1/journals got {st}: {raw}"
    cv = json.loads(raw)
    assert cv.get("journal_id") == JID, cv

    entry = {
        "type": "tool.call.completed",
        "status": "ok",
        "subjects": ["tool:exec_command"],
        "summary": {"cmd": "journal smoke"},
    }
    st, raw1 = req("POST", f"/v1/journals/{JID}/entries", entry, headers={"Idempotency-Key": AID})
    assert st == 200, f"append got {st}: {raw1}"
    av1 = json.loads(raw1)
    assert av1.get("first_seq") == 1 and av1.get("head_hash"), av1

    # Idempotent retry with identical Idempotency-Key must preserve head_hash and count=1
    st, raw2 = req("POST", f"/v1/journals/{JID}/entries", entry, headers={"Idempotency-Key": AID})
    assert st == 200, f"idempotent retry got {st}: {raw2}"
    av2 = json.loads(raw2)
    assert av2.get("head_hash") == av1["head_hash"], (av1, av2)

    st, cat_raw = req("GET", f"/v1/journals/{JID}/entries?limit=10")
    assert st == 200, f"GET entries got {st}: {cat_raw}"
    lines = [json.loads(ln) for ln in cat_raw.splitlines() if ln.strip()]
    assert len(lines) == 1, lines

elif mode == "search-and-verify":
    q = "/v1/journal-entries?meta=" + urllib.parse.quote("env=prod", safe="") + "&meta=" + urllib.parse.quote("env=us-east", safe="") + "&limit=10"
    st, sraw = req("GET", q)
    assert st == 200, f"search got {st}: {sraw}"
    assert "0001-01-01" not in sraw, f"zero time leaked in {sraw}"
    rows = [json.loads(ln) for ln in sraw.splitlines() if ln.strip()]
    assert any(r.get("journal_id") == JID for r in rows), rows

    st_bad, _ = req("GET", "/v1/journal-entries?meta=env")
    assert st_bad == 400, f"malformed meta expected 400, got {st_bad}"

    st_miss, _ = req("GET", "/v1/journals/jrn_missing_smoke/entries")
    assert st_miss == 404, f"missing journal expected 404, got {st_miss}"

    st_v, vraw = req("GET", f"/v1/journals/{JID}/verify")
    assert st_v == 200, f"verify got {st_v}: {vraw}"
    vv = json.loads(vraw)
    assert vv.get("ok") is True and vv.get("projection_ok") is True, vv
    assert "seal_ok" not in vv and "artifact_bytes_available" not in vv, vv

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-journal-create-append' --claim 'journal-create-and-idempotent-append' -- python3 "$HELPER" create-and-append
check shell --req 'drive9-journal-search-verify' --claim 'journal-search-negatives-and-verify' -- python3 "$HELPER" search-and-verify

if command -v "$CLI" >/dev/null 2>&1 || [ -x "$CLI" ]; then
    check shell --req 'drive9-journal-cli' --claim 'journal-cli-cat-and-verify' -- sh -c '
        "$CLI" journal cat "$JID" | grep -q "journal smoke"
        "$CLI" journal verify "$JID" >/dev/null'
fi
