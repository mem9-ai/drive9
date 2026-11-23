#!/bin/sh
# drive9/api/tokens: `[prod-ext]` Scoped token lifecycle, refresh, revoke, and pseudoroot projection (`--no-mount`).
# Probes `GET /v1/tokens`; exits 2 (`unsupported`) on `[core]`-only servers returning 404/501.
set -eu

require --req 'drive9-tokens-capability' --claim 'tokens-api-supported' -- \
    d9-http --status-only --expect 200 /v1/tokens

check shell --req 'drive9-tokens-lifecycle-and-pseudoroot' --claim 'scoped-tokens-issue-gate-list-toggle-refresh-revoke-and-pseudoroot' -- sh -c '
    python3 - "$SERVER" "$API_KEY" <<'\''PY'\''
import json, os, sys, time, urllib.error, urllib.request

BASE = sys.argv[1].rstrip("/")
OWNER = sys.argv[2]

def req(method, path, body=None, auth=OWNER, extra_headers=None):
    hdrs = {}
    if auth is not None:
        hdrs["Authorization"] = f"Bearer {auth}"
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            hdrs["Content-Type"] = "application/json"
        elif isinstance(body, str):
            data = body.encode()
        else:
            data = body
    if extra_headers:
        hdrs.update(extra_headers)
    r = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw and b"{" in raw[:4] else raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw and b"{" in raw[:4] else raw.decode("utf-8", "replace"))

# 1) Dispatcher credential shapes
st, _ = req("POST", "/v1/tokens", {"ttl_seconds": 60, "scopes": [{"prefix": "/scratch", "ops": ["read"]}]}, auth=None)
assert st == 401, st
st, _ = req("POST", "/v1/tokens", {"ttl_seconds": 60, "scopes": [{"prefix": "/scratch", "ops": ["read"]}]},
            extra_headers={"X-TiDBCloud-Public-Key": "pub", "X-TiDBCloud-Private-Key": "priv"})
assert st == 400, st

# 2) Issue scoped token + gate checks
st, issued = req("POST", "/v1/tokens", {"subject": "sim-scoped", "ttl_seconds": 3600, "scopes": [{"prefix": "/e2e-tokens", "ops": ["read", "list"]}]})
assert st == 201 and issued.get("scope_kind") == "fs_scoped", (st, issued)
s_tok, s_id = issued["token"], issued["token_id"]

for m, p, b in [("POST", "/v1/tokens", {"ttl_seconds": 60, "scopes": [{"prefix": "/x", "ops": ["read"]}]}),
                ("GET", "/v1/tokens", None),
                ("DELETE", f"/v1/tokens/{s_id}", None)]:
    st, _ = req(m, p, b, auth=s_tok)
    assert st == 403, (m, p, st)

# 3) Owner list omits secrets + deactivate/activate + refresh + revoke/delete
st, lst = req("GET", "/v1/tokens")
assert st == 200 and isinstance(lst.get("tokens"), list) and len(lst["tokens"]) >= 1
raw_lst = json.dumps(lst)
assert "jwt_hash" not in raw_lst and s_tok not in raw_lst

st, d = req("POST", f"/v1/tokens/{s_id}/deactivate", {})
assert st == 200 and d.get("status") == "disabled"
st, _ = req("POST", "/v1/tokens/refresh", {}, auth=s_tok)
assert st == 401, st
st, a = req("POST", f"/v1/tokens/{s_id}/activate", {})
assert st == 200 and a.get("status") == "active"

st, r_iss = req("POST", "/v1/tokens", {"subject": "sim-refresh", "ttl_seconds": 3600, "scopes": [{"prefix": "/e2e-tokens", "ops": ["read"]}]})
assert st == 201
st, r_rot = req("POST", "/v1/tokens/refresh", {}, auth=r_iss["token"])
assert st == 200 and r_rot["token"] != r_iss["token"] and r_rot["token_id"] == r_iss["token_id"]

st, rev = req("POST", "/v1/tokens/revoke", {"api_key": r_rot["token"]})
assert st == 200 and rev == {"status": "ok"}, (st, rev)
st, dl = req("DELETE", f"/v1/tokens/{s_id}")
assert st == 200 and dl == {"status": "ok"}, (st, dl)

# 4) Pseudoroot projected namespace
suf = f"{int(time.time())}-{os.getpid()}"
p1, p2, sec = f"pseudo-p1-{suf}", f"pseudo-p2-{suf}", f"pseudo-sec-{suf}"
for dname in (p1, p2, sec):
    st, _ = req("POST", f"/v1/fs/{dname}?mkdir")
    assert st == 200
assert req("PUT", f"/v1/fs/{p1}/a.txt", "a")[0] == 200
assert req("PUT", f"/v1/fs/{sec}/hidden.txt", "hidden")[0] == 200

st, ps = req("POST", "/v1/tokens", {
    "subject": "sim-pseudo", "ttl_seconds": 600,
    "scopes": [{"prefix": "/", "ops": ["pseudoroot"]},
               {"prefix": f"/{p1}", "ops": ["read", "list"]},
               {"prefix": f"/{p2}", "ops": ["read", "list"]}]
})
assert st == 201
ptok, pid = ps["token"], ps["token_id"]
st, root_ls = req("GET", "/v1/fs/?list", auth=ptok)
assert st == 200 and sorted(e["name"] for e in root_ls["entries"]) == sorted([p1, p2])
assert req("GET", f"/v1/fs/{sec}?list", auth=ptok)[0] == 404
assert req("GET", f"/v1/fs/{sec}/hidden.txt", auth=ptok)[0] == 403
assert req("GET", f"/v1/fs/{p1}/a.txt?tasks=1", auth=ptok)[0] == 200
assert req("GET", f"/v1/fs/{p1}/a.txt?tasks=1&unexpected=1", auth=ptok)[0] == 403
req("DELETE", f"/v1/tokens/{pid}")
for dname in (p1, p2, sec):
    req("DELETE", f"/v1/fs/{dname}?recursive=1")
PY
'
