#!/bin/sh
# drive9/api/extract-config-embedding: `[prod-ext]` Hosted tenant embedding-config,
# validation, masking, semantic grep recall, and disable verification (`--no-mount`).
# Exits 2 (`unsupported`) unless hosted TiDB Cloud + embedding provider env vars are set.
set -eu

require --req 'drive9-extract-embed-prereq' --claim 'hosted-embedding-config-env-configured' -- sh -c '
    case "${SERVER:-}" in https://*) ;; *) exit 2 ;; esac
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_EMBED_API_BASE:-}" ] &&
    [ -n "${DRIVE9_E2E_EMBED_API_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_EMBED_MODEL:-}" ]
'

check shell --req 'drive9-extract-embed-lifecycle' --claim 'embedding-config-validation-masking-semantic-grep-and-disable' -- sh -c '
    python3 - "$SERVER" <<'\''PY'\''
import json, os, sys, time, urllib.error, urllib.parse, urllib.request

BASE = sys.argv[1].rstrip("/")
PUB = os.environ["DRIVE9_TIDBCLOUD_PUBLIC_KEY"]
PRIV = os.environ["DRIVE9_TIDBCLOUD_PRIVATE_KEY"]
P_BASE = os.environ["DRIVE9_E2E_EMBED_API_BASE"]
P_KEY = os.environ["DRIVE9_E2E_EMBED_API_KEY"]
P_MODEL = os.environ["DRIVE9_E2E_EMBED_MODEL"]
UNREACH = os.environ.get("DRIVE9_E2E_UNREACHABLE_API_BASE", "https://example.com:81/v1")

def call(method, path, mode, body=None, ctype="application/json"):
    hdrs = {}
    if mode == "cp":
        hdrs["X-TiDBCloud-Public-Key"] = PUB
        hdrs["X-TiDBCloud-Private-Key"] = PRIV
    else:
        hdrs["Authorization"] = f"Bearer {mode}"
    data = None
    if body is not None:
        data = json.dumps(body).encode() if isinstance(body, dict) else (body.encode() if isinstance(body, str) else body)
        hdrs["Content-Type"] = ctype
    r = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw.decode("utf-8", "replace"))

st, pv = call("POST", "/v1/provision", "cp")
assert st == 202, (st, pv)
tid, owner = pv["tenant_id"], pv["api_key"]
cfg_path = f"/v1/admin/tenants/{tid}/embedding-config"

try:
    deadline = time.time() + 600
    while time.time() < deadline:
        s_code, s_val = call("GET", "/v1/status", owner)
        if s_code == 200 and isinstance(s_val, dict) and s_val.get("status") == "active":
            break
        time.sleep(5)
    else:
        raise AssertionError("tenant did not reach active")

    st, before = call("GET", cfg_path, "cp")
    assert st == 200 and before.get("source") == "none", (st, before)

    bad_key = "drive9-e2e-invalid-provider-key"
    st, err_v = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": P_BASE, "api_key": bad_key, "model": P_MODEL})
    assert st == 400 and bad_key not in json.dumps(err_v)

    st, _ = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": UNREACH, "api_key": P_KEY, "model": P_MODEL})
    assert st in (502, 504), st

    st, cfg = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": P_BASE, "api_key": P_KEY, "model": P_MODEL})
    assert st == 200 and cfg.get("enabled") is True and cfg.get("source") == "custom" and "*" in cfg.get("api_key", "") and cfg.get("api_key") != P_KEY
    time.sleep(2)

    assert call("POST", "/v1/fs/embedding-e2e?mkdir", owner)[0] == 200
    assert call("PUT", "/v1/fs/embedding-e2e/target.txt", owner,
                "A veterinarian carefully treats an injured kitten inside a quiet clinic.", ctype="text/plain")[0] == 200
    assert call("PUT", "/v1/fs/embedding-e2e/distractor.txt", owner,
                "A cargo ship carries steel containers across the open ocean.", ctype="text/plain")[0] == 200

    found = False
    q = urllib.parse.urlencode({"grep": "animal doctor caring for a wounded feline", "limit": "20"})
    deadline = time.time() + 180
    while time.time() < deadline:
        st, hits = call("GET", f"/v1/fs/embedding-e2e?{q}", owner)
        if st == 200 and isinstance(hits, list) and any(str(x.get("path", "")).endswith("/embedding-e2e/target.txt") for x in hits):
            found = True
            break
        time.sleep(3)
    assert found, "semantic grep did not recall /embedding-e2e/target.txt"

    st, dis = call("PUT", cfg_path, "cp", {"enabled": False})
    assert st == 200 and dis.get("enabled") is False
finally:
    call("PUT", cfg_path, "cp", {"enabled": False})
    call("DELETE", "/v1/fs/embedding-e2e?recursive", owner)
    call("DELETE", f"/v1/admin/tenants/{tid}", "cp")
PY
'
