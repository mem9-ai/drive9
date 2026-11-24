#!/bin/sh
# drive9/api/extract-config-image: `[prod-ext]` Hosted tenant image-extract config,
# validation, masking, tag generation, and disable verification (`--no-mount`).
# Exits 2 (`unsupported`) unless hosted TiDB Cloud + image provider env vars are configured.
set -eu

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE_FIXTURE="${DRIVE9_IMAGE_FIXTURE_PATH:-$CASE_DIR/fixtures/cat03.jpg}"
export CASE_DIR IMAGE_FIXTURE

require --req 'drive9-extract-image-prereq' --claim 'hosted-image-extract-env-configured' -- sh -c '
    case "${SERVER:-}" in https://*) ;; *) exit 2 ;; esac
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_IMAGE_EXTRACT_API_BASE:-}" ] &&
    [ -n "${DRIVE9_E2E_IMAGE_EXTRACT_API_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_IMAGE_EXTRACT_MODEL:-}" ] &&
    [ -s "$IMAGE_FIXTURE" ]
'

check shell --req 'drive9-extract-image-lifecycle' --claim 'image-extract-config-validation-masking-recall-and-disable' -- sh -c '
    python3 - "$SERVER" "$IMAGE_FIXTURE" <<'\''PY'\''
import json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

BASE = sys.argv[1].rstrip("/")
FIXTURE = open(sys.argv[2], "rb").read()
PUB = os.environ["DRIVE9_TIDBCLOUD_PUBLIC_KEY"]
PRIV = os.environ["DRIVE9_TIDBCLOUD_PRIVATE_KEY"]
P_BASE = os.environ["DRIVE9_E2E_IMAGE_EXTRACT_API_BASE"]
P_KEY = os.environ["DRIVE9_E2E_IMAGE_EXTRACT_API_KEY"]
P_MODEL = os.environ["DRIVE9_E2E_IMAGE_EXTRACT_MODEL"]
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
cfg_path = f"/v1/admin/tenants/{tid}/extract-config/image"

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
    assert st == 400 and bad_key not in json.dumps(err_v), (st, err_v)

    st, _ = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": UNREACH, "api_key": P_KEY, "model": P_MODEL})
    assert st in (502, 504), st

    prompt = ("Analyze the image and return only one JSON object with a short caption_en and an attributes object. "
              "The attributes object MUST contain the required key e2e_marker set to one short lowercase English noun.")
    st, cfg = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": P_BASE, "api_key": P_KEY, "model": P_MODEL, "prompt": prompt})
    assert st == 200 and cfg.get("enabled") is True and cfg.get("source") == "custom" and "*" in cfg.get("api_key", "") and cfg.get("api_key") != P_KEY

    assert call("POST", "/v1/fs/image-extract-e2e?mkdir", owner)[0] == 200
    assert call("PUT", "/v1/fs/image-extract-e2e/fixture.jpg", owner, FIXTURE, ctype="image/jpeg")[0] == 200

    marker = ""
    deadline = time.time() + 180
    while time.time() < deadline:
        st, sv = call("GET", "/v1/fs/image-extract-e2e/fixture.jpg?stat", owner)
        if st == 200 and isinstance(sv, dict):
            cand = (sv.get("tags") or {}).get("e2e_marker", "")
            if re.match(r"^[a-z][a-z0-9_-]{0,31}$", cand):
                marker = cand
                break
        time.sleep(3)
    assert marker, "generated e2e_marker tag did not appear"

    q = urllib.parse.urlencode({"find": "", "tag": f"e2e_marker={marker}"})
    st, found = call("GET", f"/v1/fs/image-extract-e2e?{q}", owner)
    assert st == 200 and any(str(x.get("path", "")).endswith("/image-extract-e2e/fixture.jpg") for x in (found or []))

    st, dis = call("PUT", cfg_path, "cp", {"enabled": False})
    assert st == 200 and dis.get("enabled") is False
finally:
    call("PUT", cfg_path, "cp", {"enabled": False})
    call("DELETE", "/v1/fs/image-extract-e2e?recursive", owner)
    call("DELETE", f"/v1/admin/tenants/{tid}", "cp")
PY
'
