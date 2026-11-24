#!/bin/sh
# drive9/api/extract-config-video: `[prod-ext]` Hosted tenant video-extract config,
# validation, masking, semantic extraction, and disable verification (`--no-mount`).
# Exits 2 (`unsupported`) unless hosted TiDB Cloud + video provider + fixture env vars are set.
set -eu

require --req 'drive9-extract-video-prereq' --claim 'hosted-video-extract-env-configured' -- sh -c '
    case "${SERVER:-}" in https://*) ;; *) exit 2 ;; esac
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_VIDEO_EXTRACT_API_BASE:-}" ] &&
    [ -n "${DRIVE9_E2E_VIDEO_EXTRACT_API_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_VIDEO_EXTRACT_MODEL:-}" ] &&
    [ -n "${DRIVE9_E2E_VIDEO_FIXTURE_PATH:-}" ] && [ -s "${DRIVE9_E2E_VIDEO_FIXTURE_PATH:-}" ] &&
    [ -n "${DRIVE9_E2E_VIDEO_EXPECTED_MARKER:-}" ]
'

check shell --req 'drive9-extract-video-lifecycle' --claim 'video-extract-config-validation-masking-recall-and-disable' -- sh -c '
    python3 - "$SERVER" "$DRIVE9_E2E_VIDEO_FIXTURE_PATH" "$DRIVE9_E2E_VIDEO_EXPECTED_MARKER" <<'\''PY'\''
import json, os, sys, time, urllib.error, urllib.request

BASE = sys.argv[1].rstrip("/")
VIDEO_BYTES = open(sys.argv[2], "rb").read()
MARKER = sys.argv[3].lower()
PUB = os.environ["DRIVE9_TIDBCLOUD_PUBLIC_KEY"]
PRIV = os.environ["DRIVE9_TIDBCLOUD_PRIVATE_KEY"]
P_BASE = os.environ["DRIVE9_E2E_VIDEO_EXTRACT_API_BASE"]
P_KEY = os.environ["DRIVE9_E2E_VIDEO_EXTRACT_API_KEY"]
P_MODEL = os.environ["DRIVE9_E2E_VIDEO_EXTRACT_MODEL"]
PROMPT = "Describe the actual visible content across the supplied video frames in one concise English sentence. Do not invent details."
assert MARKER not in PROMPT.lower()

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
        with urllib.request.urlopen(r, timeout=180) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw.decode("utf-8", "replace"))

st, pv = call("POST", "/v1/provision", "cp")
assert st == 202, (st, pv)
tid, owner = pv["tenant_id"], pv["api_key"]
cfg_path = f"/v1/admin/tenants/{tid}/extract-config/video"

try:
    deadline = time.time() + 600
    while time.time() < deadline:
        s_code, s_val = call("GET", "/v1/status", owner)
        if s_code == 200 and isinstance(s_val, dict) and s_val.get("status") == "active":
            break
        time.sleep(5)
    else:
        raise AssertionError("tenant did not reach active")

    bad_key = "drive9-e2e-invalid-provider-key"
    st, err_v = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": P_BASE, "api_key": bad_key, "model": P_MODEL})
    assert st == 400 and bad_key not in json.dumps(err_v)

    st, cfg = call("PUT", cfg_path, "cp", {"enabled": True, "api_base": P_BASE, "api_key": P_KEY, "model": P_MODEL, "prompt": PROMPT})
    assert st == 200 and cfg.get("enabled") is True and "*" in cfg.get("api_key", "") and cfg.get("api_key") != P_KEY

    assert call("POST", "/v1/fs/video-extract-e2e?mkdir", owner)[0] == 200
    assert call("PUT", "/v1/fs/video-extract-e2e/fixture.mp4", owner, VIDEO_BYTES, ctype="video/mp4")[0] == 200

    matched = False
    deadline = time.time() + 600
    while time.time() < deadline:
        st, sv = call("GET", "/v1/fs/video-extract-e2e/fixture.mp4?stat", owner)
        if st == 200 and isinstance(sv, dict):
            blob = (json.dumps(sv.get("tags") or {}) + " " + str(sv.get("semantic_text") or "")).lower()
            if MARKER in blob:
                matched = True
                break
        time.sleep(5)
    assert matched, f"expected video marker {MARKER!r} not extracted"

    st, dis = call("PUT", cfg_path, "cp", {"enabled": False})
    assert st == 200 and dis.get("enabled") is False
finally:
    call("PUT", cfg_path, "cp", {"enabled": False})
    call("DELETE", "/v1/fs/video-extract-e2e?recursive", owner)
    call("DELETE", f"/v1/admin/tenants/{tid}", "cp")
PY
'
