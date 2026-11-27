#!/bin/sh
# drive9/api/api: Authoritative Drive9 Server HTTP Contract Twin Suite (--no-mount).
# Defined by tests/simulator/drive9-server-api-spec.md.
# Designed to run identically across three environments:
#   1. Client CI:    `drive9-simulator run --no-mount tests/cases/drive9/api/api.t` (built-in server)
#   2. Server CI:    `drive9-simulator run --no-mount --server http://127.0.0.1:9009 tests/cases/drive9/api/api.t`
#   3. Daily Action: `drive9-simulator run --no-mount --server https://... --api-key $KEY tests/cases/drive9/api/api.t`
set -eu

API_ROOT="/api-contract-$$-$(date +%s)"
HELPER="$RUN_DIR/api_helper.py"
export API_ROOT HELPER

cat > "$HELPER" <<'PYEOF'
import base64
import hashlib
import json
import os
import struct
import sys
import urllib.error
import urllib.parse
import urllib.request
import zlib

SERVER = os.environ["SERVER"].rstrip("/")
API_KEY = os.environ["API_KEY"]
API_ROOT = os.environ["API_ROOT"]
EXISTING = os.environ.get("EXISTING_TENANT", "0") == "1"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def req(method, path, body=b"", headers=None, auth=API_KEY):
    if isinstance(body, str):
        body = body.encode("utf-8")
    hdrs = {}
    if auth is not None:
        hdrs["Authorization"] = f"Bearer {auth}"
    if headers:
        hdrs.update(headers)
    url = path if path.startswith("http://") or path.startswith("https://") else SERVER + path
    r = urllib.request.Request(url, data=body if method in ("PUT", "POST", "PATCH") else None, method=method)
    for k, v in hdrs.items():
        r.add_header(k, str(v))
    try:
        with OPENER.open(r, timeout=60) as resp:
            raw = resp.read() if method != "HEAD" else resp.read()
            out_h = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, out_h, raw
    except urllib.error.HTTPError as e:
        raw = e.read() if method != "HEAD" else b""
        out_h = {k.lower(): v for k, v in e.headers.items()}
        return e.code, out_h, raw


def req_json(method, path, obj=None, headers=None, auth=API_KEY):
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    body = b"" if obj is None else json.dumps(obj).encode("utf-8")
    st, h, raw = req(method, path, body=body, headers=hdrs, auth=auth)
    val = None
    if raw:
        try:
            val = json.loads(raw.decode("utf-8"))
        except Exception:
            val = raw.decode("utf-8", "replace")
    return st, h, val


def get_follow_presigned(path, headers=None):
    st, h, raw = req("GET", path, headers=headers)
    if st in (301, 302, 303, 307, 308):
        loc = h.get("location", "")
        assert loc, f"redirect {st} missing Location header: {h}"
        fwd = {}
        if headers and "Range" in headers:
            fwd["Range"] = headers["Range"]
        r = urllib.request.Request(loc, method="GET", headers=fwd)
        try:
            with urllib.request.urlopen(r, timeout=60) as resp:
                return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()
    return st, h, raw


def put_presigned(url, signed_headers, payload):
    hdrs = {}
    if isinstance(signed_headers, dict):
        for k, v in signed_headers.items():
            hdrs[k] = str(v)
    r = urllib.request.Request(url, data=payload, method="PUT")
    for k, v in hdrs.items():
        r.add_header(k, v)
    with urllib.request.urlopen(r, timeout=120) as resp:
        out_h = {k.lower(): v for k, v in resp.headers.items()}
        assert resp.status in (200, 201, 204), f"presigned PUT status={resp.status}"
        return out_h


def crc32c_b64(data: bytes) -> str:
    # Castagnoli CRC-32C (polynomial 0x1EDC6F41, reflected 0x82F63B78)
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if (crc & 1) else (crc >> 1)
    crc ^= 0xFFFFFFFF
    return base64.b64encode(struct.pack(">I", crc)).decode("ascii")


def step_health_and_auth():
    st, _, raw = req("GET", "/healthz", auth=None)
    assert st == 200, f"/healthz expected 200, got {st}: {raw}"

    st, _, _ = req("GET", f"/v1/fs{API_ROOT}/nope", auth=None)
    assert st == 401, f"unauthenticated expected 401, got {st}"

    st, _, _ = req("GET", f"/v1/fs{API_ROOT}/nope", auth="drive9_invalid_token_000")
    assert st == 401, f"bad bearer expected 401, got {st}"

    st, _, val = req_json("GET", "/v1/status", auth=None, headers={"X-API-Key": API_KEY})
    assert st == 200 and val.get("status") == "active", f"X-API-Key auth failed: {st} {val}"

    st, _, _ = req("GET", "/v1/nonexistent-contract-route")
    assert st == 404, f"unknown route expected 404, got {st}"

    st, _, _ = req("GET", "/v1/fs:batch-stat")
    assert st == 405, f"GET /v1/fs:batch-stat expected 405, got {st}"


def step_status_and_provision():
    st, _, val = req_json("GET", "/v1/status")
    assert st == 200, f"/v1/status got {st}: {val}"
    assert val.get("status") == "active", val
    assert int(val.get("inline_threshold", 0)) > 0, val
    assert int(val.get("max_upload_bytes", 0)) > 0, val

    if not EXISTING:
        st_p, _, pv = req_json("POST", "/v1/provision", obj=None, auth=None)
        if st_p == 202 and isinstance(pv, dict) and pv.get("api_key"):
            sub_key = pv["api_key"]
            import time
            deadline = time.time() + 60
            active = False
            while time.time() < deadline:
                st_s, _, sv = req_json("GET", "/v1/status", auth=sub_key)
                if st_s == 200 and isinstance(sv, dict) and sv.get("status") == "active":
                    active = True
                    break
                time.sleep(0.5)
            assert active, "sub-tenant did not reach active"
            st_d, _, _ = req("DELETE", "/v1/tenant", auth=sub_key)
            assert st_d in (200, 202), f"DELETE /v1/tenant got {st_d}"
            st_after, _, _ = req("GET", "/v1/status", auth=sub_key)
            assert st_after in (401, 403), f"deleted tenant /v1/status expected 401/403, got {st_after}"
        else:
            # Hosted control plane requiring TiDB Cloud credentials rejects anonymous provision cleanly
            assert st_p in (400, 401, 403), f"unexpected /v1/provision status {st_p}: {pv}"


def step_mkdir_put_get_list():
    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/sub/dir?mkdir=1")
    assert st == 200, f"mkdir default failed: {st}"
    st, h, _ = req("HEAD", f"/v1/fs{API_ROOT}/sub/dir")
    assert st == 200 and h.get("x-dat9-isdir") == "true", h
    assert h.get("x-dat9-mode") == "493", f"default mkdir mode expected 493 (0755), got {h.get('x-dat9-mode')}"

    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/sub/dir?mkdir=1")
    assert st == 409, f"duplicate mkdir expected 409, got {st}"

    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/priv?mkdir=1&mode=448")
    assert st == 200, f"mkdir mode=448 failed: {st}"
    st, h, _ = req("HEAD", f"/v1/fs{API_ROOT}/priv")
    assert st == 200 and h.get("x-dat9-mode") == "448", f"mkdir mode=448 got {h.get('x-dat9-mode')}"

    for i in range(10):
        payload = f"small-item-{i}\n".encode()
        st, _, _ = req("PUT", f"/v1/fs{API_ROOT}/batch10/f{i:02d}.txt", body=payload)
        assert st == 200, f"PUT f{i} failed: {st}"
    st, _, lv = req_json("GET", f"/v1/fs{API_ROOT}/batch10?list=1")
    assert st == 200 and len(lv.get("entries", [])) == 10, lv
    st, _, got = get_follow_presigned(f"/v1/fs{API_ROOT}/batch10/f07.txt")
    assert st == 200 and got == b"small-item-7\n", (st, got)


def step_stat_head_validators():
    body1 = b"validator payload v1"
    st, _, pv = req_json("PUT", f"/v1/fs{API_ROOT}/val.txt", headers={"Content-Type": "application/octet-stream"})
    # write actual bytes via raw req
    st, _, raw = req("PUT", f"/v1/fs{API_ROOT}/val.txt", body=body1)
    assert st == 200, (st, raw)

    st, sh, sv = req_json("GET", f"/v1/fs{API_ROOT}/val.txt?stat=1")
    assert st == 200, (st, sv)
    assert sv["isdir"] is False and sv["size"] == len(body1), sv
    assert int(sv["revision"]) >= 1 and sv.get("resource_id"), sv
    assert int(sv["nlink"]) == 1, sv
    btime1 = int(sv["btime"])

    st, hh, hbody = req("HEAD", f"/v1/fs{API_ROOT}/val.txt")
    assert st == 200 and hbody == b"", f"HEAD must have empty body, got {len(hbody)} bytes"
    etag1 = hh.get("etag", "")
    assert etag1.startswith('"') and etag1.endswith('"'), f"expected strong ETag, got {etag1}"
    assert hh.get("content-length") == str(len(body1)), hh
    assert hh.get("accept-ranges") == "bytes", hh
    assert hh.get("x-dat9-isdir") == "false", hh
    assert hh.get("x-dat9-resource-id") == sv["resource_id"], hh
    assert hh.get("x-dat9-nlink") == "1", hh
    assert hh.get("x-dat9-mode") == "420", hh
    assert hh.get("x-dat9-content-layout") == "single", hh
    assert hh.get("x-dat9-storage-type") in ("db9", "s3"), hh

    # Same-size overwrite MUST rotate strong ETag while preserving btime and resource_id
    body2 = b"validator payload v2"
    st, _, _ = req("PUT", f"/v1/fs{API_ROOT}/val.txt", body=body2)
    assert st == 200
    st, hh2, _ = req("HEAD", f"/v1/fs{API_ROOT}/val.txt")
    assert st == 200
    assert hh2.get("etag") != etag1, f"ETag did not change on same-size overwrite: {etag1}"
    assert hh2.get("x-dat9-resource-id") == sv["resource_id"], hh2
    assert int(hh2.get("x-dat9-btime", "0")) == btime1, (btime1, hh2)


def step_cas_and_range_preconditions():
    p = f"/v1/fs{API_ROOT}/cas.txt"
    st, _, _ = req("PUT", p, body=b"cas-v1", headers={"X-Dat9-Expected-Revision": "0"})
    assert st == 200, f"create-only PUT failed: {st}"
    st, _, _ = req("PUT", p, body=b"cas-dup", headers={"X-Dat9-Expected-Revision": "0"})
    assert st == 409, f"duplicate create-only PUT expected 409, got {st}"
    st, _, _ = req("PUT", p, body=b"cas-v2", headers={"X-Dat9-Expected-Revision": "1"})
    assert st == 200, f"matching CAS PUT expected 200, got {st}"
    st, _, _ = req("PUT", p, body=b"cas-stale", headers={"X-Dat9-Expected-Revision": "1"})
    assert st == 409, f"stale CAS PUT expected 409, got {st}"
    st, _, _ = req("PUT", p, body=b"cas-bad", headers={"X-Dat9-Expected-Revision": "not-an-int"})
    assert st == 400, f"malformed expected_revision expected 400, got {st}"
    st, _, _ = req("PUT", p, body=b"short", headers={"X-Dat9-Content-Length": "999"})
    assert st == 400, f"mismatched X-Dat9-Content-Length expected 400, got {st}"

    rp = f"/v1/fs{API_ROOT}/range.txt"
    payload = b"0123456789abcdef"
    st, _, _ = req("PUT", rp, body=payload)
    assert st == 200
    _, hh, _ = req("HEAD", rp)
    etag = hh["etag"]

    st, rh, rbody = req("GET", rp, headers={"Range": "bytes=4-9"})
    assert st == 206 and rbody == b"456789", (st, rbody)
    assert rh.get("content-range") == "bytes 4-9/16", rh

    st, rh, rbody = req("GET", rp, headers={"Range": "bytes=-4"})
    assert st == 206 and rbody == b"cdef", (st, rbody)

    st, rh, _ = req("GET", rp, headers={"Range": "bytes=100-200"})
    assert st == 416 and rh.get("content-range") == "bytes */16", (st, rh)

    # If-Match
    st, _, _ = req("GET", rp, headers={"If-Match": '"stale-validator"'})
    assert st == 412, f"If-Match mismatch expected 412, got {st}"
    st, _, got = req("GET", rp, headers={"If-Match": etag})
    assert st == 200 and got == payload, st

    # If-Range
    st, _, got = req("GET", rp, headers={"Range": "bytes=0-3", "If-Range": '"stale-validator"'})
    assert st == 200 and got == payload, f"stale If-Range must fall back to 200 full body, got {st}"
    st, _, got = req("GET", rp, headers={"Range": "bytes=0-3", "If-Range": etag})
    assert st == 206 and got == b"0123", (st, got)


def step_create_chmod_setmeta():
    cp = f"/v1/fs{API_ROOT}/created.txt"
    st, _, cv = req_json("POST", cp + "?create=1")
    assert st == 200 and cv.get("revision") == 1 and cv.get("resource_id"), cv
    st, _, _ = req_json("POST", cp + "?create=1")
    assert st == 409, f"duplicate ?create expected 409, got {st}"

    # chmod on file & dir preserves revision
    st, _, _ = req_json("POST", cp + "?chmod=1", {"mode": 384})  # 0600
    assert st == 200
    _, hh, _ = req("HEAD", cp)
    assert hh.get("x-dat9-mode") == "384" and hh.get("x-dat9-revision") == "1", hh

    st, _, _ = req_json("POST", f"/v1/fs{API_ROOT}/priv?chmod=1", {"mode": 493})
    assert st == 200
    _, dhh, _ = req("HEAD", f"/v1/fs{API_ROOT}/priv")
    assert dhh.get("x-dat9-mode") == "493", dhh

    st, _, _ = req_json("POST", f"/v1/fs{API_ROOT}/missing-chmod.txt?chmod=1", {"mode": 420})
    assert st == 404
    st, _, _ = req("POST", cp + "?chmod=1", body=b"")
    assert st == 400

    # PUT tags + description & setmeta validation
    mp = f"/v1/fs{API_ROOT}/meta.yaml"
    st, _, _ = req("PUT", mp, body=b"bad", headers={"X-Dat9-Tag": "missing_equals"})
    assert st == 400, f"malformed X-Dat9-Tag expected 400, got {st}"

    st, _, _ = req(
        "PUT",
        mp,
        body=b"env: staging\nneedle_token: yes\n",
        headers={"X-Dat9-Tag": "env=staging", "X-Dat9-Description": "initial config"},
    )
    assert st == 200
    st, _, sv = req_json("GET", mp + "?stat=1")
    assert st == 200 and sv.get("tags", {}).get("env") == "staging", sv
    assert sv.get("description") == "initial config", sv

    st, _, _ = req_json("POST", mp + "?setmeta=1", {})
    assert st == 400, "empty setmeta {} expected 400"
    st, _, _ = req_json("POST", f"/v1/fs{API_ROOT}/priv?setmeta=1", {"description": "dir"})
    assert st == 400, "setmeta on directory expected 400"
    st, _, _ = req_json("POST", mp + "?setmeta=1", {"description": "x" * 2001})
    assert st == 400, "2001-rune description expected 400"

    st, _, _ = req_json("POST", mp + "?setmeta=1", {"tags": {"env": "prod", "tier": "core"}, "description": "updated"})
    assert st == 200
    st, _, sv2 = req_json("GET", mp + "?stat=1")
    assert sv2.get("tags") == {"env": "prod", "tier": "core"} and sv2.get("description") == "updated", sv2


def step_encoding_copy_rename_selectors():
    raw_rel = f"{API_ROOT}/enc/中文 space #?.txt"
    enc_path = "/v1/fs" + "/".join(urllib.parse.quote(seg, safe="") for seg in raw_rel.split("/"))
    st, _, _ = req("PUT", enc_path, body=b"special-bytes")
    assert st == 200, f"PUT special path failed: {st}"
    st, _, got = req("GET", enc_path)
    assert st == 200 and got == b"special-bytes", (st, got)
    _, hh, _ = req("HEAD", enc_path)
    orig_id = hh["x-dat9-resource-id"]
    orig_rev = hh["x-dat9-revision"]

    # Invalid path encoding
    st, _, _ = req(
        "POST",
        f"/v1/fs{API_ROOT}/enc/bad.txt?copy=1",
        headers={"X-Dat9-Copy-Source": f"{API_ROOT}/enc/ascii.txt", "X-Dat9-Path-Encoding": "utf8"},
    )
    assert st == 400, f"invalid X-Dat9-Path-Encoding expected 400, got {st}"

    b64_src = base64.urlsafe_b64encode(raw_rel.encode("utf-8")).decode("ascii").rstrip("=")
    cp_dst = f"/v1/fs{API_ROOT}/enc/copied.txt"
    st, _, _ = req(
        "POST",
        cp_dst + "?copy=1",
        headers={"X-Dat9-Copy-Source": b64_src, "X-Dat9-Path-Encoding": "base64url"},
    )
    assert st == 200, f"base64url copy failed: {st}"
    st, _, got = req("GET", cp_dst)
    assert st == 200 and got == b"special-bytes"

    rn_dst = f"/v1/fs{API_ROOT}/enc/renamed.txt"
    st, _, _ = req(
        "POST",
        rn_dst + "?rename=1",
        headers={"X-Dat9-Rename-Source": b64_src, "X-Dat9-Path-Encoding": "base64url"},
    )
    assert st == 200, f"base64url rename failed: {st}"
    st, _, _ = req("GET", enc_path)
    assert st == 404, "renamed source must return 404"
    _, rnh, _ = req("HEAD", rn_dst)
    assert rnh.get("x-dat9-resource-id") == orig_id and rnh.get("x-dat9-revision") == orig_rev, rnh

    # POST single-selector enforcement
    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/enc/copied.txt")
    assert st == 400, f"POST with 0 selectors expected 400, got {st}"
    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/enc/copied.txt?mkdir=1&create=1")
    assert st == 409, f"POST with 2 selectors expected 409, got {st}"


def step_links_and_delete():
    master = f"{API_ROOT}/links/master.txt"
    alias = f"{API_ROOT}/links/alias.txt"
    st, _, _ = req("PUT", f"/v1/fs{master}", body=b"v1-shared")
    assert st == 200
    st, _, _ = req("POST", f"/v1/fs{alias}?hardlink=1", headers={"X-Dat9-Hardlink-Source": master})
    assert st == 200
    _, hm, _ = req("HEAD", f"/v1/fs{master}")
    _, ha, _ = req("HEAD", f"/v1/fs{alias}")
    assert hm["x-dat9-resource-id"] == ha["x-dat9-resource-id"], (hm, ha)
    assert int(hm["x-dat9-nlink"]) == 2 and int(ha["x-dat9-nlink"]) == 2, (hm, ha)

    st, _, _ = req("PUT", f"/v1/fs{alias}", body=b"v2-via-alias")
    assert st == 200
    _, _, gm = req("GET", f"/v1/fs{master}")
    assert gm == b"v2-via-alias", gm

    # Unlink master -> alias survives with nlink=1 and intact content
    st, _, _ = req("DELETE", f"/v1/fs{master}")
    assert st == 200
    st, ha2, ga = req("GET", f"/v1/fs{alias}")
    assert st == 200 and ga == b"v2-via-alias", (st, ga)
    _, ha_head, _ = req("HEAD", f"/v1/fs{alias}")
    assert ha_head.get("x-dat9-nlink") == "1", ha_head

    # Symlink lifecycle
    sym = f"/v1/fs{API_ROOT}/links/sym"
    st, _, _ = req_json("POST", sym + "?symlink=1", {"target": ""})
    assert st == 400, "empty symlink target expected 400"
    st, _, sv = req_json("POST", sym + "?symlink=1", {"target": "alias.txt"})
    assert st == 200 and sv.get("revision") == 1, sv
    st, _, _ = req_json("POST", sym + "?symlink=1", {"target": "alias.txt"})
    assert st == 409, "duplicate symlink expected 409"
    st, _, _ = req("HEAD", sym)
    assert st == 200
    st, _, _ = req("DELETE", sym)
    assert st == 200

    # Delete semantics
    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/links")
    assert st == 409, f"non-empty dir delete expected 409, got {st}"
    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/links?kind=dir")
    assert st == 409, f"?kind=dir on non-empty dir expected 409, got {st}"
    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/links/alias.txt?kind=bogus")
    assert st == 400, f"?kind=bogus expected 400, got {st}"
    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/links/alias.txt?kind=file")
    assert st == 200
    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/links/alias.txt?kind=file")
    assert st == 404

    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}/batch10?recursive=1")
    assert st == 200
    st, _, lv = req_json("GET", f"/v1/fs{API_ROOT}/batch10?list=1")
    assert st == 200 and lv.get("entries") == [], f"?list on deleted dir expected empty 200, got {st} {lv}"
    st, _, _ = req("GET", f"/v1/fs{API_ROOT}/batch10?stat=1")
    assert st == 404, "?stat on deleted dir expected 404"


def step_grep_find_tasks():
    st, _, _ = req("GET", f"/v1/fs{API_ROOT}?grep=")
    assert st == 400, "empty ?grep= expected 400"
    st, _, gv = req_json("GET", f"/v1/fs{API_ROOT}?grep=needle_token")
    assert st == 200 and any(r.get("path", "").endswith("/meta.yaml") for r in gv), gv

    st, _, fv = req_json("GET", f"/v1/fs{API_ROOT}?find=1&name=*.yaml")
    assert st == 200 and any(r.get("path", "").endswith("/meta.yaml") for r in fv), fv

    st, _, fv_tag = req_json("GET", f"/v1/fs{API_ROOT}?find=1&tag=" + urllib.parse.quote("tier=core", safe=""))
    assert st == 200 and len(fv_tag) == 1, fv_tag
    st, _, fv_key = req_json("GET", f"/v1/fs{API_ROOT}?find=1&tag=tier")
    assert st == 200 and len(fv_key) == 1, fv_key
    st, _, fv_miss = req_json("GET", f"/v1/fs{API_ROOT}?find=1&tag=" + urllib.parse.quote("tier=other", safe=""))
    assert st == 200 and len(fv_miss) == 0, fv_miss

    # ?tasks contract (including #1028 usage & pending_reason projections when rows exist)
    mp = f"/v1/fs{API_ROOT}/meta.yaml"
    st, th, tv = req_json("GET", mp + "?tasks")
    assert st == 200 and th.get("x-dat9-tasks") == "1", (st, th, tv)
    assert tv.get("path") == f"{API_ROOT}/meta.yaml" and isinstance(tv.get("tasks"), list), tv
    for row in tv["tasks"]:
        assert isinstance(row.get("task_type"), str) and row.get("status") in ("queued", "processing", "succeeded", "failed", "pending"), row
        u = row.get("usage")
        assert isinstance(u, dict) and all(isinstance(u.get(k), (int, float)) for k in ("input_tokens", "output_tokens", "total_tokens", "llm_calls", "audio_duration_seconds")), row
        if row.get("status") == "pending":
            assert isinstance(row.get("pending_reason"), str), row

    st, _, _ = req("GET", f"/v1/fs{API_ROOT}/priv?tasks")
    assert st == 400, "?tasks on directory expected 400"
    st, _, _ = req("GET", "/v1/fs/?tasks")
    assert st == 400, "?tasks on root / expected 400"
    st, _, _ = req("GET", f"/v1/fs{API_ROOT}/missing-task.txt?tasks")
    assert st == 404, "?tasks on missing file expected 404"


def step_batch_routes():
    _, _, status_v = req_json("GET", "/v1/status")
    inline_cap = int(status_v.get("inline_threshold", 50000))

    # batch-stat (>256 -> 400)
    st, _, _ = req_json("POST", "/v1/fs:batch-stat", {"paths": [f"{API_ROOT}/p{i}" for i in range(257)]})
    assert st == 400, "batch-stat >256 expected 400"

    st, _, _ = req("POST", f"/v1/fs{API_ROOT}/explicit-dir?mkdir=1")
    assert st == 200
    st, _, bsv = req_json(
        "POST",
        "/v1/fs:batch-stat",
        {"paths": [f"{API_ROOT}/meta.yaml", f"{API_ROOT}/explicit-dir", f"{API_ROOT}/no-such"], "include_checksum": True},
    )
    assert st == 200, bsv
    r = bsv["results"]
    assert r[0]["status"] == 200 and r[0]["isDir"] is False and len(r[0].get("checksum_sha256", "")) == 64, r[0]
    assert r[1]["status"] == 200 and r[1]["isDir"] is True, r[1]
    assert r[2]["status"] == 404, r[2]

    # batch-read-small (>128 -> 400, max_bytes -> 413)
    st, _, _ = req_json("POST", "/v1/fs:batch-read-small", {"paths": [f"{API_ROOT}/p{i}" for i in range(129)]})
    assert st == 400, "batch-read-small >128 expected 400"

    st, _, brv = req_json(
        "POST",
        "/v1/fs:batch-read-small",
        {"paths": [f"{API_ROOT}/val.txt", f"{API_ROOT}/meta.yaml", "/bad\\path", f"{API_ROOT}/explicit-dir", f"{API_ROOT}/no-such"], "max_bytes": 22},
    )
    assert st == 200, brv
    rr = brv["results"]
    assert rr[0]["status"] == 200 and base64.b64decode(rr[0]["data"]) == b"validator payload v2", rr[0]
    assert rr[1]["status"] == 413, f"file > max_bytes expected per-item 413, got {rr[1]}"
    assert rr[2]["status"] == 400, f"invalid path in batch-read-small expected 400, got {rr[2]}"
    assert rr[3]["status"] == 404, f"dir in batch-read-small expected 404, got {rr[3]}"
    assert rr[4]["status"] == 404, rr[4]

    # batch-write
    st, _, _ = req_json(
        "POST",
        "/v1/fs:batch-write",
        {"items": [{"path": f"{API_ROOT}/bw/{i}.txt", "data": ""} for i in range(129)]},
    )
    assert st == 400, "batch-write >128 expected 400"

    b64_ok = base64.b64encode(b"bw-one").decode()
    b64_big = base64.b64encode(b"x" * inline_cap).decode()
    st, _, bwv = req_json(
        "POST",
        "/v1/fs:batch-write",
        {
            "items": [
                {"path": f"{API_ROOT}/bw/a.txt", "expected_revision": 0, "data": b64_ok, "hasMode": True, "mode": 416},
                {"path": f"{API_ROOT}/explicit-dir", "expected_revision": 1, "data": b64_ok},
                {"path": f"{API_ROOT}/bw/huge.txt", "expected_revision": 0, "data": b64_big},
            ]
        },
    )
    assert st == 200, bwv
    wr = bwv["results"]
    assert wr[0]["status"] == 200 and wr[0]["revision"] == 1, wr[0]
    assert wr[1]["status"] == 409, f"dir target in batch-write expected 409, got {wr[1]}"
    assert wr[2]["status"] == 413, f">= inline_threshold item expected 413, got {wr[2]}"

    _, a_head, _ = req("HEAD", f"/v1/fs{API_ROOT}/bw/a.txt")
    assert a_head.get("x-dat9-mode") == "416", a_head

    # CAS overwrite + stale CAS conflict in batch-write
    st, _, bwv2 = req_json(
        "POST",
        "/v1/fs:batch-write",
        {
            "items": [
                {"path": f"{API_ROOT}/bw/a.txt", "expected_revision": 1, "data": base64.b64encode(b"bw-two").decode()},
                {"path": f"{API_ROOT}/bw/missing-cas.txt", "expected_revision": 5, "data": b64_ok},
            ]
        },
    )
    assert st == 200, bwv2
    assert bwv2["results"][0]["status"] == 200 and bwv2["results"][0]["revision"] == 2, bwv2
    assert bwv2["results"][1]["status"] == 409, bwv2


def step_uploads_v1():
    _, _, status_v = req_json("GET", "/v1/status")
    max_up = int(status_v["max_upload_bytes"])

    # Multi-part V1 roundtrip (8 MiB + 64 KiB -> 2 parts on 8 MiB part_size)
    part1 = (b"Drive9-V1-Part1-" * 524288)  # 8 MiB
    part2 = (b"Drive9-V1-Part2-" * 4096)    # 64 KiB
    full = part1 + part2
    full_sha = hashlib.sha256(full).hexdigest()
    csums = [crc32c_b64(part1), crc32c_b64(part2)]

    up_path = f"{API_ROOT}/up/v1-multi.bin"
    st, _, iv = req_json(
        "POST",
        "/v1/uploads/initiate",
        {"path": up_path, "total_size": len(full), "part_checksums": csums},
    )
    assert st == 202, (st, iv)
    uid = iv["upload_id"]
    parts = iv["parts"]
    assert len(parts) == 2, iv

    # list active uploads
    st, _, lv = req_json("GET", "/v1/uploads?path=" + urllib.parse.quote(up_path, safe=""))
    assert st == 200 and any(u["upload_id"] == uid for u in lv.get("uploads", [])), lv

    # resume validation
    st, _, _ = req_json("POST", f"/v1/uploads/{uid}/resume", {})
    assert st == 400, "resume without part_checksums expected 400"
    st, _, rv = req_json("POST", f"/v1/uploads/{uid}/resume", {"part_checksums": csums})
    assert st == 200 and rv["upload_id"] == uid, rv

    for idx, payload in enumerate((part1, part2)):
        pinfo = parts[idx]
        url = pinfo["url"]
        if url.startswith("/"):
            url = SERVER + url
        hdrs = dict(pinfo.get("headers") or {})
        if url.startswith(SERVER):
            hdrs.setdefault("Authorization", f"Bearer {API_KEY}")
        put_presigned(url, hdrs, payload)

    st, _, cv = req_json("POST", f"/v1/uploads/{uid}/complete", {"checksum_sha256": full_sha})
    assert st == 200, (st, cv)

    st, _, got = get_follow_presigned(f"/v1/fs{up_path}")
    assert st == 200 and hashlib.sha256(got).hexdigest() == full_sha, f"V1 roundtrip sha mismatch: st={st} len={len(got)}"

    st, _, _ = req_json("POST", f"/v1/uploads/{uid}/complete", {})
    assert st == 409, f"re-complete on finished V1 session expected 409, got {st}"

    # Supersede on re-initiate + DELETE abort
    sup_path = f"{API_ROOT}/up/v1-sup.bin"
    c1 = [crc32c_b64(b"hello")]
    _, _, s1 = req_json("POST", "/v1/uploads/initiate", {"path": sup_path, "total_size": 5, "part_checksums": c1})
    _, _, s2 = req_json("POST", "/v1/uploads/initiate", {"path": sup_path, "total_size": 5, "part_checksums": c1})
    st_old, _, _ = req_json("POST", f"/v1/uploads/{s1['upload_id']}/resume", {"part_checksums": c1})
    assert st_old == 409, f"superseded V1 upload expected 409 on resume, got {st_old}"
    st_del, _, _ = req("DELETE", f"/v1/uploads/{s2['upload_id']}")
    assert st_del == 200
    st_ab, _, _ = req_json("POST", f"/v1/uploads/{s2['upload_id']}/complete", {})
    assert st_ab == 409

    # Upload limit boundary (only when not on a shared production tenant)
    if not EXISTING:
        st_over, _, _ = req_json(
            "POST",
            "/v1/uploads/initiate",
            {"path": f"{API_ROOT}/up/over.bin", "total_size": max_up + 1, "part_checksums": c1},
        )
        assert st_over == 413, f"max_upload_bytes+1 expected 413, got {st_over}"


def step_uploads_v2_and_extent():
    # 1. V2 single layout roundtrip
    data = (b"Drive9-V2-Single-" * 4000)  # 68,000 bytes (> inline_threshold)
    sha = hashlib.sha256(data).hexdigest()
    sha_b64 = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
    v2_path = f"{API_ROOT}/up/v2-single.bin"

    st, _, iv = req_json("POST", "/v2/uploads/initiate", {"path": v2_path, "total_size": len(data)})
    assert st == 202 and iv["total_parts"] == 1, (st, iv)
    uid = iv["upload_id"]

    # presign + presign-batch (S3 UploadPart wire digest is base64 SHA-256)
    st, _, pv = req_json(
        "POST",
        f"/v2/uploads/{uid}/presign",
        {"part_number": 1, "checksum": {"algorithm": "sha256", "value": sha_b64}},
    )
    assert st == 200 and pv.get("url"), (st, pv)
    st, _, pbv = req_json(
        "POST",
        f"/v2/uploads/{uid}/presign-batch",
        {"parts": [{"part_number": 1, "checksum": {"algorithm": "sha256", "value": sha_b64}}]},
    )
    assert st == 200 and len(pbv.get("parts", [])) == 1, (st, pbv)

    put_h = put_presigned(pv["url"], pv.get("headers") or {}, data)
    etag = put_h.get("etag", sha).strip('"') or sha

    st, _, cv = req_json(
        "POST",
        f"/v2/uploads/{uid}/complete",
        {"parts": [{"number": 1, "etag": etag}], "checksum_sha256": sha},
    )
    assert st == 200 and cv.get("status") == "completed" and cv.get("resource_id"), (st, cv)

    st, _, got = get_follow_presigned(f"/v1/fs{v2_path}")
    assert st == 200 and hashlib.sha256(got).hexdigest() == sha, "V2 single download sha mismatch"

    # V2 abort -> complete 409
    _, _, iv2 = req_json("POST", "/v2/uploads/initiate", {"path": f"{API_ROOT}/up/v2-abort.bin", "total_size": 16})
    st_ab, _, _ = req_json("POST", f"/v2/uploads/{iv2['upload_id']}/abort", {})
    assert st_ab == 200
    st_c, _, _ = req_json("POST", f"/v2/uploads/{iv2['upload_id']}/complete", {"parts": [{"number": 1, "etag": "x"}]})
    assert st_c == 409

    # 2. V2 extent layout + PATCH dirty_blocks + X-Dat9-Extent-Read-Plan + truncate
    ext_data = (b"0123456789abcdef" * 512)  # 8192 bytes
    ext_sha = hashlib.sha256(ext_data).hexdigest()
    ext_path = f"{API_ROOT}/up/v2-extent.bin"

    st, _, eiv = req_json(
        "POST",
        "/v2/uploads/initiate",
        {"path": ext_path, "total_size": len(ext_data), "layout": "extent"},
    )
    assert st == 202, (st, eiv)
    euid = eiv["upload_id"]
    st, _, epv = req_json(
        "POST",
        f"/v2/uploads/{euid}/presign",
        {"part_number": 1, "checksum": {"algorithm": "sha256", "value": ext_sha}},
    )
    assert st == 200, (st, epv)
    if not epv.get("skip_upload", False):
        put_presigned(epv["url"], epv.get("headers") or {}, ext_data)
    st, _, ecv = req_json(
        "POST",
        f"/v2/uploads/{euid}/complete",
        {"parts": [{"number": 1, "etag": ext_sha}]},
    )
    assert st == 200 and ecv.get("status") == "completed", (st, ecv)

    # HEAD extent headers + plain GET returns the bytes (no plan opt-in) +
    # opt-in GET returns the 206 block plan. Extent layout is an internal
    # storage choice: a client that cannot parse a plan still reads the file.
    st, eh, _ = req("HEAD", f"/v1/fs{ext_path}")
    assert st == 200 and eh.get("x-dat9-content-layout") == "extent", eh
    st, eh, ebody = req("GET", f"/v1/fs{ext_path}")
    assert st == 200 and hashlib.sha256(ebody).hexdigest() == ext_sha, (st, eh)
    assert eh.get("x-dat9-content-layout") == "extent", eh
    assert eh.get("x-dat9-extent-length") == str(len(ext_data)), eh

    st, _, plan = req_json("GET", f"/v1/fs{ext_path}", headers={"X-Dat9-Extent-Read-Plan": "1"})
    assert st == 206 and plan["size"] == len(ext_data) and len(plan["blocks"]) == 1, (st, plan)

    # PATCH dirty_blocks at offset 256
    patch_bytes = b"EXTENT-PATCH-PAYLOAD"
    patch_sha = hashlib.sha256(patch_bytes).hexdigest()
    st, _, ppv = req_json(
        "PATCH",
        f"/v1/fs{ext_path}",
        {
            "new_size": len(ext_data),
            "expected_revision": 1,
            "dirty_blocks": [{"chunk": 0, "off": 256, "size": len(patch_bytes), "checksum": patch_sha}],
        },
    )
    assert st == 202, (st, ppv)
    puid = ppv["upload_id"]
    blk = ppv["blocks"][0]
    if not blk.get("skip_upload", False):
        put_presigned(blk["url"], blk.get("headers") or {}, patch_bytes)
    st, _, pcv = req_json(
        "POST",
        f"/v2/uploads/{puid}/complete",
        {"parts": [{"number": 1, "etag": patch_sha, "chunk": 0, "off": 256, "size": len(patch_bytes)}]},
    )
    assert st == 200, (st, pcv)

    # Read patched sub-range via extent read plan and verify actual bytes from block URLs
    st, _, rplan = req_json(
        "GET",
        f"/v1/fs{ext_path}",
        headers={"X-Dat9-Extent-Read-Plan": "1", "Range": f"bytes=256-{256 + len(patch_bytes) - 1}"},
    )
    assert st == 206, (st, rplan)
    assembled = bytearray()
    for b in rplan["blocks"]:
        if b.get("zero"):
            assembled.extend(b"\x00" * int(b["length"]))
        else:
            boff = int(b.get("block_offset", 0))
            blen = int(b["length"])
            r = urllib.request.Request(b["url"], headers={"Range": f"bytes={boff}-{boff + blen - 1}"})
            with urllib.request.urlopen(r, timeout=60) as resp:
                assembled.extend(resp.read())
    assert bytes(assembled) == patch_bytes, f"patched extent read mismatch: {assembled!r}"

    # Metadata-only PATCH truncate (dirty_blocks: [] + parts: [])
    st, _, tpv = req_json(
        "PATCH",
        f"/v1/fs{ext_path}",
        {"new_size": 1024, "expected_revision": 2, "dirty_blocks": []},
    )
    assert st == 202, (st, tpv)
    st, _, tcv = req_json("POST", f"/v2/uploads/{tpv['upload_id']}/complete", {"parts": []})
    assert st == 200, (st, tcv)
    _, th, _ = req("HEAD", f"/v1/fs{ext_path}")
    assert th.get("content-length") == "1024", th


def step_mounts_and_cleanup():
    mid = f"mnt-contract-{os.getpid()}"
    st, _, mv = req_json(
        "POST",
        "/v1/mounts",
        {"mount_id": mid, "mount_point": "/tmp/ws-contract", "host": "ci-host", "pid": 4242, "mode": "read_write", "ttl_seconds": 60},
    )
    assert st == 200 and mv["mount_id"] == mid and mv["heartbeat_interval_seconds"] == 20, (st, mv)

    st, _, lv = req_json("GET", "/v1/mounts")
    assert st == 200 and any(m["mount_id"] == mid for m in lv.get("mounts", [])), lv

    st, _, gv = req_json("GET", f"/v1/mounts/{mid}")
    assert st == 200 and gv["mount_point"] == "/tmp/ws-contract", gv

    st, _, hv = req_json("POST", f"/v1/mounts/{mid}/heartbeat", {"ttl_seconds": 180})
    assert st == 200 and hv["mount_id"] == mid and hv["expires_at"] > mv["expires_at"], (mv, hv)

    st, _, _ = req("PUT", "/v1/mounts")
    assert st == 405, f"PUT /v1/mounts expected 405, got {st}"

    st, _, _ = req("DELETE", f"/v1/mounts/{mid}")
    assert st == 204, f"DELETE /v1/mounts/{mid} expected 204, got {st}"
    st, _, _ = req("GET", f"/v1/mounts/{mid}")
    assert st == 404
    st, _, _ = req("DELETE", f"/v1/mounts/{mid}")
    assert st == 204, "idempotent DELETE /v1/mounts/{mid} expected 204"

    st, _, _ = req("DELETE", f"/v1/fs{API_ROOT}?recursive=1")
    assert st in (200, 404), f"cleanup failed: {st}"


STEPS = {
    "health-auth": step_health_and_auth,
    "status-provision": step_status_and_provision,
    "mkdir-put-list": step_mkdir_put_get_list,
    "stat-head": step_stat_head_validators,
    "cas-range": step_cas_and_range_preconditions,
    "create-chmod-setmeta": step_create_chmod_setmeta,
    "encoding-copy-rename": step_encoding_copy_rename_selectors,
    "links-delete": step_links_and_delete,
    "grep-find-tasks": step_grep_find_tasks,
    "batch-routes": step_batch_routes,
    "uploads-v1": step_uploads_v1,
    "uploads-v2-extent": step_uploads_v2_and_extent,
    "mounts-cleanup": step_mounts_and_cleanup,
}

if __name__ == "__main__":
    fn = STEPS[sys.argv[1]]
    fn()
    print(f"ok: {sys.argv[1]}")
PYEOF

check shell --req 'drive9-api-health-auth' --claim 'api-health-and-auth' -- python3 "$HELPER" health-auth
check shell --req 'drive9-api-status-provision' --claim 'api-status-and-provision' -- python3 "$HELPER" status-provision
check shell --req 'drive9-api-mkdir-put-list' --claim 'api-mkdir-put-and-list' -- python3 "$HELPER" mkdir-put-list
check shell --req 'drive9-api-stat-head' --claim 'api-stat-and-head-validators' -- python3 "$HELPER" stat-head
check shell --req 'drive9-api-cas-range' --claim 'api-cas-range-and-preconditions' -- python3 "$HELPER" cas-range
check shell --req 'drive9-api-create-chmod-setmeta' --claim 'api-create-chmod-and-setmeta' -- python3 "$HELPER" create-chmod-setmeta
check shell --req 'drive9-api-encoding-copy-rename' --claim 'api-encoding-copy-and-rename' -- python3 "$HELPER" encoding-copy-rename
check shell --req 'drive9-api-links-delete' --claim 'api-links-and-delete' -- python3 "$HELPER" links-delete
check shell --req 'drive9-api-grep-find-tasks' --claim 'api-grep-find-and-tasks' -- python3 "$HELPER" grep-find-tasks
check shell --req 'drive9-api-batch-routes' --claim 'api-batch-stat-read-write' -- python3 "$HELPER" batch-routes
check shell --req 'drive9-api-uploads-v1' --claim 'api-uploads-v1-lifecycle' -- python3 "$HELPER" uploads-v1
check shell --req 'drive9-api-uploads-v2-extent' --claim 'api-uploads-v2-and-extent' -- python3 "$HELPER" uploads-v2-extent
check shell --req 'drive9-api-mounts-cleanup' --claim 'api-mount-leases-and-cleanup' -- python3 "$HELPER" mounts-cleanup
