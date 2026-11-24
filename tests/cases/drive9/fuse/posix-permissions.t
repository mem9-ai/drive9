#!/bin/sh
# drive9/fuse/posix-permissions: API + CLI + FUSE POSIX permission parity & remount survival.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TS="$(date +%s)-$$"
PERM_ROOT="perm-${TS}"
export TS PERM_ROOT

check shell --req 'drive9-perm-api-modes' --claim 'perm-api-mkdir-put-chmod-and-list' -- python3 -c '
import json, os, urllib.error, urllib.request

S = os.environ["SERVER"].rstrip("/")
K = os.environ["API_KEY"]
R = os.environ["PERM_ROOT"]

def req(m, p, body=None):
    hdrs = {"Authorization": f"Bearer {K}"}
    data = body.encode() if isinstance(body, str) else body
    r = urllib.request.Request(S + p, data=data, headers=hdrs, method=m)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()

st, _, _ = req("POST", f"/v1/fs/{R}/dir-default?mkdir=1")
assert st == 200
_, h, _ = req("HEAD", f"/v1/fs/{R}/dir-default")
assert h.get("x-dat9-mode") == "493", h

st, _, raw = req("GET", f"/v1/fs/{R}?list=1")
entries = {e["name"]: e for e in json.loads(raw)["entries"]}
assert entries["dir-default"]["hasMode"] is True and entries["dir-default"]["mode"] == 493, entries

st, _, _ = req("POST", f"/v1/fs/{R}/dir-explicit?mkdir=1&mode=448")
assert st == 200
_, h, _ = req("HEAD", f"/v1/fs/{R}/dir-explicit")
assert h.get("x-dat9-mode") == "448", h

st, _, _ = req("PUT", f"/v1/fs/{R}/file.txt", "permission-smoke")
assert st == 200
_, h, _ = req("HEAD", f"/v1/fs/{R}/file.txt")
assert h.get("x-dat9-mode") == "420", h

st, _, _ = req("POST", f"/v1/fs/{R}/file.txt?chmod=1", "{\"mode\":384}")
assert st == 200
_, h, _ = req("HEAD", f"/v1/fs/{R}/file.txt")
assert h.get("x-dat9-mode") == "384", h

st, _, _ = req("POST", f"/v1/fs/{R}/dir-default?chmod=1", "{\"mode\":448}")
assert st == 200
_, h, _ = req("HEAD", f"/v1/fs/{R}/dir-default")
assert h.get("x-dat9-mode") == "448", h

st, _, _ = req("POST", f"/v1/fs/{R}/no-such.txt?chmod=1", "{\"mode\":384}")
assert st == 404, st

st, _, raw = req("GET", f"/v1/fs/{R}?list=1")
entries = {e["name"]: e for e in json.loads(raw)["entries"]}
assert entries["file.txt"]["mode"] == 384, entries
'

check shell --req 'drive9-perm-cli-chmod' --claim 'perm-cli-fs-chmod' -- sh -c '
    printf "cli-perm-%s" "$TS" > "$RUN_DIR/cli-perm.txt"
    "$CLI" fs mkdir ":/$PERM_ROOT/cli-dir"
    "$CLI" fs cp "$RUN_DIR/cli-perm.txt" ":/$PERM_ROOT/cli-file.txt"
    "$CLI" fs chmod 600 "/$PERM_ROOT/cli-file.txt"
    "$CLI" fs chmod 700 "/$PERM_ROOT/cli-dir"
    python3 -c "
import os, urllib.request
def hmode(p):
    r = urllib.request.Request(os.environ[\"SERVER\"].rstrip(\"/\") + \"/v1/fs\" + p, headers={\"Authorization\": \"Bearer \" + os.environ[\"API_KEY\"]}, method=\"HEAD\")
    return urllib.request.urlopen(r, timeout=30).headers.get(\"X-Dat9-Mode\")
R = os.environ[\"PERM_ROOT\"]
assert hmode(f\"/{R}/cli-file.txt\") == \"384\"
assert hmode(f\"/{R}/cli-dir\") == \"448\"
"
'

mkdir -p "$MOUNT/$PERM_ROOT/fuse-dir" "$MOUNT/$PERM_ROOT/fuse-mkdir-mode"
printf 'fuse-perm-%s\n' "$TS" > "$MOUNT/$PERM_ROOT/fuse-file.txt"
chmod 600 "$MOUNT/$PERM_ROOT/fuse-file.txt"
chmod 700 "$MOUNT/$PERM_ROOT/fuse-dir"
chmod 750 "$MOUNT/$PERM_ROOT/fuse-mkdir-mode"

drain --mode require-ok --timeout 120s

check shell --req 'drive9-perm-fuse-modes' --claim 'perm-fuse-local-and-remote-modes' -- sh -c '
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-file.txt")" = "600"
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-dir")" = "700"
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-mkdir-mode")" = "750"
    python3 -c "
import os, urllib.request
def hmode(p):
    r = urllib.request.Request(os.environ[\"SERVER\"].rstrip(\"/\") + \"/v1/fs\" + p, headers={\"Authorization\": \"Bearer \" + os.environ[\"API_KEY\"]}, method=\"HEAD\")
    return urllib.request.urlopen(r, timeout=30).headers.get(\"X-Dat9-Mode\")
R = os.environ[\"PERM_ROOT\"]
assert hmode(f\"/{R}/fuse-file.txt\") == \"384\"
assert hmode(f\"/{R}/fuse-dir\") == \"448\"
assert hmode(f\"/{R}/fuse-mkdir-mode\") == \"488\"
"
'

remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-perm-remount-modes' --claim 'perm-modes-survive-fresh-remount' -- sh -c '
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-file.txt")" = "600"
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-dir")" = "700"
    test "$(d9-stat mode "$MOUNT/$PERM_ROOT/fuse-mkdir-mode")" = "750"
    rm -rf "$MOUNT/$PERM_ROOT"
'

drain --mode require-ok --timeout 120s
