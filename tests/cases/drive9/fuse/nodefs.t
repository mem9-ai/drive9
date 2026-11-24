#!/bin/sh
# drive9/fuse/nodefs: Node.js fs API smoke + bidirectional CLI cross-channel + fresh remount.
set -eu

require --claim 'node-available' -- command -v node

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
NODEFS_JS="$CASE_DIR/tools/nodefs_smoke.js"
MANIFEST_JSON="$RUN_DIR/nodefs-manifest.json"
LARGE_BYTES="${FUSE_NODEFS_LARGE_BYTES:-$((9 * 1024 * 1024))}"
export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"
export UV_USE_IO_URING=0
export NODEFS_JS MANIFEST_JSON LARGE_BYTES

check shell --req 'drive9-nodefs-create' --claim 'nodefs-api-matrix-create-and-verify' -- sh -c '
    FUSE_NODEFS_LARGE_BYTES="$LARGE_BYTES" node "$NODEFS_JS" create "$MOUNT/nodefs" "$MANIFEST_JSON"
'

drain --mode require-ok --timeout 180s

check shell --req 'drive9-nodefs-cross-channel' --claim 'nodefs-bidirectional-cli-cross-channel' -- sh -c '
    want_sha=$(python3 -c "import hashlib,json,sys; d=json.load(open(sys.argv[1])); print(hashlib.sha256(d[\"cross_content\"].encode()).hexdigest())" "$MANIFEST_JSON")
    "$CLI" fs cat "/nodefs/cross-channel/node-written.txt" > "$RUN_DIR/node-written.out"
    got_sha=$(d9-stat sha256 "$RUN_DIR/node-written.out")
    test "$got_sha" = "$want_sha"

    python3 -c "import os,sys; open(sys.argv[1],\"wb\").write(os.urandom(1024*1024))" "$RUN_DIR/cli-fixture.bin"
    fix_sha=$(d9-stat sha256 "$RUN_DIR/cli-fixture.bin")
    "$CLI" fs cp "$RUN_DIR/cli-fixture.bin" ":/nodefs/cli-written.bin" >/dev/null

    i=0
    probe_sha=""
    while [ "$i" -lt 40 ]; do
        if [ -f "$MOUNT/nodefs/cli-written.bin" ]; then
            probe_sha=$(node "$NODEFS_JS" probe "$MOUNT/nodefs/cli-written.bin" 2>/dev/null | awk "{print \$1}")
            if [ "$probe_sha" = "$fix_sha" ]; then break; fi
        fi
        sleep 0.5
        i=$((i + 1))
    done
    test "$probe_sha" = "$fix_sha"
    rm -f "$MOUNT/nodefs/cli-written.bin"
'

drain --mode require-ok --timeout 180s
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-nodefs-remount-verify' --claim 'nodefs-manifest-survives-fresh-remount' -- sh -c '
    node "$NODEFS_JS" verify "$MOUNT/nodefs" "$MANIFEST_JSON"
'
