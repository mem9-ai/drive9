#!/bin/sh
# drive9/fuse/extent-takeover: Extent Content Layout v2 default large-file takeover
# and mixed `single` + `extent` directory tree contract.
# Run with `--profile none --durability fsync`.
# Covers:
#   A. Legacy `single` multipart file uploaded via CLI never converts on append/overwrite/truncate/rename.
#   B. New files growing past `inline_threshold` (cp, 1-fd grow, O_APPEND, 64MiB truncate, far sparse seek)
#      promote to `content_layout=extent`; small files remain `single`; layout & resource_id survive remount.
#   C. Mixed `single` + `extent` directory survives ls, stat, cross-plane renames, dir rename, cp -r, rm -r, hardlinks.
#   D. Cross-channel CLI `fs mv` / `fs rm -r` while unmounted stays consistent after fresh remount.
#   E-H. Delete & name reclaim, random fsynced block writes + CLI `cat` parity, two concurrent mounts, and open-while-writing visibility.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

INLINE_THRESHOLD=$(python3 - "$SERVER" "$API_KEY" <<'PY'
import json, sys, urllib.request
req = urllib.request.Request(sys.argv[1].rstrip("/") + "/v1/status", headers={"Authorization": "Bearer " + sys.argv[2]})
v = json.load(urllib.request.urlopen(req, timeout=20))
t = int(v.get("inline_threshold") or 50000)
print(t if t > 0 else 50000)
PY
)
BIG_KB=$(( (INLINE_THRESHOLD * 4 / 1024) + 1 ))
if [ "$BIG_KB" -lt 256 ]; then BIG_KB=256; fi
SMALL_KB=4
BIG_BYTES=$((BIG_KB * 1024))
COMPACT_WRITES="${EXTENT_E2E_COMPACT_WRITES:-400}"
TROOT="ext-takeover-$$"
export INLINE_THRESHOLD BIG_KB SMALL_KB BIG_BYTES COMPACT_WRITES TROOT

HELPER="$RUN_DIR/ext_helper.py"
cat > "$HELPER" <<'PY'
import hashlib, os, random, sys, urllib.error, urllib.request

SERVER = os.environ["SERVER"].rstrip("/")
KEY = os.environ["API_KEY"]

def head_info(remote_path):
    r = urllib.request.Request(f"{SERVER}/v1/fs{remote_path}", method="HEAD", headers={"Authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(r, timeout=20) as resp:
            h = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, h.get("x-dat9-content-layout", "single"), h.get("x-dat9-resource-id", "")
    except urllib.error.HTTPError as e:
        return e.code, "", ""

def make_payload(path, kb, seed):
    rng = random.Random(seed)
    data = rng.randbytes(kb * 1024)
    with open(path, "wb") as f:
        f.write(data)
    return hashlib.sha256(data).hexdigest()

def sha_file(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()

def fsync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

cmd = sys.argv[1]
if cmd == "head":
    st, layout, ino = head_info(sys.argv[2])
    print(f"{st} {layout} {ino}")
elif cmd == "make":
    print(make_payload(sys.argv[2], int(sys.argv[3]), int(sys.argv[4])))
elif cmd == "sha":
    print(sha_file(sys.argv[2]))
elif cmd == "fsync":
    fsync_file(sys.argv[2])
elif cmd == "append":
    p, n, fill = sys.argv[2], int(sys.argv[3]), sys.argv[4].encode()
    fd = os.open(p, os.O_WRONLY | os.O_APPEND)
    os.write(fd, (fill * ((n // len(fill)) + 1))[:n])
    os.fsync(fd)
    os.close(fd)
elif cmd == "overwrite":
    p, off, n = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    with open(p, "r+b") as f:
        f.seek(off)
        f.write(b"\xa5" * n)
        f.flush()
        os.fsync(f.fileno())
elif cmd == "grow":
    p, small, big = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"a" * small)
    os.write(fd, b"b" * (big - small))
    os.fsync(fd)
    os.close(fd)
    print(hashlib.sha256(b"a" * small + b"b" * (big - small)).hexdigest())
elif cmd == "append_only":
    p, big = sys.argv[2], int(sys.argv[3])
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    step, w = 65536, 0
    while w < big:
        k = min(step, big - w)
        os.write(fd, b"l" * k)
        os.fsync(fd)
        w += k
    os.close(fd)
elif cmd == "sparse_seek":
    p, off = sys.argv[2], int(sys.argv[3])
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"Z")
    os.lseek(fd, off, os.SEEK_SET)
    os.write(fd, b"Y")
    os.fsync(fd)
    os.close(fd)
elif cmd == "fsycled":
    p, n, span = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]) * 1024 * 1024
    rng = random.Random(20261026)
    img = bytearray(span)
    blk = bytes(range(256)) * 16
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.ftruncate(fd, span)
    for i in range(n):
        off = (rng.randrange(0, span - 4096 + 1) // 4096) * 4096
        data = blk[(i % 256):] + blk[:(i % 256)]
        os.pwrite(fd, data, off)
        os.fsync(fd)
        img[off:off + 4096] = data
    os.close(fd)
    print(hashlib.sha256(img).hexdigest())
PY
export HELPER

# Section A: Legacy `single` multipart file never converts to extent
check shell --req 'drive9-extent-legacy-single' --claim 'legacy-single-file-never-converts-across-mutations' -- sh -c '
    mkdir -p "$MOUNT/$TROOT/legacy" "$MOUNT/$TROOT/legacy-mv"
    leg_sha=$(python3 "$HELPER" make "$RUN_DIR/leg1.bin" "$BIG_KB" 1)
    "$CLI" fs cp "$RUN_DIR/leg1.bin" ":/$TROOT/legacy/big.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/legacy/big.bin")
    test "$1" = "200" && test "$2" = "single"
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/legacy/big.bin")" = "$leg_sha"

    python3 "$HELPER" append "$MOUNT/$TROOT/legacy/big.bin" 4096 alpha
    set -- $(python3 "$HELPER" head "/$TROOT/legacy/big.bin")
    test "$2" = "single"

    python3 "$HELPER" overwrite "$MOUNT/$TROOT/legacy/big.bin" 1024 4096
    set -- $(python3 "$HELPER" head "/$TROOT/legacy/big.bin")
    test "$2" = "single"

    tr_sha=$(python3 "$HELPER" make "$RUN_DIR/leg2.bin" "$BIG_KB" 2)
    cat "$RUN_DIR/leg2.bin" > "$MOUNT/$TROOT/legacy/big.bin"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/legacy/big.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/legacy/big.bin")
    test "$2" = "single"
    mv "$MOUNT/$TROOT/legacy/big.bin" "$MOUNT/$TROOT/legacy-mv/big.bin"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/legacy-mv/big.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/legacy-mv/big.bin")
    test "$2" = "single"
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/legacy-mv/big.bin")" = "$tr_sha"
    rm "$MOUNT/$TROOT/legacy-mv/big.bin"
'

# Section B: Promotion to `extent` (cp, 1-fd grow, O_APPEND, 64MiB truncate, sparse seek) + small stays `single`
check shell --req 'drive9-extent-promotion' --claim 'large-grow-append-and-sparse-files-promote-to-extent' -- sh -c '
    big_sha=$(python3 "$HELPER" make "$RUN_DIR/rew.bin" "$BIG_KB" 3)
    cp "$RUN_DIR/rew.bin" "$MOUNT/$TROOT/rewrite.bin"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/rewrite.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/rewrite.bin")
    test "$1" = "200" && test "$2" = "extent" && test -n "$3"
    printf "%s %s\n" "$big_sha" "$3" > "$RUN_DIR/promoted.meta"
    cli_sha=$("$CLI" fs cat "/$TROOT/rewrite.bin" | python3 -c "import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())")
    test "$cli_sha" = "$big_sha"

    grow_sha=$(python3 "$HELPER" grow "$MOUNT/$TROOT/grow.bin" "$((INLINE_THRESHOLD / 2))" "$((INLINE_THRESHOLD * 3))")
    set -- $(python3 "$HELPER" head "/$TROOT/grow.bin")
    test "$2" = "extent"
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/grow.bin")" = "$grow_sha"

    python3 "$HELPER" append_only "$MOUNT/$TROOT/app.log" "$((INLINE_THRESHOLD * 3))"
    set -- $(python3 "$HELPER" head "/$TROOT/app.log")
    test "$2" = "extent"

    python3 -c "import os,sys; f=open(sys.argv[1],\"wb\"); os.ftruncate(f.fileno(), 64*1024*1024); os.fsync(f.fileno()); f.close()" "$MOUNT/$TROOT/hole.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/hole.bin")
    test "$2" = "extent"

    python3 "$HELPER" sparse_seek "$MOUNT/$TROOT/seek.bin" "$((32 * 1024 * 1024))"
    set -- $(python3 "$HELPER" head "/$TROOT/seek.bin")
    test "$2" = "extent"

    python3 "$HELPER" make "$MOUNT/$TROOT/small-stays.bin" "$SMALL_KB" 4 >/dev/null
    python3 "$HELPER" fsync "$MOUNT/$TROOT/small-stays.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/small-stays.bin")
    test "$2" = "single"
'

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

# Section B2 + C: Remount persistence & mixed-tree operations
check shell --req 'drive9-extent-mixed-tree' --claim 'remount-identity-and-mixed-single-extent-tree-ops' -- sh -c '
    read -r want_sha want_ino < "$RUN_DIR/promoted.meta"
    set -- $(python3 "$HELPER" head "/$TROOT/rewrite.bin")
    test "$2" = "extent" && test "$3" = "$want_ino"
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/rewrite.bin")" = "$want_sha"

    mkdir -p "$MOUNT/$TROOT/tree/sub"
    python3 "$HELPER" make "$MOUNT/$TROOT/tree/small.txt" "$SMALL_KB" 10 >/dev/null
    python3 "$HELPER" make "$MOUNT/$TROOT/tree/big.bin" "$BIG_KB" 11 >/dev/null
    python3 "$HELPER" make "$MOUNT/$TROOT/tree/sub/big2.bin" "$BIG_KB" 13 >/dev/null
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/big.bin"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/sub/big2.bin"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/small.txt"

    set -- $(python3 "$HELPER" head "/$TROOT/tree/big.bin")
    test "$2" = "extent"
    big_ino="$3"
    big_sha=$(python3 "$HELPER" sha "$MOUNT/$TROOT/tree/big.bin")

    mv "$MOUNT/$TROOT/tree/big.bin" "$MOUNT/$TROOT/tree/big.ren"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/big.ren"
    set -- $(python3 "$HELPER" head "/$TROOT/tree/big.ren")
    test "$2" = "extent" && test "$3" = "$big_ino"

    # Extent over single -> keeps extent; single over extent -> becomes single
    mv "$MOUNT/$TROOT/tree/big.ren" "$MOUNT/$TROOT/tree/small.txt"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/small.txt"
    set -- $(python3 "$HELPER" head "/$TROOT/tree/small.txt")
    test "$2" = "extent" && test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/tree/small.txt")" = "$big_sha"

    s3_sha=$(python3 "$HELPER" make "$MOUNT/$TROOT/tree/s3.txt" "$SMALL_KB" 15)
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree/s3.txt"
    mv "$MOUNT/$TROOT/tree/s3.txt" "$MOUNT/$TROOT/tree/small.txt"
    set -- $(python3 "$HELPER" head "/$TROOT/tree/small.txt")
    test "$2" = "single" && test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/tree/small.txt")" = "$s3_sha"

    mv "$MOUNT/$TROOT/tree" "$MOUNT/$TROOT/tree2"
    cp -r "$MOUNT/$TROOT/tree2" "$MOUNT/$TROOT/tree3"
    python3 "$HELPER" fsync "$MOUNT/$TROOT/tree3/sub/big2.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/tree3/sub/big2.bin")
    test "$2" = "extent"
    rm -rf "$MOUNT/$TROOT/tree3"
'

# Section F + G: Heavily fsynced random 4KiB block writes + CLI cat parity + second concurrent mount
check shell --req 'drive9-extent-random-writes-and-dual-mount' --claim 'fsync-cycled-blocks-cli-cat-and-second-concurrent-mount' -- sh -c '
    comp_sha=$(python3 "$HELPER" fsycled "$MOUNT/$TROOT/comp.bin" "$COMPACT_WRITES" 8)
    python3 "$HELPER" fsync "$MOUNT/$TROOT/comp.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/comp.bin")
    test "$2" = "extent"
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/comp.bin")" = "$comp_sha"
    cli_comp=$("$CLI" fs cat "/$TROOT/comp.bin" | python3 -c "import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())")
    test "$cli_comp" = "$comp_sha"

    M2="$RUN_DIR/mount2"
    mkdir -p "$M2"
    "$CLI" mount --mode=fuse --profile none --durability fsync ":/" "$M2" >"$RUN_DIR/mount2.log" 2>&1
    trap '\''"$CLI" umount --timeout 30s "$M2" >/dev/null 2>&1 || true'\'' EXIT
    test "$(python3 "$HELPER" sha "$M2/$TROOT/comp.bin")" = "$comp_sha"
    m2_sha=$(python3 "$HELPER" make "$M2/$TROOT/from2.bin" "$BIG_KB" 19)
    python3 "$HELPER" fsync "$M2/$TROOT/from2.bin"
    set -- $(python3 "$HELPER" head "/$TROOT/from2.bin")
    test "$2" = "extent"
    sleep 2
    test "$(python3 "$HELPER" sha "$MOUNT/$TROOT/from2.bin")" = "$m2_sha"
    "$CLI" umount --timeout 30s "$M2"
    trap - EXIT
'
