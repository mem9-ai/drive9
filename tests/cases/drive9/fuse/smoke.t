#!/bin/sh
# drive9/fuse/smoke: End-to-end FUSE mount smoke & release gate.
# - nested mkdir -p, create/overwrite/append, symlink + hardlink (nlink=2, shared resource_id, alias write)
# - fd truncate + explicit os.truncate(path, 0) + truncate-then-rename + truncate-then-unlink
# - size & mtime monotonicity, file & directory rename, .git/config.lock updates
# - bidirectional CLI <-> FUSE visibility + open-handle flush on `mount drain`
# - 8 MiB large file + 10KiB -> 8MiB A -> 8MiB B -> 10KiB tier transition + fresh remount survival
# - POSIX error semantics + read-only sub-mount EROFS + mount.log health check
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TS="$(date +%s)-$$"
ROOT_REL="fuse-smoke-${TS}"
export TS ROOT_REL

check shell --req 'drive9-fuse-crud-links-truncate' --claim 'fuse-crud-links-and-truncate-variants' -- sh -c '
    R="$MOUNT/$ROOT_REL"
    mkdir -p "$R/alpha/beta"
    test -d "$R/alpha/beta"

    printf "create-%s" "$TS" > "$R/alpha/text.txt"
    test "$(cat "$R/alpha/text.txt")" = "create-$TS"
    printf "overwrite-%s" "$TS" > "$R/alpha/text.txt"
    printf -- "-append" >> "$R/alpha/text.txt"
    test "$(cat "$R/alpha/text.txt")" = "overwrite-$TS-append"

    ln -s "text.txt" "$R/alpha/sym.txt"
    test -L "$R/alpha/sym.txt"
    test "$(readlink "$R/alpha/sym.txt")" = "text.txt"
    test "$(cat "$R/alpha/sym.txt")" = "overwrite-$TS-append"

    ln "$R/alpha/text.txt" "$R/alpha/hard.txt"
    test "$(cat "$R/alpha/hard.txt")" = "overwrite-$TS-append"
    printf "hardlink-%s" "$TS" > "$R/alpha/hard.txt"
    test "$(cat "$R/alpha/text.txt")" = "hardlink-$TS"

    : > "$R/alpha/text.txt"
    test "$(wc -c < "$R/alpha/text.txt" | tr -d " ")" = "0"

    printf "path-trunc" > "$R/alpha/ptrunc.txt"
    python3 -c "import os,sys; os.truncate(sys.argv[1], 0)" "$R/alpha/ptrunc.txt"
    test -z "$(cat "$R/alpha/ptrunc.txt")"

    printf "path-trunc-rn" > "$R/alpha/ptrunc-src.txt"
    python3 -c "import os,sys; os.truncate(sys.argv[1], 0)" "$R/alpha/ptrunc-src.txt"
    mv "$R/alpha/ptrunc-src.txt" "$R/alpha/ptrunc-dst.txt"
    test ! -e "$R/alpha/ptrunc-src.txt"
    test -z "$(cat "$R/alpha/ptrunc-dst.txt")"

    printf "path-trunc-rm" > "$R/alpha/ptrunc-rm.txt"
    python3 -c "import os,sys; os.truncate(sys.argv[1], 0)" "$R/alpha/ptrunc-rm.txt"
    rm -f "$R/alpha/ptrunc-rm.txt"
    test ! -e "$R/alpha/ptrunc-rm.txt"
'

drain --mode require-ok --timeout 120s
check sync-ok --req 'drive9-fuse-initial-drain'

check shell --req 'drive9-fuse-remote-links-rename-gitlock' --claim 'fuse-remote-parity-dir-rename-and-gitlock' -- sh -c '
    R="$MOUNT/$ROOT_REL"
    RR="/$ROOT_REL"
    test "$("$CLI" fs cat "$RR/alpha/sym.txt")" = "text.txt"
    s1=$("$CLI" fs stat -o json "$RR/alpha/text.txt")
    s2=$("$CLI" fs stat -o json "$RR/alpha/hard.txt")
    python3 -c "import json,sys; a,b=json.loads(sys.argv[1]),json.loads(sys.argv[2]); assert a[\"resource_id\"]==b[\"resource_id\"] and int(b[\"nlink\"])==2" "$s1" "$s2"

    # Attribute size & mtime monotonicity
    printf "attr-base" > "$R/alpha/attr.txt"
    m1=$(python3 -c "import os,sys; st=os.stat(sys.argv[1]); print(f\"{st.st_size}:{int(st.st_mtime)}\")" "$R/alpha/attr.txt")
    sleep 1
    printf -- "-more" >> "$R/alpha/attr.txt"
    m2=$(python3 -c "import os,sys; st=os.stat(sys.argv[1]); print(f\"{st.st_size}:{int(st.st_mtime)}\")" "$R/alpha/attr.txt")
    python3 -c "
import sys
s1,t1 = map(int, sys.argv[1].split(\":\"))
s2,t2 = map(int, sys.argv[2].split(\":\"))
assert s2 > s1 and t2 >= t1, (s1,t1,s2,t2)
" "$m1" "$m2"

    # File & directory rename
    mv "$R/alpha/text.txt" "$R/alpha/text-renamed.txt"
    test ! -e "$R/alpha/text.txt"
    mv "$R/alpha" "$R/alpha-renamed"
    test ! -e "$R/alpha"
    test -d "$R/alpha-renamed/beta"

    # Git config lockfile semantics
    mkdir -p "$R/git-probe"
    git -C "$R/git-probe" init -q
    git -C "$R/git-probe" config core.filemode false
    git -C "$R/git-probe" remote add origin "https://example.invalid/repo.git"
    test "$(git -C "$R/git-probe" config --get remote.origin.url)" = "https://example.invalid/repo.git"
    test ! -e "$R/git-probe/.git/config.lock"
'

check shell --req 'drive9-fuse-cross-channel-and-open-drain' --claim 'fuse-cross-channel-and-open-fd-drain' -- sh -c '
    R="$MOUNT/$ROOT_REL"
    RR="/$ROOT_REL"
    printf "from-cli-%s" "$TS" > "$RUN_DIR/cli-seed.txt"
    "$CLI" fs cp "$RUN_DIR/cli-seed.txt" ":$RR/cli-to-mount.txt"

    i=0
    while [ "$i" -lt 30 ]; do
        if [ -f "$R/cli-to-mount.txt" ]; then break; fi
        sleep 0.5
        i=$((i + 1))
    done
    test "$(cat "$R/cli-to-mount.txt")" = "from-cli-$TS"

    printf "from-mount-%s" "$TS" > "$R/mount-to-cli.txt"

    # Keep an unclosed fd open while `mount drain` flushes it to remote
    python3 -c "
import os, subprocess, sys, time
mnt, mp, cli, rr, want = sys.argv[1:6]
fd = os.open(mp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
os.write(fd, want.encode())
try:
    r = subprocess.run([cli, \"mount\", \"drain\", \"--timeout\", \"120s\", \"--json\", mnt], capture_output=True, text=True, check=True)
    got = subprocess.run([cli, \"fs\", \"cat\", rr], capture_output=True, text=True, check=True).stdout
    assert got == want, (got, want)
finally:
    os.close(fd)
" "$MOUNT" "$R/open-drain.txt" "$CLI" "$RR/open-drain.txt" "open-drain-$TS"
    test "$("$CLI" fs cat "$RR/mount-to-cli.txt")" = "from-mount-$TS"
'

check shell --req 'drive9-fuse-tier-transition' --claim 'fuse-10k-8m-8m-10k-tier-transition' -- python3 -c '
import hashlib, os, subprocess, sys

root = os.path.join(os.environ["MOUNT"], os.environ["ROOT_REL"])
rr = "/" + os.environ["ROOT_REL"]
cli = os.environ["CLI"]
mnt = os.environ["MOUNT"]
tier = os.path.join(root, "tier.bin")

def write_pat(size, seed):
    blk = bytes(((i * 31 + seed) & 0xFF) for i in range(65536))
    h = hashlib.sha256()
    with open(tier, "wb") as f:
        rem = size
        while rem > 0:
            c = blk[:min(rem, len(blk))]
            f.write(c)
            h.update(c)
            rem -= len(c)
        f.flush()
        os.fsync(f.fileno())
    subprocess.run([cli, "mount", "drain", "--timeout", "180s", "--json", mnt], check=True, stdout=subprocess.DEVNULL)
    dl = os.path.join(os.environ["RUN_DIR"], "tier.dl")
    if os.path.exists(dl):
        os.unlink(dl)
    subprocess.run([cli, "fs", "cp", f":{rr}/tier.bin", dl], check=True)
    got = hashlib.sha256(open(dl, "rb").read()).hexdigest()
    want = h.hexdigest()
    assert got == want, (size, seed, got, want)
    return want

write_pat(10240, 17)
write_pat(8388608, 43)
write_pat(8388608, 67)
final_sha = write_pat(10240, 89)
with open(os.path.join(os.environ["RUN_DIR"], "tier_final.sha"), "w") as f:
    f.write(final_sha)
'

drain --mode require-ok --timeout 180s
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-fuse-remount-and-errors' --claim 'fuse-tier-remount-errors-and-readonly' -- sh -c '
    R="$MOUNT/$ROOT_REL"
    want=$(cat "$RUN_DIR/tier_final.sha")
    got=$(python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],\"rb\").read()).hexdigest())" "$R/tier.bin")
    test "$got" = "$want"
    test -z "$(cat "$R/alpha-renamed/ptrunc-dst.txt")"

    # Error semantics
    ! cat "$R/no-such-file.txt" 2>/dev/null
    mkdir -p "$R/dupdir"
    ! mkdir "$R/dupdir" 2>/dev/null
    mkdir -p "$R/nonempty"
    printf "x" > "$R/nonempty/x.txt"
    ! rmdir "$R/nonempty" 2>/dev/null
    rm -f "$R/nonempty/x.txt"
    rmdir "$R/nonempty" 2>/dev/null || test ! -e "$R/nonempty"
    ! rm "$R/missing.txt" 2>/dev/null

    # Read-only sub-mount EROFS verification
    ro_mp="$RUN_DIR/ro-mount"
    mkdir -p "$ro_mp"
    "$CLI" mount --mode=fuse --read-only --cache-dir "$RUN_DIR/ro-cache" --foreground ":/$ROOT_REL" "$ro_mp" >"$RUN_DIR/ro-mount.log" 2>&1 &
    ro_pid=$!
    cleanup_ro() {
        "$CLI" umount "$ro_mp" >/dev/null 2>&1 || fusermount3 -uz "$ro_mp" >/dev/null 2>&1 || true
        kill "$ro_pid" >/dev/null 2>&1 || true
    }
    trap cleanup_ro EXIT
    i=0
    while [ "$i" -lt 40 ]; do
        if [ -f "$ro_mp/tier.bin" ]; then break; fi
        sleep 0.5
        i=$((i + 1))
    done
    test -f "$ro_mp/tier.bin"
    ! sh -c "printf x > \"$ro_mp/ro-write.txt\"" 2>/dev/null
    ! rm "$ro_mp/tier.bin" 2>/dev/null
    cleanup_ro
    trap - EXIT

    rm -rf "$R"
    ! grep -E "panic:|fatal error:" "$RUN_DIR"/mount-*.log 2>/dev/null
'

drain --mode require-ok --timeout 120s
check sync-ok --req 'drive9-fuse-final-drain'
