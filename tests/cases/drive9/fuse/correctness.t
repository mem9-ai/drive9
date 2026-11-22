#!/bin/sh
# drive9/fuse/correctness: Deterministic FUSE read/manifest/grep/link correctness workload.
# - Remote tree seeded via CLI (empty file, nested markdown, space & unicode filenames,
#   binary file, 9 MiB large file, relative symlink, hardlink, empty dir)
# - Verified through fresh FUSE mount: find -type f/d/l manifests, cat + SHA-256,
#   stat size, hardlink nlink=2, symlink readlink, and recursive grep across space/unicode/symlink
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TS="$(date +%s)-$$"
ROOT_REL="fuse-corr-${TS}"
FIX="$RUN_DIR/fixture"
export TS ROOT_REL FIX

check shell --req 'drive9-fuse-corr-seed-remote' --claim 'fuse-correctness-seed-remote-tree' -- sh -c '
    mkdir -p "$FIX/text" "$FIX/nested/deep" "$FIX/binary" "$FIX/large" "$FIX/links"
    : > "$FIX/empty.txt"
    printf "alpha line\nDRIVE9_NEEDLE_ALPHA common-read-correctness\nunicode 你好 café\n" > "$FIX/text/alpha.txt"
    printf "space path line\nDRIVE9_NEEDLE_SPACE common-read-correctness\n" > "$FIX/text/space name.txt"
    printf "中文路径\nDRIVE9_NEEDLE_UNICODE\n" > "$FIX/text/中文-文件.txt"
    printf "# Story\n\nThis nested file carries DRIVE9_NEEDLE_NESTED and common-read-correctness.\n" > "$FIX/nested/deep/story.md"

    python3 -c "
import pathlib, sys
small, large = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
small.write_bytes(bytes(range(256)) * 17 + b\"\\x00drive9-binary-tail\\xff\")
chunk = bytearray((i * 31 + 7) % 256 for i in range(1024 * 1024))
with large.open(\"wb\") as f:
    for i in range(9):
        chunk[0] = i % 256
        f.write(chunk)
    f.write(b\"drive9-large-tail\")
" "$FIX/binary/small.bin" "$FIX/large/large-9mb.bin"

    for d in "" "text" "nested" "nested/deep" "binary" "large" "links" "empty-dir"; do
        "$CLI" fs mkdir ":/$ROOT_REL${d:+/$d}" >/dev/null
    done
    for rel in "binary/small.bin" "empty.txt" "large/large-9mb.bin" "nested/deep/story.md" "text/alpha.txt" "text/space name.txt" "text/中文-文件.txt"; do
        "$CLI" fs cp "$FIX/$rel" ":/$ROOT_REL/$rel" >/dev/null
    done
    "$CLI" fs symlink "../text/alpha.txt" ":/$ROOT_REL/links/alpha-link" >/dev/null
    "$CLI" fs hardlink "/$ROOT_REL/text/alpha.txt" "/$ROOT_REL/links/alpha-hardlink.txt" >/dev/null
'

remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-fuse-corr-manifest-and-grep' --claim 'fuse-correctness-manifest-checksum-and-grep' -- python3 -c '
import hashlib, os, subprocess, sys

root = os.path.join(os.environ["MOUNT"], os.environ["ROOT_REL"])
fix = os.environ["FIX"]

want_files = [
    "binary/small.bin",
    "empty.txt",
    "large/large-9mb.bin",
    "links/alpha-hardlink.txt",
    "nested/deep/story.md",
    "text/alpha.txt",
    "text/space name.txt",
    "text/中文-文件.txt",
]
want_dirs = [".", "binary", "empty-dir", "large", "links", "nested", "nested/deep", "text"]
want_links = ["links/alpha-link"]

got_files, got_dirs, got_links = [".", ], [], []
got_files = []
got_dirs = ["."]
for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
    dirnames[:] = [d for d in dirnames if d != ".go-fuse-epoll-hack"]
    for d in dirnames:
        full = os.path.join(dirpath, d)
        rel = os.path.relpath(full, root).replace(os.sep, "/")
        if os.path.islink(full):
            got_links.append(rel)
        else:
            got_dirs.append(rel)
    for f in filenames:
        if f == ".go-fuse-epoll-hack":
            continue
        full = os.path.join(dirpath, f)
        rel = os.path.relpath(full, root).replace(os.sep, "/")
        if os.path.islink(full):
            got_links.append(rel)
        else:
            got_files.append(rel)

assert sorted(got_files) == want_files, (sorted(got_files), want_files)
assert sorted(got_dirs) == want_dirs, (sorted(got_dirs), want_dirs)
assert sorted(got_links) == want_links, (sorted(got_links), want_links)

def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()

for rel in want_files:
    mp = os.path.join(root, rel)
    fp = os.path.join(fix, "text/alpha.txt" if rel == "links/alpha-hardlink.txt" else rel)
    assert os.path.getsize(mp) == os.path.getsize(fp), rel
    assert sha(mp) == sha(fp), rel

assert os.stat(os.path.join(root, "text/alpha.txt")).st_nlink == 2
assert os.stat(os.path.join(root, "links/alpha-hardlink.txt")).st_nlink == 2
assert os.readlink(os.path.join(root, "links/alpha-link")) == "../text/alpha.txt"
assert sha(os.path.join(root, "links/alpha-link")) == sha(os.path.join(fix, "text/alpha.txt"))

# Grep checks across space, unicode, and symlink paths
r1 = subprocess.run(["grep", "-R", "-l", "DRIVE9_NEEDLE_SPACE", os.path.join(root, "text")], capture_output=True, text=True, check=True)
assert r1.stdout.strip().endswith("text/space name.txt"), r1.stdout
r2 = subprocess.run(["grep", "-R", "-l", "DRIVE9_NEEDLE_UNICODE", os.path.join(root, "text")], capture_output=True, text=True, check=True)
assert r2.stdout.strip().endswith("text/中文-文件.txt"), r2.stdout
subprocess.run(["grep", "-q", "DRIVE9_NEEDLE_ALPHA", os.path.join(root, "links/alpha-link")], check=True)
r_miss = subprocess.run(["grep", "-R", "-q", "DRIVE9_NEEDLE_ABSENT", root])
assert r_miss.returncode != 0
'

rm -rf "$MOUNT/$ROOT_REL"
drain --mode require-ok --timeout 120s
check sync-ok --req 'drive9-fuse-corr-cleanup'
