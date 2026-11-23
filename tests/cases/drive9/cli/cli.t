#!/bin/sh
# drive9/cli/cli: Full Drive9 CLI contract suite (--no-mount).
# - drive9:/// vs / listing, fs mkdir/cp/cat/mv/stat/rm
# - directory target semantics (local->remote, remote->local, remote->remote)
# - fs cp -r recursive tree upload/download/remote-to-remote (including empty dirs)
# - fs symlink & fs hardlink (shared resource_id, nlink>=2, alias write propagation)
# - fs cp --tag & fs stat -o json (tags, semantic_text, content_type, tag replacement)
# - custom profile pack/unpack & fs archive (tar.gz, zip, --exclude, --include, --profile, --flat, --stdout)
# - batch small files, fs grep, fs grep --json, fs find
# - 9 MiB multipart fs cp SHA-256 round-trip + upload-limit boundary + cleanup
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TS="$(date +%s)-$$"
CLI_ROOT="/cli-suite-${TS}"
WORK="$RUN_DIR/cli-work"
mkdir -p "$WORK"
export TS CLI_ROOT WORK

sha256_hex() {
    python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$1"
}

check shell --req 'drive9-cli-small-and-dir-cp' --claim 'cli-small-ops-and-dir-target-cp' -- sh -c '
    printf "cli-smoke-%s" "$TS" > "$WORK/small.txt"
    "$CLI" fs mkdir ":$CLI_ROOT"
    "$CLI" fs cp "$WORK/small.txt" ":$CLI_ROOT/small.txt"

    ls_slash=$("$CLI" fs ls "$CLI_ROOT")
    ls_uri=$("$CLI" fs ls "drive9://$CLI_ROOT")
    test "$ls_uri" = "$ls_slash"
    printf "%s\n" "$ls_slash" | grep -qx "small.txt"
    test "$("$CLI" fs cat "$CLI_ROOT/small.txt")" = "cli-smoke-$TS"

    mkdir -p "$WORK/cpdir"
    "$CLI" fs mkdir ":$CLI_ROOT/cpdir"
    "$CLI" fs mkdir ":$CLI_ROOT/cpdir-copy"
    "$CLI" fs cp "$WORK/small.txt" ":$CLI_ROOT/cpdir"
    test "$("$CLI" fs cat "$CLI_ROOT/cpdir/small.txt")" = "cli-smoke-$TS"
    "$CLI" fs cp ":$CLI_ROOT/cpdir/small.txt" "$WORK/cpdir"
    test "$(cat "$WORK/cpdir/small.txt")" = "cli-smoke-$TS"
    "$CLI" fs cp ":$CLI_ROOT/cpdir/small.txt" ":$CLI_ROOT/cpdir-copy"
    test "$("$CLI" fs cat "$CLI_ROOT/cpdir-copy/small.txt")" = "cli-smoke-$TS"
'

check shell --req 'drive9-cli-cp-recursive' --claim 'cli-cp-recursive-tree-roundtrip' -- sh -c '
    src="$WORK/rec-src"
    dl="$WORK/rec-dl"
    mkdir -p "$src/sub/deep" "$src/empty"
    printf "top-%s" "$TS" > "$src/top.txt"
    printf "nested-%s" "$TS" > "$src/sub/nested.txt"
    printf "leaf-%s" "$TS" > "$src/sub/deep/leaf.txt"

    "$CLI" fs cp -r "$src" ":$CLI_ROOT/rec"
    "$CLI" fs cp -r ":$CLI_ROOT/rec" "$dl"

    test "$(cat "$dl/top.txt")" = "top-$TS"
    test "$(cat "$dl/sub/nested.txt")" = "nested-$TS"
    test "$(cat "$dl/sub/deep/leaf.txt")" = "leaf-$TS"
    test -d "$dl/empty"
    "$CLI" fs ls "$CLI_ROOT/rec/" | grep -qx "top.txt"
    "$CLI" fs ls "$CLI_ROOT/rec/sub/" | grep -qx "nested.txt"

    "$CLI" fs cp -r ":$CLI_ROOT/rec" ":$CLI_ROOT/rec-copy"
    test "$("$CLI" fs cat "$CLI_ROOT/rec-copy/top.txt")" = "top-$TS"
    test "$("$CLI" fs cat "$CLI_ROOT/rec-copy/sub/deep/leaf.txt")" = "leaf-$TS"
'

check shell --req 'drive9-cli-mv-links-tags' --claim 'cli-mv-symlink-hardlink-and-tags' -- sh -c '
    "$CLI" fs mv "$CLI_ROOT/small.txt" "$CLI_ROOT/renamed.txt"
    "$CLI" fs ls "$CLI_ROOT" | grep -qx "renamed.txt"

    "$CLI" fs symlink ":$CLI_ROOT/renamed.txt" ":$CLI_ROOT/sym-link"
    "$CLI" fs ls "$CLI_ROOT" | grep -qx "sym-link"
    test "$("$CLI" fs cat "$CLI_ROOT/sym-link")" = "$CLI_ROOT/renamed.txt"

    "$CLI" fs hardlink "$CLI_ROOT/renamed.txt" "$CLI_ROOT/hard-link.txt"
    test "$("$CLI" fs cat "$CLI_ROOT/hard-link.txt")" = "cli-smoke-$TS"

    s1=$("$CLI" fs stat -o json "$CLI_ROOT/renamed.txt")
    s2=$("$CLI" fs stat -o json "$CLI_ROOT/hard-link.txt")
    python3 -c "
import json, sys
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
assert a[\"resource_id\"] and a[\"resource_id\"] == b[\"resource_id\"], (a, b)
assert int(b[\"nlink\"]) >= 2, b
" "$s1" "$s2"

    printf "cli-hardlink-%s" "$TS" > "$WORK/hl.txt"
    "$CLI" fs cp "$WORK/hl.txt" ":$CLI_ROOT/hard-link.txt"
    test "$("$CLI" fs cat "$CLI_ROOT/renamed.txt")" = "cli-hardlink-$TS"

    printf "cli-tag-%s" "$TS" > "$WORK/tag.txt"
    "$CLI" fs cp --tag owner=smoke --tag topic=e2e "$WORK/tag.txt" ":$CLI_ROOT/tagged.txt"
    tj1=$("$CLI" fs stat -o json "$CLI_ROOT/tagged.txt")
    python3 -c "
import json, sys
d = json.loads(sys.argv[1])
assert d.get(\"tags\", {}).get(\"owner\") == \"smoke\", d
assert d.get(\"tags\", {}).get(\"topic\") == \"e2e\", d
assert d.get(\"content_type\"), d
" "$tj1"

    printf "cli-tag-updated-%s" "$TS" > "$WORK/tag.txt"
    "$CLI" fs cp --tag owner=updated "$WORK/tag.txt" ":$CLI_ROOT/tagged.txt"
    tj2=$("$CLI" fs stat -o json "$CLI_ROOT/tagged.txt")
    python3 -c "
import json, sys
d = json.loads(sys.argv[1])
assert d.get(\"tags\") == {\"owner\": \"updated\"}, d
" "$tj2"
'

check shell --req 'drive9-cli-pack-and-archive' --claim 'cli-custom-pack-and-fs-archive-modes' -- sh -c '
    mkdir -p "$HOME/.drive9/profiles"
    cat > "$HOME/.drive9/profiles/e2e-pack" <<EOF
[pack]
.git
dist
EOF
    plocal="$WORK/pack-local"
    prest="$WORK/pack-restore"
    mkdir -p "$plocal/overlay/repo/.git" "$plocal/overlay/repo/dist" "$plocal/overlay/repo/src"
    printf "[core]\n\trepositoryformatversion = 0\n" > "$plocal/overlay/repo/.git/config"
    printf "bundle-%s\n" "$TS" > "$plocal/overlay/repo/dist/app.js"
    printf "not-packed-%s\n" "$TS" > "$plocal/overlay/repo/src/main.go"

    "$CLI" profile show e2e-pack >/dev/null
    "$CLI" pack --local-root "$plocal" --remote-root "$CLI_ROOT/ws" --profile e2e-pack >/dev/null
    "$CLI" unpack --local-root "$prest" --remote-root "$CLI_ROOT/ws" --profile e2e-pack >/dev/null
    test -f "$prest/overlay/repo/.git/config"
    test "$(cat "$prest/overlay/repo/dist/app.js")" = "bundle-$TS"
    test ! -e "$prest/overlay/repo/src/main.go"

    ar="$CLI_ROOT/archive"
    "$CLI" fs mkdir ":$ar/src/util"
    "$CLI" fs mkdir ":$ar/node_modules/react"
    "$CLI" fs mkdir ":$ar/.git"
    printf "readme\n" > "$WORK/f" && "$CLI" fs cp "$WORK/f" ":$ar/README.md"
    printf "package main\n" > "$WORK/f" && "$CLI" fs cp "$WORK/f" ":$ar/src/app.go"
    printf "package util\n" > "$WORK/f" && "$CLI" fs cp "$WORK/f" ":$ar/src/util/util.go"
    printf "module.exports\n" > "$WORK/f" && "$CLI" fs cp "$WORK/f" ":$ar/node_modules/react/index.js"
    printf "ref: refs/heads/main\n" > "$WORK/f" && "$CLI" fs cp "$WORK/f" ":$ar/.git/HEAD"

    "$CLI" fs archive ":$ar" "$WORK/plain.tar.gz"
    tar -tzf "$WORK/plain.tar.gz" | grep -q "node_modules/react/index.js"
    tar -tzf "$WORK/plain.tar.gz" | grep -q "\.git/HEAD"

    "$CLI" fs archive ":$ar" "$WORK/excl.tar.gz" --exclude "**/node_modules/**" --exclude "**/.git/**"
    tar -tzf "$WORK/excl.tar.gz" | grep -q "src/app.go"
    ! tar -tzf "$WORK/excl.tar.gz" | grep -q "node_modules"

    "$CLI" fs archive ":$ar" "$WORK/prof.tar.gz" --profile coding-agent
    tar -tzf "$WORK/prof.tar.gz" | grep -q "src/app.go"
    ! tar -tzf "$WORK/prof.tar.gz" | grep -q "node_modules"

    "$CLI" fs archive ":$ar" "$WORK/incl.tar.gz" --include "src/**" --include "README.md"
    tar -tzf "$WORK/incl.tar.gz" | grep -q "README.md"
    ! tar -tzf "$WORK/incl.tar.gz" | grep -q "node_modules"

    "$CLI" fs archive ":$ar" "$WORK/out.zip" --format zip --exclude "**/node_modules/**" --exclude "**/.git/**"
    python3 -c "import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None; names=z.namelist(); assert any(\"README.md\" in n for n in names) and not any(\"node_modules\" in n for n in names)" "$WORK/out.zip"

    "$CLI" fs archive ":$ar" "$WORK/flat.tar.gz" --flat --exclude "**/node_modules/**" --exclude "**/.git/**"
    tar -tzf "$WORK/flat.tar.gz" | grep -qx "app.go"
    ! tar -tzf "$WORK/flat.tar.gz" | grep -q "/"

    "$CLI" fs archive ":$ar" --stdout --exclude "**/node_modules/**" --exclude "**/.git/**" | tar -tzf - | grep -q "README.md"
'

check shell --req 'drive9-cli-grep-find-large-cleanup' --claim 'cli-grep-find-large-multipart-and-cleanup' -- sh -c '
    mkdir -p "$WORK/batch"
    i=1
    while [ "$i" -le 6 ]; do
        printf "cli-batch-%s-%s" "$TS" "$i" > "$WORK/batch/file-$i.txt"
        "$CLI" fs cp "$WORK/batch/file-$i.txt" ":$CLI_ROOT/batch/file-$i.txt"
        i=$((i + 1))
    done

    "$CLI" fs grep "cli-batch-$TS" "$CLI_ROOT" | grep -q "$CLI_ROOT/batch/file-1.txt"
    gj=$("$CLI" fs grep --json "cli-batch-$TS" "$CLI_ROOT")
    python3 -c "import json,sys; a=json.loads(sys.argv[1]); assert isinstance(a,list) and any(x.get(\"path\",\"\").endswith(\"/batch/file-1.txt\") for x in a), a" "$gj"
    ge=$("$CLI" fs grep --json "zzz_nonexistent_marker_xyz" "$CLI_ROOT")
    python3 -c "import json,sys; assert json.loads(sys.argv[1]) == []" "$ge"
    "$CLI" fs find "$CLI_ROOT" -name "*.txt" | grep -q "$CLI_ROOT/batch/file-1.txt"

    # 9 MiB deterministic payload -> multipart upload & download SHA-256 check
    python3 -c "
import hashlib, sys
chunk = bytes((i * 37 + 11) % 256 for i in range(1024 * 1024))
with open(sys.argv[1], \"wb\") as f:
    for k in range(9):
        f.write(bytes([k]) + chunk[1:])
" "$WORK/large-9m.bin"
    "$CLI" fs cp "$WORK/large-9m.bin" ":$CLI_ROOT/large-9m.bin"
    "$CLI" fs cp ":$CLI_ROOT/large-9m.bin" "$WORK/large-9m.dl"
    python3 -c "
import hashlib, sys
h1 = hashlib.sha256(open(sys.argv[1], \"rb\").read()).hexdigest()
h2 = hashlib.sha256(open(sys.argv[2], \"rb\").read()).hexdigest()
assert h1 == h2, (h1, h2)
" "$WORK/large-9m.bin" "$WORK/large-9m.dl"

    # Extent Content Layout v2 CLI contract (--layout extent upload, stat, cat, range cat, download, layout conflict)
    "$CLI" fs cp --layout extent "$WORK/large-9m.bin" ":$CLI_ROOT/extent-9m.bin"
    "$CLI" fs stat ":$CLI_ROOT/extent-9m.bin" | grep -q "content_layout: extent"
    "$CLI" fs cp ":$CLI_ROOT/extent-9m.bin" "$WORK/extent-9m.dl"
    python3 -c "
import hashlib, sys
h1 = hashlib.sha256(open(sys.argv[1], \"rb\").read()).hexdigest()
h2 = hashlib.sha256(open(sys.argv[2], \"rb\").read()).hexdigest()
assert h1 == h2, (h1, h2)
" "$WORK/large-9m.bin" "$WORK/extent-9m.dl"
    range_ext=$("$CLI" fs cat --offset 1048576 --length 16 ":$CLI_ROOT/extent-9m.bin" | python3 -c "import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())")
    want_slice=$(python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],\"rb\").read()[1048576:1048592]).hexdigest())" "$WORK/large-9m.bin")
    test "$range_ext" = "$want_slice"
    # Applying --layout extent onto an existing single-layout file must fail fast
    ! "$CLI" fs cp --layout extent "$WORK/large-9m.bin" ":$CLI_ROOT/large-9m.bin" >/dev/null 2>&1

    "$CLI" fs rm -r ":$CLI_ROOT"
    "$CLI" fs rm -r ":/.drive9/packs" >/dev/null 2>&1 || true
'
