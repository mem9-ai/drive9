#!/bin/sh
# drive9/cli/object-store: Native `s3://` object-store CLI contract (`--no-mount`).
# Uses `OBJECT_S3_URI` when explicitly set; otherwise runs hermetically against
# `drive9-simulator`'s built-in `/s3` S3 endpoint (`s3://drive9/obj-smoke-...`).
set -eu

export CLI="$CLI"
WORKDIR="$RUN_DIR/obj-work"
mkdir -p "$WORKDIR"

if [ -n "${OBJECT_S3_URI:-}" ]; then
    OBJ_BASE=$(python3 -c 'import sys; u=sys.argv[1]; b,s,q=u.partition("?"); print(b.rstrip("/"))' "$OBJECT_S3_URI")
    OBJ_QUERY=$(python3 -c 'import sys; u=sys.argv[1]; b,s,q=u.partition("?"); print(("?"+q) if s else "")' "$OBJECT_S3_URI")
else
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-drive9local}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-drive9local}"
    export AWS_REGION="${AWS_REGION:-us-east-1}"
    export AWS_DEFAULT_REGION="$AWS_REGION"
    S3_EP="$SERVER/s3"
    # Ensure bucket exists on the S3 endpoint; exit 2 (unsupported) if target server has no /s3.
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$S3_EP/drive9" || printf '000')
    if [ "$code" != "200" ] && [ "$code" != "204" ]; then
        check shell --req 'drive9-obj-endpoint' --claim 'object-store-endpoint-available' -- sh -c 'exit 2'
        exit 0
    fi
    ENC_EP=$(python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$S3_EP")
    OBJ_BASE="s3://drive9/obj-smoke-$$-$(date +%s)"
    OBJ_QUERY="?region=${AWS_REGION}&endpoint=${ENC_EP}&forcePathStyle=true"
fi

export WORKDIR OBJ_BASE OBJ_QUERY

check shell --req 'drive9-obj-crud-and-copy' --claim 'object-store-mkdir-cp-ls-stat-cat-range-and-mv' -- sh -c '
    obj() {
        if [ -z "$1" ]; then
            printf "%s/%s" "$OBJ_BASE" "$OBJ_QUERY"
        else
            printf "%s/%s%s" "$OBJ_BASE" "$1" "$OBJ_QUERY"
        fi
    }
    run_fs() {
        sub="$1"; shift
        "$CLI" fs "$sub" --auth=local "$@"
    }

    # Empty bucket rejection
    if run_fs ls "s3:///" >/dev/null 2>&1; then
        echo "expected s3:/// to fail" >&2
        exit 1
    fi

    printf "hello-object" > "$WORKDIR/hello.txt"
    run_fs mkdir "$(obj "")"
    run_fs cp "$WORKDIR/hello.txt" "$(obj hello.txt)"
    run_fs ls "$(obj "")" | grep -q "hello.txt"
    run_fs stat "$(obj hello.txt)" | grep -qi "file"
    test "$(run_fs cat "$(obj hello.txt)")" = "hello-object"
    test "$(run_fs cat --offset 0 --length 5 "$(obj hello.txt)")" = "hello"

    run_fs cp "$(obj hello.txt)" "$WORKDIR/hello.down"
    test "$(cat "$WORKDIR/hello.down")" = "hello-object"
    run_fs cp "$(obj hello.txt)" "$(obj hello.copy)"
    test "$(run_fs cat "$(obj hello.copy)")" = "hello-object"
    run_fs mv "$(obj hello.copy)" "$(obj hello.moved)"
    test "$(run_fs cat "$(obj hello.moved)")" = "hello-object"
    ! run_fs stat "$(obj hello.copy)" >/dev/null 2>&1
'

check shell --req 'drive9-obj-tree-stdio-and-guards' --claim 'object-store-recursive-tree-stdio-rm-and-rejected-ops' -- sh -c '
    obj() {
        if [ -z "$1" ]; then
            printf "%s/%s" "$OBJ_BASE" "$OBJ_QUERY"
        else
            printf "%s/%s%s" "$OBJ_BASE" "$1" "$OBJ_QUERY"
        fi
    }
    run_fs() {
        sub="$1"; shift
        "$CLI" fs "$sub" --auth=local "$@"
    }

    mkdir -p "$WORKDIR/tree/sub"
    printf "leaf-a" > "$WORKDIR/tree/a.txt"
    printf "leaf-b" > "$WORKDIR/tree/sub/b.txt"
    run_fs cp -r "$WORKDIR/tree" "$(obj tree)"
    run_fs ls "$(obj tree)" | grep -q "a.txt"
    test "$(run_fs cat "$(obj tree/sub/b.txt)")" = "leaf-b"

    printf "from-stdin" | run_fs cp - "$(obj stdin.txt)"
    test "$(run_fs cat "$(obj stdin.txt)")" = "from-stdin"
    test "$(run_fs cp "$(obj stdin.txt)" -)" = "from-stdin"

    ! run_fs chmod 644 "$(obj hello.txt)" >/dev/null 2>&1
    ! run_fs find "$(obj "")" >/dev/null 2>&1
    ! run_fs grep hello "$(obj "")" >/dev/null 2>&1
    ! run_fs symlink "$(obj hello.txt)" "$(obj link)" >/dev/null 2>&1
    ! run_fs hardlink "$(obj hello.txt)" "$(obj hard)" >/dev/null 2>&1
    ! run_fs mv "$(obj hello.txt)" "$WORKDIR/local-mv.txt" >/dev/null 2>&1

    run_fs rm "$(obj hello.moved)"
    run_fs rm -r "$(obj tree)"
    run_fs rm -r "$(obj "")" >/dev/null 2>&1 || true
'
