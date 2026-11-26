#!/bin/sh
# drive9/fuse/cross-fs-scoped-token: a scoped token stays inside its zone and
# cannot reach another tenant's filesystem, over HTTP and through a mount.
#
# Requires the scoped-token API (a `[prod-ext]` surface). A server that does not
# serve it reports this case as unsupported before any workload runs, so a core
# server never silently passes a scope test that did nothing.
set -eu

TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/cross-fs-scoped-token"
MP_B="$RUN_DIR/cross-fs-scoped-b"
MP_SCOPED="$RUN_DIR/cross-fs-scoped-mount"
b_state="$WORK_ROOT/tenant-b.json"
LIB="$(cd "$(dirname "$0")" && pwd)/cross-fs-lib.sh"
mkdir -p "$WORK_ROOT"
export TS WORK_ROOT MP_B MP_SCOPED b_state LIB

cleanup() {
    . "$LIB"
    cross_fs_teardown
}
trap cleanup EXIT INT TERM

require --req 'drive9-scoped-token-issuance' --claim 'scoped-token-api-available' -- \
    d9-http --status-only --expect 201 -X POST -H 'Content-Type: application/json' \
    -d '{"ttl_seconds":600,"scopes":[{"prefix":"/probe","ops":["read"]}]}' /v1/tokens

check shell --req 'drive9-cross-fs-scoped-second-tenant' --claim 'tenant-b-provisioned-and-mounted' -- sh -c '
    set -eu
    . "$LIB"
    cross_fs_provision_b
    printf "only-in-b-%s\n" "$TS" > "$MP_B/only-b-${TS}.txt"
'

check shell --req 'drive9-cross-fs-scoped-token' --claim 'scoped-token-scope-and-tenant-enforced' -- sh -c '
    set -eu
    . "$LIB"
    zone="/zone-${TS}"
    d9-http -X POST -H "Content-Type: application/json" \
        -d "{\"subject\":\"cross-fs-${TS}\",\"ttl_seconds\":600,\"scopes\":[{\"prefix\":\"$zone\",\"ops\":[\"read\",\"write\",\"list\"]}]}" \
        /v1/tokens > "$WORK_ROOT/scoped.json"
    scoped=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"token\"])" "$WORK_ROOT/scoped.json")
    test -n "$scoped"

    # Inside the zone: allowed and readable back.
    code=$(d9-http --status-only -X PUT -H "Authorization: Bearer $scoped" -d "inside" "/v1/fs${zone}/inside.txt")
    test "$code" = "200"
    test "$(d9-http -H "Authorization: Bearer $scoped" "/v1/fs${zone}/inside.txt")" = "inside"

    # Outside the zone: refused, and nothing lands through the tenant mount.
    code=$(d9-http --status-only -X PUT -H "Authorization: Bearer $scoped" -d "outside" "/v1/fs/outside-${TS}.txt")
    test "$code" = "403"
    test ! -e "$MOUNT/outside-${TS}.txt"

    # The token belongs to tenant A, so tenant B exclusive paths are absent.
    code=$(d9-http --status-only -H "Authorization: Bearer $scoped" "/v1/fs/only-b-${TS}.txt")
    test "$code" = "404"
'

check shell --req 'drive9-cross-fs-scoped-mount' --claim 'scoped-mount-enforces-zone' -- sh -c '
    set -eu
    . "$LIB"
    zone="/zone-${TS}"
    d9-http -X POST -H "Content-Type: application/json" \
        -d "{\"subject\":\"cross-fs-mount-${TS}\",\"ttl_seconds\":600,\"scopes\":[{\"prefix\":\"$zone\",\"ops\":[\"read\",\"write\",\"list\"]}]}" \
        /v1/tokens > "$WORK_ROOT/scoped-mount.json"
    scoped=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"token\"])" "$WORK_ROOT/scoped-mount.json")
    mkdir -p "$MP_SCOPED"
    if ! "$CLI" mount --server "$SERVER" --api-key "$scoped" --mode=fuse --profile none \
        --cache-dir "$WORK_ROOT/cache-scoped" ":/" "$MP_SCOPED" > "$WORK_ROOT/mount-scoped.log" 2>&1; then
        echo "cross-fs: this server refuses a scoped credential at mount time" >&2
        sed -n 1,5p "$WORK_ROOT/mount-scoped.log" >&2 || true
        exit 2
    fi
    deadline=$(( $(date +%s) + 60 ))
    until "$CLI" mount health "$MP_SCOPED" >/dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            cat "$WORK_ROOT/mount-scoped.log" >&2
            exit 1
        fi
        sleep 1
    done
    printf "mounted-inside\n" > "$MP_SCOPED${zone}/mounted.txt"
    test "$(cat "$MP_SCOPED${zone}/mounted.txt")" = "mounted-inside"
    if printf "mounted-outside\n" > "$MP_SCOPED/mounted-outside-${TS}.txt" 2>/dev/null; then
        echo "scoped mount wrote outside the token zone" >&2
        exit 1
    fi
'
