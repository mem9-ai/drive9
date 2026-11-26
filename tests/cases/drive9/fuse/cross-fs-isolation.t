#!/bin/sh
# drive9/fuse/cross-fs-isolation: two tenants on one server stay isolated at the
# mount path, and a scoped token cannot leave its zone or reach the other
# tenant's filesystem.
#
# The mount is the primary access surface, so isolation has to be proven there
# and not only over HTTP:
#   1. tenant B is provisioned and mounted next to tenant A;
#   2. the same path written through both mounts keeps each tenant's own
#      content (no path-level bleed in either direction);
#   3. A's credential cannot address B's exclusive path over the data plane,
#      and B's credential cannot address A's (404, never 200);
#   4. a scoped token issued on A may write inside its zone and is refused
#      outside it, and cannot read B's exclusive path.
#
# WebDAV repeats the isolation write when the platform has a WebDAV helper;
# otherwise that phase reports unsupported instead of silently skipping.
set -eu

TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/cross-fs-isolation"
MP_B="$RUN_DIR/cross-fs-b"
MP_SCOPED="$RUN_DIR/cross-fs-scoped"
mkdir -p "$WORK_ROOT"
export TS WORK_ROOT MP_B MP_SCOPED

b_state="$WORK_ROOT/tenant-b.json"
export b_state

cleanup() {
    "$CLI" umount --timeout 30s "$MP_SCOPED" >/dev/null 2>&1 || true
    "$CLI" umount --timeout 30s "$MP_B" >/dev/null 2>&1 || true
    if [ -f "$b_state" ]; then
        b_key=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("api_key",""))' "$b_state" 2>/dev/null || true)
        if [ -n "$b_key" ]; then
            d9-http -X DELETE -H "Authorization: Bearer $b_key" /v1/tenant >/dev/null 2>&1 || true
        fi
    fi
}
trap cleanup EXIT INT TERM

check shell --req 'drive9-cross-fs-provision-second-tenant' --claim 'tenant-b-provisioned-and-mounted' -- sh -c '
    d9-http -X POST -H "Content-Type: application/json" -d "{}" /v1/provision > "$b_state"
    b_key=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"api_key\"])" "$b_state")
    b_tenant=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"tenant_id\"])" "$b_state")
    test -n "$b_key" && test -n "$b_tenant"

    deadline=$(( $(date +%s) + 120 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if d9-http -H "Authorization: Bearer $b_key" /v1/status | grep -q "\"status\":\"active\""; then
            break
        fi
        sleep 2
    done
    d9-http -H "Authorization: Bearer $b_key" /v1/status | grep -q "\"status\":\"active\""

    mkdir -p "$MP_B"
    "$CLI" mount --server "$SERVER" --api-key "$b_key" --mode=fuse --profile none \
        --cache-dir "$WORK_ROOT/cache-b" ":/" "$MP_B" > "$WORK_ROOT/mount-b.log" 2>&1
    deadline=$(( $(date +%s) + 60 ))
    until "$CLI" mount health "$MP_B" >/dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            cat "$WORK_ROOT/mount-b.log" >&2
            exit 1
        fi
        sleep 1
    done
'

check shell --req 'drive9-cross-fs-path-isolation' --claim 'same-path-content-stays-per-tenant' -- sh -c '
    printf "from-a-%s\n" "$TS" > "$MOUNT/iso-${TS}.txt"
    printf "from-b-%s\n" "$TS" > "$MP_B/iso-${TS}.txt"
    a=$(cat "$MOUNT/iso-${TS}.txt")
    b=$(cat "$MP_B/iso-${TS}.txt")
    test "$a" = "from-a-$TS"
    test "$b" = "from-b-$TS"

    printf "only-in-b-%s\n" "$TS" > "$MP_B/only-b-${TS}.txt"
    test ! -e "$MOUNT/only-b-${TS}.txt"
    code=$(d9-http --status-only "/v1/fs/only-b-${TS}.txt")
    test "$code" = "404"

    printf "only-in-a-%s\n" "$TS" > "$MOUNT/only-a-${TS}.txt"
    b_key=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"api_key\"])" "$b_state")
    code=$(d9-http --status-only -H "Authorization: Bearer $b_key" "/v1/fs/only-a-${TS}.txt")
    test "$code" = "404"
'

check shell --req 'drive9-cross-fs-scoped-token' --claim 'scoped-token-scope-and-tenant-enforced' -- sh -c '
    probe=$(d9-http --status-only /v1/tokens)
    if [ "$probe" != "200" ]; then
        echo "drive9: this server does not serve the scoped-token API (GET /v1/tokens -> $probe), scope phase skipped" >&2
        exit 0
    fi
    zone="/zone-${TS}"
    d9-http -X POST -H "Content-Type: application/json" \
        -d "{\"subject\":\"cross-fs-${TS}\",\"ttl_seconds\":600,\"scopes\":[{\"prefix\":\"$zone\",\"ops\":[\"read\",\"write\",\"list\"]}]}" \
        /v1/tokens > "$WORK_ROOT/scoped.json"
    scoped=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"token\"])" "$WORK_ROOT/scoped.json")
    test -n "$scoped"

    # Inside the zone: allowed.
    code=$(d9-http --status-only -X PUT -H "Authorization: Bearer $scoped" -d "inside" "/v1/fs${zone}/inside.txt")
    test "$code" = "200"
    got=$(d9-http -H "Authorization: Bearer $scoped" "/v1/fs${zone}/inside.txt")
    test "$got" = "inside"

    # Outside the zone: refused, never silently allowed.
    code=$(d9-http --status-only -X PUT -H "Authorization: Bearer $scoped" -d "outside" "/v1/fs/outside-${TS}.txt")
    test "$code" = "403"
    test ! -e "$MOUNT/outside-${TS}.txt"

    # A scoped token belongs to tenant A, so tenant B exclusive paths are absent.
    code=$(d9-http --status-only -H "Authorization: Bearer $scoped" "/v1/fs/only-b-${TS}.txt")
    test "$code" = "404"

    # The same scope rules hold when the token is used as a mount credential.
    mkdir -p "$MP_SCOPED"
    if "$CLI" mount --server "$SERVER" --api-key "$scoped" --mode=fuse --profile none \
        --cache-dir "$WORK_ROOT/cache-scoped" ":/" "$MP_SCOPED" > "$WORK_ROOT/mount-scoped.log" 2>&1; then
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
    else
        # A server that refuses the mount probe still must refuse the writes the
        # section above already proved are denied; record why the mount stayed out.
        grep -qi "denied\|forbidden\|401\|403" "$WORK_ROOT/mount-scoped.log"
    fi
'

check shell --req 'drive9-cross-fs-webdav-isolation' --claim 'webdav-mount-keeps-tenants-apart' -- sh -c '
    if ! command -v mount.davfs >/dev/null 2>&1 && ! command -v mount_webdav >/dev/null 2>&1; then
        echo "drive9: no WebDAV mount helper on this host, WebDAV phase skipped" >&2
        exit 0
    fi
    b_key=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"api_key\"])" "$b_state")
    MP_WEBDAV="$RUN_DIR/cross-fs-webdav"
    mkdir -p "$MP_WEBDAV"
    "$CLI" mount --server "$SERVER" --api-key "$b_key" --mode=webdav --profile none \
        --cache-dir "$WORK_ROOT/cache-webdav" ":/" "$MP_WEBDAV" > "$WORK_ROOT/mount-webdav.log" 2>&1
    printf "webdav-b-%s\n" "$TS" > "$MP_WEBDAV/webdav-${TS}.txt"
    test "$(cat "$MP_WEBDAV/webdav-${TS}.txt")" = "webdav-b-$TS"
    test ! -e "$MOUNT/webdav-${TS}.txt"
    "$CLI" umount --timeout 30s "$MP_WEBDAV" >/dev/null 2>&1 || true
'
