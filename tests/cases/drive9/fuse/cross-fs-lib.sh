#!/bin/sh
# Shared setup for the cross-filesystem isolation cases.
#
# Each case provisions a second tenant and mounts it next to the tenant the
# engine already mounted, then proves that path state and credentials stay
# apart. Sourced by `check ... -- sh -c` bodies; the caller sets TS, WORK_ROOT,
# MP_B and b_state, and installs cross_fs_teardown as its EXIT trap.

# cross_fs_provision_b: POST /v1/provision, wait for active, mount B on MP_B.
cross_fs_provision_b() {
    rm -f "$b_state"
    if ! d9-http -X POST -H "Content-Type: application/json" -d "{}" -o "$b_state" /v1/provision >/dev/null 2>&1; then
        echo "cross-fs: POST /v1/provision failed; a second tenant is not provisionable here" >&2
        return 2
    fi
    if ! python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d.get("api_key") and d.get("tenant_id")' "$b_state" 2>/dev/null; then
        echo "cross-fs: POST /v1/provision returned no tenant" >&2
        return 2
    fi
    b_key=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["api_key"])' "$b_state")

    deadline=$(( $(date +%s) + 120 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if d9-http -H "Authorization: Bearer $b_key" /v1/status 2>/dev/null | grep -q '"status": *"active"'; then
            break
        fi
        sleep 2
    done
    d9-http -H "Authorization: Bearer $b_key" /v1/status 2>/dev/null | grep -q '"status": *"active"'

    mkdir -p "$MP_B"
    "$CLI" mount --server "$SERVER" --api-key "$b_key" --mode=fuse --profile none \
        --cache-dir "$WORK_ROOT/cache-b" ":/" "$MP_B" > "$WORK_ROOT/mount-b.log" 2>&1
    deadline=$(( $(date +%s) + 60 ))
    until "$CLI" mount health "$MP_B" >/dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            cat "$WORK_ROOT/mount-b.log" >&2
            return 1
        fi
        sleep 1
    done
}

cross_fs_b_key() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["api_key"])' "$b_state"
}

# cross_fs_teardown: unmount everything and drop tenant B.
cross_fs_teardown() {
    status=$?
    "$CLI" umount --timeout 30s "$RUN_DIR/cross-fs-webdav" >/dev/null 2>&1 || true
    if [ -n "${MP_SCOPED:-}" ]; then
        "$CLI" umount --timeout 30s "$MP_SCOPED" >/dev/null 2>&1 || true
    fi
    "$CLI" umount --timeout 30s "${MP_B:-$RUN_DIR/cross-fs-b}" >/dev/null 2>&1 || true
    if [ -f "${b_state:-}" ]; then
        b_key=$(cross_fs_b_key 2>/dev/null || true)
        if [ -n "$b_key" ]; then
            d9-http -X DELETE -H "Authorization: Bearer $b_key" /v1/tenant >/dev/null 2>&1 || true
        fi
    fi
    return $status
}
