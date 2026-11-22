#!/bin/sh
# drive9/cli/native-provision: `[prod-ext]` TiDB Cloud Native tenant create/status/fs/delete (`--no-mount`).
# Requires `DRIVE9_TIDBCLOUD_PUBLIC_KEY` and `DRIVE9_TIDBCLOUD_PRIVATE_KEY`; exits 2 (`unsupported`) when absent.
set -eu

export CLI="$CLI"

require --req 'drive9-native-credentials' --claim 'tidbcloud-native-credentials-configured' -- sh -c '
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] && [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ]
'

CREATED_JSON="$RUN_DIR/native-created.json"
export CREATED_JSON

cleanup() {
    if [ -f "$CREATED_JSON" ]; then
        key=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("api_key",""))' "$CREATED_JSON" 2>/dev/null || true)
        if [ -n "$key" ]; then
            env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$RUN_DIR/native-home" "$CLI" delete \
                --server "$SERVER" \
                --api-key "$key" \
                --tidbcloud-public-key "$DRIVE9_TIDBCLOUD_PUBLIC_KEY" \
                --tidbcloud-private-key "$DRIVE9_TIDBCLOUD_PRIVATE_KEY" \
                -y >/dev/null 2>&1 || true
        fi
    fi
}
trap cleanup EXIT INT TERM

check shell --req 'drive9-native-lifecycle' --claim 'native-tenant-create-active-fs-roundtrip-and-delete' -- sh -c '
    mkdir -p "$RUN_DIR/native-home"
    env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$RUN_DIR/native-home" "$CLI" create \
        --server "$SERVER" \
        --tidbcloud-public-key "$DRIVE9_TIDBCLOUD_PUBLIC_KEY" \
        --tidbcloud-private-key "$DRIVE9_TIDBCLOUD_PRIVATE_KEY" \
        --tidbcloud-spending-limit 1000 \
        --json > "$CREATED_JSON"

    t_id=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); assert d.get(\"tenant_id\") and d.get(\"api_key\"); print(d[\"tenant_id\"])" "$CREATED_JSON")
    t_key=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[\"api_key\"])" "$CREATED_JSON")
    test -n "$t_id" && test -n "$t_key"

    deadline=$(( $(date +%s) + 600 ))
    active=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st=$(d9-http -H "Authorization: Bearer $t_key" "$SERVER/v1/status" 2>/dev/null || true)
        if python3 -c "import json,sys; sys.exit(0 if json.loads(sys.argv[1] or \"{}\").get(\"status\")==\"active\" else 1)" "$st" 2>/dev/null; then
            active=1
            break
        fi
        sleep 5
    done
    test "$active" = "1"

    printf "hello-native-%s" "$t_id" > "$RUN_DIR/native.txt"
    DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$t_key" HOME="$RUN_DIR/native-home" "$CLI" fs mkdir "/native-smoke"
    DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$t_key" HOME="$RUN_DIR/native-home" "$CLI" fs cp "$RUN_DIR/native.txt" ":/native-smoke/hello.txt"
    got=$(DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$t_key" HOME="$RUN_DIR/native-home" "$CLI" fs cat "/native-smoke/hello.txt")
    test "$got" = "hello-native-$t_id"

    env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$RUN_DIR/native-home" "$CLI" delete \
        --server "$SERVER" \
        --api-key "$t_key" \
        --tidbcloud-public-key "$DRIVE9_TIDBCLOUD_PUBLIC_KEY" \
        --tidbcloud-private-key "$DRIVE9_TIDBCLOUD_PRIVATE_KEY" \
        -y
    rm -f "$CREATED_JSON"
'
