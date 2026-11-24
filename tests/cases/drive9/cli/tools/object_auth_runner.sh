#!/bin/sh
# Shared `[prod-ext]` hosted `--auth=server` STS mint, namespace isolation,
# Drive9<->object-store copy, and session refresh helper for `object-auth-*.t`.
set -eu

SCHEME="${1:?scheme required}"
DEFAULT_REGION="${2:-us-east-1}"
REQUIRE_ROLE_ARN="${3:-0}"
PROV_TAG="${4:-$(printf '%s' "$SCHEME" | tr '[:lower:]' '[:upper:]')}"

pick_var() {
    eval "v1=\${DRIVE9_E2E_${PROV_TAG}_$1:-}"
    eval "v2=\${DRIVE9_E2E_OBJECT_$1:-}"
    if [ -n "$v1" ]; then
        printf '%s' "$v1"
    else
        printf '%s' "$v2"
    fi
}

REGION="$(pick_var REGION)"
[ -n "$REGION" ] || REGION="$DEFAULT_REGION"
BUCKET="$(pick_var BUCKET)"
AK="$(pick_var ACCESS_KEY_ID)"
SK="$(pick_var SECRET_ACCESS_KEY)"
ROLE_ARN="$(pick_var ROLE_ARN)"
ACCOUNT_ID="$(pick_var ACCOUNT_ID)"
ENDPOINT="$(pick_var ENDPOINT)"
STS_ENDPOINT="$(pick_var STS_ENDPOINT)"
PREFIX="$(pick_var PREFIX)"
FORCE_PATH_STYLE="$(pick_var FORCE_PATH_STYLE)"
PUB="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}"
PRIV="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}"

if [ "$1" = "--probe-only" ]; then
    exit 0
fi

if [ -z "$PUB" ] || [ -z "$PRIV" ] || [ -z "$BUCKET" ] || [ -z "$AK" ] || [ -z "$SK" ]; then
    exit 2
fi
if [ "$REQUIRE_ROLE_ARN" = "1" ] && [ -z "$ROLE_ARN" ]; then
    exit 2
fi

CLI_HOME="$RUN_DIR/obj-auth-home-$SCHEME"
mkdir -p "$CLI_HOME"
TS="$(date +%s)-$$"
NS="e2e-obj-${SCHEME}-${TS}"
TENANT_ID=""
TENANT_KEY=""
BACKEND_ID=""

admin_cli() {
    env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI" admin "$@" \
        --server "$SERVER" --tidbcloud-public-key "$PUB" --tidbcloud-private-key "$PRIV"
}

cleanup() {
    if [ -n "$TENANT_KEY" ] && [ -n "$BUCKET" ]; then
        DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$TENANT_KEY" HOME="$CLI_HOME" \
            "$CLI" fs rm --auth=server -r "${SCHEME}://${BUCKET}/${PREFIX:+$PREFIX/}${NS}/" >/dev/null 2>&1 || true
    fi
    if [ -n "$TENANT_ID" ]; then
        admin_cli tenant object-namespace clear --tenant-id "$TENANT_ID" >/dev/null 2>&1 || true
    fi
    if [ -n "$BACKEND_ID" ]; then
        admin_cli object-backend rm --id "$BACKEND_ID" >/dev/null 2>&1 || true
    fi
    if [ -n "$TENANT_ID" ] && [ -n "$TENANT_KEY" ]; then
        env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI" delete \
            --server "$SERVER" --api-key "$TENANT_KEY" \
            --tidbcloud-public-key "$PUB" --tidbcloud-private-key "$PRIV" -y >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

create_out=$(admin_cli tenant create --json 2>/dev/null || \
    env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI" create \
        --server "$SERVER" --tidbcloud-public-key "$PUB" --tidbcloud-private-key "$PRIV" --json)
TENANT_ID=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["tenant_id"])' "$create_out")
TENANT_KEY=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["api_key"])' "$create_out")

deadline=$(( $(date +%s) + 600 ))
active=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    st=$(d9-http -H "Authorization: Bearer $TENANT_KEY" "$SERVER/v1/status" 2>/dev/null || true)
    if python3 -c 'import json,sys; sys.exit(0 if json.loads(sys.argv[1] or "{}").get("status")=="active" else 1)' "$st" 2>/dev/null; then
        active=1
        break
    fi
    sleep 5
done
test "$active" = "1"

set -- object-backend add --scheme "$SCHEME" --bucket "$BUCKET" --region "$REGION" \
    --access-key-id "$AK" --secret-access-key "$SK" --name "e2e-${SCHEME}-${TS}" --json
[ -n "$ENDPOINT" ] && set -- "$@" --endpoint "$ENDPOINT"
[ -n "$STS_ENDPOINT" ] && set -- "$@" --sts-endpoint "$STS_ENDPOINT"
[ -n "$ACCOUNT_ID" ] && set -- "$@" --account-id "$ACCOUNT_ID"
[ -n "$PREFIX" ] && set -- "$@" --prefix "$PREFIX"
if [ -n "$ROLE_ARN" ]; then
    set -- "$@" --credential-kind role --role-arn "$ROLE_ARN"
else
    set -- "$@" --credential-kind static
fi
if [ "${FORCE_PATH_STYLE:-0}" = "1" ]; then
    set -- "$@" --force-path-style
fi

add_out=$(admin_cli "$@")
BACKEND_ID=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$add_out")
test -n "$BACKEND_ID"

admin_cli object-backend get --id "$BACKEND_ID" --json >/dev/null
admin_cli tenant object-namespace set --tenant-id "$TENANT_ID" --namespace-id "$NS" >/dev/null
got_ns=$(admin_cli tenant object-namespace get --tenant-id "$TENANT_ID" --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["namespace_id"])')
test "$got_ns" = "$NS"

URI_ROOT="${SCHEME}://${BUCKET}/${PREFIX:+$PREFIX/}${NS}"
OUTSIDE_URI="${SCHEME}://${BUCKET}/not-${NS}/denied.txt"
printf 'object-auth-%s-%s\n' "$SCHEME" "$TS" > "$RUN_DIR/payload.txt"

t_cli() {
    DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$TENANT_KEY" HOME="$CLI_HOME" "$CLI" "$@"
}

t_cli fs cp --auth=server "$RUN_DIR/payload.txt" "$URI_ROOT/hello.txt" --force
t_cli fs ls --auth=server "$URI_ROOT/" | grep -q "hello.txt"
t_cli fs cat --auth=server "$URI_ROOT/hello.txt" | grep -q "object-auth-$SCHEME"
t_cli fs cp --auth=server "$URI_ROOT/hello.txt" ":/from-obj.txt"
t_cli fs cat "/from-obj.txt" | grep -q "object-auth-$SCHEME"
! t_cli fs cp --auth=server "$RUN_DIR/payload.txt" "$OUTSIDE_URI" --force >/dev/null 2>&1
